"""Camada de persistência SQLite: schema, dedupe por link e rastreamento de última visualização."""
import sqlite3
from datetime import timedelta
from typing import List, Optional

from medalert.dedup import build_signature
from medalert.models import Job
from medalert.timeutil import now_brt

_JOB_COLUMNS = (
    "job_title, link, publication_date, last_seen_at, dedup_key, job_type, region, source_url"
)

#: Host do arquivo -> página onde a vaga vive. Usado só para retroagir o
#: acervo anterior à coluna `source_url`; daqui em diante quem informa a
#: origem é o próprio scraper, que sabe a resposta certa.
_SOURCE_PAGE_BY_HOST = {
    "static.medgrupo.com.br": "https://concursos.medgrupo.com.br/",
    "pss.riosaude.rio.br": "https://pss.riosaude.rio.br",
    "riosaude.prefeitura.rio": "https://riosaude.prefeitura.rio/processos-seletivos-editais-abertos-2025/",
    "ibam-concursos.org.br": "https://www.ibam-concursos.org.br/default.asp",
}

_CREATE_JOBS_TABLE = """
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_title TEXT NOT NULL,
        link TEXT UNIQUE NOT NULL,
        publication_date TEXT,
        last_seen_at TEXT,
        dedup_key TEXT,
        job_type TEXT,
        region TEXT,
        source_url TEXT
    )
"""

# Uma linha por (vaga, destinatário). Substitui o antigo booleano `is_sent`,
# que não sobrevive à personalização: com filtro por assinante, "enviada"
# deixa de ser propriedade da vaga e passa a depender do par — a mesma vaga
# foi entregue a quem assina Macaé e não a quem assina só a capital.
_CREATE_DELIVERIES_TABLE = """
    CREATE TABLE IF NOT EXISTS deliveries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_link TEXT NOT NULL,
        chat_id TEXT NOT NULL,
        sent_at TEXT NOT NULL,
        UNIQUE(job_link, chat_id)
    )
"""


class DatabaseManager:
    def __init__(self, db_name: str = "med_alerts.db"):
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
        self._create_tables()
        self._migrate_schema()

    def _create_tables(self) -> None:
        with self.conn:
            self.conn.execute(_CREATE_JOBS_TABLE)
            self.conn.execute(_CREATE_DELIVERIES_TABLE)

    def _migrate_schema(self) -> None:
        """Adiciona colunas novas em bancos já existentes.

        CREATE TABLE IF NOT EXISTS não altera uma tabela já existente, e o
        med_alerts.db de produção já tem meses de dados no formato antigo —
        por isso a migração precisa ser aditiva e idempotente.
        """
        existing_columns = {row[1] for row in self.conn.execute("PRAGMA table_info(jobs)")}
        with self.conn:
            if "last_seen_at" not in existing_columns:
                self.conn.execute("ALTER TABLE jobs ADD COLUMN last_seen_at TEXT")
            # Nota: bancos antigos têm uma coluna `is_active` que nunca chegou
            # a ser usada — era um placeholder para esconder vagas expiradas
            # automaticamente, recurso que foi deliberadamente descartado em
            # favor de apenas exibir "visto pela última vez em X". Ela não é
            # recriada aqui e nenhum código a lê; fica só como resíduo inofensivo
            # nos bancos que já existem (removê-la exigiria reescrever a tabela).
            if "dedup_key" not in existing_columns:
                self.conn.execute("ALTER TABLE jobs ADD COLUMN dedup_key TEXT")
            if "job_type" not in existing_columns:
                self.conn.execute("ALTER TABLE jobs ADD COLUMN job_type TEXT")
            if "region" not in existing_columns:
                self.conn.execute("ALTER TABLE jobs ADD COLUMN region TEXT")
            if "source_url" not in existing_columns:
                self.conn.execute("ALTER TABLE jobs ADD COLUMN source_url TEXT")
                self._backfill_source_urls()

    def _backfill_source_urls(self) -> None:
        """Preenche a origem das vagas que já estavam no banco.

        Sem isso, só vagas descobertas a partir de agora teriam a página de
        origem — e as antigas que mais precisam dela são justamente as que
        apontam direto para um PDF, sem nenhum contexto ao redor.

        Só age sobre links que terminam em .pdf: quando o link já é uma
        página, ele próprio é a origem e não há segundo endereço a mostrar.
        """
        for dominio, pagina in _SOURCE_PAGE_BY_HOST.items():
            self.conn.execute(
                "UPDATE jobs SET source_url = ? "
                "WHERE source_url IS NULL AND lower(link) LIKE ? AND lower(link) LIKE '%.pdf' "
                "AND link <> ?",
                (pagina, f"%{dominio}%", pagina),
            )

    def insert_job(
        self,
        title: str,
        link: str,
        pub_date: str,
        job_type: Optional[str] = None,
        region: Optional[str] = None,
        source_url: Optional[str] = None,
    ) -> bool:
        """Insere uma vaga nova. Retorna False (via UNIQUE(link)) se o link já existir."""
        query = (
            "INSERT INTO jobs "
            "(job_title, link, publication_date, last_seen_at, dedup_key, "
            "job_type, region, source_url) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        try:
            with self.conn:
                self.conn.execute(
                    query,
                    (
                        title, link, pub_date, now_brt().isoformat(),
                        build_signature(title), job_type, region,
                        # Guardar a origem quando ela é o próprio link não
                        # acrescenta nada e faria a interface mostrar o mesmo
                        # endereço duas vezes.
                        source_url if source_url and source_url != link else None,
                    ),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def count_jobs(self) -> int:
        """Usado para detectar banco recém-criado (ver modo bootstrap)."""
        return self.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]

    def count_deliveries(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]

    def fetch_all_links(self) -> List[str]:
        return [row[0] for row in self.conn.execute("SELECT link FROM jobs")]

    def find_duplicate_link(self, link: str) -> Optional[str]:
        """Link de OUTRA vaga que compartilha a mesma assinatura de conteúdo.

        Serve para reconhecer o mesmo edital publicado por duas fontes (ex:
        IBAM e o portal da prefeitura), que têm links diferentes e por isso
        escapam do dedupe por UNIQUE(link). Retorna None quando a vaga não
        tem assinatura confiável ou quando é a primeira com aquela chave.
        """
        query = """
            SELECT outra.link
            FROM jobs AS atual
            JOIN jobs AS outra
              ON outra.dedup_key = atual.dedup_key AND outra.id < atual.id
            WHERE atual.link = ? AND atual.dedup_key IS NOT NULL
            ORDER BY outra.id
            LIMIT 1
        """
        row = self.conn.execute(query, (link,)).fetchone()
        return row[0] if row else None

    def touch_last_seen(self, link: str) -> None:
        """Marca que uma vaga já conhecida foi encontrada de novo nesta rodada.

        O dado só é registrado e exibido ("visto pela última vez em X"), nunca
        usado para esconder vaga automaticamente: como nenhum scraper lê a data
        real de encerramento, qualquer inferência de "expirou" arriscaria
        sumir com vaga que ainda está aberta.
        """
        query = "UPDATE jobs SET last_seen_at = ? WHERE link = ?"
        with self.conn:
            self.conn.execute(query, (now_brt().isoformat(), link))

    def record_delivery(self, job_link: str, chat_id: str) -> None:
        """Registra que uma vaga foi entregue a um destinatário específico."""
        query = "INSERT OR IGNORE INTO deliveries (job_link, chat_id, sent_at) VALUES (?, ?, ?)"
        with self.conn:
            self.conn.execute(query, (job_link, str(chat_id), now_brt().isoformat()))

    def record_deliveries_for_all(self, job_link: str, chat_ids: List[str]) -> None:
        """Resolve a vaga para todos os destinatários de uma vez, sem enviar.

        Usado quando a vaga não deve gerar alerta nenhum — no bootstrap do
        banco e nas duplicatas entre fontes —, para que ela também não volte
        pela fila de reenvio depois.
        """
        for chat_id in chat_ids:
            self.record_delivery(job_link, chat_id)

    def fetch_all_jobs(self) -> List[Job]:
        """Retorna todas as vagas já vistas, da mais recente para a mais antiga.

        Chamada 'fetch_all' (não 'fetch_active') de propósito: hoje nada no
        schema filtra por ainda-estar-aberta, então nomear como "active" seria
        enganoso.
        """
        query = f"SELECT {_JOB_COLUMNS} FROM jobs ORDER BY publication_date DESC, id DESC"
        with self.conn:
            rows = self.conn.execute(query).fetchall()
        return [self._row_to_job(row) for row in rows]

    def fetch_undelivered(
        self,
        chat_id: str,
        max_age_days: int = 7,
        limit: int = 10,
        regions: Optional[List[str]] = None,
        job_types: Optional[List[str]] = None,
    ) -> List[Job]:
        """Vagas que ainda não foram entregues a ESTE destinatário.

        Cobre dois casos com a mesma consulta: a falha transitória do Telegram
        (a vaga entrou no banco mas o envio não completou) e o excedente do
        teto por rodada, que fica pendente de propósito para sair aos poucos.

        Os limites protegem contra enxurrada: `max_age_days` evita ressuscitar
        vaga velha, e `limit` evita despejar centenas de mensagens de uma vez
        se o envio ficar quebrado por muito tempo.

        `regions`/`job_types` são as preferências do assinante. Vazias, nada é
        filtrado — é assim que funciona enquanto as preferências não existem.
        """
        cutoff = (now_brt().date() - timedelta(days=max_age_days)).isoformat()
        params: List = [str(chat_id), cutoff]
        clauses = [
            "NOT EXISTS (SELECT 1 FROM deliveries d "
            "WHERE d.job_link = jobs.link AND d.chat_id = ?)",
            "publication_date >= ?",
        ]

        if regions:
            clauses.append(f"region IN ({','.join('?' * len(regions))})")
            params.extend(regions)
        if job_types:
            clauses.append(f"job_type IN ({','.join('?' * len(job_types))})")
            params.extend(job_types)

        params.append(limit)
        query = (
            f"SELECT {_JOB_COLUMNS} FROM jobs WHERE {' AND '.join(clauses)} "
            "ORDER BY publication_date DESC, id DESC LIMIT ?"
        )
        with self.conn:
            rows = self.conn.execute(query, params).fetchall()
        return [self._row_to_job(row) for row in rows]

    @staticmethod
    def _row_to_job(row) -> Job:
        return Job(
            title=row[0],
            link=row[1],
            discovered_at=row[2],
            last_seen_at=row[3],
            dedup_key=row[4],
            job_type=row[5],
            region=row[6],
            source_url=row[7],
        )

    def close(self) -> None:
        self.conn.close()
