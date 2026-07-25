"""Camada de persistência SQLite: schema, dedupe por link e rastreamento de última visualização."""
import sqlite3
from datetime import timedelta
from typing import List, Optional

from medalert.dedup import build_signature
from medalert.models import Job
from medalert.timeutil import now_brt

_JOB_COLUMNS = "job_title, link, publication_date, last_seen_at, is_active, dedup_key"

_CREATE_JOBS_TABLE = """
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_title TEXT NOT NULL,
        link TEXT UNIQUE NOT NULL,
        publication_date TEXT,
        is_sent BOOLEAN DEFAULT 0
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
            if "is_active" not in existing_columns:
                self.conn.execute("ALTER TABLE jobs ADD COLUMN is_active INTEGER DEFAULT 1")
            if "dedup_key" not in existing_columns:
                self.conn.execute("ALTER TABLE jobs ADD COLUMN dedup_key TEXT")

    def insert_job(self, title: str, link: str, pub_date: str) -> bool:
        """Insere uma vaga nova. Retorna False (via UNIQUE(link)) se o link já existir."""
        query = (
            "INSERT INTO jobs (job_title, link, publication_date, last_seen_at, dedup_key) "
            "VALUES (?, ?, ?, ?, ?)"
        )
        try:
            with self.conn:
                self.conn.execute(
                    query,
                    (title, link, pub_date, now_brt().isoformat(), build_signature(title)),
                )
            return True
        except sqlite3.IntegrityError:
            return False

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

        Não altera is_active — nesta fase o dado só é registrado, nunca usado
        para esconder algo automaticamente (isso fica para uma fase futura,
        depois de acumular histórico real de last_seen_at).
        """
        query = "UPDATE jobs SET last_seen_at = ? WHERE link = ?"
        with self.conn:
            self.conn.execute(query, (now_brt().isoformat(), link))

    def mark_as_sent(self, link: str) -> None:
        """Marks a job as successfully sent to Telegram."""
        query = "UPDATE jobs SET is_sent = 1 WHERE link = ?"
        with self.conn:
            self.conn.execute(query, (link,))

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

    def fetch_pending_notifications(self, max_age_days: int = 7, limit: int = 10) -> List[Job]:
        """Vagas salvas cuja notificação nunca chegou a ser enviada.

        Existe porque `is_sent` era gravado e nunca lido: se o Telegram
        falhasse depois do INSERT, o alerta era perdido para sempre — o link
        já estava no banco, então nas rodadas seguintes a vaga era tratada
        como "já conhecida" e nunca mais gerava notificação.

        Os dois limites são proteções contra enxurrada: `max_age_days` evita
        ressuscitar vaga velha (um alerta de algo descoberto há meses só
        confunde), e `limit` evita despejar centenas de mensagens de uma vez
        caso o envio fique quebrado por muito tempo.
        """
        cutoff = (now_brt().date() - timedelta(days=max_age_days)).isoformat()
        query = (
            f"SELECT {_JOB_COLUMNS} FROM jobs "
            "WHERE is_sent = 0 AND publication_date >= ? "
            "ORDER BY publication_date DESC, id DESC LIMIT ?"
        )
        with self.conn:
            rows = self.conn.execute(query, (cutoff, limit)).fetchall()
        return [self._row_to_job(row) for row in rows]

    @staticmethod
    def _row_to_job(row) -> Job:
        return Job(
            title=row[0],
            link=row[1],
            discovered_at=row[2],
            last_seen_at=row[3],
            is_active=bool(row[4]) if row[4] is not None else True,
            dedup_key=row[5],
        )

    def close(self) -> None:
        self.conn.close()
