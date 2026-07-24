"""Camada de persistência SQLite: schema, dedupe por link e rastreamento de última visualização."""
import sqlite3
from typing import List

from medalert.models import Job
from medalert.timeutil import now_brt

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

    def insert_job(self, title: str, link: str, pub_date: str) -> bool:
        """Insere uma vaga nova. Retorna False (via UNIQUE(link)) se o link já existir."""
        query = "INSERT INTO jobs (job_title, link, publication_date, last_seen_at) VALUES (?, ?, ?, ?)"
        try:
            with self.conn:
                self.conn.execute(query, (title, link, pub_date, now_brt().isoformat()))
            return True
        except sqlite3.IntegrityError:
            return False

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
        query = (
            "SELECT job_title, link, publication_date, last_seen_at, is_active "
            "FROM jobs ORDER BY publication_date DESC, id DESC"
        )
        with self.conn:
            rows = self.conn.execute(query).fetchall()
        return [
            Job(
                title=row[0],
                link=row[1],
                discovered_at=row[2],
                last_seen_at=row[3],
                is_active=bool(row[4]) if row[4] is not None else True,
            )
            for row in rows
        ]

    def close(self) -> None:
        self.conn.close()
