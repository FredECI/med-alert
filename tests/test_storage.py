"""Testes da camada de persistência: dedupe, rastreamento de última visualização e migração."""
import sqlite3

from medalert.storage import DatabaseManager


def test_insert_job_returns_true_for_new_link(db):
    assert db.insert_job("Vaga Teste", "https://example.com/1", "2026-01-01") is True


def test_insert_job_returns_false_for_duplicate_link(db):
    db.insert_job("Vaga Teste", "https://example.com/1", "2026-01-01")
    assert db.insert_job("Vaga Teste (repetida)", "https://example.com/1", "2026-01-02") is False


def test_duplicate_insert_does_not_create_a_second_row(db):
    db.insert_job("Vaga Teste", "https://example.com/1", "2026-01-01")
    db.insert_job("Vaga Teste (repetida)", "https://example.com/1", "2026-01-02")
    assert len(db.fetch_all_jobs()) == 1


def test_touch_last_seen_updates_existing_row(db):
    db.insert_job("Vaga Teste", "https://example.com/1", "2026-01-01")
    db.touch_last_seen("https://example.com/1")

    jobs = db.fetch_all_jobs()
    assert len(jobs) == 1
    assert jobs[0].last_seen_at is not None


def test_fetch_all_jobs_returns_every_row_regardless_of_age(db):
    """Não existe (ainda) nenhum filtro de expiração — uma vaga antiga continua
    aparecendo com o mesmo peso que uma nova. Rastrear isso é intencional
    nesta fase; esconder fica para uma fase futura."""
    db.insert_job("Vaga Antiga", "https://example.com/old", "2020-01-01")
    db.insert_job("Vaga Nova", "https://example.com/new", "2026-01-01")

    jobs = db.fetch_all_jobs()
    assert len(jobs) == 2
    assert all(job.is_active for job in jobs)


def test_migration_adds_columns_to_pre_existing_database(tmp_path):
    """Simula um banco no formato antigo (sem last_seen_at/is_active, como o
    med_alerts.db real de produção) e garante que a migração aditiva roda sem
    perder as linhas já existentes."""
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_title TEXT NOT NULL,
            link TEXT UNIQUE NOT NULL,
            publication_date TEXT,
            is_sent BOOLEAN DEFAULT 0
        )
        """
    )
    conn.execute(
        "INSERT INTO jobs (job_title, link, publication_date) VALUES (?, ?, ?)",
        ("Vaga Legada", "https://example.com/legacy", "2026-01-01"),
    )
    conn.commit()
    conn.close()

    manager = DatabaseManager(db_name=str(db_path))
    try:
        jobs = manager.fetch_all_jobs()
        assert len(jobs) == 1
        assert jobs[0].title == "Vaga Legada"
        assert jobs[0].last_seen_at is None
    finally:
        manager.close()
