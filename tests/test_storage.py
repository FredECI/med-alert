"""Testes da camada de persistência: dedupe, rastreamento de última visualização e migração."""
import sqlite3

from medalert.storage import DatabaseManager
from medalert.timeutil import now_brt


def _recent_date() -> str:
    """Data de hoje em BRT — usada para cair dentro da janela de reenvio."""
    return now_brt().strftime("%Y-%m-%d")


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


def test_find_duplicate_link_matches_same_edital_from_another_source(db):
    db.insert_job("[IBAM] Municipio de Saquarema - Ed. 03/2026", "https://ibam.example/1", "2026-07-01")
    db.insert_job("[Saquarema] Concurso Público 003/2026 - Saúde", "https://saquarema.example/2", "2026-07-02")

    assert db.find_duplicate_link("https://saquarema.example/2") == "https://ibam.example/1"


def test_find_duplicate_link_returns_none_for_the_first_occurrence(db):
    db.insert_job("[IBAM] Municipio de Saquarema - Ed. 03/2026", "https://ibam.example/1", "2026-07-01")

    assert db.find_duplicate_link("https://ibam.example/1") is None


def test_find_duplicate_link_returns_none_without_a_confident_signature(db):
    """Sem número de edital não há assinatura, então nada é agrupado — mesmo
    que os títulos sejam parecidos."""
    db.insert_job("[IBAM] Saquarema concurso saúde", "https://ibam.example/a", "2026-07-01")
    db.insert_job("[Saquarema] Concurso saúde", "https://saquarema.example/b", "2026-07-02")

    assert db.find_duplicate_link("https://saquarema.example/b") is None


def test_fetch_undelivered_returns_jobs_not_yet_sent_to_that_chat(db):
    db.insert_job("Vaga pendente", "https://example.com/pendente", _recent_date())

    pending = db.fetch_undelivered("111")

    assert [job.link for job in pending] == ["https://example.com/pendente"]


def test_fetch_undelivered_skips_what_was_already_delivered(db):
    db.insert_job("Vaga enviada", "https://example.com/enviada", _recent_date())
    db.record_delivery("https://example.com/enviada", "111")

    assert db.fetch_undelivered("111") == []


def test_delivery_is_tracked_per_recipient(db):
    """O ponto central da mudança: com filtro por assinante, "enviada" deixa
    de ser propriedade da vaga e passa a depender do par vaga-destinatário."""
    db.insert_job("Vaga", "https://example.com/v", _recent_date())
    db.record_delivery("https://example.com/v", "111")

    assert db.fetch_undelivered("111") == []
    assert len(db.fetch_undelivered("222")) == 1


def test_fetch_undelivered_ignores_old_jobs(db):
    """Proteção contra ressuscitar alerta antigo: se o envio ficou quebrado
    por muito tempo, notificar uma vaga de meses atrás só confunde."""
    db.insert_job("Vaga antiga", "https://example.com/antiga", "2020-01-01")

    assert db.fetch_undelivered("111", max_age_days=7) == []


def test_fetch_undelivered_respects_the_limit(db):
    for index in range(5):
        db.insert_job(f"Vaga {index}", f"https://example.com/{index}", _recent_date())

    assert len(db.fetch_undelivered("111", limit=2)) == 2


def test_fetch_undelivered_filters_by_subscriber_preferences(db):
    """Base da personalização: a mesma consulta que alimenta a fila de envio
    já sabe respeitar região e tipo escolhidos pelo assinante."""
    db.insert_job("A", "https://example.com/a", _recent_date(),
                  job_type="concurso", region="norte_fluminense")
    db.insert_job("B", "https://example.com/b", _recent_date(),
                  job_type="noticia", region="norte_fluminense")
    db.insert_job("C", "https://example.com/c", _recent_date(),
                  job_type="concurso", region="capital_metropolitana")

    apenas_concurso_norte = db.fetch_undelivered(
        "111", regions=["norte_fluminense"], job_types=["concurso"]
    )

    assert [job.link for job in apenas_concurso_norte] == ["https://example.com/a"]


def test_fetch_undelivered_without_preferences_returns_everything(db):
    db.insert_job("A", "https://example.com/a", _recent_date(),
                  job_type="concurso", region="norte_fluminense")
    db.insert_job("B", "https://example.com/b", _recent_date(),
                  job_type="noticia", region="capital_metropolitana")

    assert len(db.fetch_undelivered("111")) == 2


def test_source_url_is_dropped_when_equal_to_the_link(db):
    """Guardar a origem quando ela é o próprio link faria a interface mostrar
    o mesmo endereço duas vezes."""
    db.insert_job("V", "https://example.com/p", _recent_date(), source_url="https://example.com/p")

    assert db.fetch_all_jobs()[0].source_url is None


def test_source_url_is_kept_when_it_differs(db):
    db.insert_job("V", "https://example.com/edital.pdf", _recent_date(),
                  source_url="https://example.com/pagina")

    assert db.fetch_all_jobs()[0].source_url == "https://example.com/pagina"


def test_existing_pdf_only_jobs_get_their_origin_backfilled(tmp_path):
    """Sem retroagir, só as vagas novas teriam a página de origem — e as
    antigas que mais precisam dela são justamente as que apontam direto para
    um PDF, sem nenhum contexto ao redor."""
    caminho = str(tmp_path / "antigo.db")
    antigo = DatabaseManager(db_name=caminho)
    antigo.insert_job("Residência", "https://static.medgrupo.com.br/x/edital01.pdf", "2026-01-01")
    antigo.insert_job("Notícia", "https://g1.globo.com/uma-materia", "2026-01-01")
    antigo.conn.execute("ALTER TABLE jobs DROP COLUMN source_url")  # simula banco anterior
    antigo.conn.commit()
    antigo.close()

    migrado = DatabaseManager(db_name=caminho)
    try:
        por_link = {j.link: j.source_url for j in migrado.fetch_all_jobs()}
        assert por_link["https://static.medgrupo.com.br/x/edital01.pdf"] == "https://concursos.medgrupo.com.br/"
        assert por_link["https://g1.globo.com/uma-materia"] is None, "link que já é página não ganha origem"
    finally:
        migrado.close()


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
