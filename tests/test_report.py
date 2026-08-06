"""Testes do gerador de relatórios: CSV, dados para o Jekyll e status do robô."""
import csv
import json

from medalert.report import ReportGenerator, write_robot_status


def test_csv_sanitizes_titles_with_embedded_newlines(tmp_path, db):
    """Regressão: generate_csv() já escreveu texto cru com \\n/\\r no passado
    (visível hoje em produção no vagas_abertas.csv real), enquanto só o
    Markdown antigo limpava isso."""
    dirty_title = "[Trabalha Brasil - Cabo Frio] Vaga de Médico\n   \n\n\n\nClínico Geral"
    db.insert_job(dirty_title, "https://example.com/dirty", "2026-01-01")

    reporter = ReportGenerator(db_manager=db)
    csv_path = tmp_path / "vagas.csv"
    reporter.generate_csv(filename=str(csv_path))

    with open(csv_path, encoding="utf-8") as f:
        rows = list(csv.reader(f, delimiter=";"))

    csv_title = rows[1][1]
    assert "\n" not in csv_title
    assert csv_title == "[Trabalha Brasil - Cabo Frio] Vaga de Médico Clínico Geral"


def test_generate_jobs_data_splits_source_from_title(tmp_path, db):
    db.insert_job("[PCI RJ] Concurso da Fundação Saúde", "https://example.com/a", "2026-07-20")

    reporter = ReportGenerator(db_manager=db)
    out_path = tmp_path / "_data" / "jobs.json"
    reporter.generate_jobs_data(filename=str(out_path))

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert len(payload) == 1
    assert payload[0]["source"] == "PCI RJ"
    assert payload[0]["title"] == "Concurso da Fundação Saúde"
    assert payload[0]["link"] == "https://example.com/a"
    assert payload[0]["discovered_at"] == "2026-07-20"


def test_generate_jobs_data_handles_title_without_source_prefix(tmp_path, db):
    db.insert_job("Vaga sem prefixo de fonte", "https://example.com/b", "2026-07-20")

    reporter = ReportGenerator(db_manager=db)
    out_path = tmp_path / "_data" / "jobs.json"
    reporter.generate_jobs_data(filename=str(out_path))

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload[0]["source"] == ""
    assert payload[0]["title"] == "Vaga sem prefixo de fonte"


def test_generate_jobs_data_formats_last_seen_for_display(tmp_path, db):
    db.insert_job("[G1] Concurso médico", "https://example.com/c", "2026-07-20")
    db.touch_seen("https://example.com/c")

    reporter = ReportGenerator(db_manager=db)
    out_path = tmp_path / "_data" / "jobs.json"
    reporter.generate_jobs_data(filename=str(out_path))

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    # dd/mm/aaaa às hh:mm — não o timestamp ISO cru gravado no banco.
    assert " às " in payload[0]["last_seen_at"]
    assert "T" not in payload[0]["last_seen_at"]


def test_generate_jobs_data_creates_missing_data_directory(tmp_path, db):
    db.insert_job("[G1] Concurso médico", "https://example.com/d", "2026-07-20")

    reporter = ReportGenerator(db_manager=db)
    out_path = tmp_path / "nested" / "_data" / "jobs.json"
    reporter.generate_jobs_data(filename=str(out_path))

    assert out_path.exists()


def test_generate_jobs_data_infers_source_for_legacy_untagged_titles(tmp_path, db):
    """Vagas anteriores à padronização do prefixo "[Fonte]" (~46% do histórico)
    apareciam no site sem identificação nenhuma — o domínio do link resolve
    sem precisar reescrever o banco."""
    db.insert_job(
        "Prefeitura de Casimiro de Abreu",
        "https://www.pciconcursos.com.br/noticias/prefeitura-abre-processo",
        "2026-04-16",
    )

    reporter = ReportGenerator(db_manager=db)
    out_path = tmp_path / "_data" / "jobs.json"
    reporter.generate_jobs_data(filename=str(out_path))

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload[0]["source"] == "PCI"
    assert payload[0]["title"] == "Prefeitura de Casimiro de Abreu"


def test_explicit_source_prefix_wins_over_domain_inference(tmp_path, db):
    db.insert_job(
        "[PCI RJ] Concurso da Fundação Saúde",
        "https://www.pciconcursos.com.br/noticias/outro",
        "2026-07-20",
    )

    reporter = ReportGenerator(db_manager=db)
    out_path = tmp_path / "_data" / "jobs.json"
    reporter.generate_jobs_data(filename=str(out_path))

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload[0]["source"] == "PCI RJ"


def test_generate_jobs_data_collapses_the_same_edital_from_two_sources(tmp_path, db):
    """As duas linhas continuam no banco (nada é perdido), mas o site mostra
    uma só — senão o mesmo processo seletivo aparece duplicado na tabela."""
    db.insert_job("[IBAM] Municipio de Cabo Frio - Ed. 07/2026", "https://ibam.example/7", "2026-07-01")
    db.insert_job("[Cabo Frio] Edital 007/2026 - saúde", "https://cabofrio.example/7", "2026-07-02")

    reporter = ReportGenerator(db_manager=db)
    out_path = tmp_path / "_data" / "jobs.json"
    reporter.generate_jobs_data(filename=str(out_path))

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert len(payload) == 1
    assert len(db.fetch_all_jobs()) == 2


def test_generate_jobs_data_keeps_jobs_without_a_confident_signature(tmp_path, db):
    """Vagas sem assinatura (dedup_key None) nunca podem ser agrupadas entre
    si — seriam vagas distintas escondidas por engano."""
    db.insert_job("[G1] Concurso de saúde no interior", "https://g1.example/a", "2026-07-01")
    db.insert_job("[PCI] Outro concurso de saúde", "https://pci.example/b", "2026-07-02")

    reporter = ReportGenerator(db_manager=db)
    out_path = tmp_path / "_data" / "jobs.json"
    reporter.generate_jobs_data(filename=str(out_path))

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert len(payload) == 2


def test_write_robot_status_reports_counts_and_failed_labels(tmp_path):
    out_path = tmp_path / "_data" / "robot_status.json"
    write_robot_status(scrapers_total=10, failed_labels=["BingNewsScraper"], filename=str(out_path))

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["scrapers_total"] == 10
    assert payload["scrapers_ok"] == 9
    assert payload["scrapers_failed"] == ["BingNewsScraper"]
    assert " às " in payload["last_run_at"]


def test_write_robot_status_with_no_failures(tmp_path):
    out_path = tmp_path / "_data" / "robot_status.json"
    write_robot_status(scrapers_total=10, failed_labels=[], filename=str(out_path))

    payload = json.loads(out_path.read_text(encoding="utf-8"))
    assert payload["scrapers_ok"] == 10
    assert payload["scrapers_failed"] == []
