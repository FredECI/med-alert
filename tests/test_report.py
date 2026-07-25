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
    db.touch_last_seen("https://example.com/c")

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
