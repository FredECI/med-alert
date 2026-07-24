"""Testes do gerador de relatórios — cobre a paridade de sanitização entre CSV e Markdown."""
import csv

from medalert.report import ReportGenerator


def test_csv_and_markdown_both_sanitize_titles_with_embedded_newlines(tmp_path, db):
    """Regressão do bug em que apenas generate_markdown() limpava \\n/\\r dos
    títulos raspados — generate_csv() escrevia o texto cru, quebrando células
    da planilha (visível hoje em produção no vagas_abertas.csv real)."""
    dirty_title = "Vaga de Médico\n   \n\n\n\nClínico Geral"
    db.insert_job(dirty_title, "https://example.com/dirty", "2026-01-01")

    reporter = ReportGenerator(db_manager=db)

    csv_path = tmp_path / "vagas.csv"
    md_path = tmp_path / "index.md"
    reporter.generate_csv(filename=str(csv_path))
    reporter.generate_markdown(filename=str(md_path))

    with open(csv_path, encoding="utf-8") as f:
        rows = list(csv.reader(f, delimiter=";"))
    csv_title = rows[1][1]

    assert "\n" not in csv_title
    assert csv_title == "Vaga de Médico Clínico Geral"

    md_content = md_path.read_text(encoding="utf-8")
    assert "Vaga de Médico Clínico Geral" in md_content


def test_generate_markdown_includes_frontmatter_and_all_jobs(tmp_path, db):
    db.insert_job("Vaga A", "https://example.com/a", "2026-01-01")
    db.insert_job("Vaga B", "https://example.com/b", "2026-01-02")

    reporter = ReportGenerator(db_manager=db)
    md_path = tmp_path / "index.md"
    reporter.generate_markdown(filename=str(md_path))

    content = md_path.read_text(encoding="utf-8")
    assert content.startswith("---\nlayout: default\n")
    assert "Vaga A" in content
    assert "Vaga B" in content
