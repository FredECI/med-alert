"""Trava o contrato entre o que o Python publica, o que o template renderiza
e o que o JavaScript do site consome.

São três peças acopladas por convenção de nome, sem nada que as force a
concordar: `report.py` escreve as chaves em `_data/jobs.json`, o template
Liquid as transforma em atributos `data-*`, e o `search.js` filtra por esses
atributos. Renomear em um lugar só não quebra build nenhum — o filtro
simplesmente para de encontrar linhas, em silêncio.
"""
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]
JOB_ROW = RAIZ / "_includes" / "job_row.html"
HEADER = RAIZ / "_includes" / "header.html"
SEARCH_JS = RAIZ / "assets" / "js" / "search.js"


@pytest.fixture(scope="module")
def job_row() -> str:
    return JOB_ROW.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def search_js() -> str:
    return SEARCH_JS.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def header() -> str:
    return HEADER.read_text(encoding="utf-8")


def test_row_exposes_the_attributes_the_filter_reads(job_row, search_js):
    for atributo in ("data-region", "data-type"):
        assert atributo in job_row, f"o template precisa emitir {atributo}"
        assert atributo in search_js, f"o filtro precisa ler {atributo}"


def test_chips_declare_the_same_dimensions_the_filter_knows(header, search_js):
    """Os chips dizem qual dimensão alteram via data-filter; o JS indexa o
    estado por esse mesmo nome."""
    assert 'data-filter="region"' in header
    assert 'data-filter="type"' in header
    assert 'state[chip.getAttribute("data-filter")]' in search_js


def test_row_uses_the_fields_report_publishes(job_row):
    """Campos gerados por ReportGenerator.generate_jobs_data()."""
    for campo in ("region_label", "job_type_label", "source_url", "last_seen_at", "link"):
        assert f"include.job.{campo}" in job_row, f"{campo} não está sendo usado no template"


def test_news_rows_are_visually_marked(job_row):
    """Notícia não é vaga confirmada e não deve competir visualmente com as
    que são."""
    assert "job-row--noticia" in job_row
    assert "não é uma vaga confirmada" in job_row


def test_link_label_adapts_to_the_job_type(job_row):
    """"Acessar Edital" está errado para notícia e para vaga CLT."""
    assert "Ler notícia" in job_row
    assert "Ver vaga" in job_row
    assert "Acessar edital" in job_row


def test_filters_start_hidden_and_are_revealed_by_javascript(header, search_js):
    """Sem JS os chips não fariam nada, e botão que não funciona é pior do que
    botão nenhum — a tabela completa continua servindo."""
    assert 'id="filters"' in header and "hidden" in header
    assert "filtersBox.hidden = false" in search_js


def test_script_tag_busts_the_browser_cache():
    """HTML e JS ficam em entradas de cache separadas. Sem versionar a URL do
    script, quem já visitou o site recebe o HTML novo com o JS velho — e a
    falha é silenciosa: a página carrega, mas os filtros não respondem."""
    index = (RAIZ / "index.md").read_text(encoding="utf-8")

    assert "search.js" in index
    assert "?v=" in index, "a URL do script precisa mudar a cada build"


def test_url_keeps_the_filter_state(search_js):
    """Para dar para mandar a alguém um link já filtrado."""
    for parametro in ("regiao", "tipo", "busca"):
        assert f'"{parametro}"' in search_js
    assert "replaceState" in search_js
