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
    for atributo in ("data-region", "data-type", "data-status", "data-specialty"):
        assert atributo in job_row, f"o template precisa emitir {atributo}"
        assert atributo in search_js, f"o filtro precisa ler {atributo}"


def test_chips_declare_the_same_dimensions_the_filter_knows(header, search_js):
    """Os chips dizem qual dimensão alteram via data-filter; o JS indexa o
    estado por esse mesmo nome."""
    assert 'data-filter="region"' in header
    assert 'data-filter="type"' in header
    assert 'data-filter="specialty"' in header
    assert 'state[chip.getAttribute("data-filter")]' in search_js


def test_row_uses_the_fields_report_publishes(job_row):
    """Campos gerados por ReportGenerator.generate_jobs_data()."""
    for campo in ("region_label", "job_type_label", "source_url", "last_seen_at", "link",
                  "status", "status_label", "deadline", "specialties", "specialty_labels"):
        assert f"include.job.{campo}" in job_row, f"{campo} não está sendo usado no template"


def test_only_a_closed_job_is_visually_marked(job_row):
    """"desconhecido" não vira selo: um "prazo não informado" em quase toda
    linha seria ruído, e ainda passaria a impressão de que o robô sabe algo
    sobre o prazo que ele não sabe."""
    assert "job-row--encerrada" in job_row
    assert "'encerrado'" in job_row


def test_hiding_closed_jobs_is_opt_in_and_never_touches_the_unknown(header, search_js):
    """Só se esconde o que a fonte afirmou estar encerrado. Sumir com uma vaga
    de prazo desconhecido — possivelmente aberta — seria o pior erro que este
    filtro poderia cometer."""
    assert 'id="hide-closed"' in header
    assert "state.hideClosed" in search_js
    assert '"encerrado"' in search_js


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


def test_a_row_can_carry_several_specialties(job_row, search_js):
    """Especialidade é a única dimensão multivalorada na LINHA: um edital abre
    cargos de várias áreas. Comparar por igualdade, como se faz com região e
    tipo, faria o filtro não achar nada."""
    assert "join: ' '" in job_row, "as chaves saem separadas por espaço"
    assert "intersects(row" in search_js, "o filtro precisa comparar por token"


def test_url_keeps_the_filter_state(search_js):
    """Para dar para mandar a alguém um link já filtrado."""
    for parametro in ("regiao", "tipo", "especialidade", "busca"):
        assert f'"{parametro}"' in search_js
    assert "replaceState" in search_js
