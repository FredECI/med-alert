"""Testes dos portais médicos e bancas: MedGrupo, Estratégia Saúde e FGV."""
import json

from medalert.scrapers.medical_portals import (
    EstrategiaSaudeScraper,
    FgvConcursosScraper,
    MedGrupoScraper,
)
from tests.conftest import load_fixture


class _FakeResponse:
    def __init__(self, text):
        self.text = text

    def raise_for_status(self):
        return None


def _stub_post(monkeypatch, scraper, html):
    """MedGrupo busca a listagem por POST, não por GET, então o stub precisa
    interceptar scraper.post em vez de fetch_html."""
    chamadas = {}

    def fake_post(url, data=None, headers=None, timeout=None):
        chamadas["url"] = url
        chamadas["payload"] = json.loads(data)
        return _FakeResponse(html)

    monkeypatch.setattr(scraper.scraper, "post", fake_post)
    return chamadas


def test_medgrupo_uses_full_institution_name_and_edital_pdf_link(monkeypatch):
    scraper = MedGrupoScraper()
    _stub_post(monkeypatch, scraper, load_fixture("medgrupo.html"))

    jobs = scraper.scrape()

    titulos = {job["title"] for job in jobs}
    assert "[MedGrupo] ASSOCIAÇÃO FLUMINENSE DE ASSISTÊNCIA À MULHER, À CRIANÇA E AO IDOSO" in titulos
    afamci = next(j for j in jobs if "FLUMINENSE" in j["title"])
    assert afamci["link"].endswith("edital01_afamci_2026.pdf")


def test_medgrupo_skips_rows_without_an_edital_link(monkeypatch):
    """Sem link do edital não há como o usuário chegar na vaga, e o link é a
    chave de deduplicação do banco."""
    scraper = MedGrupoScraper()
    _stub_post(monkeypatch, scraper, load_fixture("medgrupo.html"))

    jobs = scraper.scrape()

    assert not any("SEM LINK" in job["title"] for job in jobs)
    assert len(jobs) == 2


def test_medgrupo_requests_rio_de_janeiro_and_a_year(monkeypatch):
    """O endpoint devolve 500 se `ano` vier nulo — o payload precisa manter
    tanto o filtro de estado quanto o ano."""
    scraper = MedGrupoScraper()
    chamadas = _stub_post(monkeypatch, scraper, load_fixture("medgrupo.html"))

    scraper.scrape()

    assert chamadas["payload"]["estados"] == ["1"]
    assert isinstance(chamadas["payload"]["ano"], int)


def test_estrategia_saude_keeps_only_rio_de_janeiro_entries(stub_html):
    """A página inteira já é da área da saúde, mas tem alcance nacional."""
    scraper = EstrategiaSaudeScraper()
    stub_html(scraper, load_fixture("estrategia_saude.html"))

    jobs = scraper.scrape()

    titulos = {job["title"] for job in jobs}
    assert "[Estratégia Saúde] Concurso SES RJ" in titulos
    assert "[Estratégia Saúde] Concurso Saquarema Saúde RJ" in titulos
    assert len(jobs) == 2


def test_estrategia_saude_drops_other_states(stub_html):
    scraper = EstrategiaSaudeScraper()
    stub_html(scraper, load_fixture("estrategia_saude.html"))

    jobs = scraper.scrape()

    assert not any("SESA AP" in job["title"] for job in jobs)
    assert not any("Araucária" in job["title"] for job in jobs)


def test_fgv_keeps_rio_health_and_national_health_employers(monkeypatch):
    scraper = FgvConcursosScraper()
    monkeypatch.setattr(scraper, "fetch_html", lambda url: load_fixture("fgv_concursos.html"))

    jobs = scraper.scrape()
    titulos = " | ".join(job["title"] for job in jobs)

    # EBSERH não cita o RJ no nome, mas administra hospitais universitários
    # federais no Rio — entra pela lista de empregadores nacionais da saúde.
    assert "EBSERH" in titulos
    assert "Polícia Militar do Estado do Rio de Janeiro - Saúde" in titulos


def test_fgv_drops_health_from_other_states_and_non_health_from_rio(monkeypatch):
    scraper = FgvConcursosScraper()
    monkeypatch.setattr(scraper, "fetch_html", lambda url: load_fixture("fgv_concursos.html"))

    jobs = scraper.scrape()
    titulos = " | ".join(job["title"] for job in jobs)

    assert "Tocantins" not in titulos  # saúde, mas fora do RJ
    assert "Defensoria" not in titulos  # RJ, mas não é saúde
    assert "Paraná" not in titulos


def test_fgv_builds_absolute_links(monkeypatch):
    scraper = FgvConcursosScraper()
    monkeypatch.setattr(scraper, "fetch_html", lambda url: load_fixture("fgv_concursos.html"))

    jobs = scraper.scrape()

    assert all(job["link"].startswith("https://conhecimento.fgv.br/concursos/") for job in jobs)
