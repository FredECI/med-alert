"""Testes dos scrapers de portais de emprego, contra fixtures HTML fixos.

O caso mais importante aqui é a regressão do vazamento entre estados no
Trabalha Brasil: uma vaga real de Bom Jesus do Norte-ES apareceu no site
focado no RJ porque o scraper não validava o slug de estado da URL retornada.
"""
from medalert.scrapers.portals import JCConcursosScraper, PandaPeUnimedScraper, TrabalhaBrasilScraper
from tests.conftest import load_fixture


def test_jc_concursos_scraper_accepts_concursos_and_noticia_paths(stub_html):
    scraper = JCConcursosScraper()
    stub_html(scraper, load_fixture("jc_concursos.html"))

    jobs = scraper.scrape()

    titles = {job["title"] for job in jobs}
    assert len(jobs) == 2
    assert any("Fundação Saúde RJ" in t for t in titles)
    assert any("Bombeiros RJ" in t for t in titles)


def test_jc_concursos_scraper_drops_short_navigation_titles(stub_html):
    scraper = JCConcursosScraper()
    stub_html(scraper, load_fixture("jc_concursos.html"))

    jobs = scraper.scrape()

    assert not any(job["title"] == "[JC Concursos] RJ" for job in jobs)


def test_pandape_unimed_scraper_keeps_relevant_detail_links(stub_html):
    scraper = PandaPeUnimedScraper()
    stub_html(scraper, load_fixture("pandape_unimed.html"))

    jobs = scraper.scrape()

    assert len(jobs) == 1
    assert "Clínico Geral" in jobs[0]["title"]


def test_pandape_unimed_scraper_drops_non_medical_vacancy(stub_html):
    scraper = PandaPeUnimedScraper()
    stub_html(scraper, load_fixture("pandape_unimed.html"))

    jobs = scraper.scrape()

    assert not any("Recepcionista" in job["title"] for job in jobs)


def test_trabalha_brasil_scraper_rejects_cross_state_leak(monkeypatch):
    """Regressão do bug real: o site injeta 'vagas similares' de outros
    estados; sem checar o slug '-rj/' na própria URL, uma vaga de
    Bom Jesus do Norte-ES vazava para o radar do Rio de Janeiro."""
    scraper = TrabalhaBrasilScraper(cities=["Macae"])
    monkeypatch.setattr(scraper, "fetch_html", lambda url: load_fixture("trabalha_brasil.html"))
    monkeypatch.setattr("medalert.scrapers.portals.time.sleep", lambda seconds: None)

    jobs = scraper.scrape()

    assert len(jobs) == 1
    assert "macae-rj" in jobs[0]["link"]
    assert not any("bom-jesus-do-norte-es" in job["link"] for job in jobs)


def test_trabalha_brasil_scraper_sanitizes_before_truncating(monkeypatch):
    """Regressão: truncar antes de limpar espaços/quebras gastava boa parte
    dos 60 caracteres com whitespace, deixando o título quase vazio."""
    dirty_html = (
        '<a href="/vagas-de-emprego-em-macae-rj/medico/999">'
        "Vaga\n   \n\n\nde\n   Médico Clínico Geral Plantonista Urgente Macaé"
        "</a>"
    )
    scraper = TrabalhaBrasilScraper(cities=["Macae"])
    monkeypatch.setattr(scraper, "fetch_html", lambda url: dirty_html)
    monkeypatch.setattr("medalert.scrapers.portals.time.sleep", lambda seconds: None)

    jobs = scraper.scrape()

    assert len(jobs) == 1
    title = jobs[0]["title"]
    assert "\n" not in title
    assert "Vaga de Médico Clínico Geral" in title
