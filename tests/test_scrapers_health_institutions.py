"""Testes dos scrapers de instituições de saúde (RioSaúde, Fiotec, IBAM)."""
from medalert.scrapers.health_institutions import FiotecScraper, IbamScraper, RioSaudeScraper
from tests.conftest import load_fixture


def test_riosaude_keeps_edital_paragraphs_with_pdf_link(stub_html):
    scraper = RioSaudeScraper()
    stub_html(scraper, load_fixture("riosaude.html"))

    jobs = scraper.scrape()

    assert len(jobs) == 2
    assert all(job["title"].startswith("[RioSaúde]") for job in jobs)
    titles = " ".join(job["title"] for job in jobs)
    assert "Edital 001/2026" in titles
    assert "Edital 005/2026" in titles
    assert any(job["link"].endswith("edital-01-2026.pdf") for job in jobs)


def test_riosaude_ignores_content_outside_entry_content(stub_html):
    scraper = RioSaudeScraper()
    stub_html(scraper, load_fixture("riosaude.html"))

    jobs = scraper.scrape()

    assert not any("conselho de administração" in job["title"] for job in jobs)


def test_fiotec_keeps_hospital_related_calls(stub_html):
    scraper = FiotecScraper()
    stub_html(scraper, load_fixture("fiotec.html"))

    jobs = scraper.scrape()

    titles = " ".join(job["title"] for job in jobs)
    assert "Diversos cargos - INC/RJ" in titles
    assert "Anestesiologista - Into/RJ" in titles
    assert all(job["title"].startswith("[Fiotec]") for job in jobs)
    assert all(job["link"].startswith("https://www.fiotec.fiocruz.br/") for job in jobs)


def test_fiotec_drops_unrelated_non_health_call(stub_html):
    scraper = FiotecScraper()
    stub_html(scraper, load_fixture("fiotec.html"))

    jobs = scraper.scrape()

    assert not any("Conab" in job["title"] for job in jobs)


def test_ibam_keeps_health_vacancy_in_target_state(stub_html):
    scraper = IbamScraper()
    stub_html(scraper, load_fixture("ibam.html"))

    jobs = scraper.scrape()

    assert len(jobs) == 1
    assert jobs[0]["title"].startswith("[IBAM]")
    assert "Casimiro de Abreu" in jobs[0]["title"]
    assert "Agente Comunitário de Saúde" in jobs[0]["title"]
    assert jobs[0]["link"].endswith("documento/edps-ca0126.pdf")


def test_ibam_drops_non_health_vacancy_in_target_state(stub_html):
    scraper = IbamScraper()
    stub_html(scraper, load_fixture("ibam.html"))

    jobs = scraper.scrape()

    assert not any("Guarda-Vidas" in job["title"] for job in jobs)


def test_ibam_drops_health_vacancy_outside_target_state(stub_html):
    scraper = IbamScraper()
    stub_html(scraper, load_fixture("ibam.html"))

    jobs = scraper.scrape()

    assert not any("Lages" in job["title"] for job in jobs)
