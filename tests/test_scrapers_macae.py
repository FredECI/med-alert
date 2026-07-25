"""Testes do scraper institucional da Prefeitura de Macaé, contra um fixture HTML fixo."""
from medalert.scrapers.macae import MacaeGovScraper
from tests.conftest import load_fixture


def test_keeps_news_with_gov_trigger_and_medical_content(stub_html):
    scraper = MacaeGovScraper()
    stub_html(scraper, load_fixture("macae_noticias.html"))

    jobs = scraper.scrape()

    assert len(jobs) == 1
    assert "área de saúde" in jobs[0]["title"]
    assert jobs[0]["title"].startswith("[Pref. Macaé]")


def test_drops_news_without_trigger_or_medical_content(stub_html):
    scraper = MacaeGovScraper()
    stub_html(scraper, load_fixture("macae_noticias.html"))

    jobs = scraper.scrape()

    assert not any("praça" in job["title"] for job in jobs)


def test_ignores_links_without_noticia_in_href(stub_html):
    scraper = MacaeGovScraper()
    stub_html(scraper, load_fixture("macae_noticias.html"))

    jobs = scraper.scrape()

    assert not any("Institucional" in job["title"] for job in jobs)
