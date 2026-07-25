"""Testes dos scrapers de agregadores de notícias, contra fixtures HTML fixos."""
from medalert.scrapers.news import BingNewsScraper, G1Scraper, GoogleNewsScraper
from tests.conftest import load_fixture


def test_google_news_scraper_keeps_relevant_article_and_fixes_relative_link(stub_html):
    scraper = GoogleNewsScraper()
    stub_html(scraper, load_fixture("google_news.html"))

    jobs = scraper.scrape()

    assert len(jobs) == 1
    assert jobs[0]["title"] == "[Notícia/Radar] Concurso oferece vagas de médico no Rio de Janeiro"
    assert jobs[0]["link"] == "https://news.google.com/articles/CBMi123abc"


def test_google_news_scraper_drops_unrelated_article(stub_html):
    scraper = GoogleNewsScraper()
    stub_html(scraper, load_fixture("google_news.html"))

    jobs = scraper.scrape()

    assert not any("Show de música" in job["title"] for job in jobs)


def test_g1_scraper_requires_both_state_and_relevance(stub_html):
    scraper = G1Scraper()
    stub_html(scraper, load_fixture("g1_concursos.html"))

    jobs = scraper.scrape()

    assert len(jobs) == 1
    assert "Rio de Janeiro" in jobs[0]["title"]
    assert jobs[0]["title"].startswith("[G1]")


def test_g1_scraper_only_looks_at_feed_post_link_class(stub_html):
    scraper = G1Scraper()
    stub_html(scraper, load_fixture("g1_concursos.html"))

    jobs = scraper.scrape()

    assert not any("Menu" in job["title"] for job in jobs)


def test_bing_news_scraper_matches_on_title_and_snippet_together(stub_html):
    scraper = BingNewsScraper()
    stub_html(scraper, load_fixture("bing_news.html"))

    jobs = scraper.scrape()

    assert len(jobs) == 1
    assert jobs[0]["title"] == "[Radar/News] Concurso abre vagas de médico no Rio de Janeiro"


def test_bing_news_scraper_drops_non_medical_card(stub_html):
    scraper = BingNewsScraper()
    stub_html(scraper, load_fixture("bing_news.html"))

    jobs = scraper.scrape()

    assert not any("professores" in job["title"] for job in jobs)
