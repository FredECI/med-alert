"""Testes dos scrapers de instituições de saúde (RioSaúde, Fiotec, IBAM)."""
from medalert.scrapers.health_institutions import FiotecScraper, IbamScraper, RioSaudeScraper
from medalert.scrapers.health_institutions import RioSaudePssScraper
from tests.conftest import load_fixture


def _stub_pss(monkeypatch, scraper):
    """O PSS faz duas buscas: a página (para achar o bundle) e o bundle em si."""
    def fake_fetch(url):
        return load_fixture("riosaude_pss_bundle.js") if url.endswith(".js") else load_fixture("riosaude_pss.html")

    monkeypatch.setattr(scraper, "fetch_html", fake_fetch)


def test_riosaude_pss_groups_cargos_by_edital(monkeypatch):
    """Um edital abre dezenas de cargos; notificar cargo a cargo viraria
    enxurrada, então cada edital vira uma entrada só."""
    scraper = RioSaudePssScraper()
    _stub_pss(monkeypatch, scraper)

    jobs = scraper.scrape()

    assert len(jobs) == 2  # editais 003/2026 e 007/2026
    edital_003 = next(j for j in jobs if "003/2026" in j["title"])
    assert "ANESTESIOLOGISTA" in edital_003["title"]
    assert edital_003["link"] == "https://pss.riosaude.rio.br/003-2026.pdf"


def test_riosaude_pss_summarises_extra_cargos(monkeypatch):
    """O título mostra as primeiras especialidades e resume o resto."""
    scraper = RioSaudePssScraper()
    _stub_pss(monkeypatch, scraper)

    jobs = scraper.scrape()
    edital_003 = next(j for j in jobs if "003/2026" in j["title"])

    assert "(+1)" in edital_003["title"]  # 4 cargos médicos, 3 exibidos


def test_riosaude_pss_ignores_previous_years(monkeypatch):
    """A página guarda todo o histórico; editais de anos anteriores já se
    encerraram e só poluiriam o radar."""
    scraper = RioSaudePssScraper()
    _stub_pss(monkeypatch, scraper)

    jobs = scraper.scrape()

    assert not any("2025" in job["title"] for job in jobs)


def test_riosaude_pss_ignores_non_medical_cargos(monkeypatch):
    scraper = RioSaudePssScraper()
    _stub_pss(monkeypatch, scraper)

    jobs = scraper.scrape()

    assert not any("AGENTE DE REGULAÇÃO" in job["title"] for job in jobs)


def test_riosaude_pss_returns_empty_when_bundle_reference_is_gone(monkeypatch):
    """O nome do bundle muda a cada deploy; se o padrão sumir, o scraper
    precisa degradar em silêncio em vez de explodir."""
    scraper = RioSaudePssScraper()
    monkeypatch.setattr(scraper, "fetch_html", lambda url: "<html><body>sem bundle</body></html>")

    assert scraper.scrape() == []


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
