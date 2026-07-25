"""Testes do scraper genérico do PCI Concursos, contra um fixture HTML fixo."""
from medalert.scrapers.pci import PCIListScraper, build_pci_scrapers
from tests.conftest import load_fixture


def test_with_state_filter_keeps_only_in_state_relevant_jobs(stub_html):
    scraper = PCIListScraper(url="https://example.com/", title_prefix="[PCI]", apply_state_filter=True)
    stub_html(scraper, load_fixture("pci_listing.html"))

    jobs = scraper.scrape()

    assert len(jobs) == 1
    assert "Macaé" in jobs[0]["title"]
    assert jobs[0]["title"].startswith("[PCI]")


def test_without_state_filter_keeps_all_relevant_jobs_regardless_of_state(stub_html):
    """Cobre a correção do PCIEstadualScraper: sua URL já é escopada pro RJ
    inteiro, então não deve reaplicar o filtro de cidade por cima."""
    scraper = PCIListScraper(url="https://example.com/", title_prefix="[PCI RJ]", apply_state_filter=False)
    stub_html(scraper, load_fixture("pci_listing.html"))

    jobs = scraper.scrape()

    titles = {job["title"] for job in jobs}
    assert len(jobs) == 2
    assert any("Macaé" in t for t in titles)
    assert any("São Paulo" in t for t in titles)


def test_ignores_navigation_and_non_medical_links(stub_html):
    scraper = PCIListScraper(url="https://example.com/", title_prefix="[PCI]", apply_state_filter=True)
    stub_html(scraper, load_fixture("pci_listing.html"))

    jobs = scraper.scrape()

    assert not any("Sobre o site" in job["title"] for job in jobs)
    assert not any("motorista" in job["title"].lower() for job in jobs)


def test_label_uses_title_prefix_to_distinguish_instances():
    scraper = PCIListScraper(url="https://example.com/", title_prefix="[PCI Saúde]", apply_state_filter=True)
    assert scraper.label == "PCI Saúde"


def test_build_pci_scrapers_returns_the_three_known_variants():
    scrapers = build_pci_scrapers()
    labels = {s.label for s in scrapers}

    assert len(scrapers) == 3
    assert labels == {"PCI", "PCI Saúde", "PCI RJ"}
