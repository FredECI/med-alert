"""Testes dos scrapers institucionais municipais: Araruama, Cabo Frio, Rio das
Ostras (notícias e concurso público) e Casimiro de Abreu, cada um contra um
fixture HTML fixo."""
from medalert.scrapers.municipal_gov import (
    AraruamaGovScraper,
    CaboFrioSaudeScraper,
    CasimiroDeAbreuGovScraper,
    RioDasOstrasConcursoScraper,
    RioDasOstrasNoticiasScraper,
    SaquaremaGovScraper,
)
from tests.conftest import load_fixture


def test_araruama_keeps_medical_concurso_item(stub_html):
    scraper = AraruamaGovScraper()
    stub_html(scraper, load_fixture("araruama.html"))

    jobs = scraper.scrape()

    assert len(jobs) == 1
    assert jobs[0]["title"].startswith("[Araruama]")
    assert "MÉDICO CLÍNICO GERAL" in jobs[0]["title"]
    assert jobs[0]["link"].endswith("resultado-final-do-concurso-publico-para-medico-clinico-geral")


def test_araruama_drops_non_medical_seduc_items(stub_html):
    scraper = AraruamaGovScraper()
    stub_html(scraper, load_fixture("araruama.html"))

    jobs = scraper.scrape()

    assert not any("SEDUC" in job["title"] for job in jobs)


def test_cabofrio_keeps_health_hiring_news(stub_html):
    scraper = CaboFrioSaudeScraper()
    stub_html(scraper, load_fixture("cabofrio_saude.html"))

    jobs = scraper.scrape()

    assert len(jobs) == 1
    assert jobs[0]["title"].startswith("[Cabo Frio]")
    assert "processo seletivo" in jobs[0]["title"].lower()
    assert jobs[0]["link"] == (
        "https://noticias.cabofrio.rj.gov.br/prefeitura-de-cabo-frio-abre-inscricoes-"
        "para-processo-seletivo-de-agente-comunitario-de-saude/"
    )


def test_cabofrio_drops_general_health_news_without_hiring_signal(stub_html):
    scraper = CaboFrioSaudeScraper()
    stub_html(scraper, load_fixture("cabofrio_saude.html"))

    jobs = scraper.scrape()

    assert not any("Oficinas de Letramento" in job["title"] for job in jobs)
    assert not any("Fórum de Saúde do Homem" in job["title"] for job in jobs)


def test_riodasostras_noticias_keeps_health_hiring_news(stub_html):
    scraper = RioDasOstrasNoticiasScraper()
    stub_html(scraper, load_fixture("riodasostras_noticias.html"))

    jobs = scraper.scrape()

    assert len(jobs) == 1
    assert jobs[0]["title"].startswith("[Rio das Ostras]")
    assert "processo seletivo" in jobs[0]["title"].lower()


def test_riodasostras_noticias_drops_unrelated_and_non_hiring_news(stub_html):
    scraper = RioDasOstrasNoticiasScraper()
    stub_html(scraper, load_fixture("riodasostras_noticias.html"))

    jobs = scraper.scrape()

    assert not any("tartaruga" in job["title"].lower() for job in jobs)
    # Menciona saúde (is_relevant bate) mas não é uma vaga real (sem has_job_signal)
    assert not any("conselho de saúde" in job["title"].lower() for job in jobs)


def test_riodasostras_concurso_keeps_health_specific_concurso(stub_html):
    scraper = RioDasOstrasConcursoScraper()
    stub_html(scraper, load_fixture("riodasostras_concurso.html"))

    jobs = scraper.scrape()

    assert len(jobs) == 1
    assert jobs[0]["title"].startswith("[Rio das Ostras]")
    assert "Saúde" in jobs[0]["title"]
    assert jobs[0]["link"].endswith("/concursopublico-ro052026/")


def test_riodasostras_concurso_drops_generic_entries_without_link_or_health_term(stub_html):
    scraper = RioDasOstrasConcursoScraper()
    stub_html(scraper, load_fixture("riodasostras_concurso.html"))

    jobs = scraper.scrape()

    assert not any("2ª Prova Prática" in job["title"] for job in jobs)
    assert not any("SAAE" in job["title"] for job in jobs)
    assert not any("Fundação Rio das Ostras de Cultura" in job["title"] for job in jobs)


def test_saquarema_keeps_medical_concurso_item(stub_html):
    scraper = SaquaremaGovScraper()
    stub_html(scraper, load_fixture("saquarema.html"))

    jobs = scraper.scrape()

    assert len(jobs) == 1
    assert jobs[0]["title"].startswith("[Saquarema]")
    assert "Agente Comunitário de Saúde" in jobs[0]["title"]


def test_saquarema_drops_non_medical_concurso_items(stub_html):
    scraper = SaquaremaGovScraper()
    stub_html(scraper, load_fixture("saquarema.html"))

    jobs = scraper.scrape()

    assert not any("educação" in job["title"].lower() for job in jobs)
    assert not any("guarda-vidas" in job["title"].lower() for job in jobs)


def test_casimiro_keeps_health_secretariat_concurso(stub_html):
    scraper = CasimiroDeAbreuGovScraper()
    stub_html(scraper, load_fixture("casimiro.html"))

    jobs = scraper.scrape()

    assert len(jobs) == 1
    assert jobs[0]["title"].startswith("[Casimiro de Abreu]")
    assert "SECRETARIA DE SAÚDE" in jobs[0]["title"]
    assert jobs[0]["link"] == "https://transparencia.casimirodeabreu.rj.gov.br/concursopublico.php?grup=99"


def test_casimiro_drops_non_medical_rows_and_ignores_no_concurso_declarations(stub_html):
    scraper = CasimiroDeAbreuGovScraper()
    stub_html(scraper, load_fixture("casimiro.html"))

    jobs = scraper.scrape()

    assert not any("ASSISTÊNCIA SOCIAL" in job["title"] for job in jobs)
    assert not any("PROCURADORIA GERAL" in job["title"] for job in jobs)
    # As declarações "não houve concursos" (aba DECLARAÇÕES) não devem virar jobs
    assert not any("não houve" in job["title"].lower() for job in jobs)
