"""Testes dos scrapers de agregadores genéricos (InfoJobs, Concursos no
Brasil e DOM-RJ), contra fixtures fixos."""
from medalert.scrapers.aggregators import ConcursosNoBrasilScraper, DomRjScraper, InfoJobsScraper
from tests.conftest import load_fixture


def test_infojobs_keeps_medical_vacancy_with_correct_title_and_link(stub_html):
    scraper = InfoJobsScraper()
    stub_html(scraper, load_fixture("infojobs.html"))

    jobs = scraper.scrape()

    assert len(jobs) == 1
    assert jobs[0]["title"] == "[InfoJobs] Médico Do Trabalho - RJ"
    assert jobs[0]["link"] == "https://www.infojobs.com.br/vaga-de-medico-do-trabalho-rj-em-rio-janeiro__11783371.aspx"


def test_infojobs_drops_non_medical_card_despite_being_on_the_medico_search_page(stub_html):
    """A busca do próprio InfoJobs (embutida na URL) é por substring — ela
    também devolve "Vendedora Interno Medicamentos", que não é vaga pra
    médico. is_relevant() (casamento por palavra inteira) deve descartar."""
    scraper = InfoJobsScraper()
    stub_html(scraper, load_fixture("infojobs.html"))

    jobs = scraper.scrape()

    assert not any("Medicamentos" in job["title"] for job in jobs)


def test_infojobs_ignores_company_profile_and_nav_links(stub_html):
    scraper = InfoJobsScraper()
    stub_html(scraper, load_fixture("infojobs.html"))

    jobs = scraper.scrape()

    assert not any("Bencorp" in job["title"] for job in jobs)
    assert not any("Anuncie" in job["title"] for job in jobs)


def test_concursosnobrasil_keeps_health_concurso_using_link_title_attribute(stub_html):
    scraper = ConcursosNoBrasilScraper()
    stub_html(scraper, load_fixture("concursosnobrasil.html"))

    jobs = scraper.scrape()

    assert len(jobs) == 1
    assert jobs[0]["title"] == "[Concursos no Brasil] Concurso Prefeitura de Volta Redonda (RJ) abre 355 vagas na Saúde"
    assert "volta-redonda" in jobs[0]["link"]


def test_concursosnobrasil_drops_non_medical_concurso(stub_html):
    """A tabela lista concursos de qualquer área — Guapimirim é um concurso
    geral (sem menção a saúde/médico) e deve ser descartado."""
    scraper = ConcursosNoBrasilScraper()
    stub_html(scraper, load_fixture("concursosnobrasil.html"))

    jobs = scraper.scrape()

    assert not any("Guapimirim" in job["title"] for job in jobs)


def test_domrj_keeps_hit_with_job_signal_and_builds_pdf_viewer_link(stub_html):
    scraper = DomRjScraper()
    stub_html(scraper, load_fixture("domrj.html"))

    jobs = scraper.scrape()

    assert len(jobs) == 1
    assert jobs[0]["title"].startswith("[DOM-RJ]")
    assert "MÉDICO" in jobs[0]["title"] or "CONVOCAÇÃO" in jobs[0]["title"].upper()
    assert jobs[0]["link"] == "https://doweb.rio.rj.gov.br/ver/14879/85/sa%C3%BAde"


def test_domrj_drops_hit_that_mentions_saude_without_any_job_signal(stub_html):
    """Regressão-alvo do has_job_signal(): um aviso de suspensão de
    atendimento na UPA Bangu menciona "Saúde" (bate is_relevant), mas não é
    concurso/vaga/edital nenhum — sem has_job_signal() ele vazaria como
    'vaga' encontrada."""
    scraper = DomRjScraper()
    stub_html(scraper, load_fixture("domrj.html"))

    jobs = scraper.scrape()

    assert not any("Bangu" in job["title"] for job in jobs)
    assert not any("suspensão" in job["title"].lower() for job in jobs)
