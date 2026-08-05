"""Testes do texto das mensagens do Telegram."""
from medalert.messages import build_failure_alert, build_job_message, escape_html
from medalert.models import Job


def _job(**kwargs) -> Job:
    base = dict(
        title="Concurso para médico",
        link="https://example.com/edital.pdf",
        discovered_at="2026-08-05",
        job_type="concurso",
        region="norte_fluminense",
    )
    base.update(kwargs)
    return Job(**base)


def test_escape_protects_the_three_html_characters():
    assert escape_html("A & B <c>") == "A &amp; B &lt;c&gt;"


def test_scraped_titles_with_brackets_and_underscores_survive():
    """Regressão do bug que motivou a troca de Markdown para HTML: títulos
    raspados vêm cheios de `[`, `]` e `_`, e no Markdown do Telegram qualquer
    um desbalanceado fazia a API recusar a mensagem (400) e o alerta sumir."""
    titulo = "[Trabalha Brasil - Cabo Frio] Vaga de Médico_Clínico *urgente* [2026]"

    msg = build_job_message(_job(title=titulo))

    assert titulo in msg, "o título vai literal; nada nele é marcação em HTML"


def test_header_changes_with_the_job_type():
    assert "Novo concurso público" in build_job_message(_job(job_type="concurso"))
    assert "Nova residência médica" in build_job_message(_job(job_type="residencia"))
    assert "Nova vaga de emprego" in build_job_message(_job(job_type="emprego"))
    assert "Novo processo seletivo" in build_job_message(_job(job_type="processo_seletivo"))


def test_news_is_announced_as_a_lead_not_as_a_vacancy():
    """Uma matéria sobre um concurso que "deve sair" não é vaga aberta.
    Anunciá-la como oportunidade prometeria algo que o link não entrega."""
    msg = build_job_message(_job(job_type="noticia"))

    assert "Radar" in msg
    assert "não uma vaga confirmada" in msg
    assert "Nova oportunidade" not in msg


def test_link_label_matches_the_job_type():
    assert "Acessar edital" in build_job_message(_job(job_type="concurso"))
    assert "Ver vaga" in build_job_message(_job(job_type="emprego"))
    assert "Ler notícia" in build_job_message(_job(job_type="noticia"))


def test_region_and_type_are_shown_in_readable_form():
    msg = build_job_message(_job(region="regiao_dos_lagos", job_type="residencia"))

    assert "Região dos Lagos" in msg
    assert "Residência médica" in msg


def test_source_url_appears_only_when_it_differs_from_the_main_link():
    com_origem = build_job_message(_job(source_url="https://example.com/pagina"))
    sem_origem = build_job_message(_job(source_url=None))

    assert "Página de origem" in com_origem
    assert "https://example.com/pagina" in com_origem
    assert "Página de origem" not in sem_origem


def test_discovery_date_is_labelled_honestly():
    """Nenhum scraper lê a data real de publicação da fonte — chamar isso de
    "Data" prometeria uma precisão que o dado não tem."""
    assert "Descoberta em 2026-08-05" in build_job_message(_job())


def test_unknown_type_falls_back_to_a_generic_header():
    msg = build_job_message(_job(job_type=None))

    assert "Nova oportunidade encontrada" in msg
    assert "Acessar" in msg


def test_failure_alert_lists_the_broken_scrapers():
    msg = build_failure_alert(3, ["PCIScraper: timeout", "G1Scraper: 500"])

    assert "falha total" in msg.lower()
    assert "PCIScraper: timeout" in msg
    assert "3 scrapers" in msg
