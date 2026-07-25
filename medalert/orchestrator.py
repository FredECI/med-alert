"""Orquestração da execução: roda todos os scrapers, persiste, notifica e gera relatórios."""
import logging
from typing import List, Optional

from medalert.config import TARGET_CITIES, load_telegram_bot_token, load_telegram_chat_ids
from medalert.notify import TelegramNotifier
from medalert.report import ReportGenerator, write_robot_status
from medalert.scrapers.aggregators import ConcursosNoBrasilScraper, InfoJobsScraper
from medalert.scrapers.base import BaseScraper
from medalert.scrapers.health_institutions import FiotecScraper, IbamScraper, RioSaudeScraper
from medalert.scrapers.macae import MacaeGovScraper
from medalert.scrapers.municipal_gov import (
    AraruamaGovScraper,
    CaboFrioSaudeScraper,
    CasimiroDeAbreuGovScraper,
    RioDasOstrasConcursoScraper,
    RioDasOstrasNoticiasScraper,
    SaquaremaGovScraper,
)
from medalert.scrapers.news import BingNewsScraper, G1Scraper, GoogleNewsScraper
from medalert.scrapers.pci import build_pci_scrapers
from medalert.scrapers.portals import JCConcursosScraper, PandaPeUnimedScraper, TrabalhaBrasilScraper
from medalert.storage import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)


def build_scrapers() -> List[BaseScraper]:
    return [
        *build_pci_scrapers(),
        GoogleNewsScraper(),
        G1Scraper(),
        JCConcursosScraper(),
        BingNewsScraper(),
        TrabalhaBrasilScraper(cities=TARGET_CITIES),
        PandaPeUnimedScraper(),
        MacaeGovScraper(),
        # Fase 4 — instituições de saúde
        RioSaudeScraper(),
        FiotecScraper(),
        IbamScraper(),
        # Fase 4 — prefeituras/portais municipais
        AraruamaGovScraper(),
        CaboFrioSaudeScraper(),
        RioDasOstrasNoticiasScraper(),
        RioDasOstrasConcursoScraper(),
        SaquaremaGovScraper(),
        CasimiroDeAbreuGovScraper(),
        # Fase 4 — agregadores
        InfoJobsScraper(),
        ConcursosNoBrasilScraper(),
        # DomRjScraper() está DESATIVADO de propósito — ver a docstring da
        # classe em medalert/scrapers/aggregators.py. Resumo: a busca do
        # Diário Oficial devolve fragmentos de página, não vagas, e casa com
        # ~200-400 páginas por semana independente do termo buscado.
        # Mantido no código para poder ser reaproveitado se um dia houver um
        # endpoint por edital em vez de busca full-text.
    ]


def _build_job_message(title: str, pub_date: str, link: str) -> str:
    return (
        f"🚨 *Nova Oportunidade/Processo Encontrado!*\n\n"
        f"🏥 *Vaga:* {title}\n"
        f"📅 *Data:* {pub_date}\n\n"
        f"🔗 [Clique aqui para acessar]({link})"
    )


def _retry_pending_notifications(db: DatabaseManager, notifier: TelegramNotifier) -> int:
    """Reenvia alertas de vagas que ficaram com is_sent = 0.

    Cobre a falha transitória do Telegram (rede, rate limit, indisponibilidade):
    antes disso a vaga entrava no banco, o envio falhava e o alerta era perdido
    de vez, porque nas rodadas seguintes o link já existia e a vaga deixava de
    ser "nova". Duplicatas entre fontes são puladas aqui também — elas são
    marcadas como enviadas justamente para não entrarem nesta fila.
    """
    pending = db.fetch_pending_notifications()
    if not pending:
        return 0

    logging.info(f"↻ {len(pending)} notificação(ões) pendente(s) de execuções anteriores.")
    messages_sent = 0
    for job in pending:
        sends = notifier.send_message(_build_job_message(job.title, job.discovered_at, job.link))
        if sends > 0:
            db.mark_as_sent(job.link)
            messages_sent += sends

    return messages_sent


def run(
    scrapers: Optional[List[BaseScraper]] = None,
    db: Optional[DatabaseManager] = None,
    notifier: Optional[TelegramNotifier] = None,
) -> int:
    """Executa uma rodada completa. Retorna 0 em sucesso (ou falha parcial),
    1 se TODOS os scrapers falharem — isso faz a execução do GitHub Actions
    ficar vermelha em vez de sair silenciosamente com código 0, e dispara um
    alerta no Telegram resumindo o que falhou.

    Os parâmetros são injetáveis (todos opcionais, com os valores reais como
    padrão) só para permitir testar o comportamento de falha total sem rede
    nem banco de produção — main.py sempre chama run() sem argumentos.
    """
    scrapers = scrapers if scrapers is not None else build_scrapers()
    # Só fechamos a conexão se fomos nós que a abrimos — fechar um recurso
    # injetado por quem chamou deixaria o objeto do chamador inutilizável.
    owns_db = db is None
    db = db if db is not None else DatabaseManager()
    if notifier is None:
        notifier = TelegramNotifier(bot_token=load_telegram_bot_token(), chat_ids=load_telegram_chat_ids())

    logging.info(f"Starting MedAlert RJ Scraper Engine... (Broadcasting to {len(notifier.chat_ids)} chats)")

    new_jobs_count = 0
    scraper_failures: List[str] = []  # "Classe: erro" — detalhe para o alerta do Telegram
    failed_labels: List[str] = []  # só o nome da fonte — para o painel público de saúde

    # Reenvia o que ficou para trás em execuções anteriores ANTES de raspar.
    # Fica no começo de propósito: se o envio falhou agora há pouco, tentar
    # de novo na mesma rodada provavelmente falharia igual — o que se quer
    # recuperar é a falha transitória da rodada passada.
    messages_sent = _retry_pending_notifications(db, notifier)

    # Loop de execução blindado
    for scraper in scrapers:
        try:
            jobs = scraper.scrape()

            for job in jobs:
                is_new = db.insert_job(
                    title=job["title"],
                    link=job["link"],
                    pub_date=job["pub_date"],
                )

                if is_new:
                    new_jobs_count += 1
                    logging.info(f"🆕 NEW JOB SAVED: {job['title']}")

                    # O mesmo edital costuma aparecer em duas fontes (ex: IBAM
                    # e o portal da prefeitura) com links diferentes, então o
                    # UNIQUE(link) não pega. A vaga fica registrada, mas o
                    # alerta é suprimido para não notificar duas vezes.
                    duplicate_of = db.find_duplicate_link(job["link"])
                    if duplicate_of:
                        logging.info(f"🔁 Duplicata de outra fonte, alerta suprimido: {job['title']}")
                        db.mark_as_sent(job["link"])
                        continue

                    sends = notifier.send_message(_build_job_message(job["title"], job["pub_date"], job["link"]))
                    if sends > 0:
                        db.mark_as_sent(job["link"])
                        messages_sent += sends
                else:
                    # Vaga já conhecida: registra que ela ainda está sendo encontrada.
                    db.touch_last_seen(job["link"])

        except Exception as e:
            # Se UM scraper explodir (ex: site fora do ar, erro 500), ele avisa no log mas continua para o próximo!
            nome_scraper = scraper.__class__.__name__
            logging.error(f"❌ Erro crítico ao executar {nome_scraper}: {e}. Pulando para o próximo.")
            scraper_failures.append(f"{nome_scraper}: {e}")
            failed_labels.append(scraper.label)
            continue

    logging.info(f"Execution finished. {new_jobs_count} new jobs added. {messages_sent} Telegram alerts sent.")

    # Os artefatos são regenerados em TODA execução, de propósito.
    #
    # Já houve uma tentativa de só regenerar quando `new_jobs_count > 0`, para
    # o guard de commit do workflow ("só commita se algo mudou") poder pular
    # rodadas vazias. Isso não funciona: `touch_last_seen()` grava no banco
    # sempre que uma vaga conhecida é reencontrada, então `med_alerts.db` muda
    # em toda rodada e o commit acontece de qualquer jeito. O efeito líquido
    # era só deixar o site desatualizado — o "visto pela última vez em X"
    # exibido ficava congelado na data da última vaga nova, enquanto o banco
    # já tinha o valor atual.
    reporter = ReportGenerator(db_manager=db)
    reporter.generate_csv()
    reporter.generate_jobs_data()
    write_robot_status(scrapers_total=len(scrapers), failed_labels=failed_labels)

    if owns_db:
        db.close()

    if scrapers and len(scraper_failures) == len(scrapers):
        logging.error("❌ Todos os scrapers falharam nesta execução.")
        failure_summary = "\n".join(f"• {failure}" for failure in scraper_failures)
        alert_msg = (
            f"🔴 *MedAlert: falha total na execução*\n\n"
            f"Todos os {len(scrapers)} scrapers falharam nesta rodada:\n\n"
            f"{failure_summary}"
        )
        notifier.send_message(alert_msg)
        return 1

    return 0
