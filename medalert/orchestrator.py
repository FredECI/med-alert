"""Orquestração da execução: roda todos os scrapers, persiste, notifica e gera relatórios."""
import logging
from typing import List, Optional

from medalert.config import TARGET_CITIES, load_telegram_bot_token, load_telegram_chat_ids
from medalert.notify import TelegramNotifier
from medalert.report import ReportGenerator, write_robot_status
from medalert.scrapers.base import BaseScraper
from medalert.scrapers.macae import MacaeGovScraper
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
    ]


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
    db = db if db is not None else DatabaseManager()
    if notifier is None:
        notifier = TelegramNotifier(bot_token=load_telegram_bot_token(), chat_ids=load_telegram_chat_ids())

    logging.info(f"Starting MedAlert RJ Scraper Engine... (Broadcasting to {len(notifier.chat_ids)} chats)")

    new_jobs_count = 0
    messages_sent = 0
    scraper_failures: List[str] = []  # "Classe: erro" — detalhe para o alerta do Telegram
    failed_labels: List[str] = []  # só o nome da fonte — para o painel público de saúde

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

                    msg = (
                        f"🚨 *Nova Oportunidade/Processo Encontrado!*\n\n"
                        f"🏥 *Vaga:* {job['title']}\n"
                        f"📅 *Data:* {job['pub_date']}\n\n"
                        f"🔗 [Clique aqui para acessar]({job['link']})"
                    )

                    sends = notifier.send_message(msg)
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

    # Só regenera CSV/jobs.json quando há vaga nova de verdade — gerar sempre
    # (mesmo sem novidade) fazia o index.md mudar todo run só por um timestamp
    # cosmético, o que anulava o guard de commit do workflow ("só commita se
    # algo mudou"). O status do robô abaixo é diferente: "rodou e está
    # saudável" é informação nova mesmo sem vaga nova, por isso é sempre escrito.
    if new_jobs_count > 0:
        reporter = ReportGenerator(db_manager=db)
        reporter.generate_csv()
        reporter.generate_jobs_data()
    else:
        logging.info("Nenhuma vaga nova — CSV e jobs.json não foram regenerados nesta rodada.")

    write_robot_status(scrapers_total=len(scrapers), failed_labels=failed_labels)

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
