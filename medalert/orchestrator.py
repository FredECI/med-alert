"""Orquestração da execução: roda todos os scrapers, persiste, notifica e gera relatórios."""
import logging
from typing import List

from medalert.config import TARGET_CITIES, load_telegram_bot_token, load_telegram_chat_ids
from medalert.notify import TelegramNotifier
from medalert.report import ReportGenerator
from medalert.scrapers.base import BaseScraper
from medalert.scrapers.macae import MacaeGovScraper
from medalert.scrapers.news import BingNewsScraper, G1Scraper, GoogleNewsScraper
from medalert.scrapers.pci import PCIEstadualScraper, PCIScraper, PCISaudeScraper
from medalert.scrapers.portals import JCConcursosScraper, PandaPeUnimedScraper, TrabalhaBrasilScraper
from medalert.storage import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)


def build_scrapers() -> List[BaseScraper]:
    return [
        PCIScraper(),
        GoogleNewsScraper(),
        G1Scraper(),
        PCISaudeScraper(),
        JCConcursosScraper(),
        PCIEstadualScraper(),
        BingNewsScraper(),
        TrabalhaBrasilScraper(cities=TARGET_CITIES),
        PandaPeUnimedScraper(),
        MacaeGovScraper(),
    ]


def run() -> int:
    """Executa uma rodada completa. Retorna 0 em sucesso (ou falha parcial),
    1 se TODOS os scrapers falharem — isso faz a execução do GitHub Actions
    ficar vermelha em vez de sair silenciosamente com código 0."""
    telegram_bot_token = load_telegram_bot_token()
    telegram_chat_ids = load_telegram_chat_ids()

    logging.info(f"Starting MedAlert RJ Scraper Engine... (Broadcasting to {len(telegram_chat_ids)} chats)")

    db = DatabaseManager()
    notifier = TelegramNotifier(bot_token=telegram_bot_token, chat_ids=telegram_chat_ids)
    scrapers = build_scrapers()

    new_jobs_count = 0
    messages_sent = 0
    scrapers_failed = 0

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
            scrapers_failed += 1
            continue

    logging.info(f"Execution finished. {new_jobs_count} new jobs added. {messages_sent} Telegram alerts sent.")

    # --- GERAR RELATÓRIOS APÓS A VARREDURA ---
    reporter = ReportGenerator(db_manager=db)
    reporter.generate_csv()
    reporter.generate_markdown()
    # -----------------------------------------

    db.close()

    if scrapers and scrapers_failed == len(scrapers):
        logging.error("❌ Todos os scrapers falharam nesta execução.")
        return 1

    return 0
