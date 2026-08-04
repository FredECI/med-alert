"""Orquestração da execução: roda todos os scrapers, persiste, notifica e gera relatórios."""
import logging
from typing import Dict, List, Optional

from medalert.config import TARGET_CITIES, load_telegram_bot_token, load_telegram_chat_ids
from medalert.notify import TelegramNotifier
from medalert.report import ReportGenerator, write_robot_status
from medalert.scrapers.aggregators import ConcursosNoBrasilScraper, InfoJobsScraper
from medalert.scrapers.base import BaseScraper
from medalert.scrapers.health_institutions import (
    FiotecScraper,
    IbamScraper,
    RioSaudePssScraper,
    RioSaudeScraper,
)
from medalert.scrapers.medical_portals import (
    EstrategiaSaudeScraper,
    FgvConcursosScraper,
    MedGrupoScraper,
)
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
from medalert.taxonomy import ESTADUAL_NACIONAL, classify_job_type, classify_region

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
        RioSaudePssScraper(),
        FiotecScraper(),
        IbamScraper(),
        # Fase 5 — portais médicos e bancas
        MedGrupoScraper(),
        EstrategiaSaudeScraper(),
        FgvConcursosScraper(),
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


#: Teto de alertas de vaga NOVA por execução, POR DESTINATÁRIO.
#:
#: Num dia normal aparecem de zero a poucas vagas, então este limite nunca é
#: atingido. Ele existe para o caso de acúmulo: ao ligar uma fonte nova, todo
#: o acervo dela entra de uma vez (ao acrescentar o MedGrupo, foram 66 vagas
#: numa rodada só) — e 66 mensagens seguidas atropelariam o usuário e o
#: rate limit do Telegram. O excedente fica sem registro de entrega e sai aos
#: poucos nas rodadas seguintes, pela mesma fila que cobre falha transitória.
#: Nada se perde: no site as vagas aparecem todas de imediato.
MAX_NEW_NOTIFICATIONS_PER_RUN = 15


def _build_job_message(title: str, pub_date: str, link: str) -> str:
    return (
        f"🚨 *Nova Oportunidade/Processo Encontrado!*\n\n"
        f"🏥 *Vaga:* {title}\n"
        f"📅 *Data:* {pub_date}\n\n"
        f"🔗 [Clique aqui para acessar]({link})"
    )


def _resolve_region(title: str, scraper_region: Optional[str]) -> str:
    """Região da vaga: o texto manda, a fonte cobre o que ele não disser.

    Deduzir só do título erra muito na prática, porque o título costuma trazer
    o nome da instituição e não o do município ("ASSOCIAÇÃO FLUMINENSE DE
    ASSISTÊNCIA..."). Quando a fonte é geograficamente específica — o portal
    de uma prefeitura — ela sabe a resposta que faltou.
    """
    do_texto = classify_region(title)
    if do_texto == ESTADUAL_NACIONAL and scraper_region:
        return scraper_region
    return do_texto


def _backfill_legacy_deliveries(db: DatabaseManager, chat_ids: List[str]) -> None:
    """Marca o acervo pré-existente como já entregue, uma única vez.

    Protege quem atualiza o código SEM zerar o banco: sem isso, ao estrear a
    tabela de entregas nenhuma vaga teria registro e a fila de reenvio trataria
    todo o histórico recente como não enviado, disparando uma enxurrada de
    alertas repetidos.
    """
    if db.count_deliveries() > 0 or db.count_jobs() == 0:
        return

    links = db.fetch_all_links()
    logging.info(f"↩ Estreando a tabela de entregas: marcando {len(links)} vaga(s) já existentes como entregues.")
    for link in links:
        db.record_deliveries_for_all(link, chat_ids)


def _deliver_pending(db: DatabaseManager, notifier: TelegramNotifier) -> int:
    """Envia, para cada destinatário, o que ainda não foi entregue a ele.

    Cobre a falha transitória do Telegram e o excedente do teto por rodada.
    Fica no começo da execução de propósito: se o envio falhou agora há pouco,
    tentar de novo na mesma rodada falharia igual — o que se quer recuperar é
    a falha da rodada anterior.
    """
    messages_sent = 0
    for chat_id in notifier.chat_ids:
        pending = db.fetch_undelivered(chat_id, limit=MAX_NEW_NOTIFICATIONS_PER_RUN)
        if not pending:
            continue

        logging.info(f"↻ {len(pending)} pendência(s) para o chat {chat_id}.")
        for job in pending:
            if notifier.send_to(chat_id, _build_job_message(job.title, job.discovered_at, job.link)):
                db.record_delivery(job.link, chat_id)
                messages_sent += 1

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
    # Alertas de vaga nova já enviados nesta rodada, por destinatário.
    sent_per_chat: Dict[str, int] = {chat_id: 0 for chat_id in notifier.chat_ids}

    # Banco vazio significa primeira execução (ou reset deliberado): o acervo
    # inteiro seria "novo" e viraria dias de alertas irrelevantes. Nesse caso
    # a rodada apenas popula, sem notificar ninguém.
    bootstrap = db.count_jobs() == 0
    if bootstrap:
        logging.info("🌱 Banco vazio: rodada de bootstrap — vagas serão salvas SEM notificar.")

    _backfill_legacy_deliveries(db, notifier.chat_ids)
    messages_sent = _deliver_pending(db, notifier)

    # Loop de execução blindado
    for scraper in scrapers:
        try:
            jobs = scraper.scrape()

            for job in jobs:
                is_new = db.insert_job(
                    title=job["title"],
                    link=job["link"],
                    pub_date=job["pub_date"],
                    job_type=classify_job_type(job["title"], scraper.job_type),
                    region=_resolve_region(job["title"], scraper.region),
                )

                if is_new:
                    new_jobs_count += 1
                    logging.info(f"🆕 NEW JOB SAVED: {job['title']}")

                    if bootstrap:
                        db.record_deliveries_for_all(job["link"], notifier.chat_ids)
                        continue

                    # O mesmo edital costuma aparecer em duas fontes (ex: IBAM
                    # e o portal da prefeitura) com links diferentes, então o
                    # UNIQUE(link) não pega. A vaga fica registrada, mas o
                    # alerta é suprimido para não notificar duas vezes.
                    if db.find_duplicate_link(job["link"]):
                        logging.info(f"🔁 Duplicata de outra fonte, alerta suprimido: {job['title']}")
                        db.record_deliveries_for_all(job["link"], notifier.chat_ids)
                        continue

                    message = _build_job_message(job["title"], job["pub_date"], job["link"])
                    for chat_id in notifier.chat_ids:
                        if sent_per_chat[chat_id] >= MAX_NEW_NOTIFICATIONS_PER_RUN:
                            # Fica pendente de propósito: sai nas próximas
                            # rodadas, sem atropelar o destinatário agora.
                            continue
                        if notifier.send_to(chat_id, message):
                            db.record_delivery(job["link"], chat_id)
                            messages_sent += 1
                            sent_per_chat[chat_id] += 1
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
