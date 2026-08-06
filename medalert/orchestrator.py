"""Orquestração da execução: roda todos os scrapers, persiste, notifica e gera relatórios."""
import logging
from typing import Dict, List, Optional

from medalert.config import TARGET_CITIES, load_admin_chat_id, load_telegram_bot_token
from medalert.enrich import read_deadline
from medalert.messages import (
    build_failure_alert,
    build_job_message,
    build_job_message_from_parts,
)
from medalert.notify import DeliveryResult, TelegramNotifier, is_resolved
from medalert.subscribers import Subscriber, fetch_subscribers, prune_blocked
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
from medalert.taxonomy import (
    CRONOGRAMA,
    ESTADUAL_NACIONAL,
    NAO_ESPECIFICADA,
    NOTICIA,
    can_suppress_alert,
    classify_job_type,
    classify_region,
    classify_specialties,
    status_from_deadline,
)
from medalert.timeutil import today_str

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


#: Teto de editais lidos por execução.
#:
#: Ler um edital custa um download de algumas centenas de KB mais o parsing do
#: PDF. Num dia normal aparecem poucas vagas novas e o teto nunca é atingido;
#: ele existe para o caso de acúmulo — ao ligar uma fonte nova entram dezenas
#: de vagas de uma vez, e baixar todas na mesma rodada estouraria o
#: `timeout-minutes` do workflow. O excedente é lido nas rodadas seguintes.
MAX_ENRICHMENTS_PER_RUN = 25


def _vale_ler_o_edital(job: Dict, job_type: str) -> bool:
    """Se compensa baixar o documento desta vaga para procurar o prazo.

    Dois recortes, ambos por precisão e não por economia:

    * Só PDF. É o que a extração de prazo foi medida contra (ver
      tests/fixtures/prazos.json); ligá-la em páginas HTML sem ter medido
      seria estender a promessa de "erro zero" para terreno não testado.
    * Notícia nunca. Uma matéria sobre concurso costuma citar datas de
      editais anteriores ou previsões, e ler ali produziria prazo errado com
      aparência de certo — além de poder marcar como "encerrada" justamente a
      pista antecipada que dá valor ao radar.
    """
    return job["link"].lower().endswith(".pdf") and job_type != NOTICIA


def _read_deadline(job: Dict, scraper: BaseScraper) -> Optional[str]:
    """Prazo lido do edital da vaga, ou None.

    Isolado num try porque enriquecimento é bônus, não pré-requisito: PDF
    corrompido, site fora do ar ou layout novo não podem impedir a vaga de
    entrar no banco e aparecer no site.
    """
    try:
        return read_deadline(job["link"], scraper.scraper)
    except Exception as e:
        logging.info(f"📄 Falha ao ler o edital de {job['link']}: {e}")
        return None


def _resolve_specialties(job: Dict) -> List[str]:
    """Especialidades da vaga: a fonte manda, o título cobre o resto.

    Quando o scraper traz a informação, ela é sempre melhor — o MedGrupo é
    perguntado no catálogo dele próprio, e o RioSaúde PSS conhece a lista
    completa de cargos, da qual o título mostra só os três primeiros.

    Lista vazia é resposta legítima, não falha: 72% das vagas não dizem a
    especialidade em lugar nenhum legível. Elas passam a valer por
    NAO_ESPECIFICADA, que é uma opção de assinatura marcada por padrão.
    """
    da_fonte = job.get("specialties")
    if da_fonte:
        return list(da_fonte)
    return classify_specialties(job["title"])


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


def _deliver_pending(
    db: DatabaseManager,
    notifier: TelegramNotifier,
    subscribers: List[Subscriber],
    blocked: List[str],
) -> int:
    """Envia a cada assinante o que ainda não foi entregue a ele.

    Cobre a falha transitória do Telegram e o excedente do teto por rodada.
    Fica no começo da execução de propósito: se o envio falhou agora há pouco,
    tentar de novo na mesma rodada falharia igual — o que se quer recuperar é
    a falha da rodada anterior.

    A consulta já respeita as preferências do assinante, então cada um só vê
    pendência do que ele de fato quer receber.
    """
    messages_sent = 0
    for sub in subscribers:
        if sub.wants_nothing():
            continue

        pending = db.fetch_undelivered(
            sub.chat_id,
            limit=MAX_NEW_NOTIFICATIONS_PER_RUN,
            regions=sub.regions,
            job_types=sub.job_types,
            specialties=sub.specialties,
        )
        if not pending:
            continue

        logging.info(f"↻ {len(pending)} pendência(s) para o chat {sub.chat_id}.")
        for job in pending:
            # A vaga pode ter fechado enquanto esperava na fila. Resolvemos
            # sem enviar: deixá-la pendente faria o robô reoferecer para
            # sempre um prazo que já passou.
            if can_suppress_alert(job.status, job.status_source):
                db.record_delivery(job.link, sub.chat_id)
                continue

            resultado = notifier.send_to(sub.chat_id, build_job_message(job))
            if resultado is DeliveryResult.BLOCKED:
                blocked.append(sub.chat_id)
                break  # não insiste com quem bloqueou
            if is_resolved(resultado):
                db.record_delivery(job.link, sub.chat_id)
                if resultado is DeliveryResult.SENT:
                    messages_sent += 1

    return messages_sent


def _wants(sub: Subscriber, region: str, job_type: str, specialties: List[str]) -> bool:
    """Se esta vaga é o que a pessoa pediu.

    Região e tipo são igualdade; especialidade é interseção, porque uma vaga
    tem várias. Vaga sem especialidade reconhecida vale por NAO_ESPECIFICADA —
    uma chave normal, marcada por padrão —, então quem não mexeu em nada
    continua recebendo tudo, e a regra segue sendo interseção pura.
    """
    if region not in sub.regions or job_type not in sub.job_types:
        return False
    da_vaga = specialties or [NAO_ESPECIFICADA]
    return any(e in sub.specialties for e in da_vaga)


def run(
    scrapers: Optional[List[BaseScraper]] = None,
    db: Optional[DatabaseManager] = None,
    notifier: Optional[TelegramNotifier] = None,
    subscribers: Optional[List[Subscriber]] = None,
) -> int:
    """Executa uma rodada completa. Retorna 0 em sucesso (ou falha parcial),
    1 se TODOS os scrapers falharem — isso faz a execução do GitHub Actions
    ficar vermelha em vez de sair silenciosamente com código 0, e avisa quem
    mantém o robô.

    Os parâmetros são injetáveis (todos opcionais, com os valores reais como
    padrão) só para permitir testar sem rede nem banco de produção —
    main.py sempre chama run() sem argumentos.
    """
    scrapers = scrapers if scrapers is not None else build_scrapers()
    # Só fechamos a conexão se fomos nós que a abrimos — fechar um recurso
    # injetado por quem chamou deixaria o objeto do chamador inutilizável.
    owns_db = db is None
    db = db if db is not None else DatabaseManager()
    if notifier is None:
        notifier = TelegramNotifier(bot_token=load_telegram_bot_token())
    if subscribers is None:
        subscribers = fetch_subscribers()

    # None é diferente de lista vazia: vazia significa "ninguém se cadastrou"
    # e a rodada segue normalmente; None significa "não consegui saber quem
    # são". Nesse caso a coleta continua (o site é atualizado), mas nenhuma
    # entrega é registrada — as vagas ficam pendentes e saem na próxima
    # rodada, em vez de serem dadas como entregues para ninguém.
    subscribers_unknown = subscribers is None
    if subscribers_unknown:
        logging.error("⚠️ Lista de assinantes indisponível: coletando sem notificar ninguém.")
        subscribers = []

    logging.info(f"Starting MedAlert RJ Scraper Engine... ({len(subscribers)} assinante(s))")

    new_jobs_count = 0
    scraper_failures: List[str] = []  # "Classe: erro" — detalhe para o aviso ao admin
    failed_labels: List[str] = []  # só o nome da fonte — para o painel público de saúde
    blocked: List[str] = []  # quem bloqueou o bot nesta rodada
    # Alertas de vaga nova já enviados nesta rodada, por destinatário.
    sent_per_chat: Dict[str, int] = {sub.chat_id: 0 for sub in subscribers}
    chat_ids = [sub.chat_id for sub in subscribers]

    # Banco vazio significa primeira execução (ou reset deliberado): o acervo
    # inteiro seria "novo" e viraria dias de alertas irrelevantes. Nesse caso
    # a rodada apenas popula, sem notificar ninguém.
    bootstrap = db.count_jobs() == 0
    if bootstrap:
        logging.info("🌱 Banco vazio: rodada de bootstrap — vagas serão salvas SEM notificar.")

    # Só vale ler o edital de vaga que ainda não conhecemos: o prazo de uma
    # vaga já registrada não muda (e, se mudar por retificação, o link do PDF
    # muda junto e ela entra como nova).
    conhecidos = set(db.fetch_all_links())
    enriquecimentos = 0
    sem_prazo: List[str] = []  # editais lidos em que o prazo não foi identificado

    _backfill_legacy_deliveries(db, chat_ids)
    messages_sent = 0 if subscribers_unknown else _deliver_pending(db, notifier, subscribers, blocked)

    # Loop de execução blindado
    for scraper in scrapers:
        try:
            jobs = scraper.scrape()

            for job in jobs:
                job_kind = classify_job_type(job["title"], scraper.job_type)
                job_specialties = _resolve_specialties(job)

                # A fonte manda: quando ela própria declara a situação (o
                # MedGrupo marca "encerradas"), não há por que baixar o PDF
                # para confirmar o que já foi afirmado.
                if (
                    not job.get("status_source")
                    and job["link"] not in conhecidos
                    and enriquecimentos < MAX_ENRICHMENTS_PER_RUN
                    and _vale_ler_o_edital(job, job_kind)
                ):
                    enriquecimentos += 1
                    prazo = _read_deadline(job, scraper)
                    if prazo:
                        job["deadline"] = prazo
                        job["status"] = status_from_deadline(prazo, today_str())
                        job["status_source"] = CRONOGRAMA
                    else:
                        # Registrado um a um de propósito. É esta lista, no log
                        # da execução, que diz quais redações o extrator ainda
                        # não sabe ler — e é assim que os padrões novos saem de
                        # casos reais em vez de saírem de suposição.
                        sem_prazo.append(job["title"])

                is_new = db.insert_job(
                    title=job["title"],
                    link=job["link"],
                    pub_date=job["pub_date"],
                    job_type=job_kind,
                    region=_resolve_region(job["title"], scraper.region),
                    source_url=job.get("source_url"),
                    status=job.get("status"),
                    deadline=job.get("deadline"),
                    status_source=job.get("status_source"),
                    specialties=_resolve_specialties(job),
                )

                if is_new:
                    new_jobs_count += 1
                    logging.info(f"🆕 NEW JOB SAVED: {job['title']}")

                    if bootstrap or subscribers_unknown:
                        if bootstrap:
                            db.record_deliveries_for_all(job["link"], chat_ids)
                        continue

                    # Descoberta hoje, mas com as inscrições já encerradas
                    # segundo a própria fonte. Anunciá-la como "nova
                    # oportunidade" seria falso: o prazo acabou antes de a
                    # gente saber que ela existia. Fica registrada para o site
                    # e resolvida para não voltar pela fila de pendentes.
                    if can_suppress_alert(job.get("status"), job.get("status_source")):
                        logging.info(f"⏱ Inscrições já encerradas, alerta suprimido: {job['title']}")
                        db.record_deliveries_for_all(job["link"], chat_ids)
                        continue

                    # O mesmo edital costuma aparecer em duas fontes (ex: IBAM
                    # e o portal da prefeitura) com links diferentes, então o
                    # UNIQUE(link) não pega. A vaga fica registrada, mas o
                    # alerta é suprimido para não notificar duas vezes.
                    if db.find_duplicate_link(job["link"]):
                        logging.info(f"🔁 Duplicata de outra fonte, alerta suprimido: {job['title']}")
                        db.record_deliveries_for_all(job["link"], chat_ids)
                        continue

                    job_region = _resolve_region(job["title"], scraper.region)
                    message = build_job_message_from_parts(
                        title=job["title"],
                        discovered_at=job["pub_date"],
                        link=job["link"],
                        job_type=job_kind,
                        region=job_region,
                        source_url=job.get("source_url"),
                        deadline=job.get("deadline"),
                        status_source=job.get("status_source"),
                        specialties=job_specialties,
                    )

                    for sub in subscribers:
                        if not _wants(sub, job_region, job_kind, job_specialties):
                            # Não é o que essa pessoa pediu. Não registramos
                            # entrega: se ela mudar as preferências depois, a
                            # vaga ainda pode alcançá-la pela fila de pendentes.
                            continue
                        if sent_per_chat[sub.chat_id] >= MAX_NEW_NOTIFICATIONS_PER_RUN:
                            # Fica pendente de propósito: sai nas próximas
                            # rodadas, sem atropelar o destinatário agora.
                            continue

                        resultado = notifier.send_to(sub.chat_id, message)
                        if resultado is DeliveryResult.BLOCKED:
                            blocked.append(sub.chat_id)
                            continue
                        if is_resolved(resultado):
                            db.record_delivery(job["link"], sub.chat_id)
                            if resultado is DeliveryResult.SENT:
                                messages_sent += 1
                                sent_per_chat[sub.chat_id] += 1
                else:
                    # Vaga já conhecida: registra que ela ainda está sendo
                    # encontrada e reavalia a situação das inscrições, que
                    # muda com o tempo (ver DatabaseManager.touch_seen).
                    db.touch_seen(
                        job["link"],
                        status=job.get("status"),
                        deadline=job.get("deadline"),
                        status_source=job.get("status_source"),
                        specialties=job_specialties,
                    )

        except Exception as e:
            # Se UM scraper explodir (ex: site fora do ar, erro 500), ele avisa no log mas continua para o próximo!
            nome_scraper = scraper.__class__.__name__
            logging.error(f"❌ Erro crítico ao executar {nome_scraper}: {e}. Pulando para o próximo.")
            scraper_failures.append(f"{nome_scraper}: {e}")
            failed_labels.append(scraper.label)
            continue

    logging.info(f"Execution finished. {new_jobs_count} new jobs added. {messages_sent} Telegram alerts sent.")

    if sem_prazo:
        # Esta lista é a fila de trabalho da extração de prazo: cada item é
        # uma redação que o extrator encontrou no mundo real e não soube ler.
        # Escrever padrão a partir daqui é diferente de escrever a partir de
        # suposição sobre como um edital "costuma" ser.
        logging.info(f"⏳ {len(sem_prazo)} edital(is) lido(s) sem prazo identificado:")
        for titulo in sem_prazo:
            logging.info(f"    · {titulo}")

    # Os artefatos são regenerados em TODA execução, de propósito.
    #
    # Já houve uma tentativa de só regenerar quando `new_jobs_count > 0`, para
    # o guard de commit do workflow ("só commita se algo mudou") poder pular
    # rodadas vazias. Isso não funciona: `touch_seen()` grava no banco
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

    # Quem bloqueou o bot sai da lista: insistir gastaria uma chamada de API
    # por rodada, para sempre, com alguém que nunca mais vai receber.
    if blocked:
        removidos = prune_blocked(sorted(set(blocked)))
        logging.info(f"🚫 {removidos} assinante(s) removido(s) por terem bloqueado o bot.")

    if scrapers and len(scraper_failures) == len(scrapers):
        logging.error("❌ Todos os scrapers falharam nesta execução.")
        # Vai só para quem mantém o robô: falha de scraper é problema de
        # operação, não notícia para quem se inscreveu para saber de vaga.
        notifier.send_admin(
            load_admin_chat_id(),
            build_failure_alert(len(scrapers), scraper_failures),
        )
        return 1

    return 0
