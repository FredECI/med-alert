"""Testes do orquestrador: código de saída, alerta de Telegram em falha total
e o status do robô escrito a cada execução.

scrapers/db/notifier são injetáveis exatamente para permitir estes testes sem
rede real nem tocar no banco de produção — main.py sempre chama run() sem
argumentos, usando os valores reais. run() escreve _data/robot_status.json
(caminho relativo) sempre que roda, então cada teste muda o diretório de
trabalho para um tmp_path — sem isso, rodar a suíte a partir da raiz do repo
escreveria esse arquivo dentro do repo de verdade.
"""
import json
import os

import pytest

from medalert.notify import DeliveryResult
from medalert.orchestrator import MAX_NEW_NOTIFICATIONS_PER_RUN, run
from medalert.scrapers.base import BaseScraper
from medalert.storage import DatabaseManager
from medalert.subscribers import Subscriber
from medalert.taxonomy import JOB_TYPES, REGIONS
from medalert.timeutil import now_brt


def _today() -> str:
    """Vagas precisam ser recentes para caírem na janela de reenvio."""
    return now_brt().strftime("%Y-%m-%d")


@pytest.fixture(autouse=True)
def _isolate_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


def _assinante(chat_id="123", regions=None, job_types=None) -> Subscriber:
    """Assinante que aceita tudo, salvo quando o teste restringe de propósito."""
    return Subscriber(
        chat_id=chat_id,
        regions=list(REGIONS) if regions is None else regions,
        job_types=list(JOB_TYPES) if job_types is None else job_types,
    )


@pytest.fixture(autouse=True)
def _lista_de_assinantes(monkeypatch):
    """Evita que os testes batam no Worker real.

    Sem isso, fetch_subscribers() devolveria None (sem as variáveis de
    ambiente) e a rodada entraria no modo "não sei quem são" — que existe para
    proteger produção, mas mascararia todos os testes de notificação.
    """
    monkeypatch.setattr("medalert.orchestrator.fetch_subscribers", lambda: [_assinante()])


@pytest.fixture(autouse=True)
def _sistema_ja_rodou(db):
    """Tira o banco do estado "vazio".

    Banco vazio aciona o modo bootstrap, que popula sem notificar — o que
    mascararia todos os testes de notificacao. Uma vaga semente representa
    o estado real: um sistema que ja rodou antes.
    """
    db.insert_job("[Semente] execucao anterior", "https://example.com/semente", "2020-01-01")
    db.record_delivery("https://example.com/semente", "123")


class _FailingScraper(BaseScraper):
    url = "https://example.com/broken"

    def scrape(self):
        raise RuntimeError("site fora do ar")


class _EmptyScraper(BaseScraper):
    url = "https://example.com/ok"

    def scrape(self):
        return []


class _KnownJobScraper(BaseScraper):
    """Devolve sempre a mesma vaga — simula uma fonte cujo anúncio continua
    publicado, o caso que exercita touch_seen()."""
    url = "https://example.com/known"

    def scrape(self):
        return [{
            "title": "[T] Vaga conhecida",
            "link": "https://example.com/conhecida",
            "pub_date": "2026-01-01",
        }]


class _FakeNotifier:
    def __init__(self, resultado=DeliveryResult.SENT):
        self.sent = []
        self.resultado = resultado

    def send_to(self, chat_id, text):
        if self.resultado is DeliveryResult.SENT:
            self.sent.append((chat_id, text))
        return self.resultado

    def send_admin(self, admin_chat_id, text):
        self.sent.append((admin_chat_id or "admin", text))
        return True


def test_run_returns_zero_when_no_scrapers_fail(db):
    notifier = _FakeNotifier()

    exit_code = run(scrapers=[_EmptyScraper(), _EmptyScraper()], db=db, notifier=notifier)

    assert exit_code == 0
    assert notifier.sent == []


def test_run_returns_zero_when_only_some_scrapers_fail(db):
    notifier = _FakeNotifier()

    exit_code = run(scrapers=[_FailingScraper(), _EmptyScraper()], db=db, notifier=notifier)

    assert exit_code == 0
    assert notifier.sent == []


def test_run_returns_nonzero_and_alerts_when_all_scrapers_fail(db):
    notifier = _FakeNotifier()

    exit_code = run(scrapers=[_FailingScraper(), _FailingScraper()], db=db, notifier=notifier)

    assert exit_code == 1
    assert len(notifier.sent) == 1
    _, texto = notifier.sent[0]
    assert "falha total" in texto.lower()
    assert "site fora do ar" in texto


def test_run_does_not_alert_when_scrapers_list_is_empty(db):
    notifier = _FakeNotifier()

    exit_code = run(scrapers=[], db=db, notifier=notifier)

    assert exit_code == 0
    assert notifier.sent == []


def test_run_writes_robot_status_every_time_even_with_no_new_jobs(db):
    run(scrapers=[_EmptyScraper(), _FailingScraper()], db=db, notifier=_FakeNotifier())

    with open("_data/robot_status.json", encoding="utf-8") as f:
        status = json.load(f)

    assert status["scrapers_total"] == 2
    assert status["scrapers_ok"] == 1
    assert status["scrapers_failed"] == ["_FailingScraper"]


def test_run_regenerates_site_data_even_when_nothing_new(db):
    """Os artefatos são regerados em toda execução. Já se tentou pular isso
    quando não havia vaga nova, mas o banco muda mesmo assim (last_seen_at),
    então o commit acontecia do mesmo jeito e o único efeito era o site
    exibir um "visto pela última vez" congelado."""
    run(scrapers=[_EmptyScraper()], db=db, notifier=_FakeNotifier())

    assert os.path.exists("_data/jobs.json")
    assert os.path.exists("vagas_abertas.csv")
    with open("_data/robot_status.json", encoding="utf-8") as f:
        status = json.load(f)
    assert status["scrapers_ok"] == 1


class _FlakyNotifier:
    """Falha nas primeiras N tentativas — simula indisponibilidade transitória."""

    def __init__(self, failures):
        self.remaining_failures = failures
        self.sent = []

    def send_to(self, chat_id, text):
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            return DeliveryResult.RETRY
        self.sent.append((chat_id, text))
        return DeliveryResult.SENT

    def send_admin(self, admin_chat_id, text):
        self.sent.append((admin_chat_id or "admin", text))
        return True


class _NewJobScraper(BaseScraper):
    url = "https://example.com/new"

    def __init__(self, title, link):
        super().__init__()
        self._title = title
        self._link = link

    def scrape(self):
        return [{"title": self._title, "link": self._link, "pub_date": _today()}]


def test_failed_notification_is_retried_on_the_next_run(db):
    """Regressão do bug em que is_sent era gravado e nunca lido: uma falha
    transitória do Telegram perdia o alerta para sempre, porque na rodada
    seguinte o link já existia e a vaga deixava de ser 'nova'."""
    scraper = _NewJobScraper("[T] Vaga importante", "https://example.com/importante")
    notifier = _FlakyNotifier(failures=1)

    run(scrapers=[scraper], db=db, notifier=notifier)
    assert notifier.sent == []  # primeira tentativa falhou

    run(scrapers=[scraper], db=db, notifier=notifier)

    assert len(notifier.sent) == 1
    assert "Vaga importante" in notifier.sent[0][1]


def test_successful_notification_is_not_sent_twice(db):
    scraper = _NewJobScraper("[T] Vaga única", "https://example.com/unica")
    notifier = _FakeNotifier()

    run(scrapers=[scraper], db=db, notifier=notifier)
    run(scrapers=[scraper], db=db, notifier=notifier)

    assert len(notifier.sent) == 1


def test_same_edital_from_two_sources_alerts_only_once(db):
    """O mesmo processo seletivo publicado pelo IBAM e pela prefeitura tem
    links diferentes, então escapa do UNIQUE(link) — mas não deve gerar dois
    alertas para a mesma vaga."""
    ibam = _NewJobScraper(
        "[IBAM] Municipio de Saquarema - Ed. 04/2026 PS", "https://ibam.example/4"
    )
    prefeitura = _NewJobScraper(
        "[Saquarema] Concurso Público 004/2026 - Saúde", "https://saquarema.example/4"
    )
    notifier = _FakeNotifier()

    run(scrapers=[ibam, prefeitura], db=db, notifier=notifier)

    assert len(notifier.sent) == 1
    # as duas continuam registradas (+ a vaga semente do fixture)
    assert len(db.fetch_all_jobs()) == 3


def test_suppressed_duplicate_does_not_enter_the_retry_queue(db):
    """A duplicata é marcada como resolvida; se ficasse pendente, o reenvio
    da rodada seguinte mandaria justamente o alerta que se quis suprimir."""
    ibam = _NewJobScraper("[IBAM] Municipio de Araruama - Ed. 09/2026", "https://ibam.example/9")
    prefeitura = _NewJobScraper("[Araruama] Edital 009/2026 saúde", "https://araruama.example/9")
    notifier = _FakeNotifier()

    run(scrapers=[ibam, prefeitura], db=db, notifier=notifier)
    run(scrapers=[], db=db, notifier=notifier)

    assert len(notifier.sent) == 1


class _ManyJobsScraper(BaseScraper):
    """Simula ligar uma fonte nova: todo o acervo dela entra de uma vez."""
    url = "https://example.com/many"

    def __init__(self, quantidade):
        super().__init__()
        self.quantidade = quantidade

    def scrape(self):
        return [
            {"title": f"[T] Vaga {i}", "link": f"https://example.com/v{i}", "pub_date": _today()}
            for i in range(self.quantidade)
        ]


def test_new_job_alerts_are_capped_per_run(db):
    """Ao acrescentar o MedGrupo entraram 66 vagas numa rodada só — mandar
    tudo de uma vez atropelaria o usuário e o rate limit do Telegram."""
    notifier = _FakeNotifier()

    run(scrapers=[_ManyJobsScraper(40)], db=db, notifier=notifier)

    assert len(notifier.sent) == MAX_NEW_NOTIFICATIONS_PER_RUN
    # nada se perde: todas entram no site (+ a vaga semente do fixture)
    assert len(db.fetch_all_jobs()) == 41


def test_capped_alerts_are_delivered_by_later_runs(db):
    """O excedente fica pendente e sai aos poucos pela fila de reenvio."""
    notifier = _FakeNotifier()
    scraper = _ManyJobsScraper(20)

    run(scrapers=[scraper], db=db, notifier=notifier)
    enviados_na_primeira = len(notifier.sent)

    run(scrapers=[scraper], db=db, notifier=notifier)

    assert enviados_na_primeira == MAX_NEW_NOTIFICATIONS_PER_RUN
    assert len(notifier.sent) > enviados_na_primeira


def test_bootstrap_run_populates_without_notifying(tmp_path):
    """Banco zerado significa que TODO o acervo seria 'novo'. Sem isso, um
    reset deliberado viraria dias de alertas irrelevantes pingando pelo teto."""
    vazio = DatabaseManager(db_name=str(tmp_path / "vazio.db"))
    notifier = _FakeNotifier()
    try:
        run(scrapers=[_ManyJobsScraper(30)], db=vazio, notifier=notifier)

        assert notifier.sent == []
        assert len(vazio.fetch_all_jobs()) == 30  # tudo salvo e visível no site
    finally:
        vazio.close()


def test_jobs_found_after_the_bootstrap_run_do_alert(tmp_path):
    """O silêncio vale só para a rodada de bootstrap: o que aparecer depois
    é novidade de verdade e precisa notificar."""
    banco = DatabaseManager(db_name=str(tmp_path / "b.db"))
    notifier = _FakeNotifier()
    try:
        run(scrapers=[_ManyJobsScraper(3)], db=banco, notifier=notifier)
        assert notifier.sent == []

        run(scrapers=[_NewJobScraper("[T] Vaga posterior", "https://example.com/dep")],
            db=banco, notifier=notifier)

        assert len(notifier.sent) == 1
        assert "Vaga posterior" in notifier.sent[0][1]
    finally:
        banco.close()


def test_classification_is_persisted_with_each_job(db):
    """Sem região e tipo gravados não há o que filtrar depois."""
    scraper = _NewJobScraper("[Pref. Macaé] Concurso para médico", "https://example.com/macae")
    scraper.job_type = "concurso"

    run(scrapers=[scraper], db=db, notifier=_FakeNotifier())

    vaga = next(j for j in db.fetch_all_jobs() if "macae" in j.link)
    assert vaga.region == "norte_fluminense"
    assert vaga.job_type == "concurso"


def test_source_region_fills_in_when_the_title_has_no_city(db):
    """O título costuma trazer o nome da instituição, não o do município.
    Quando a fonte é de uma cidade específica, ela responde o que o texto
    não disse — sem isso a vaga cairia em 'Estadual/Nacional' e sumiria do
    filtro de quem assina só a sua região."""
    scraper = _NewJobScraper("[RioSaúde] Edital 003/2026", "https://example.com/rs")
    scraper.region = "capital_metropolitana"

    run(scrapers=[scraper], db=db, notifier=_FakeNotifier())

    vaga = next(j for j in db.fetch_all_jobs() if j.link.endswith("/rs"))
    assert vaga.region == "capital_metropolitana"


def test_city_in_the_title_wins_over_the_source_default(db):
    """O texto é mais específico que o padrão da fonte quando ele existe."""
    scraper = _NewJobScraper("[Fonte] Concurso em Saquarema", "https://example.com/sq")
    scraper.region = "capital_metropolitana"

    run(scrapers=[scraper], db=db, notifier=_FakeNotifier())

    vaga = next(j for j in db.fetch_all_jobs() if j.link.endswith("/sq"))
    assert vaga.region == "regiao_dos_lagos"


def test_legacy_database_is_not_resent_when_deliveries_table_appears(tmp_path):
    """Quem atualizar o código sem zerar o banco não pode receber o histórico
    inteiro de novo: ao estrear a tabela de entregas, o acervo existente é
    marcado como já entregue."""
    caminho = str(tmp_path / "legado.db")
    antigo = DatabaseManager(db_name=caminho)
    for i in range(5):
        antigo.insert_job(f"Vaga antiga {i}", f"https://example.com/a{i}", _today())
    antigo.conn.execute("DELETE FROM deliveries")  # simula banco anterior à tabela
    antigo.conn.commit()
    antigo.close()

    banco = DatabaseManager(db_name=caminho)
    notifier = _FakeNotifier()
    try:
        run(scrapers=[], db=banco, notifier=notifier)
        assert notifier.sent == []
    finally:
        banco.close()


def test_subscriber_only_gets_what_matches_their_preferences(db):
    """O ponto da Fase 4: cada um recebe só o recorte que escolheu."""
    macae = _NewJobScraper("[Pref. Macaé] Concurso para médico", "https://example.com/macae")
    macae.job_type = "concurso"
    notifier = _FakeNotifier()

    so_lagos = _assinante("999", regions=["regiao_dos_lagos"], job_types=["concurso"])
    run(scrapers=[macae], db=db, notifier=notifier, subscribers=[so_lagos])

    assert notifier.sent == [], "vaga do Norte Fluminense não é para quem assina só a Região dos Lagos"


def test_subscriber_gets_what_they_asked_for(db):
    macae = _NewJobScraper("[Pref. Macaé] Concurso para médico", "https://example.com/macae")
    macae.job_type = "concurso"
    notifier = _FakeNotifier()

    quer = _assinante("999", regions=["norte_fluminense"], job_types=["concurso"])
    run(scrapers=[macae], db=db, notifier=notifier, subscribers=[quer])

    assert len(notifier.sent) == 1
    assert notifier.sent[0][0] == "999"


def test_each_subscriber_is_filtered_independently(db):
    macae = _NewJobScraper("[Pref. Macaé] Concurso para médico", "https://example.com/macae")
    macae.job_type = "concurso"
    notifier = _FakeNotifier()

    run(scrapers=[macae], db=db, notifier=notifier, subscribers=[
        _assinante("111", regions=["norte_fluminense"], job_types=["concurso"]),
        _assinante("222", regions=["capital_metropolitana"], job_types=["concurso"]),
    ])

    assert [chat for chat, _ in notifier.sent] == ["111"]


def test_unmatched_job_stays_pending_so_preference_changes_can_reach_it(db):
    """Uma vaga fora do filtro não é marcada como entregue — se a pessoa
    ampliar as preferências depois, ela ainda chega pela fila de pendentes."""
    macae = _NewJobScraper("[Pref. Macaé] Concurso para médico", "https://example.com/macae")
    macae.job_type = "concurso"

    restrito = _assinante("999", regions=["regiao_dos_lagos"], job_types=["concurso"])
    run(scrapers=[macae], db=db, notifier=_FakeNotifier(), subscribers=[restrito])

    ampliado = _assinante("999", regions=["regiao_dos_lagos", "norte_fluminense"], job_types=["concurso"])
    notifier = _FakeNotifier()
    run(scrapers=[], db=db, notifier=notifier, subscribers=[ampliado])

    assert len(notifier.sent) == 1, "após ampliar o filtro, a vaga pendente é entregue"


def test_delivery_is_skipped_when_the_subscriber_list_is_unavailable(db, monkeypatch):
    """Se o Worker estiver fora do ar, a coleta segue mas nada é dado como
    entregue — as vagas ficam pendentes e saem na rodada seguinte, em vez de
    serem perdidas silenciosamente."""
    monkeypatch.setattr("medalert.orchestrator.fetch_subscribers", lambda: None)
    notifier = _FakeNotifier()

    run(scrapers=[_NewJobScraper("[T] Vaga", "https://example.com/v")], db=db, notifier=notifier)

    assert notifier.sent == []
    assert len(db.fetch_all_jobs()) == 2  # a vaga foi salva (semente + esta)
    assert len(db.fetch_undelivered("123")) >= 1, "continua pendente para a próxima rodada"


def test_blocked_subscriber_is_pruned(db, monkeypatch):
    """Quem bloqueia o bot sai da lista: insistir gastaria uma chamada de API
    por rodada, para sempre, com alguém que nunca mais vai receber."""
    removidos = []
    monkeypatch.setattr("medalert.orchestrator.prune_blocked", lambda ids: removidos.extend(ids) or len(ids))

    run(
        scrapers=[_NewJobScraper("[T] Vaga", "https://example.com/v")],
        db=db,
        notifier=_FakeNotifier(resultado=DeliveryResult.BLOCKED),
        subscribers=[_assinante("999")],
    )

    assert removidos == ["999"]


def test_subscriber_wanting_nothing_is_skipped(db):
    """Sem região ou sem tipo nenhuma vaga casaria — não vale nem consultar."""
    notifier = _FakeNotifier()

    run(scrapers=[], db=db, notifier=notifier, subscribers=[_assinante("999", regions=[])])

    assert notifier.sent == []


def test_total_failure_alert_goes_to_the_admin_not_to_subscribers(db):
    """"Todos os scrapers falharam" é problema de operação; mandar isso para a
    base inteira seria ruído para quem só quer saber de vaga."""
    notifier = _FakeNotifier()

    exit_code = run(
        scrapers=[_FailingScraper()], db=db, notifier=notifier,
        subscribers=[_assinante("111"), _assinante("222")],
    )

    assert exit_code == 1
    assert len(notifier.sent) == 1, "um aviso só, para o admin — não um por assinante"
    assert "falha total" in notifier.sent[0][1].lower()


class _ClosedJobScraper(BaseScraper):
    """Fonte que declara o prazo encerrado — o caso do MedGrupo, onde 44 dos
    83 concursos do RJ vinham marcados como "encerradas"."""
    url = "https://example.com/fechada"

    def scrape(self):
        return [{
            "title": "[T] Vaga com inscrições encerradas",
            "link": "https://example.com/fechada",
            "pub_date": _today(),
            "status": "encerrado",
            "status_source": "fonte",
        }]


def test_new_job_already_closed_does_not_alert(db):
    """Descoberta hoje, mas com o prazo vencido antes de a gente saber que
    existia. Anunciá-la como "nova oportunidade" seria simplesmente falso."""
    notifier = _FakeNotifier()

    run(scrapers=[_ClosedJobScraper()], db=db, notifier=notifier)

    assert notifier.sent == []
    assert len(db.fetch_all_jobs()) == 2, "continua registrada e visível no site"


def test_closed_job_does_not_come_back_through_the_pending_queue(db):
    """O alerta suprimido não pode voltar pela fila de reenvio na rodada
    seguinte — seria mandar justamente o que se decidiu não mandar."""
    notifier = _FakeNotifier()

    run(scrapers=[_ClosedJobScraper()], db=db, notifier=notifier)
    run(scrapers=[], db=db, notifier=notifier)

    assert notifier.sent == []


def test_job_that_closes_while_pending_is_offered_at_most_once_more(db):
    """Documenta um atraso de uma rodada, conhecido e aceito.

    A fila de pendências é drenada no INÍCIO da execução, antes de os scrapers
    rodarem — então ela enxerga a situação da rodada anterior. Uma vaga que
    fechou desde ontem ainda sai uma última vez; a partir daí, nunca mais.

    Corrigir o atraso exigiria entregar as pendências depois da coleta, o que
    faria o excedente do teto por rodada sair na mesma execução em que foi
    represado — justamente o que o teto existe para evitar. Uma mensagem
    atrasada é mais barata que uma enxurrada.
    """
    aberta = _NewJobScraper("[T] Vaga", "https://example.com/fechada")
    run(scrapers=[aberta], db=db, notifier=_FlakyNotifier(failures=99))

    primeira = _FakeNotifier()
    run(scrapers=[_ClosedJobScraper()], db=db, notifier=primeira)
    assert len(primeira.sent) == 1, "a última mensagem sai com o status de ontem"

    segunda = _FakeNotifier()
    run(scrapers=[_ClosedJobScraper()], db=db, notifier=segunda)
    assert segunda.sent == [], "com o status já atualizado, não é mais oferecida"


def test_uncertain_deadline_never_silences_an_alert(db):
    """A regra central: uma data lida da redação corrida do edital pode estar
    errada, e o custo do erro é assimétrico — anunciar vaga fechada gasta uma
    mensagem, silenciar vaga aberta custa a oportunidade."""
    class _TextoScraper(BaseScraper):
        url = "https://example.com/incerta"

        def scrape(self):
            return [{
                "title": "[T] Vaga com prazo lido do texto",
                "link": "https://example.com/incerta",
                "pub_date": _today(),
                "status": "encerrado",
                "status_source": "texto",
            }]

    notifier = _FakeNotifier()
    run(scrapers=[_TextoScraper()], db=db, notifier=notifier)

    assert len(notifier.sent) == 1


def test_status_declared_by_the_source_is_persisted(db):
    run(scrapers=[_ClosedJobScraper()], db=db, notifier=_FakeNotifier())

    vaga = next(j for j in db.fetch_all_jobs() if j.link.endswith("/fechada"))
    assert vaga.status == "encerrado"
    assert vaga.status_source == "fonte"


def test_known_job_gets_its_status_refreshed_when_the_source_closes_it(db):
    """Sem isso, a situação congelaria no valor do dia da descoberta — e o
    acervo inteiro ficaria marcado como estava meses atrás."""
    run(scrapers=[_NewJobScraper("[T] Vaga", "https://example.com/fechada")],
        db=db, notifier=_FakeNotifier())

    run(scrapers=[_ClosedJobScraper()], db=db, notifier=_FakeNotifier())

    vaga = next(j for j in db.fetch_all_jobs() if j.link.endswith("/fechada"))
    assert vaga.status == "encerrado"


def test_run_refreshes_last_seen_shown_on_the_site_for_known_jobs(db):
    """Regressão do descompasso banco↔site: uma vaga já conhecida é
    reencontrada, o banco atualiza last_seen_at e o jobs.json publicado
    precisa refletir isso, mesmo sem nenhuma vaga nova na rodada."""
    db.insert_job("[T] Vaga conhecida", "https://example.com/conhecida", "2026-01-01")

    run(scrapers=[_KnownJobScraper()], db=db, notifier=_FakeNotifier())

    with open("_data/jobs.json", encoding="utf-8") as f:
        payload = json.load(f)

    conhecida = next(j for j in payload if "conhecida" in j["link"])
    assert conhecida["last_seen_at"] is not None
