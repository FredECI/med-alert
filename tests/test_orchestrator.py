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

from medalert.orchestrator import MAX_NEW_NOTIFICATIONS_PER_RUN, run
from medalert.scrapers.base import BaseScraper
from medalert.storage import DatabaseManager
from medalert.timeutil import now_brt


def _today() -> str:
    """Vagas precisam ser recentes para caírem na janela de reenvio."""
    return now_brt().strftime("%Y-%m-%d")


@pytest.fixture(autouse=True)
def _isolate_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


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
    publicado, o caso que exercita touch_last_seen()."""
    url = "https://example.com/known"

    def scrape(self):
        return [{
            "title": "[T] Vaga conhecida",
            "link": "https://example.com/conhecida",
            "pub_date": "2026-01-01",
        }]


class _FakeNotifier:
    def __init__(self, chat_ids=None):
        self.chat_ids = chat_ids if chat_ids is not None else ["123"]
        self.sent = []

    def send_to(self, chat_id, text):
        self.sent.append((chat_id, text))
        return True

    def send_message(self, text):
        return sum(1 for chat_id in self.chat_ids if self.send_to(chat_id, text))


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
        self.chat_ids = ["123"]
        self.remaining_failures = failures
        self.sent = []

    def send_to(self, chat_id, text):
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            return False
        self.sent.append((chat_id, text))
        return True

    def send_message(self, text):
        return sum(1 for chat_id in self.chat_ids if self.send_to(chat_id, text))


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
