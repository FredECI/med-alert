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

from medalert.orchestrator import run
from medalert.scrapers.base import BaseScraper
from medalert.timeutil import now_brt


def _today() -> str:
    """Vagas precisam ser recentes para caírem na janela de reenvio."""
    return now_brt().strftime("%Y-%m-%d")


@pytest.fixture(autouse=True)
def _isolate_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


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
    def __init__(self):
        self.chat_ids = ["123"]
        self.sent = []

    def send_message(self, text):
        self.sent.append(text)
        return 1


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
    assert "falha total" in notifier.sent[0].lower()
    assert "site fora do ar" in notifier.sent[0]


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

    def send_message(self, text):
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            return 0
        self.sent.append(text)
        return 1


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
    assert "Vaga importante" in notifier.sent[0]


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
    assert len(db.fetch_all_jobs()) == 2  # as duas continuam registradas


def test_suppressed_duplicate_does_not_enter_the_retry_queue(db):
    """A duplicata é marcada como resolvida; se ficasse pendente, o reenvio
    da rodada seguinte mandaria justamente o alerta que se quis suprimir."""
    ibam = _NewJobScraper("[IBAM] Municipio de Araruama - Ed. 09/2026", "https://ibam.example/9")
    prefeitura = _NewJobScraper("[Araruama] Edital 009/2026 saúde", "https://araruama.example/9")
    notifier = _FakeNotifier()

    run(scrapers=[ibam, prefeitura], db=db, notifier=notifier)
    run(scrapers=[], db=db, notifier=notifier)

    assert len(notifier.sent) == 1


def test_run_refreshes_last_seen_shown_on_the_site_for_known_jobs(db):
    """Regressão do descompasso banco↔site: uma vaga já conhecida é
    reencontrada, o banco atualiza last_seen_at e o jobs.json publicado
    precisa refletir isso, mesmo sem nenhuma vaga nova na rodada."""
    db.insert_job("[T] Vaga conhecida", "https://example.com/conhecida", "2026-01-01")

    run(scrapers=[_KnownJobScraper()], db=db, notifier=_FakeNotifier())

    with open("_data/jobs.json", encoding="utf-8") as f:
        payload = json.load(f)

    assert len(payload) == 1
    assert payload[0]["last_seen_at"] is not None
