"""Testes do orquestrador: código de saída e alerta de Telegram em falha total.

scrapers/db/notifier são injetáveis exatamente para permitir estes testes sem
rede real nem tocar no banco de produção — main.py sempre chama run() sem
argumentos, usando os valores reais.
"""
from medalert.orchestrator import run
from medalert.scrapers.base import BaseScraper


class _FailingScraper(BaseScraper):
    url = "https://example.com/broken"

    def scrape(self):
        raise RuntimeError("site fora do ar")


class _EmptyScraper(BaseScraper):
    url = "https://example.com/ok"

    def scrape(self):
        return []


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
