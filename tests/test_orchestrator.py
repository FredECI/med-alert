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


def test_run_skips_jobs_data_but_not_robot_status_when_nothing_new(db):
    run(scrapers=[_EmptyScraper()], db=db, notifier=_FakeNotifier())

    assert not os.path.exists("_data/jobs.json")
    with open("_data/robot_status.json", encoding="utf-8") as f:
        status = json.load(f)
    assert status["scrapers_ok"] == 1
