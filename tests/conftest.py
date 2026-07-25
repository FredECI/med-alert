"""Fixtures compartilhadas pelos testes do MedAlert."""
from pathlib import Path

import pytest

from medalert.storage import DatabaseManager

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test_med_alerts.db"
    manager = DatabaseManager(db_name=str(db_path))
    yield manager
    manager.close()


@pytest.fixture
def stub_html(monkeypatch):
    """Faz scraper.fetch_html(url) devolver um HTML fixo, sem tocar a rede."""

    def _stub(scraper, html: str):
        monkeypatch.setattr(scraper, "fetch_html", lambda url: html)

    return _stub
