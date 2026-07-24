"""Fixtures compartilhadas pelos testes do MedAlert."""
import pytest

from medalert.storage import DatabaseManager


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test_med_alerts.db"
    manager = DatabaseManager(db_name=str(db_path))
    yield manager
    manager.close()
