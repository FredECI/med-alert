"""Testes da classe base: retry/backoff montado na sessão HTTP."""
from medalert.scrapers.base import BaseScraper


def test_retry_adapter_is_mounted_for_both_schemes():
    scraper = BaseScraper()

    https_adapter = scraper.scraper.get_adapter("https://example.com")
    http_adapter = scraper.scraper.get_adapter("http://example.com")

    assert https_adapter.max_retries.total == 2
    assert http_adapter.max_retries.total == 2


def test_retry_only_applies_to_get_and_server_errors():
    scraper = BaseScraper()
    retry = scraper.scraper.get_adapter("https://example.com").max_retries

    assert set(retry.allowed_methods) == {"GET"}
    assert 503 in retry.status_forcelist
