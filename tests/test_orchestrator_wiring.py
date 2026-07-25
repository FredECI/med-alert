"""Garante que todo scraper escrito está de fato ligado ao orquestrador.

Sem isso, dá para escrever um scraper novo com testes verdinhos que nunca
roda em produção, porque ninguém lembrou de acrescentá-lo a build_scrapers().
"""
import importlib
import inspect
import pkgutil

import medalert.scrapers
from medalert.orchestrator import build_scrapers
from medalert.scrapers.aggregators import DomRjScraper
from medalert.scrapers.base import BaseScraper

# Scrapers implementados mas deliberadamente fora de build_scrapers().
# A exceção é explícita para que "esqueci de ligar" continue quebrando o
# teste, enquanto "decidi não ligar, e aqui está o porquê" fica registrado.
INTENTIONALLY_DISABLED = {
    # Busca full-text do Diário Oficial: devolve fragmentos de página em vez
    # de vagas (~200-400 por semana, qualquer que seja o termo). Detalhes na
    # docstring da classe.
    DomRjScraper,
}


def _all_scraper_classes():
    """Descobre toda subclasse concreta de BaseScraper em medalert/scrapers/."""
    classes = set()
    for module_info in pkgutil.iter_modules(medalert.scrapers.__path__):
        module = importlib.import_module(f"medalert.scrapers.{module_info.name}")
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if issubclass(obj, BaseScraper) and obj is not BaseScraper:
                classes.add(obj)
    return classes


def test_every_scraper_class_is_wired_into_build_scrapers():
    wired = {type(scraper) for scraper in build_scrapers()}
    missing = _all_scraper_classes() - wired - INTENTIONALLY_DISABLED

    assert not missing, (
        "Scrapers implementados mas ausentes de build_scrapers(): "
        f"{sorted(cls.__name__ for cls in missing)}"
    )


def test_disabled_scrapers_are_really_disabled():
    """Se um scraper da lista de exceções voltar a ser ligado, a lista precisa
    ser atualizada junto — senão a exceção vira letra morta e para de proteger."""
    wired = {type(scraper) for scraper in build_scrapers()}

    assert not (wired & INTENTIONALLY_DISABLED), (
        "Scraper marcado como desativado está em build_scrapers(): "
        f"{sorted(cls.__name__ for cls in wired & INTENTIONALLY_DISABLED)}"
    )


def test_build_scrapers_has_no_duplicate_entries():
    """Duas instâncias da mesma classe sem parâmetros diferentes significaria
    buscar a mesma URL duas vezes por rodada."""
    labels = [scraper.label for scraper in build_scrapers()]
    duplicates = {label for label in labels if labels.count(label) > 1}

    assert not duplicates, f"Fontes duplicadas em build_scrapers(): {sorted(duplicates)}"


def test_every_scraper_has_a_distinct_label():
    """O label é o que identifica a fonte no painel de saúde do site e no
    alerta de falha do Telegram — labels repetidos tornam o diagnóstico ambíguo."""
    labels = [scraper.label for scraper in build_scrapers()]

    assert len(labels) == len(set(labels))
