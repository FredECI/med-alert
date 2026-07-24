"""Filtros de relevância (área médica) e regionalização (RJ) usados por todos os scrapers."""
import re
from typing import Iterable

KEYWORDS = [
    "médico", "medico", "clínico geral", "clinico geral",
    "saúde da família", "saude da familia", "crm",
    "esf", "psf", "ubs", "upa", "plantão", "plantao",
    "medicina", "pronto socorro", "pronto atendimento",
    "posto de saúde", "posto de saude", "generalista",
    "saúde", "saude",
]

STATE_FILTERS = [
    "rj", "rio de janeiro", "macaé", "macae",
    "região dos lagos", "regiao dos lagos", "rio das ostras",
    "campos", "campos dos goytacazes", "carapebus",
    "quissamã", "quissama", "cabo frio", "búzios", "buzios",
    "são joão da barra", "sao joao da barra", "casimiro",
    "saquarema", "araruama", "arraial do cabo",
]


def _contains_word(text_lower: str, phrase: str) -> bool:
    pattern = r"\b" + re.escape(phrase) + r"\b"
    return re.search(pattern, text_lower) is not None


def is_relevant(text: str, keywords: Iterable[str] = KEYWORDS) -> bool:
    text_lower = text.lower()
    return any(_contains_word(text_lower, keyword) for keyword in keywords)


def is_in_target_state(text: str, state_filters: Iterable[str] = STATE_FILTERS) -> bool:
    """Usa o mesmo casamento por palavra inteira de is_relevant().

    A versão original comparava também `state in text_lower` (substring pura),
    o que fazia filtros curtos como "rj" darem falso-positivo em qualquer
    palavra que contivesse essas duas letras em sequência (ex: "surja").
    """
    text_lower = text.lower()
    return any(_contains_word(text_lower, state) for state in state_filters)
