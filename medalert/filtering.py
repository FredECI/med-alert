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

# Termos que indicam que o texto é REALMENTE sobre uma oportunidade de
# trabalho (concurso/processo seletivo/vaga), não só que ele menciona saúde
# de passagem. Fontes de notícia geral (Google News, Bing News, portais de
# cidade) precisam bater nisso ALÉM de is_relevant() — sem essa segunda
# trava, matéria de saúde sem nenhuma vaga real (ex: um texto institucional
# ou patrocinado que só cita "saúde") passa pelo filtro de palavra-chave.
JOB_SIGNAL_TERMS = [
    "concurso", "concurso público", "concurso publico",
    "processo seletivo", "seleção pública", "selecao publica",
    "edital", "vaga", "vagas",
    "inscrição", "inscricao", "inscrições", "inscricoes",
    "contratação", "contratacao",
    "convocação", "convocacao",
    "banco de currículos", "banco de curriculos",
]


def _contains_word(text_lower: str, phrase: str) -> bool:
    pattern = r"\b" + re.escape(phrase) + r"\b"
    return re.search(pattern, text_lower) is not None


def is_relevant(text: str, keywords: Iterable[str] = KEYWORDS) -> bool:
    text_lower = text.lower()
    return any(_contains_word(text_lower, keyword) for keyword in keywords)


def has_job_signal(text: str, terms: Iterable[str] = JOB_SIGNAL_TERMS) -> bool:
    """True se o texto tiver alguma palavra que indique concurso/vaga real,
    não só o assunto saúde em geral."""
    text_lower = text.lower()
    return any(_contains_word(text_lower, term) for term in terms)


def is_in_target_state(text: str, state_filters: Iterable[str] = STATE_FILTERS) -> bool:
    """Usa o mesmo casamento por palavra inteira de is_relevant().

    A versão original comparava também `state in text_lower` (substring pura),
    o que fazia filtros curtos como "rj" darem falso-positivo em qualquer
    palavra que contivesse essas duas letras em sequência (ex: "surja").
    """
    text_lower = text.lower()
    return any(_contains_word(text_lower, state) for state in state_filters)
