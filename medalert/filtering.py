"""Filtros de relevância (área médica) e regionalização (RJ) usados por todos os scrapers."""
import re
from typing import Iterable

KEYWORDS = [
    "médico", "medico", "médica", "medica",
    "clínico geral", "clinico geral",
    "saúde da família", "saude da familia", "crm",
    "esf", "psf", "ubs", "upa", "plantão", "plantao",
    "medicina", "pronto socorro", "pronto atendimento",
    "posto de saúde", "posto de saude", "generalista",
    "saúde", "saude",
]

STATE_FILTERS = [
    "rj", "rio de janeiro", "macaé", "macae",
    "região dos lagos", "regiao dos lagos", "rio das ostras",
    # "campos" sozinho NÃO entra aqui: é palavra comum em português
    # ("campos de atuação") e ainda casaria com cidades de outros estados
    # (ex: "Campos do Jordão/SP"), vazando vaga de fora do RJ — a mesma
    # classe de bug do vazamento do Espírito Santo no Trabalha Brasil.
    # Na prática nada se perde: as fontes que citam a cidade quase sempre
    # trazem junto "RJ" ou o nome completo, que continuam na lista.
    "campos dos goytacazes", "carapebus",
    "quissamã", "quissama", "cabo frio", "búzios", "buzios",
    "são joão da barra", "sao joao da barra", "casimiro",
    "saquarema", "araruama", "arraial do cabo",
]

# Um texto só é considerado "oportunidade de trabalho" se tiver linguagem de
# contratação — não basta mencionar saúde. Mas nem todo termo tem a mesma
# força probatória, por isso a lista é dividida:
#
# FORTES: praticamente só aparecem em contexto de contratação pública.
STRONG_JOB_TERMS = [
    "concurso", "concursos", "concurso público", "concurso publico",
    "processo seletivo", "processos seletivos",
    "seleção pública", "selecao publica",
    "edital", "editais",
    "contratação", "contratacao",
    "convocação", "convocacao",
    "banco de currículos", "banco de curriculos",
    "trabalhe conosco",
]

# FRACOS: aparecem tanto em vaga real quanto em evento — "vagas limitadas"
# numa palestra, "inscrições abertas" num congresso. Só valem quando o texto
# não é sobre um evento/formação.
WEAK_JOB_TERMS = [
    "vaga", "vagas",
    "inscrição", "inscricao", "inscrições", "inscricoes",
]

# Marcadores de evento/capacitação. Um fórum de saúde que "recebe inscrições"
# não é vaga de emprego — caso real que passou pelo filtro antigo e apareceu
# como vaga vinda do portal de notícias de Cabo Frio.
EVENT_TERMS = [
    "fórum", "forum", "congresso", "congressos",
    "palestra", "palestras", "curso", "cursos",
    "capacitação", "capacitacao", "treinamento", "treinamentos",
    "seminário", "seminario", "simpósio", "simposio",
    "workshop", "workshops", "oficina", "oficinas", "webinar", "webinars",
]


# Medicina veterinária compartilha quase todo o vocabulário com a humana
# ("médico veterinário", "medicina veterinária"), então bate em KEYWORDS sem
# ser o que o radar procura. A remoção é feita por SUBTRAÇÃO em vez de
# rejeição: o trecho veterinário é apagado do texto e a relevância é
# reavaliada no que sobrou. Isso importa porque concurso municipal costuma
# listar vários cargos de uma vez — "Médico Clínico Geral e Médico
# Veterinário" continua sendo uma vaga de interesse, e rejeitar o anúncio
# inteiro só porque cita veterinário perderia a vaga humana junto.
VETERINARY_PHRASES = [
    "médico veterinário", "medico veterinario",
    "médica veterinária", "medica veterinaria",
    "medicina veterinária", "medicina veterinaria",
    "veterinários", "veterinarios", "veterinárias", "veterinarias",
    "veterinário", "veterinario", "veterinária", "veterinaria",
    "crmv",
]

# Frases maiores saem primeiro para que "médico veterinário" seja removido
# por inteiro, em vez de sobrar um "médico" órfão depois de tirar só o
# adjetivo — o que faria a vaga voltar a parecer relevante.
_VETERINARY_PHRASES_LONGEST_FIRST = sorted(VETERINARY_PHRASES, key=len, reverse=True)


def _contains_word(text_lower: str, phrase: str) -> bool:
    pattern = r"\b" + re.escape(phrase) + r"\b"
    return re.search(pattern, text_lower) is not None


def strip_veterinary(text_lower: str) -> str:
    """Remove do texto os trechos que se referem a medicina veterinária."""
    for phrase in _VETERINARY_PHRASES_LONGEST_FIRST:
        text_lower = re.sub(r"\b" + re.escape(phrase) + r"\b", " ", text_lower)
    return text_lower


def is_relevant(text: str, keywords: Iterable[str] = KEYWORDS) -> bool:
    text_lower = strip_veterinary(text.lower())
    return any(_contains_word(text_lower, keyword) for keyword in keywords)


def has_job_signal(text: str) -> bool:
    """True se o texto indicar uma contratação real, não só o assunto saúde.

    Um termo forte ("concurso", "edital", "processo seletivo") basta sozinho.
    Um termo fraco ("vaga", "inscrições") só conta se o texto não for sobre
    evento/curso — senão, coisas como "Fórum de Saúde recebe inscrições" ou
    "Palestra tem vagas limitadas" entram como se fossem oportunidade de
    emprego. Note que a checagem é por palavra inteira, então "curso" não
    casa dentro de "concurso".
    """
    text_lower = text.lower()

    if any(_contains_word(text_lower, term) for term in STRONG_JOB_TERMS):
        return True

    if any(_contains_word(text_lower, term) for term in EVENT_TERMS):
        return False

    return any(_contains_word(text_lower, term) for term in WEAK_JOB_TERMS)


def is_in_target_state(text: str, state_filters: Iterable[str] = STATE_FILTERS) -> bool:
    """Usa o mesmo casamento por palavra inteira de is_relevant().

    A versão original comparava também `state in text_lower` (substring pura),
    o que fazia filtros curtos como "rj" darem falso-positivo em qualquer
    palavra que contivesse essas duas letras em sequência (ex: "surja").
    """
    text_lower = text.lower()
    return any(_contains_word(text_lower, state) for state in state_filters)
