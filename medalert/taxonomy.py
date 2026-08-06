"""Classificação das vagas em REGIÃO e TIPO — as duas dimensões pelas quais o
assinante vai poder filtrar o que recebe.

Sem isso não há o que filtrar: até aqui o robô só guardava título e link, sem
nenhuma noção de onde a vaga fica nem de que natureza ela é.
"""
import re
from typing import Optional

from medalert.textutils import strip_accents

# ==========================================
# TIPOS DE VAGA
# ==========================================
CONCURSO = "concurso"
PROCESSO_SELETIVO = "processo_seletivo"
RESIDENCIA = "residencia"
EMPREGO = "emprego"
NOTICIA = "noticia"

JOB_TYPES = [CONCURSO, PROCESSO_SELETIVO, RESIDENCIA, EMPREGO, NOTICIA]

JOB_TYPE_LABELS = {
    CONCURSO: "Concurso público",
    PROCESSO_SELETIVO: "Processo seletivo / temporário",
    RESIDENCIA: "Residência médica",
    EMPREGO: "Emprego CLT / privado",
    NOTICIA: "Notícia / radar",
}

# ==========================================
# REGIÕES
# ==========================================
CAPITAL_METROPOLITANA = "capital_metropolitana"
REGIAO_DOS_LAGOS = "regiao_dos_lagos"
NORTE_FLUMINENSE = "norte_fluminense"
OUTRAS_RJ = "outras_rj"
#: Vale para vaga do estado inteiro (SES-RJ, PMERJ), federal (EBSERH) ou
#: quando não dá para identificar o município. Fica como opção própria de
#: assinatura em vez de ser enviada a todos à força: quem só quer a sua
#: região não precisa receber concurso de alcance nacional.
ESTADUAL_NACIONAL = "estadual_nacional"

REGIONS = [
    CAPITAL_METROPOLITANA,
    REGIAO_DOS_LAGOS,
    NORTE_FLUMINENSE,
    OUTRAS_RJ,
    ESTADUAL_NACIONAL,
]

REGION_LABELS = {
    CAPITAL_METROPOLITANA: "Capital e Região Metropolitana",
    REGIAO_DOS_LAGOS: "Região dos Lagos",
    NORTE_FLUMINENSE: "Norte Fluminense (Macaé e região)",
    OUTRAS_RJ: "Outras regiões do RJ",
    ESTADUAL_NACIONAL: "Estadual / Nacional",
}

#: Município -> região. Escrito sem acento porque a comparação normaliza o
#: texto antes (ver strip_accents).
CITY_TO_REGION = {
    # Capital e Região Metropolitana
    "rio de janeiro": CAPITAL_METROPOLITANA,
    "niteroi": CAPITAL_METROPOLITANA,
    "sao goncalo": CAPITAL_METROPOLITANA,
    "duque de caxias": CAPITAL_METROPOLITANA,
    "nova iguacu": CAPITAL_METROPOLITANA,
    "belford roxo": CAPITAL_METROPOLITANA,
    "sao joao de meriti": CAPITAL_METROPOLITANA,
    "mesquita": CAPITAL_METROPOLITANA,
    "nilopolis": CAPITAL_METROPOLITANA,
    "queimados": CAPITAL_METROPOLITANA,
    "mage": CAPITAL_METROPOLITANA,
    "itaborai": CAPITAL_METROPOLITANA,
    "marica": CAPITAL_METROPOLITANA,
    "itaguai": CAPITAL_METROPOLITANA,
    "seropedica": CAPITAL_METROPOLITANA,
    "guapimirim": CAPITAL_METROPOLITANA,
    # Região dos Lagos
    "cabo frio": REGIAO_DOS_LAGOS,
    "araruama": REGIAO_DOS_LAGOS,
    "saquarema": REGIAO_DOS_LAGOS,
    "arraial do cabo": REGIAO_DOS_LAGOS,
    "buzios": REGIAO_DOS_LAGOS,
    "armacao dos buzios": REGIAO_DOS_LAGOS,
    "sao pedro da aldeia": REGIAO_DOS_LAGOS,
    "iguaba grande": REGIAO_DOS_LAGOS,
    # Norte Fluminense (área de foco do projeto)
    "macae": NORTE_FLUMINENSE,
    "campos dos goytacazes": NORTE_FLUMINENSE,
    "rio das ostras": NORTE_FLUMINENSE,
    "carapebus": NORTE_FLUMINENSE,
    "quissama": NORTE_FLUMINENSE,
    "sao joao da barra": NORTE_FLUMINENSE,
    "casimiro de abreu": NORTE_FLUMINENSE,
    "casimiro": NORTE_FLUMINENSE,
    "conceicao de macabu": NORTE_FLUMINENSE,
    "sao francisco de itabapoana": NORTE_FLUMINENSE,
    # Outras regiões do RJ
    "petropolis": OUTRAS_RJ,
    "teresopolis": OUTRAS_RJ,
    "nova friburgo": OUTRAS_RJ,
    "volta redonda": OUTRAS_RJ,
    "barra mansa": OUTRAS_RJ,
    "resende": OUTRAS_RJ,
    "angra dos reis": OUTRAS_RJ,
    "paraty": OUTRAS_RJ,
    "vassouras": OUTRAS_RJ,
    "valenca": OUTRAS_RJ,
    "tres rios": OUTRAS_RJ,
    "cachoeiras de macacu": OUTRAS_RJ,
    "rio bonito": OUTRAS_RJ,
    "silva jardim": OUTRAS_RJ,
    "bom jesus do itabapoana": OUTRAS_RJ,
    "itaperuna": OUTRAS_RJ,
    "santo antonio de padua": OUTRAS_RJ,
    "barra do pirai": OUTRAS_RJ,
    "pirai": OUTRAS_RJ,
    "mangaratiba": OUTRAS_RJ,
    "paty do alferes": OUTRAS_RJ,
    "miguel pereira": OUTRAS_RJ,
}

#: Marcas de alcance estadual/federal. Precisam ser testadas ANTES dos
#: municípios: "Governo do Estado do Rio de Janeiro" contém "rio de janeiro",
#: mas não é uma vaga da capital.
_STATEWIDE_MARKERS = [
    "estado do rio de janeiro",
    "governo do estado",
    "secretaria de estado",
    "ses rj",
    "ses-rj",
    "estadual",
    "nacional",
    "federal",
    "ebserh",
    "ministerio da saude",
]

_RESIDENCIA_MARKERS = ["residencia", "residente", "pre requisito", "pre-requisito"]
_PSS_MARKERS = ["processo seletivo", "pss", "selecao simplificada", "temporario"]


def _contains(normalized_text: str, phrase: str) -> bool:
    return re.search(r"\b" + re.escape(phrase) + r"\b", normalized_text) is not None


def classify_region(text: str) -> str:
    """Deduz a região de uma vaga a partir do texto do título.

    Sem município reconhecido, cai em ESTADUAL_NACIONAL — que é honesto:
    ou a vaga vale para todo o estado/país, ou não sabemos onde é.
    """
    normalized = strip_accents(text).lower()

    if any(_contains(normalized, marker) for marker in _STATEWIDE_MARKERS):
        return ESTADUAL_NACIONAL

    # Nomes maiores primeiro: "são joão da barra" não pode ser decidido por
    # um município de nome mais curto contido nele.
    for city in sorted(CITY_TO_REGION, key=len, reverse=True):
        if _contains(normalized, city):
            return CITY_TO_REGION[city]

    return ESTADUAL_NACIONAL


def classify_job_type(text: str, default: str) -> str:
    """Refina o tipo declarado pelo scraper com pistas do título.

    O padrão da fonte manda quando ela é um agregador de notícia: uma matéria
    sobre um concurso continua sendo notícia, não a publicação oficial. Para
    as demais fontes, pistas fortes no título (residência, processo seletivo)
    valem mais que o padrão genérico.
    """
    if default == NOTICIA:
        return NOTICIA

    normalized = strip_accents(text).lower()

    if any(_contains(normalized, marker) for marker in _RESIDENCIA_MARKERS):
        return RESIDENCIA

    if any(_contains(normalized, marker) for marker in _PSS_MARKERS):
        return PROCESSO_SELETIVO

    return default


def region_label(region: Optional[str]) -> str:
    return REGION_LABELS.get(region or "", "Não classificada")


def job_type_label(job_type: Optional[str]) -> str:
    return JOB_TYPE_LABELS.get(job_type or "", "Não classificado")


# ==========================================
# SITUAÇÃO DAS INSCRIÇÕES
# ==========================================
# Ao contrário de região e tipo, isto NÃO é uma dimensão de assinatura —
# ninguém pede para receber vaga encerrada. Por isso não precisa de paridade
# com o Worker (ver tests/test_taxonomy_parity.py): serve para suprimir alerta
# e marcar o site, não para decidir quem recebe o quê.
ABERTO = "aberto"
ENCERRADO = "encerrado"
#: Padrão honesto e deliberado. Uma fonte que não diz "encerradas" está
#: dizendo "consulte o edital", nunca "está aberto" — inferir abertura de um
#: silêncio produziria exatamente o erro que este campo existe para evitar.
DESCONHECIDO = "desconhecido"

STATUSES = [ABERTO, ENCERRADO, DESCONHECIDO]

STATUS_LABELS = {
    ABERTO: "Inscrições abertas",
    ENCERRADO: "Inscrições encerradas",
    DESCONHECIDO: "Prazo não informado",
}

# Procedência da conclusão sobre a situação — é ela que decide o que o sistema
# tem PERMISSÃO de fazer, e não apenas o que ele sabe.
FONTE = "fonte"  # a própria fonte declarou (ex: MedGrupo, coluna "encerradas")
CRONOGRAMA = "cronograma"  # tabela de cronograma do edital, formato fixo
TEXTO = "texto"  # lido da redação corrida do edital — admite erro

#: Origens confiáveis o bastante para CALAR um alerta.
#:
#: `TEXTO` fica de fora de propósito, e essa é a regra central do recurso: o
#: custo do erro é assimétrico. Anunciar uma vaga já fechada gasta uma
#: mensagem; silenciar uma vaga aberta custa a oportunidade — que é a razão de
#: o projeto existir. Então a leitura incerta pode informar, nunca esconder.
TRUSTED_STATUS_SOURCES = frozenset({FONTE, CRONOGRAMA})


def status_label(status: Optional[str]) -> str:
    return STATUS_LABELS.get(status or "", STATUS_LABELS[DESCONHECIDO])


def can_suppress_alert(status: Optional[str], status_source: Optional[str]) -> bool:
    """Se dá para deixar de avisar sobre esta vaga por ela estar encerrada."""
    return status == ENCERRADO and status_source in TRUSTED_STATUS_SOURCES
