"""Classificação das vagas em REGIÃO e TIPO — as duas dimensões pelas quais o
assinante vai poder filtrar o que recebe.

Sem isso não há o que filtrar: até aqui o robô só guardava título e link, sem
nenhuma noção de onde a vaga fica nem de que natureza ela é.
"""
import re
from typing import List, Optional

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
# ESPECIALIDADES
# ==========================================
# Famílias, não especialidades individuais. O CFM reconhece 55 especialidades e
# o próprio MedGrupo lista 144 opções; com essa granularidade "Cardiologia"
# teria 4 vagas no acervo inteiro e o filtro pareceria quebrado. Agrupadas,
# toda família tem volume utilizável — e cabem numa tela de Telegram.
CLINICA_MEDICA = "clinica_medica"
CIRURGIA = "cirurgia"
PEDIATRIA = "pediatria"
GINECO_OBSTETRICIA = "gineco_obstetricia"
ANESTESIOLOGIA = "anestesiologia"
INTENSIVA_EMERGENCIA = "intensiva_emergencia"
ATENCAO_PRIMARIA = "atencao_primaria"
PSIQUIATRIA = "psiquiatria"
DIAGNOSTICO = "diagnostico"
ESPECIALIDADES_CLINICAS = "especialidades_clinicas"
TRABALHO_PERICIA = "trabalho_pericia"
#: Opção de verdade, e não ausência de resposta.
#:
#: 72% das vagas não dizem a especialidade em lugar nenhum legível. Se o filtro
#: funcionasse como região e tipo, quem marcasse "Pediatria" deixaria de
#: receber a maioria de tudo — inclusive editais que TÊM vaga de pediatria mas
#: não dizem no título. Sendo uma chave normal, marcada por padrão, a regra de
#: correspondência continua sendo interseção simples, sem exceção nenhuma no
#: código, e o comportamento padrão é inclusivo.
NAO_ESPECIFICADA = "nao_especificada"

SPECIALTIES = [
    CLINICA_MEDICA,
    CIRURGIA,
    PEDIATRIA,
    GINECO_OBSTETRICIA,
    ANESTESIOLOGIA,
    INTENSIVA_EMERGENCIA,
    ATENCAO_PRIMARIA,
    PSIQUIATRIA,
    DIAGNOSTICO,
    ESPECIALIDADES_CLINICAS,
    TRABALHO_PERICIA,
    NAO_ESPECIFICADA,
]

SPECIALTY_LABELS = {
    CLINICA_MEDICA: "Clínica médica",
    CIRURGIA: "Cirurgia e especialidades cirúrgicas",
    PEDIATRIA: "Pediatria e neonatologia",
    GINECO_OBSTETRICIA: "Ginecologia e obstetrícia",
    ANESTESIOLOGIA: "Anestesiologia",
    INTENSIVA_EMERGENCIA: "Terapia intensiva e emergência",
    ATENCAO_PRIMARIA: "Saúde da família e atenção básica",
    PSIQUIATRIA: "Psiquiatria e saúde mental",
    DIAGNOSTICO: "Diagnóstico por imagem e patologia",
    ESPECIALIDADES_CLINICAS: "Outras especialidades clínicas",
    TRABALHO_PERICIA: "Medicina do trabalho, perícia e auditoria",
    NAO_ESPECIFICADA: "Sem especialidade identificada",
}

#: Vocabulário de cada família. Escrito sem acento porque a comparação
#: normaliza o texto antes. Os termos mais longos são testados primeiro (ver
#: classify_specialties), para "cirurgia pediátrica" não virar só "cirurgia".
SPECIALTY_TERMS = {
    CLINICA_MEDICA: ["clinica medica", "clinico geral", "clinica geral", "medico clinico",
                     "clinico(a)", "medicina interna"],
    CIRURGIA: ["cirurg", "ortoped", "traumatolog", "urolog", "neurocirurg", "vascular",
               "angiolog", "proctolog", "coloproctolog", "mastolog", "buco", "plastic",
               "bariatric", "transplante", "queimado"],
    PEDIATRIA: ["pediatr", "neonatolog", "neonatal"],
    GINECO_OBSTETRICIA: ["ginecolog", "obstetr", "obstetra", "tocoginecolog", "medicina fetal",
                         "reproducao assistida", "reproducao humana", "sexologia"],
    ANESTESIOLOGIA: ["anestesi"],
    INTENSIVA_EMERGENCIA: ["intensiv", "terapia intensiva", "uti", "urgenc", "emergenc",
                           "plantonista", "samu", "pronto socorro", "pronto-socorro"],
    # "familia e da comunidade" além de "familia e comunidade": o nome oficial
    # da especialidade tem o segundo artigo, e sem ele o termo não casava.
    ATENCAO_PRIMARIA: ["saude da familia", "familia e comunidade", "familia e da comunidade",
                       "atencao basica", "atencao primaria", "estrategia saude",
                       "medicina preventiva"],
    PSIQUIATRIA: ["psiquiatr", "saude mental", "dependencia quimica", "psicoterapia"],
    DIAGNOSTICO: ["radiolog", "ultrassonograf", "ultrassom", "patolog", "endoscop",
                  "diagnostico por imagem", "medicina nuclear", "citopatolog", "hemoterap",
                  "densitometria", "mamografia", "radioterapia", "neurofisiologia",
                  "ecocardiograf", "ecocardiograma", "ergometria"],
    ESPECIALIDADES_CLINICAS: ["cardiolog", "endocrinolog", "nefrolog", "neurolog", "pneumolog",
                              "reumatolog", "gastroenterolog", "hepatolog", "dermatolog",
                              "oftalmolog", "otorrino", "oncolog", "hematolog", "infectolog",
                              "geriatr", "alergolog", "imunolog", "nutrolog", "paliativ",
                              "genetica medica", "medicina esportiva", "acupuntura",
                              "homeopat", "hansenolog", "medicina tropical", "toxicolog",
                              "medicina do sono", "medicina da dor", "foniatria",
                              "medicina fisica", "reabilitacao", "eletrofisiologia",
                              "estimulacao cardiaca", "medicina do adolescente"],
    TRABALHO_PERICIA: ["medicina do trabalho", "pericia", "perito", "auditor", "medicina legal",
                       "regulacao", "medico do trabalho", "medicina de trafego",
                       "medicina aeroespacial"],
}


def classify_specialties(text: str) -> List[str]:
    """Famílias de especialidade citadas no texto.

    Devolve LISTA porque especialidade é multivalorada, ao contrário de região
    e tipo: um único edital do RioSaúde abre vinte cargos médicos diferentes.

    Vazio quando nada é reconhecido — e quem chama traduz isso em
    NAO_ESPECIFICADA, que é uma resposta, não uma falha.

    Feito para texto CURTO e estruturado: título da vaga, lista de cargos,
    nome de programa. Não use no corpo inteiro do edital: ali "clínica médica"
    aparece em bibliografia de prova, tabela de salário, nome de supervisor e
    pré-requisito do candidato — medido em 219 ocorrências irrelevantes contra
    ~80 de vaga real, o que marcaria quase todo edital com quase toda família.
    """
    normalized = strip_accents(text).lower()
    achadas = [
        familia
        for familia, termos in SPECIALTY_TERMS.items()
        if any(termo in normalized for termo in termos)
    ]
    return [f for f in SPECIALTIES if f in achadas]  # ordem estável da taxonomia


def specialty_labels(keys: Optional[List[str]]) -> List[str]:
    return [SPECIALTY_LABELS.get(k, k) for k in (keys or [])]


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


def status_from_deadline(deadline: Optional[str], today: str) -> str:
    """Situação deduzida do prazo lido no edital.

    A comparação é estrita: no próprio dia do encerramento a vaga ainda conta
    como aberta. O edital costuma dar até as 23h59 daquele dia, e antecipar o
    fechamento em 24 horas tiraria do radar justamente quem corre no último dia.
    """
    if not deadline:
        return DESCONHECIDO
    return ENCERRADO if deadline < today else ABERTO
