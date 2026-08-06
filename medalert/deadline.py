"""Extração do prazo de inscrição a partir do texto de um edital.

Abster é a resposta correta quando não há certeza, e não um estado
provisório: um prazo errado esconde vaga aberta, que é o oposto da função do
projeto. Por isso o único critério que quebra o CI é `erros > 0` — cobertura
baixa é aceitável, erro não é (ver tests/test_deadline_corpus.py).

Como funciona
-------------
Não é um casamento de padrão que devolve a primeira data encontrada. São três
etapas separadas, porque a dificuldade não é achar uma data — um edital tem
dezenas — e sim decidir qual delas é o fim das inscrições:

1. **Candidatas.** Cada padrão conhecido de cronograma propõe uma data, junto
   com o trecho entre a âncora ("inscrição") e a data. Um edital pode gerar
   várias candidatas, e isso é esperado.
2. **Vetos.** A candidata é descartada quando o caminho até ela passa por
   outro assunto — pagamento de boleto, prova, recurso, publicação no D.O.U.
   Esses são os erros que o conjunto de referência registrou como armadilhas
   reais, não hipóteses.
3. **Escolha.** Sem candidata, abstém. Com candidatas, vale a MAIOR.

REGRA DO CONFLITO — vale A MAIOR data
-------------------------------------
Quando o edital oferece mais de uma data de encerramento, escolhe-se a maior.
Acontece em dois casos reais do conjunto: o documento que se contradiz (a
prosa diz 28/05 e o cronograma do anexo diz 31/05) e a retificação cujo
tachado sumiu na extração, deixando "18/07/2025 30/07/2025" colados — e
retificação quase sempre prorroga, então a maior costuma ser a versão vigente.

O fundamento é a mesma assimetria que rege o resto do projeto: com a maior, a
vaga só é dada por encerrada quando HOJE já passou de TODAS as leituras, isto
é, quando não existe interpretação do edital em que ela ainda esteja aberta.
Escolher a menor a fecharia antes da hora em troca de nada.

Duas ressalvas que precisam sobreviver a qualquer mudança aqui:

1. A regra vale só entre candidatas já validadas como fim de inscrição. Como
   máximo global do documento ela escolheria sempre a data da prova ou do
   resultado, que são posteriores por definição — e o erro seria sistemático,
   não ocasional.
2. Data escolhida por conflito é menos certa que data por concordância, e por
   isso quem chama a marca com `status_source = TEXTO`: exibe com ressalva e
   nunca cala um alerta (ver taxonomy.can_suppress_alert). Mostrar 31/05
   quando a prosa dizia 28/05 pode fazer alguém se programar para o dia errado.
"""
import re
from datetime import date
from typing import List, Optional, Tuple

from medalert.editaltext import normalize

_MESES = {
    "janeiro": 1, "fevereiro": 2, "marco": 3, "abril": 4, "maio": 5, "junho": 6,
    "julho": 7, "agosto": 8, "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
}

_DATA_NUM = r"\d{1,2}/\d{1,2}/\d{4}"
_DATA_EXT = r"\d{1,2} de (?:" + "|".join(_MESES) + r") de \d{4}"
_DATA = rf"(?:{_DATA_NUM}|{_DATA_EXT})"

#: Âncora: qualquer flexão de "inscrição"/"inscrições".
_ANCORA = r"inscric\w*"

#: Ligação entre o início e o fim de um período. "a", "e", "ate", "à".
_ATE = r"(?:a|e|ate|até)"

# ==========================================
# Padrões de candidata
# ==========================================
# Cada um devolve (data_do_fim, trecho_entre_a_ancora_e_a_data).
#
# Dois formatos convivem nos editais e precisam de padrões diferentes: uns
# põem o rótulo DEPOIS da data ("19/11/2025 encerramento das inscrições"),
# outros ANTES ("período de inscrição 26/01/2026 a 04/02/2026").

#: Rótulo depois da data, com hora opcional no meio. Cobre o cronograma
#: tabular do RioSaúde ("20/05/2026 / 23h59min término das inscrições on-line")
#: e o do Hospital Adventista Silvestre, sem hora.
_ROTULO_DEPOIS = re.compile(
    rf"({_DATA})\s*(?:/\s*\d{{1,2}}h\d{{0,2}}\s*(?:min)?\s*)?"
    rf"(?:termino|encerramento|fim|ultimo dia)\s+d(?:as|e)\s+{_ANCORA}"
)

#: Cronograma tabular sem separador de linha: o fim do intervalo de uma linha
#: encosta no rótulo da linha seguinte. Foi o que aconteceu na Santa Casa de
#: Campos — "isenção da taxa de inscrição 29/12/2025 a 05/01/2026 encerramento
#: das inscrições 23/01/2026" —, onde 05/01 é o fim da ISENÇÃO, não das
#: inscrições. Uma data precedida de "<data> a" pertence à linha de cima.
_CAUDA_DE_INTERVALO = re.compile(rf"{_DATA_NUM}\s*{_ATE}\s*$")

#: Rótulo antes de um intervalo: a data que interessa é a SEGUNDA.
#: A distância entre âncora e intervalo chega a ~120 caracteres num dos
#: editais (o do IEDE intercala o endereço do site), daí a folga.
_INTERVALO_APOS_ANCORA = re.compile(
    rf"{_ANCORA}(.{{0,130}}?)({_DATA}|\d{{1,2}}/\d{{1,2}}|\d{{1,2}})\s*{_ATE}\s*({_DATA})"
)

#: Rótulo antes, com hora e um único fim ("até as 17h00 do dia 10/10/2025").
_ATE_O_DIA_APOS_ANCORA = re.compile(
    rf"{_ANCORA}(.{{0,160}}?)ate\s+as?\s+[\dh:.]{{2,8}}\s*(?:\([^)]{{0,40}}\))?\s*"
    rf"(?:h(?:oras)?\s*)?(?:d[oe]\s+dia\s+|de\s+)({_DATA})"
)

# ==========================================
# Vetos
# ==========================================
#: Assuntos que também carregam data e ficam perto de "inscrição" no texto.
#: Todos vieram de erro real observado no conjunto de referência: o boleto do
#: primeiro dia útil APÓS o encerramento, o prazo de RQE do candidato, a data
#: de publicação no Diário Oficial.
_VETOS = (
    "pagamento", "boleto", "bancari", "taxa", "isencao",
    "prova", "resultado", "recurso", "matricula", "homologa", "impugnacao",
    "d.o.u", "diario oficial", "publicacao", "publicado",
    "rqe", "resolucao", "portaria", "decreto", "nascimento",
)


def _para_iso(texto_data: str) -> Optional[str]:
    texto_data = texto_data.strip()
    if "/" in texto_data:
        dia, mes, ano = texto_data.split("/")
    else:
        dia, _, mes_nome, _, ano = texto_data.split(" ", 4)
        mes = _MESES.get(mes_nome.strip())
        if not mes:
            return None
    try:
        return date(int(ano), int(mes), int(dia)).isoformat()
    except ValueError:
        # Data impossível (31/02, ano absurdo). Descartar é melhor do que
        # deixar uma exceção derrubar a rodada por causa de um erro de digitação.
        return None


def _vetado(trecho: str) -> bool:
    return any(veto in trecho for veto in _VETOS)


#: Quanto se olha para trás da âncora em busca do assunto que a governa.
_JANELA_ANTES = 120


def _inicio_da_janela(texto: str, ancora: int, fins_de_data: List[int]) -> int:
    """Onde começa o contexto que decide se a candidata vale.

    Vai da âncora para trás até a data anterior — que num cronograma é o fim
    da linha de cima. Sem esse corte, o veto leria o evento anterior e
    descartaria uma data boa; sem olhar para trás nenhum, "interposição de
    recursos sobre a homologação das inscrições 17 e 18/01/2026" passaria como
    prazo de inscrição, que foi o erro real que motivou este código.
    """
    piso = max(0, ancora - _JANELA_ANTES)
    anteriores = [fim for fim in fins_de_data if fim <= ancora]
    return max(piso, anteriores[-1]) if anteriores else piso


def _candidatas(texto: str) -> List[Tuple[str, str]]:
    """(data ISO, trecho que a justifica) para cada leitura plausível do fim."""
    fins_de_data = [m.end() for m in re.finditer(_DATA, texto)]
    achadas: List[Tuple[str, str]] = []

    for m in _ROTULO_DEPOIS.finditer(texto):
        if _CAUDA_DE_INTERVALO.search(texto[max(0, m.start() - 18):m.start()]):
            continue
        # Rótulo colado na data: a própria expressão já diz do que se trata.
        achadas.append((m.group(1), ""))

    for padrao in (_INTERVALO_APOS_ANCORA, _ATE_O_DIA_APOS_ANCORA):
        for m in padrao.finditer(texto):
            inicio = _inicio_da_janela(texto, m.start(), fins_de_data)
            achadas.append((m.groups()[-1], texto[inicio:m.end()]))

    resultado = []
    for bruta, contexto in achadas:
        iso = _para_iso(bruta)
        if iso and not _vetado(contexto):
            resultado.append((iso, contexto))
    return resultado


def _retificacoes(texto: str, aceitas: List[str]) -> List[str]:
    """Data colada logo depois de uma candidata já aceita.

    É a assinatura da retificação: no PDF a data antiga aparece tachada, mas o
    tachado é visual e some na extração, deixando as duas lado a lado
    ("18/07/2025 30/07/2025"). Sem isto, só a versão revogada seria lida.
    """
    extras = []
    for iso in set(aceitas):
        dia, mes, ano = iso.split("-")[::-1]
        for formato in (f"{dia}/{mes}/{ano}", f"{int(dia)}/{int(mes)}/{ano}"):
            for m in re.finditer(re.escape(formato) + rf"\s{{1,3}}({_DATA_NUM})", texto):
                seguinte = _para_iso(m.group(1))
                if seguinte:
                    extras.append(seguinte)
    return extras


def extract_deadline(edital_text: str) -> Optional[str]:
    """Data de encerramento das inscrições (AAAA-MM-DD), ou None ao abster.

    None não significa "falhei": significa "o edital não diz, ou diz de forma
    que não dá para afirmar com segurança". Quem chama trata os dois casos
    igual — a vaga aparece sem prazo, exatamente como aparecia antes.
    """
    if not edital_text:
        return None

    texto = normalize(edital_text)
    candidatas = [iso for iso, _ in _candidatas(texto)]
    if not candidatas:
        return None

    candidatas.extend(_retificacoes(texto, candidatas))
    return max(candidatas)
