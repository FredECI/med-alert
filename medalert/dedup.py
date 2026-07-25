"""Assinatura de conteúdo para reconhecer o MESMO concurso vindo de fontes diferentes.

O dedupe primário do projeto é por link (constraint UNIQUE no banco), o que
resolve bem repetição dentro de uma mesma fonte. Só que o mesmo edital
publicado pelo IBAM e pelo portal da própria prefeitura tem links diferentes
— vira duas linhas no banco e dois alertas no Telegram para a mesma vaga.

Esta assinatura é deliberadamente CONSERVADORA: só devolve uma chave quando
encontra os DOIS sinais fortes juntos (município reconhecido + número de
edital no formato NN/AAAA). Sem os dois, devolve None e nada é deduplicado.
A assimetria é intencional — um alerta duplicado incomoda, mas esconder uma
vaga que na verdade era diferente faz o usuário perder a oportunidade, que é
justamente o que o projeto existe para evitar.
"""
import re
import unicodedata
from typing import Optional

# Só nomes de MUNICÍPIO entram aqui. Marcadores estaduais/regionais ("rj",
# "rio de janeiro", "região dos lagos") ficam de fora de propósito: são
# genéricos demais e uma coincidência de número de edital entre dois
# municípios diferentes do estado não pode virar uma fusão indevida.
_CITY_VOCABULARY = [
    "macae", "rio das ostras", "campos dos goytacazes", "carapebus",
    "quissama", "cabo frio", "buzios", "sao joao da barra", "casimiro",
    "saquarema", "araruama", "arraial do cabo", "niteroi", "sao goncalo",
    "duque de caxias", "nova iguacu", "petropolis", "volta redonda",
]

# Datas no formato dd/mm/aaaa contêm um "mm/aaaa" que se parece com número de
# edital. Sem remover isso antes, "29/06/2026" virava a assinatura "6/2026" —
# e duas vagas DIFERENTES da mesma cidade publicadas no mesmo mês seriam
# fundidas. Caso real: dois anúncios distintos do JC Concursos sobre Volta
# Redonda, ambos reduzidos a "volta redonda|6/2026".
_DATE_RE = re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b")

# Ex: "Ed. 01/2026", "Edital nº 003/2026", "Concurso Público 1/2026"
_EDITAL_NUMBER_RE = re.compile(r"\b(\d{1,4})\s*/\s*(20\d{2})\b")

# O número só conta como identificador de edital quando o texto logo antes
# dele o qualifica — do contrário qualquer "N/AAAA" solto viraria chave.
# A busca é por presença na janela (e não por adjacência exata) porque a
# ordem das palavras varia muito entre fontes: "Ed. 01/2026",
# "Concurso Público nº 001/2026", "Processo Seletivo 04/2026".
_EDITAL_MARKER_RE = re.compile(
    r"\bedital\b|\beditais\b|\bed\.|\bconcursos?\b|\bprocessos?\s+seletivos?\b"
    r"|\bselecao\b|\bpss\b|\bn[º°]|\bnumero\b"
)

#: Quanto texto antes do número é inspecionado em busca do qualificador.
_MARKER_LOOKBEHIND = 40


def _strip_accents(text: str) -> str:
    """Remove acentos para o casamento não depender de 'macaé' vs 'macae'.

    Resolve na origem a duplicação manual de variantes acentuadas que o
    restante das listas de filtro ainda carrega.
    """
    decomposed = unicodedata.normalize("NFD", text)
    return "".join(char for char in decomposed if unicodedata.category(char) != "Mn")


def build_signature(title: str) -> Optional[str]:
    """Devolve uma chave estável do tipo 'casimiro|1/2026', ou None.

    None significa "não tenho confiança suficiente para afirmar que duas
    vagas são a mesma" — e nesse caso nada é deduplicado.
    """
    normalized = _strip_accents(str(title)).lower()

    numero = _find_edital_number(normalized)
    if not numero:
        return None

    cities = sorted({city for city in _CITY_VOCABULARY if _contains_city(normalized, city)})
    if not cities:
        return None

    return f"{'+'.join(cities)}|{numero}"


def _find_edital_number(normalized_text: str) -> Optional[str]:
    """Número do edital no formato canônico 'N/AAAA', ou None."""
    # Datas fora do caminho antes de procurar o número (ver _DATE_RE).
    without_dates = _DATE_RE.sub(" ", normalized_text)

    for match in _EDITAL_NUMBER_RE.finditer(without_dates):
        prefix = without_dates[max(0, match.start() - _MARKER_LOOKBEHIND):match.start()]
        if _EDITAL_MARKER_RE.search(prefix):
            # Zeros à esquerda variam entre fontes ("01/2026" vs "001/2026").
            return f"{int(match.group(1))}/{match.group(2)}"

    return None


def _contains_city(normalized_text: str, city: str) -> bool:
    return re.search(r"\b" + re.escape(city) + r"\b", normalized_text) is not None
