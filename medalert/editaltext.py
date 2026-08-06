"""Normalização do texto extraído de um edital em PDF.

Etapa própria, e não um detalhe do extrator de datas, porque é onde nasce boa
parte das falhas: o texto que sai de um PDF não é o texto que se lê na tela.
A extração perde a posição dos caracteres e reintroduz espaços onde o layout
os sugeria, então uma data que o olho lê como "14/05/2026" chega ao código
como "1 4/05/2026" — e qualquer `\\d{2}` falha nela em silêncio.

Todos os defeitos tratados aqui foram observados nos editais reais do
conjunto de referência, nenhum é hipotético.
"""
import re
import unicodedata

#: "28 / 11 / 2025" — o PDF espalha as barras. Visto na Faculdade de Medicina
#: de Campos, onde a linha inteira vem espaçada.
_BARRAS_ESPACADAS = re.compile(r"(?<=\d)\s*/\s*(?=\d)")

#: "1 4/05/2026" — o espaço cai DENTRO do dia. Visto no RioSaúde 004/2026.
_DIA_PARTIDO = re.compile(r"(?<!\d)(\d)\s(\d)(?=/\d{1,2}/\d)")

#: "de 2 025" — o espaço cai dentro do ano. Visto no Instituto Benjamin
#: Constant, tanto em data numérica quanto por extenso.
#:
#: Estreito de propósito: exige que o primeiro dígito seja 1 ou 2 e que não
#: venha vírgula depois. Sem essas duas guardas, o padrão emendava valores
#: monetários — "r$ 4 106,09" virava "r$ 4106,09" —, corrompendo texto que não
#: tem nada a ver com data.
_ANO_PARTIDO = re.compile(r"(?<![\d/])([12])\s(\d{3})(?![\d,])")


def strip_accents(text: str) -> str:
    decomposto = unicodedata.normalize("NFKD", text)
    return "".join(c for c in decomposto if not unicodedata.combining(c))


def normalize(raw_text: str) -> str:
    """Deixa o texto do edital em forma comparável: sem acento, minúsculo,
    espaço colapsado e com as datas remendadas."""
    texto = " ".join(strip_accents(raw_text).lower().split())
    # A ordem importa: juntar as barras primeiro faz o dia partido virar um
    # padrão reconhecível ("1 4 / 05 / 2026" -> "1 4/05/2026" -> "14/05/2026").
    texto = _BARRAS_ESPACADAS.sub("/", texto)
    texto = _DIA_PARTIDO.sub(r"\1\2", texto)
    return _ANO_PARTIDO.sub(r"\1\2", texto)
