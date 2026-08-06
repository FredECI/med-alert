"""Testes da normalização do texto extraído de PDF.

Todos os casos aqui são defeitos reais, copiados de editais do conjunto de
referência. Nenhum é hipotético: o texto que sai de um PDF não é o texto que
se lê na tela, e cada um destes fazia uma data válida passar despercebida.
"""
from medalert.editaltext import normalize, strip_accents


def test_spaces_around_slashes_are_closed():
    """Faculdade de Medicina de Campos: a linha inteira vem espaçada."""
    assert "28/11/2025" in normalize("ate as 12h de 28 / 11 / 2025-sexta feira")


def test_a_space_inside_the_day_is_repaired():
    """RioSaúde 004/2026: o espaço cai DENTRO do dia."""
    assert "14/05/2026" in normalize("entre os dias 08/05/2026 e 1 4/05/2026 para")


def test_a_space_inside_the_year_is_repaired():
    """Instituto Benjamin Constant: "de 2 025"."""
    assert "de 2025 a" in normalize("efetuadas de 5 de dezembro de 2 025 a 26 de janeiro")


def test_accents_and_case_are_flattened():
    assert normalize("Inscrições ATÉ") == "inscricoes ate"


def test_line_breaks_become_single_spaces():
    """A extração quebra o parágrafo em linhas arbitrárias, seguindo o layout
    visual e não a frase — sem colapsar, nenhum padrão de frase casaria."""
    assert normalize("periodo\n  de\n\ninscricao") == "periodo de inscricao"


def test_unrelated_numbers_are_left_alone():
    """O remendo do ano é estreito de propósito: alargá-lo emendaria valores,
    quantidades de vagas e números de lei que nada têm a ver com data."""
    assert normalize("r$ 4 106,09 e 02 vagas") == "r$ 4 106,09 e 02 vagas"


def test_strip_accents_keeps_the_letters():
    assert strip_accents("Inscrições Médicas") == "Inscricoes Medicas"
