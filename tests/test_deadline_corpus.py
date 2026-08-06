"""Portão de qualidade da extração de prazo, e testes do próprio avaliador.

Existe antes do extrator de propósito. A ordem importa: se o conjunto de
referência fosse montado depois, ele acabaria descrevendo o que o extrator já
faz em vez de o que ele precisa fazer.
"""
import pytest

from medalert.deadline import extract_deadline
from medalert.timeutil import format_date_br
from tests.deadline_corpus import avaliar, carregar


@pytest.fixture(scope="module")
def corpus():
    return carregar()


# ==========================================
# O conjunto de referência
# ==========================================
def test_corpus_has_enough_editais_to_be_meaningful(corpus):
    assert len(corpus) >= 30


def test_every_edital_has_its_text_available(corpus):
    """Sem o texto o item não mede nada, e um arquivo faltando passaria
    despercebido como se fosse só mais uma abstenção."""
    for edital in corpus:
        assert edital.texto() is not None


def test_corpus_covers_more_than_one_source(corpus):
    """Um conjunto só de RioSaúde mediria um formato tabular único e daria a
    impressão de que o problema é fácil."""
    assert len({e.fonte for e in corpus}) >= 2


def test_corpus_includes_cases_where_abstaining_is_the_right_answer(corpus):
    """Sem eles o conjunto premiaria um extrator que sempre chuta alguma coisa.

    Sobrou só uma causa de abstenção — o PDF escaneado, onde não há o que ler.
    As demais (documento que se contradiz, retificação com o tachado perdido
    na extração) passaram a ter resposta pela regra do conflito.
    """
    devem_abster = [e for e in corpus if e.encerramento is None]

    assert len(devem_abster) >= 2
    assert all(e.nota for e in devem_abster), "todo caso de abstenção precisa dizer por quê"
    assert all("ESCANEADO" in e.nota.upper() for e in devem_abster)


def test_conflicting_dates_are_resolved_by_the_latest_one(corpus):
    """A vaga só é dada por encerrada quando HOJE já passou de todas as
    leituras — ou seja, quando não existe interpretação do edital em que ela
    ainda esteja aberta. Escolher a menor a fecharia antes da hora, em troca
    de nada."""
    por_regra = [e for e in corpus if e.resolvido_por == "maior data"]

    assert len(por_regra) >= 3, "o conjunto precisa exercitar a regra do conflito"
    assert all(e.encerramento for e in por_regra)


def test_rule_resolved_entries_record_the_dates_that_disagreed(corpus):
    """A anotação precisa mostrar o conflito que a regra resolveu; sem isso,
    quem revisar o conjunto não tem como conferir se a regra foi bem aplicada."""
    for edital in corpus:
        if edital.resolvido_por:
            escolhida = format_date_br(edital.encerramento)
            assert escolhida in edital.nota, f"{edital.arquivo} não mostra a data escolhida"


def test_annotated_deadlines_are_iso_dates(corpus):
    import datetime

    for edital in corpus:
        if edital.encerramento:
            datetime.date.fromisoformat(edital.encerramento)


# ==========================================
# O avaliador
# ==========================================
def test_a_correct_extractor_scores_only_hits():
    por_arquivo = {e.texto(): e.encerramento for e in carregar()}
    perfeito = avaliar(lambda texto: por_arquivo[texto])

    assert perfeito.erros == []
    assert perfeito.abstencoes == 0
    assert perfeito.acertos == perfeito.total


def test_an_extractor_that_always_abstains_never_errs():
    """A base contra a qual todo ganho é medido: silêncio total custa
    cobertura e não custa confiança."""
    mudo = avaliar(lambda _: None)

    assert mudo.erros == []
    assert mudo.abstencoes > 0


def test_guessing_where_the_answer_is_unknowable_counts_as_error():
    """O caso mais importante: um extrator confiante num documento
    inconsistente não pode passar como se tivesse acertado."""
    chutador = avaliar(lambda _: "2026-01-01")

    assert len(chutador.erros) > 0
    assert any("era para abster" in e for e in chutador.erros)


def test_a_wrong_date_is_an_error_not_an_abstention():
    resultado = avaliar(lambda _: "1999-12-31")

    assert resultado.abstencoes == 0
    assert len(resultado.erros) == resultado.total


# ==========================================
# O portão
# ==========================================
def test_the_extractor_never_reports_a_wrong_deadline():
    """O único critério que quebra o CI.

    Cobertura baixa é aceitável e negociável; erro não é. Um prazo errado faz
    uma vaga aberta parecer encerrada — o oposto da função do projeto, e um
    estrago que ninguém reporta, porque a pessoa simplesmente não se candidata.
    """
    resultado = avaliar(extract_deadline)

    assert resultado.erros == [], "\n".join(resultado.erros)
