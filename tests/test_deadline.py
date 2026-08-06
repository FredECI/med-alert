"""Testes de unidade do extrator de prazo.

Complementam o conjunto de referência (test_deadline_corpus.py), que mede o
extrator contra editais inteiros. Aqui cada caso isola UM comportamento, para
que uma falha aponte o que quebrou em vez de dizer só "um edital mudou".

As redações são reduções de editais reais — nenhuma foi inventada.
"""
from medalert.deadline import extract_deadline


def _com(trecho: str) -> str:
    """Envolve o trecho em texto neutro: o extrator lê documentos, não frases."""
    return "edital de processo seletivo para residencia medica. " + trecho + " demais disposicoes."


# ==========================================
# Formatos que precisam ser lidos
# ==========================================
def test_reads_a_range_after_the_anchor():
    assert extract_deadline(_com(
        "regras para a inscricao: o candidato devera acessar, entre os dias "
        "13/05/2026 a 20/05/2026, para preenchimento do formulario"
    )) == "2026-05-20"


def test_reads_a_range_written_out_in_words():
    assert extract_deadline(_com(
        "as inscricoes serao efetuadas de 12 de janeiro de 2026 a 23 de janeiro de 2026"
    )) == "2026-01-23"


def test_reads_a_range_that_precedes_the_anchor():
    """A abertura do edital costuma pôr o período antes do que ele delimita."""
    assert extract_deadline(_com(
        "tornam publico que estarao abertas, no periodo de 24/11/2025 a 23/01/2026, "
        "as inscricoes para o processo seletivo"
    )) == "2026-01-23"


def test_reads_a_schedule_row_where_the_date_comes_first():
    assert extract_deadline(_com(
        "cronograma 15/09/2025 inicio das inscricoes 19/11/2025 encerramento das inscricoes "
        "25/11/2025 comissao de heteroidentificacao"
    )) == "2025-11-19"


def test_reads_a_schedule_row_with_a_time_between_date_and_label():
    assert extract_deadline(_com(
        "13/05/2026 / 17h30min inicio das inscricoes on-line "
        "20/05/2026 / 23h59min termino das inscricoes on-line"
    )) == "2026-05-20"


def test_reads_two_adjacent_dates_as_start_and_end_columns():
    assert extract_deadline(_com(
        "periodo de inscricoes (exclusivamente online ) 14/10/2025 21/11/2025 site da banca"
    )) == "2025-11-21"


def test_a_start_without_a_year_does_not_prevent_reading_the_end():
    assert extract_deadline(_com(
        "periodo de inscricao 08/12 a 19/01/2026 ate 18h"
    )) == "2026-01-19"


# ==========================================
# Armadilhas — datas que NÃO são o prazo
# ==========================================
def test_ignores_the_official_gazette_date():
    assert extract_deadline(_com(
        "na secao 3 do dou no 142, de 30/07/2025, onde se le: 2.2 inscricoes pela internet"
    )) is None


def test_ignores_the_payment_deadline():
    """O boleto vence depois do encerramento; lê-lo daria um prazo maior que
    o verdadeiro, e a regra da maior data faria dele o escolhido."""
    assert extract_deadline(_com(
        "as inscricoes serao efetuadas de 20/10/2025 a 29/12/2025. o pagamento da taxa de "
        "inscricao devera ser feito ate o primeiro dia util apos o encerramento das "
        "inscricoes 30/12/2025"
    )) == "2025-12-29"


def test_ignores_a_date_belonging_to_the_previous_table_row():
    """Numa tabela sem separador, o fim do intervalo de uma linha encosta no
    rótulo da linha seguinte — e 05/01 é o fim da ISENÇÃO, não das inscrições.

    O trecho reproduz a Santa Casa de Campos, onde a prosa traz o prazo certo
    e a tabela oferece o engano. Sem a defesa, 05/01 virava candidata; ali ela
    perdeu para 23/01 por ser menor, mas num edital com o prazo real mais cedo
    teria vencido e produzido erro com aparência de acerto.
    """
    assert extract_deadline(_com(
        "as inscricoes serao realizadas no periodo de 29/12/2025, ate as 23:59 horas de "
        "23/01/2026. cronograma: isencao da taxa de inscricao 29/12/2025 a 05/01/2026 "
        "encerramento das inscricoes 23/01/2026 divulgacao"
    )) == "2026-01-23"


def test_ignores_the_appeal_window_that_mentions_inscriptions():
    assert extract_deadline(_com(
        "interposicao de recursos sobre a homologacao das inscricoes 17 e 18/01/2026"
    )) is None


def test_a_bad_anchor_does_not_shadow_a_good_one():
    """Regressão: a varredura não sobrepunha, então "a taxa de inscrição será
    devolvida" casava primeiro, consumia o trecho e impedia a âncora seguinte
    — a que trazia a resposta — de ser avaliada."""
    assert extract_deadline(_com(
        "em nenhuma hipotese a taxa de inscricao sera devolvida. 3.7. da inscricao no "
        "processo seletivo as inscricoes serao efetuadas de 01/10/2025 a 28/11/2025"
    )) == "2025-11-28"


# ==========================================
# Conflito e abstenção
# ==========================================
def test_conflicting_dates_resolve_to_the_latest():
    """Só damos a vaga por encerrada quando hoje passou de todas as leituras."""
    assert extract_deadline(_com(
        "as inscricoes vao entre os dias 18/05/2026 a 28/05/2026. "
        "cronograma: 31/05/2026 / 23h59min termino das inscricoes"
    )) == "2026-05-31"


def test_a_struck_out_date_left_side_by_side_resolves_to_the_latest():
    """Retificação: o tachado do PDF é visual e some na extração."""
    assert extract_deadline(_com(
        "as inscricoes estarao abertas das 10h do dia 07/07/2025 ate as 23h59 "
        "do dia 18/07/2025 30/07/2025"
    )) == "2025-07-30"


def test_abstains_when_there_is_no_date_near_an_anchor():
    assert extract_deadline(_com(
        "as inscricoes serao realizadas no periodo constante no anexo i deste edital"
    )) is None


def test_abstains_on_empty_or_missing_text():
    assert extract_deadline("") is None
    assert extract_deadline(None) is None


def test_an_impossible_date_is_discarded_instead_of_raising():
    """Erro de digitação do próprio edital não pode derrubar a leitura."""
    assert extract_deadline(_com(
        "as inscricoes serao efetuadas de 01/10/2025 a 31/02/2026"
    )) is None


def test_a_period_that_ends_before_it_starts_is_not_read():
    """Leitura errada se denuncia sozinha — e sai de graça."""
    assert extract_deadline(_com(
        "estarao abertas, no periodo de 24/11/2025 a 23/01/2025, as inscricoes"
    )) is None
