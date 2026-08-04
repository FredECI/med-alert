"""Testes da classificação por região e tipo — as duas dimensões de filtro."""
from medalert.taxonomy import (
    CAPITAL_METROPOLITANA,
    CONCURSO,
    EMPREGO,
    ESTADUAL_NACIONAL,
    NORTE_FLUMINENSE,
    NOTICIA,
    OUTRAS_RJ,
    PROCESSO_SELETIVO,
    REGIAO_DOS_LAGOS,
    RESIDENCIA,
    classify_job_type,
    classify_region,
)


def test_region_recognises_each_group():
    assert classify_region("Prefeitura de Macaé abre concurso") == NORTE_FLUMINENSE
    assert classify_region("Concurso em Cabo Frio") == REGIAO_DOS_LAGOS
    assert classify_region("Vaga em Niterói") == CAPITAL_METROPOLITANA
    assert classify_region("Concurso de Petrópolis") == OUTRAS_RJ


def test_region_ignores_accent_differences():
    assert classify_region("prefeitura de macae") == classify_region("Prefeitura de Macaé")


def test_statewide_wins_over_the_capital_name():
    """'Governo do Estado do Rio de Janeiro' contém 'rio de janeiro', mas não
    é uma vaga da capital — o marcador estadual precisa ser testado antes."""
    assert classify_region("Governo do Estado do Rio de Janeiro") == ESTADUAL_NACIONAL
    assert classify_region("Secretaria de Estado de Saúde") == ESTADUAL_NACIONAL


def test_capital_is_detected_when_there_is_no_statewide_marker():
    assert classify_region("Prefeitura do Rio de Janeiro") == CAPITAL_METROPOLITANA


def test_national_employers_are_classified_as_statewide_nacional():
    assert classify_region("Concurso da EBSERH - Área Médica") == ESTADUAL_NACIONAL


def test_region_falls_back_when_no_city_is_recognised():
    """Sem município identificável, ESTADUAL_NACIONAL é a resposta honesta:
    ou a vaga vale para todo o estado, ou não sabemos onde é."""
    assert classify_region("Concurso para médico plantonista") == ESTADUAL_NACIONAL


def test_longer_city_names_win_over_shorter_ones():
    assert classify_region("São João da Barra") == NORTE_FLUMINENSE


def test_news_sources_keep_their_nature():
    """Uma matéria sobre um concurso continua sendo notícia, não a publicação
    oficial — por isso o padrão da fonte manda quando ela é agregador."""
    assert classify_job_type("Concurso público abre 300 vagas", NOTICIA) == NOTICIA


def test_strong_title_clues_refine_non_news_sources():
    assert classify_job_type("Residência médica em cardiologia", CONCURSO) == RESIDENCIA
    assert classify_job_type("Processo seletivo simplificado", CONCURSO) == PROCESSO_SELETIVO


def test_default_is_kept_without_clues():
    assert classify_job_type("Edital 01/2026", CONCURSO) == CONCURSO
    assert classify_job_type("Vaga de Médico Clínico", EMPREGO) == EMPREGO
