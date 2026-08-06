"""Testes da classificação por região e tipo — as duas dimensões de filtro —
e da situação das inscrições, que decide o que o robô pode deixar de enviar.
"""
from medalert.taxonomy import (
    ABERTO,
    CAPITAL_METROPOLITANA,
    CONCURSO,
    CRONOGRAMA,
    DESCONHECIDO,
    EMPREGO,
    ENCERRADO,
    ESTADUAL_NACIONAL,
    FONTE,
    NORTE_FLUMINENSE,
    NOTICIA,
    OUTRAS_RJ,
    PROCESSO_SELETIVO,
    REGIAO_DOS_LAGOS,
    RESIDENCIA,
    TEXTO,
    can_suppress_alert,
    classify_job_type,
    classify_region,
    status_from_deadline,
    status_label,
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


def test_only_trusted_sources_may_silence_an_alert():
    """A regra central da situação das inscrições: o custo do erro é
    assimétrico. Anunciar uma vaga já fechada gasta uma mensagem; silenciar
    uma vaga aberta custa a oportunidade que o projeto existe para entregar.
    Por isso a leitura incerta pode informar, mas nunca esconder."""
    assert can_suppress_alert(ENCERRADO, FONTE) is True
    assert can_suppress_alert(ENCERRADO, CRONOGRAMA) is True
    assert can_suppress_alert(ENCERRADO, TEXTO) is False


def test_an_open_or_unknown_job_is_never_silenced():
    assert can_suppress_alert(ABERTO, FONTE) is False
    assert can_suppress_alert(DESCONHECIDO, FONTE) is False
    assert can_suppress_alert(None, None) is False


def test_the_deadline_day_itself_still_counts_as_open():
    """O edital costuma dar até as 23h59 do dia do encerramento. Fechar 24h
    antes tiraria do radar justamente quem corre no último dia."""
    assert status_from_deadline("2026-08-06", today="2026-08-06") == ABERTO


def test_a_deadline_in_the_past_closes_the_job():
    assert status_from_deadline("2026-05-20", today="2026-08-06") == ENCERRADO


def test_no_deadline_means_unknown():
    assert status_from_deadline(None, today="2026-08-06") == DESCONHECIDO


def test_unclassified_status_reads_as_unknown_not_as_open():
    assert status_label(None) == status_label(DESCONHECIDO)
    assert "não informado" in status_label(None)
