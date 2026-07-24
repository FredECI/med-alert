"""Testes de relevância e filtro regional — cobre a regressão do bug de substring solta."""
from medalert.filtering import is_in_target_state, is_relevant


def test_is_relevant_matches_whole_word_keyword():
    assert is_relevant("Prefeitura abre concurso para médico plantonista") is True


def test_is_relevant_rejects_unrelated_text():
    assert is_relevant("Concurso para motorista de ônibus") is False


def test_is_in_target_state_matches_rj_as_whole_word():
    assert is_in_target_state("Concurso público no estado do RJ") is True


def test_is_in_target_state_matches_full_state_name():
    assert is_in_target_state("Vaga disponível no Rio de Janeiro") is True


def test_is_in_target_state_does_not_false_positive_on_substring():
    """A versão original comparava também `state in text_lower` (substring pura
    sobre o texto inteiro), então um filtro curto como 'rj' batia falso-positivo
    em qualquer palavra que contivesse essas duas letras em sequência — por
    exemplo 'surja', que não tem nenhuma relação com o estado do Rio de Janeiro."""
    assert is_in_target_state("É possível que surja uma vaga em breve no exterior") is False


def test_is_in_target_state_matches_rj_inside_url_slug():
    """Usado para validar o slug de estado embutido em URLs (ex: Trabalha Brasil),
    onde 'rj' aparece isolado por hífens/barras, não por espaços."""
    assert is_in_target_state("vagas-de-emprego-em-macae-rj/medico/123") is True


def test_is_in_target_state_rejects_other_state_slug():
    assert is_in_target_state("vagas-de-emprego-em-bom-jesus-do-norte-es/medico/123") is False
