"""Testes de relevância, filtro regional e sinal de vaga real."""
from medalert.filtering import has_job_signal, is_in_target_state, is_relevant


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


def test_has_job_signal_matches_concurso():
    assert has_job_signal("Prefeitura abre concurso para área de saúde") is True


def test_has_job_signal_matches_processo_seletivo_but_not_bare_processo():
    assert has_job_signal("Publicado o edital do processo seletivo simplificado") is True
    assert has_job_signal("Existe um processo que ajuda a explicar a obra") is False


def test_has_job_signal_rejects_health_mention_with_no_hiring_language():
    """Caso real: uma matéria patrocinada mencionando 'saúde' de passagem
    não deveria virar 'vaga nova' só porque is_relevant() bate no assunto."""
    assert has_job_signal("Por trás de obras e serviços da área de saúde do seu bairro") is False


def test_is_in_target_state_rejects_bare_campos_as_common_word():
    """'campos' solto era palavra comum demais e ainda casava com cidade de
    outro estado — mesmo tipo de vazamento do caso Espírito Santo."""
    assert is_in_target_state("Concurso para técnico em campos de atuação diversos") is False
    assert is_in_target_state("Vaga de médico em Campos do Jordão SP") is False


def test_is_in_target_state_still_matches_campos_dos_goytacazes():
    assert is_in_target_state("Prefeitura de Campos dos Goytacazes abre concurso") is True


def test_is_in_target_state_matches_campos_city_when_uf_is_present():
    """Fontes que citam a cidade quase sempre trazem a UF junto (ex: o IBAM
    lista 'Municipio de Campos/RJ'), então o 'rj' cobre o caso real."""
    assert is_in_target_state("Municipio de Campos/RJ - Edital 01/2026") is True


def test_has_job_signal_rejects_event_registrations():
    """Termos fracos ('inscrições', 'vagas') aparecem em evento/curso tanto
    quanto em contratação. O primeiro caso é real: veio do portal de notícias
    de Cabo Frio num teste ao vivo e entrou como se fosse vaga."""
    assert has_job_signal("3º Fórum de Saúde do Homem recebe inscrições de profissionais") is False
    assert has_job_signal("Congresso de Medicina abre inscrições para participantes") is False
    assert has_job_signal("Palestra sobre saúde mental tem vagas limitadas") is False
    assert has_job_signal("Curso de capacitação para agentes de saúde abre inscrições") is False


def test_has_job_signal_keeps_weak_term_when_there_is_no_event_context():
    """Sem contexto de evento, 'vagas' continua valendo como sinal — é assim
    que notícia tipo 'Prefeitura abre 50 vagas para médicos' é capturada."""
    assert has_job_signal("Prefeitura abre 50 vagas para médicos na rede municipal") is True


def test_has_job_signal_strong_term_wins_over_event_term():
    """'Concurso ... do curso técnico' tem termo de evento, mas o termo forte
    manda. Também garante que 'curso' não casa dentro de 'concurso'."""
    assert has_job_signal("Concurso público para professor do curso técnico de enfermagem") is True
    assert has_job_signal("Edital de processo seletivo inclui curso de formação") is True
