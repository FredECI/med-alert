"""Testes da assinatura de conteúdo usada para reconhecer o mesmo edital
publicado por fontes diferentes."""
from medalert.dedup import build_signature


def test_same_edital_from_two_sources_produces_the_same_signature():
    """Caso que motivou o mecanismo: o IBAM e o portal da própria prefeitura
    publicam o mesmo processo seletivo com links diferentes."""
    ibam = "[IBAM] Municipio de Casimiro de Abreu - Ed. 01/2026 PS - Processo Seletivo Público"
    prefeitura = "[Casimiro de Abreu] Concurso Público 001/2026 - Secretaria de Saúde"

    assert build_signature(ibam) == build_signature(prefeitura)
    assert build_signature(ibam) == "casimiro|1/2026"


def test_signature_ignores_accent_differences():
    assert build_signature("Macaé - Edital 5/2026") == build_signature("Macae - Edital 5/2026")


def test_signature_normalizes_leading_zeros_in_edital_number():
    assert build_signature("Saquarema Edital 007/2026") == build_signature("Saquarema Edital 7/2026")


def test_different_editions_in_the_same_city_are_not_merged():
    assert build_signature("Saquarema Edital 1/2026") != build_signature("Saquarema Edital 2/2026")


def test_same_edital_number_in_different_cities_is_not_merged():
    """Número de edital só é único dentro do município — sem o nome da cidade
    casando, duas vagas distintas seriam fundidas indevidamente."""
    assert build_signature("Saquarema Edital 1/2026") != build_signature("Araruama Edital 1/2026")


def test_returns_none_without_an_edital_number():
    """Sem os dois sinais fortes preferimos não deduplicar nada."""
    assert build_signature("[Saquarema] Concurso público para a área de saúde") is None


def test_returns_none_without_a_recognizable_city():
    assert build_signature("[PCI RJ] Edital 01/2026 para especialistas em saúde") is None


def test_state_level_markers_do_not_count_as_a_city():
    """'RJ' e 'Rio de Janeiro' são genéricos demais: dois editais de número
    igual em municípios diferentes do estado não podem virar um só."""
    assert build_signature("Governo do RJ - Edital 01/2026") is None


def test_handles_titles_without_any_signal():
    assert build_signature("") is None
    assert build_signature("Notícia qualquer sem número nem cidade") is None


def test_a_date_is_not_mistaken_for_an_edital_number():
    """Regressão encontrada validando contra o banco real: '29/06/2026' contém
    '06/2026', que a versão inicial lia como edital nº 6/2026. Estes dois
    títulos reais do JC Concursos viravam a mesma chave por causa do mês."""
    primeiro = "[JC Concursos] 29/06/2026 - 13:21Concurso da Prefeitura de Volta Redonda RJ"
    segundo = "[JC Concursos] 15/06/2026 - 11:27Concurso da Prefeitura de Volta Redonda RJ"

    assert build_signature(primeiro) is None
    assert build_signature(segundo) is None


def test_two_different_jobs_in_the_same_city_and_month_are_not_merged():
    """Consequência prática do bug da data: sem a correção, duas vagas
    distintas publicadas no mesmo mês colapsariam numa só."""
    enfermeiro = "[Saquarema] 10/07/2026 - Concurso para enfermeiro"
    medico = "[Saquarema] 22/07/2026 - Concurso para médico"

    assert build_signature(enfermeiro) is None
    assert build_signature(medico) is None


def test_number_only_counts_when_qualified_as_an_edital():
    """Um 'N/AAAA' solto no meio do texto não é identificador de edital."""
    assert build_signature("Saquarema tem 3/2026 das vagas preenchidas") is None
    assert build_signature("Saquarema - Edital 3/2026") == "saquarema|3/2026"


def test_recognizes_common_edital_number_formats():
    assert build_signature("Macaé Ed. 12/2026") == "macae|12/2026"
    assert build_signature("Macaé Edital nº 12/2026") == "macae|12/2026"
    assert build_signature("Macaé Processo Seletivo 12/2026") == "macae|12/2026"
    assert build_signature("Macaé Concurso Público nº 012/2026") == "macae|12/2026"
