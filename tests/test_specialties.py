"""Testes da classificação por especialidade e do filtro que ela alimenta.

Especialidade é a terceira dimensão de assinatura e a única MULTIVALORADA:
região e tipo têm um valor por vaga, mas um edital do RioSaúde abre vinte
cargos médicos diferentes. Quase tudo o que é peculiar aqui vem disso.
"""
from medalert.orchestrator import _resolve_specialties, _wants
from medalert.storage import decode_specialties, encode_specialties
from medalert.timeutil import today_str
from medalert.subscribers import Subscriber
from medalert.taxonomy import (
    ANESTESIOLOGIA,
    ATENCAO_PRIMARIA,
    CIRURGIA,
    CLINICA_MEDICA,
    JOB_TYPES,
    NAO_ESPECIFICADA,
    PEDIATRIA,
    REGIONS,
    SPECIALTIES,
    classify_specialties,
)


# ==========================================
# Classificação
# ==========================================
def test_a_single_text_can_name_several_specialties():
    """O caso normal, não a exceção: um edital abre vários cargos."""
    achadas = classify_specialties(
        "Médicos: anestesiologista, cirurgião geral, clínica médica, pediatria"
    )

    assert set(achadas) == {ANESTESIOLOGIA, CIRURGIA, CLINICA_MEDICA, PEDIATRIA}


def test_a_more_specific_term_also_counts_for_its_family():
    """"Cirurgia pediátrica" é das duas famílias, e quem assina qualquer uma
    das duas tem motivo legítimo para querer ver a vaga."""
    achadas = classify_specialties("Médico cirurgia pediátrica 12h")

    assert CIRURGIA in achadas
    assert PEDIATRIA in achadas


def test_accents_do_not_change_the_answer():
    assert classify_specialties("Pediatria") == classify_specialties("PEDIATRIA")


def test_the_official_name_of_family_medicine_is_recognised():
    """Regressão: o vocabulário tinha "família e comunidade", mas o nome
    oficial da especialidade é "Medicina da Família E DA Comunidade" — e sem
    o segundo artigo o termo não casava."""
    assert ATENCAO_PRIMARIA in classify_specialties("Medicina da Família e da Comunidade")


def test_nothing_recognised_returns_an_empty_list_not_a_guess():
    assert classify_specialties("[MedGrupo] HOSPITAL SANTA HELENA") == []


def test_the_order_follows_the_taxonomy_not_the_text():
    """Para a saída não mudar conforme a redação do edital — o que faria o
    banco e o site divergirem entre rodadas sem nada ter mudado de fato."""
    ordem = classify_specialties("pediatria, anestesiologia, clínica médica")

    assert ordem == [f for f in SPECIALTIES if f in ordem]


# ==========================================
# Persistência
# ==========================================
def test_specialties_survive_a_round_trip():
    assert decode_specialties(encode_specialties([CIRURGIA, PEDIATRIA])) == [CIRURGIA, PEDIATRIA]


def test_an_empty_list_is_stored_as_nothing():
    """Vazio é ausência de dado, não uma lista vazia codificada — assim uma
    vaga não classificada fica indistinguível de uma anterior à coluna."""
    assert encode_specialties([]) is None
    assert decode_specialties(None) == []


def test_the_encoding_delimits_whole_keys():
    """As barras nas pontas existem para o LIKE do filtro casar chave inteira.
    Sem elas, procurar "cirurgia" acharia "cirurgia_pediatrica" também."""
    assert encode_specialties([CIRURGIA]).startswith("|")
    assert encode_specialties([CIRURGIA]).endswith("|")


def test_query_by_specialty_returns_only_matching_jobs(db):

    db.insert_job("A", "https://x/a", today_str(), specialties=[PEDIATRIA])
    db.insert_job("B", "https://x/b", today_str(), specialties=[CIRURGIA])
    db.insert_job("C", "https://x/c", today_str(), specialties=[PEDIATRIA, CIRURGIA])

    encontradas = db.fetch_undelivered("1", specialties=[PEDIATRIA])

    assert {j.link for j in encontradas} == {"https://x/a", "https://x/c"}


def test_a_job_without_specialties_is_not_returned_by_a_specific_query(db):
    """Ela é alcançada por NAO_ESPECIFICADA, não por acaso — é o que garante
    que o assinante estrito receba só o que pediu."""

    db.insert_job("Sem", "https://x/s", today_str())

    assert db.fetch_undelivered("1", specialties=[PEDIATRIA]) == []


def test_an_unclassified_job_is_reached_through_the_unspecified_key(db):
    """Regressão de um bug que só aparecia na fila de reenvio: no banco a vaga
    sem especialidade tem a coluna NULA, e `LIKE` nunca casa com NULL. A
    equivalência "vazio vale por NAO_ESPECIFICADA", que _wants() aplica em
    Python, precisa existir também no SQL — senão as duas metades do filtro
    discordam e vagas não classificadas desaparecem em silêncio."""

    db.insert_job("Sem", "https://x/s", today_str())

    encontradas = db.fetch_undelivered("1", specialties=[PEDIATRIA, NAO_ESPECIFICADA])

    assert [j.link for j in encontradas] == ["https://x/s"]


def test_specialties_are_refreshed_when_the_job_is_seen_again(db):
    """Quando o vocabulário melhora, o acervo antigo é reclassificado sozinho
    nas rodadas seguintes — sem download e sem ferramenta à parte."""

    db.insert_job("V", "https://x/v", today_str())
    db.touch_seen("https://x/v", specialties=[PEDIATRIA])

    assert db.fetch_all_jobs()[0].specialties == [PEDIATRIA]


def test_silence_does_not_erase_specialties_already_known(db):
    """Mesma regra do status: uma rodada em que a fonte não disse nada não
    pode apagar o que ela mesma declarou antes."""

    db.insert_job("V", "https://x/v", today_str(), specialties=[PEDIATRIA])
    db.touch_seen("https://x/v", specialties=[])

    assert db.fetch_all_jobs()[0].specialties == [PEDIATRIA]


# ==========================================
# A regra de correspondência
# ==========================================
def _assinante(**kwargs) -> Subscriber:
    base = dict(chat_id="1", regions=list(REGIONS), job_types=list(JOB_TYPES),
                specialties=list(SPECIALTIES))
    base.update(kwargs)
    return Subscriber(**base)


def test_one_specialty_in_common_is_enough():
    """Interseção, não igualdade: uma vaga com dez cargos interessa a quem
    assina só um deles."""
    sub = _assinante(specialties=[PEDIATRIA])

    assert _wants(sub, REGIONS[0], JOB_TYPES[0], [CIRURGIA, PEDIATRIA, ANESTESIOLOGIA])


def test_no_specialty_in_common_means_no_alert():
    sub = _assinante(specialties=[PEDIATRIA])

    assert not _wants(sub, REGIONS[0], JOB_TYPES[0], [CIRURGIA])


def test_a_job_with_no_specialty_reaches_whoever_kept_the_default():
    """A decisão central do recurso. 72% das vagas não dizem a especialidade;
    se elas não chegassem a ninguém que filtra, o filtro esconderia a maior
    parte do acervo — inclusive vagas da área da pessoa que simplesmente não
    dizem isso no título."""
    padrao = _assinante()

    assert _wants(padrao, REGIONS[0], JOB_TYPES[0], [])


def test_opting_out_of_the_unspecified_bucket_is_possible():
    """Quem quer o corte estrito consegue: é só desmarcar a opção."""
    estrito = _assinante(specialties=[PEDIATRIA])

    assert not _wants(estrito, REGIONS[0], JOB_TYPES[0], [])


def test_region_and_type_still_have_to_match():
    """Entre dimensões vale E — especialidade não é atalho para furar as
    outras duas."""
    sub = _assinante(regions=[REGIONS[0]], specialties=[PEDIATRIA])

    assert not _wants(sub, REGIONS[1], JOB_TYPES[0], [PEDIATRIA])


# ==========================================
# De onde vem a resposta
# ==========================================
def test_the_source_wins_over_the_title():
    """O MedGrupo é perguntado no catálogo dele próprio e o RioSaúde PSS
    conhece a lista completa de cargos; o título mostra só os três primeiros."""
    job = {"title": "[RioSaúde PSS] Edital 003 — pediatria (+20)",
           "specialties": [CIRURGIA, ANESTESIOLOGIA]}

    assert _resolve_specialties(job) == [CIRURGIA, ANESTESIOLOGIA]


def test_the_title_is_used_when_the_source_says_nothing():
    assert _resolve_specialties({"title": "Concurso para médico pediatra"}) == [PEDIATRIA]


def test_an_unclassifiable_title_yields_an_empty_list():
    """Que vira NAO_ESPECIFICADA na exibição — uma resposta, não uma falha."""
    assert _resolve_specialties({"title": "[MedGrupo] HOSPITAL PASTEUR"}) == []


def test_the_unspecified_key_is_part_of_the_taxonomy():
    """Ser chave normal é o que mantém a correspondência como interseção pura,
    sem exceção nenhuma no código."""
    assert NAO_ESPECIFICADA in SPECIALTIES
