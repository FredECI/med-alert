"""Extração do prazo de inscrição a partir do texto de um edital.

Ainda não extrai nada: hoje esta função abstém sempre, e isso é deliberado.
O conjunto de referência (tests/fixtures/prazos.json) e o portão de CI que o
mede foram construídos ANTES do extrator, para que não seja o autor do
extrator quem decide se o extrator ficou bom.

Abster é a resposta correta quando não há certeza, e não um estado
provisório: um prazo errado esconde vaga aberta, que é o oposto da função do
projeto. Por isso o único critério que quebra o CI é `erros > 0` — cobertura
baixa é aceitável, erro não é.

REGRA DO CONFLITO — vale A MAIOR data
-------------------------------------
Quando o edital oferece mais de uma data de encerramento, escolha a maior.
Acontece em dois casos reais do conjunto: o documento que se contradiz (a
prosa diz 28/05 e o cronograma do anexo diz 31/05) e a retificação cujo
tachado sumiu na extração, deixando "18/07/2025 30/07/2025" colados — e
retificação quase sempre prorroga, então a maior costuma ser a versão vigente.

O fundamento é a mesma assimetria que rege o resto do projeto: com a maior, a
vaga só é dada por encerrada quando HOJE já passou de TODAS as leituras, isto
é, quando não existe interpretação do edital em que ela ainda esteja aberta.
Escolher a menor a fecharia antes da hora em troca de nada.

Duas ressalvas que precisam sobreviver à implementação:

1. A regra vale só entre candidatas já validadas como fim de inscrição. Como
   máximo global do documento ela escolheria sempre a data da prova ou do
   resultado, que são posteriores por definição — e o erro seria sistemático,
   não ocasional.
2. Data escolhida por conflito é menos certa que data por concordância, e por
   isso vai com `status_source = TEXTO`: exibe com ressalva e nunca cala um
   alerta (ver taxonomy.can_suppress_alert). Mostrar 31/05 quando a prosa
   dizia 28/05 pode fazer alguém se programar para o dia errado.
"""
from typing import Optional


def extract_deadline(edital_text: str) -> Optional[str]:
    """Data de encerramento das inscrições (AAAA-MM-DD), ou None ao abster.

    None não significa "falhei": significa "o edital não diz, ou diz de forma
    que não dá para afirmar com segurança". Quem chama trata os dois casos
    igual — a vaga aparece sem prazo, exatamente como aparecia antes.
    """
    return None
