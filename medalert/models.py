"""Modelo de dados de uma vaga/oportunidade descoberta pelos scrapers."""
from dataclasses import dataclass
from typing import Optional


@dataclass
class Job:
    title: str
    link: str
    discovered_at: str
    last_seen_at: Optional[str] = None
    #: Assinatura de conteúdo (ver medalert/dedup.py). None quando não há
    #: confiança suficiente para dizer que duas vagas são a mesma.
    dedup_key: Optional[str] = None
    #: Dimensões de personalização (ver medalert/taxonomy.py) — é por elas
    #: que o assinante escolhe o que quer receber.
    job_type: Optional[str] = None
    region: Optional[str] = None
    #: Página onde a vaga foi encontrada, quando `link` aponta direto para um
    #: arquivo (o PDF do edital). Costuma trazer anexos, cronograma e
    #: retificações que o PDF sozinho não tem. None quando `link` já é a página.
    source_url: Optional[str] = None
    #: Situação das inscrições (ver medalert/taxonomy.py). Diferente de region
    #: e job_type, que são decididos uma vez e valem para sempre, este campo é
    #: MUTÁVEL: o que está aberto hoje fecha no mês que vem, então ele é
    #: reavaliado a cada rodada em que a vaga é reencontrada.
    status: Optional[str] = None
    #: Data de encerramento das inscrições (AAAA-MM-DD), quando conhecida.
    deadline: Optional[str] = None
    #: De onde veio a conclusão sobre a situação. Determina se o sistema pode
    #: suprimir o alerta ou apenas exibir a informação com ressalva.
    status_source: Optional[str] = None
