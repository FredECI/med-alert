"""Lista de assinantes, lida do Worker (Cloudflare KV).

O KV é a única fonte da verdade: quem recebe alerta é quem se cadastrou pelo
bot. Nenhum chat ID fica no repositório — que é público — nem em variável de
ambiente.
"""
import logging
from dataclasses import dataclass, field
from typing import List, Optional

import requests

from medalert.config import load_sync_token, load_worker_url

_TIMEOUT = 15


@dataclass
class Subscriber:
    chat_id: str
    regions: List[str] = field(default_factory=list)
    job_types: List[str] = field(default_factory=list)

    def wants_nothing(self) -> bool:
        """Sem região ou sem tipo, nenhuma vaga jamais casaria."""
        return not self.regions or not self.job_types


def _to_subscriber(payload: dict) -> Subscriber:
    # As chaves vêm em português do Worker (regioes/tipos); aqui dentro o
    # projeto usa os mesmos nomes das colunas do banco.
    return Subscriber(
        chat_id=str(payload.get("chat_id", "")),
        regions=list(payload.get("regioes") or []),
        job_types=list(payload.get("tipos") or []),
    )


def fetch_subscribers() -> Optional[List[Subscriber]]:
    """Assinantes ativos, ou None se não foi possível consultar.

    A distinção importa: uma lista vazia significa "ninguém se cadastrou" e a
    rodada segue normalmente; None significa "não sei quem são" — aí a entrega
    é pulada, as vagas ficam pendentes e saem na próxima rodada, em vez de
    serem marcadas como entregues para ninguém.
    """
    base_url = load_worker_url()
    token = load_sync_token()
    if not base_url or not token:
        logging.error("Worker não configurado (MEDALERT_WORKER_URL/MEDALERT_SYNC_TOKEN).")
        return None

    try:
        response = requests.get(
            f"{base_url.rstrip('/')}/subscribers",
            headers={"Authorization": f"Bearer {token}"},
            timeout=_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as e:
        logging.error(f"Falha ao ler assinantes do Worker: {e}")
        return None

    return [_to_subscriber(item) for item in payload.get("subscribers", [])]


def prune_blocked(chat_ids: List[str]) -> int:
    """Remove do KV quem bloqueou o bot. Retorna quantos saíram de fato.

    Sem isso, o robô tentaria entregar para sempre a alguém que nunca mais vai
    receber, gastando chamada de API em toda rodada.
    """
    if not chat_ids:
        return 0

    base_url = load_worker_url()
    token = load_sync_token()
    if not base_url or not token:
        return 0

    try:
        response = requests.post(
            f"{base_url.rstrip('/')}/subscribers/prune",
            json={"chat_ids": chat_ids},
            headers={"Authorization": f"Bearer {token}"},
            timeout=_TIMEOUT,
        )
        response.raise_for_status()
        return int(response.json().get("removed", 0))
    except (requests.RequestException, ValueError) as e:
        logging.error(f"Falha ao remover assinantes bloqueados: {e}")
        return 0
