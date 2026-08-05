"""Configuração vinda de variáveis de ambiente e constantes de escopo geográfico."""
import os
from typing import List

TARGET_CITIES = ["Macae", "Rio de Janeiro", "Rio das Ostras", "Campos dos Goytacazes", "Cabo Frio"]


def load_telegram_bot_token() -> str:
    return os.getenv("TELEGRAM_BOT_TOKEN", "")


def load_worker_url() -> str:
    """Endereço do Worker que guarda os assinantes (ver worker/README.md)."""
    return os.getenv("MEDALERT_WORKER_URL", "")


def load_sync_token() -> str:
    """Autoriza o robô a ler a lista de assinantes do Worker."""
    return os.getenv("MEDALERT_SYNC_TOKEN", "")


def load_admin_chat_id() -> str:
    """Destino dos avisos de operação (ex: falha total dos scrapers).

    Fica separado dos assinantes de propósito: "todos os scrapers falharam" é
    problema de quem mantém o robô, não notícia para quem só quer saber de
    vaga. Mandar isso para a base inteira seria ruído.
    """
    return os.getenv("TELEGRAM_ADMIN_CHAT_ID", "")
