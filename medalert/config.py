"""Configuração vinda de variáveis de ambiente e constantes de escopo geográfico."""
import os
from typing import List

TARGET_CITIES = ["Macae", "Rio de Janeiro", "Rio das Ostras", "Campos dos Goytacazes", "Cabo Frio"]


def load_telegram_bot_token() -> str:
    return os.getenv("TELEGRAM_BOT_TOKEN", "")


def load_telegram_chat_ids() -> List[str]:
    raw_chat_ids = os.getenv("TELEGRAM_CHAT_IDS", "")
    return [chat_id.strip() for chat_id in raw_chat_ids.split(",") if chat_id.strip()]
