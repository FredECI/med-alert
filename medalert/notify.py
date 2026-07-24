"""Envio de notificações via Telegram para múltiplos chats, com isolamento de erro por destinatário."""
import logging
from typing import List

import requests


class TelegramNotifier:
    """Handles sending notifications to multiple Telegram chats."""

    def __init__(self, bot_token: str, chat_ids: List[str]):
        self.bot_token = bot_token
        self.chat_ids = chat_ids
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

    def send_message(self, text: str) -> int:
        """Envia a mensagem para todos os chats configurados.

        Retorna a quantidade de mensagens enviadas com sucesso.
        """
        success_count = 0

        if not self.chat_ids:
            logging.warning("Nenhum Chat ID configurado para envio.")
            return 0

        for chat_id in self.chat_ids:
            payload = {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            }
            try:
                response = requests.post(self.base_url, json=payload, timeout=10)
                response.raise_for_status()
                success_count += 1
            except requests.RequestException as e:
                logging.error(f"Failed to send Telegram message to {chat_id}. Error: {e}")

        return success_count
