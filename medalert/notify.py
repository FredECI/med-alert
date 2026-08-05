"""Envio de notificações via Telegram, com isolamento de erro por destinatário."""
import logging
from enum import Enum
from typing import List

import requests


class DeliveryResult(Enum):
    """Desfecho de uma tentativa de envio.

    A distinção entre os três existe porque cada um pede uma reação diferente:
    insistir, desistir da mensagem, ou desistir do destinatário.
    """

    #: Entregue.
    SENT = "sent"
    #: O destinatário bloqueou o bot (403). Não adianta tentar de novo — e ele
    #: deve sair da lista de assinantes.
    BLOCKED = "blocked"
    #: O Telegram recusou a mensagem (400), normalmente por formatação
    #: inválida. Retentar repetiria a mesma falha para sempre.
    REJECTED = "rejected"
    #: Falha transitória (rede, 429, 5xx). Vale tentar na próxima rodada.
    RETRY = "retry"


class TelegramNotifier:
    """Handles sending notifications to Telegram chats."""

    def __init__(self, bot_token: str):
        self.bot_token = bot_token
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

    def send_to(self, chat_id: str, text: str) -> DeliveryResult:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }
        try:
            response = requests.post(self.base_url, json=payload, timeout=10)
            response.raise_for_status()
            return DeliveryResult.SENT
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status == 403:
                logging.warning(f"{chat_id} bloqueou o bot; será removido da lista.")
                return DeliveryResult.BLOCKED
            if status == 400:
                logging.error(f"Telegram recusou a mensagem para {chat_id} (400); não será retentada.")
                return DeliveryResult.REJECTED
            logging.error(f"Falha transitória ao enviar para {chat_id} (HTTP {status}).")
            return DeliveryResult.RETRY
        except requests.RequestException as e:
            logging.error(f"Failed to send Telegram message to {chat_id}. Error: {e}")
            return DeliveryResult.RETRY

    def send_admin(self, admin_chat_id: str, text: str) -> bool:
        """Aviso de operação, endereçado a quem mantém o robô."""
        if not admin_chat_id:
            logging.warning("TELEGRAM_ADMIN_CHAT_ID não configurado; aviso não enviado.")
            return False
        return self.send_to(admin_chat_id, text) == DeliveryResult.SENT


#: Desfechos que encerram a pendência: ou foi entregue, ou não há o que fazer.
RESOLVED = {DeliveryResult.SENT, DeliveryResult.BLOCKED, DeliveryResult.REJECTED}


def is_resolved(result: DeliveryResult) -> bool:
    return result in RESOLVED


def blocked_from(results: List[tuple]) -> List[str]:
    """Chat IDs que bloquearam o bot, a partir de pares (chat_id, resultado)."""
    return [chat_id for chat_id, result in results if result is DeliveryResult.BLOCKED]
