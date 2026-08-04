"""Envio de notificações via Telegram, com isolamento de erro por destinatário."""
import logging
from typing import List

import requests

#: Erros do Telegram que NÃO adianta retentar: a mensagem foi rejeitada pelo
#: conteúdo ou o destinatário não é mais alcançável. Retentar nesses casos
#: significaria repetir a mesma falha a cada rodada, para sempre.
_PERMANENT_ERROR_CODES = {400, 403}


class TelegramNotifier:
    """Handles sending notifications to Telegram chats."""

    def __init__(self, bot_token: str, chat_ids: List[str]):
        self.bot_token = bot_token
        self.chat_ids = chat_ids
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

    def send_to(self, chat_id: str, text: str) -> bool:
        """Envia para UM destinatário. True quando a entrega foi resolvida.

        "Resolvida" inclui o erro permanente (400/403): se o Telegram recusou
        a mensagem por formatação inválida ou o usuário bloqueou o bot, não há
        o que retentar — insistir só repetiria a mesma falha em toda rodada.
        Já erro transitório (rede, 429, 5xx) devolve False para a vaga
        continuar na fila e ser tentada de novo depois.
        """
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }
        try:
            response = requests.post(self.base_url, json=payload, timeout=10)
            response.raise_for_status()
            return True
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status in _PERMANENT_ERROR_CODES:
                logging.error(
                    f"Telegram recusou definitivamente a mensagem para {chat_id} "
                    f"(HTTP {status}); não será retentada."
                )
                return True
            logging.error(f"Falha transitória ao enviar para {chat_id} (HTTP {status}).")
            return False
        except requests.RequestException as e:
            logging.error(f"Failed to send Telegram message to {chat_id}. Error: {e}")
            return False

    def send_message(self, text: str) -> int:
        """Envia a mesma mensagem para todos os chats configurados.

        Usado para avisos que não são vaga (ex: alerta de falha total), onde
        não existe destinatário específico. Retorna quantos foram resolvidos.
        """
        if not self.chat_ids:
            logging.warning("Nenhum Chat ID configurado para envio.")
            return 0

        return sum(1 for chat_id in self.chat_ids if self.send_to(chat_id, text))
