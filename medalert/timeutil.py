"""Fonte única de timestamps para o projeto — sempre Horário de Brasília (UTC-3).

Brasil não observa horário de verão desde 2019, então um offset fixo é
suficiente aqui (não é necessário um banco de fusos horários completo).
"""
from datetime import datetime, timedelta, timezone
from typing import Optional

BRT = timezone(timedelta(hours=-3))


def now_brt() -> datetime:
    return datetime.now(BRT)


def today_str() -> str:
    """Data em que o robô descobriu/revisitou o item — não é a data de publicação da fonte."""
    return now_brt().strftime("%Y-%m-%d")


def format_date_br(iso_date: Optional[str]) -> Optional[str]:
    """Converte AAAA-MM-DD (formato guardado) para DD/MM/AAAA (formato lido).

    Devolve None em vez de levantar erro: uma data malformada não pode
    derrubar a geração do site nem o envio de um alerta — o pior aceitável é
    a vaga aparecer sem o prazo.
    """
    if not iso_date:
        return None
    try:
        return datetime.strptime(iso_date, "%Y-%m-%d").strftime("%d/%m/%Y")
    except (ValueError, TypeError):
        return None
