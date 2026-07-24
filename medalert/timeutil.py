"""Fonte única de timestamps para o projeto — sempre Horário de Brasília (UTC-3).

Brasil não observa horário de verão desde 2019, então um offset fixo é
suficiente aqui (não é necessário um banco de fusos horários completo).
"""
from datetime import datetime, timedelta, timezone

BRT = timezone(timedelta(hours=-3))


def now_brt() -> datetime:
    return datetime.now(BRT)


def today_str() -> str:
    """Data em que o robô descobriu/revisitou o item — não é a data de publicação da fonte."""
    return now_brt().strftime("%Y-%m-%d")
