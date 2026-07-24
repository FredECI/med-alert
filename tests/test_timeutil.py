"""Testes do helper de fuso horário único do projeto."""
from datetime import timedelta

from medalert.timeutil import now_brt, today_str


def test_now_brt_is_timezone_aware():
    assert now_brt().tzinfo is not None


def test_now_brt_uses_brasilia_offset():
    assert now_brt().utcoffset() == timedelta(hours=-3)


def test_today_str_matches_now_brt_date():
    assert today_str() == now_brt().strftime("%Y-%m-%d")
