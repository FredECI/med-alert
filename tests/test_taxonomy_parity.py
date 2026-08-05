"""Garante que a taxonomia do Worker (JS) e a do robô (Python) não divirjam.

Este é o acoplamento mais perigoso da integração com o Telegram: o Worker
grava as preferências usando as chaves dele, e o robô filtra as vagas
comparando com as colunas `region`/`job_type` do banco. Se uma chave for
renomeada de um lado só, nenhum erro é levantado — o filtro simplesmente
para de casar e o assinante deixa de receber alertas silenciosamente.
"""
import re
from pathlib import Path

import pytest

from medalert.taxonomy import JOB_TYPES, REGIONS

_WORKER_TAXONOMY = Path(__file__).resolve().parents[1] / "worker" / "src" / "taxonomy.js"


def _extract_keys(js_source: str, const_name: str) -> list:
    """Lê as chaves de um array `export const NOME = [{ key: "...", ... }]`."""
    bloco = re.search(rf"export const {const_name} = \[(.*?)\n\];", js_source, re.S)
    assert bloco, f"não encontrei {const_name} em {_WORKER_TAXONOMY.name}"
    return re.findall(r'key:\s*"([^"]+)"', bloco.group(1))


@pytest.fixture(scope="module")
def js_source() -> str:
    return _WORKER_TAXONOMY.read_text(encoding="utf-8")


def test_worker_taxonomy_file_exists():
    assert _WORKER_TAXONOMY.exists(), "o Worker precisa da taxonomia para montar os botões"


def test_regions_match_between_python_and_worker(js_source):
    assert _extract_keys(js_source, "REGIONS") == REGIONS


def test_job_types_match_between_python_and_worker(js_source):
    assert _extract_keys(js_source, "JOB_TYPES") == JOB_TYPES


def test_worker_default_excludes_only_news(js_source):
    """O padrão combinado com o dono do projeto: tudo, menos notícia/radar."""
    assert 'filter((k) => k !== "noticia")' in js_source
