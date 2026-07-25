"""Utilitários de limpeza de texto compartilhados pelos scrapers e pelo gerador de relatórios."""
import re
from typing import Tuple

_SOURCE_PREFIX_RE = re.compile(r"^\[(?P<source>[^\]]+)\]\s*(?P<rest>.*)$")


def sanitize_title(raw_title) -> str:
    """Remove quebras de linha e colapsa espaços redundantes de títulos raspados de HTML."""
    title = str(raw_title).replace("\n", " ").replace("\r", " ").replace("|", "-")
    return " ".join(title.split())


def split_source_and_title(full_title: str) -> Tuple[str, str]:
    """Separa o prefixo de fonte entre colchetes (ex: '[PCI RJ] Concurso...')
    do resto do título. Todos os scrapers prefixam o título dessa forma, então
    isso permite mostrar a fonte separada do texto no site em vez de deixar o
    colchete embutido no título toda vez. Sem prefixo reconhecível, devolve
    source vazio e o título original."""
    match = _SOURCE_PREFIX_RE.match(str(full_title).strip())
    if not match:
        return "", str(full_title).strip()
    return match.group("source"), match.group("rest")
