"""Utilitários de limpeza de texto compartilhados pelos scrapers e pelo gerador de relatórios."""


def sanitize_title(raw_title) -> str:
    """Remove quebras de linha e colapsa espaços redundantes de títulos raspados de HTML."""
    title = str(raw_title).replace("\n", " ").replace("\r", " ").replace("|", "-")
    return " ".join(title.split())
