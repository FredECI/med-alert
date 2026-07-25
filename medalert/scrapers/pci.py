"""Scraper da família PCI Concursos (Sudeste, Saúde e RJ estadual).

As três páginas do PCI usadas por este projeto compartilham exatamente a
mesma estrutura de HTML (links soltos, com o contexto — cidade, estado, área —
no bloco pai do link). A única coisa que muda de uma fonte pra outra é a URL,
o prefixo do título, e se o filtro de cidade (state_filters) deve ser
reaplicado por cima do escopo que a própria URL já dá.
"""
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

from medalert.scrapers.base import BaseScraper
from medalert.timeutil import today_str


class PCIListScraper(BaseScraper):
    def __init__(self, url: str, title_prefix: str, apply_state_filter: bool):
        super().__init__()
        self.url = url
        self.title_prefix = title_prefix
        self.apply_state_filter = apply_state_filter

    @property
    def label(self) -> str:
        return self.title_prefix.strip("[]")

    def _find_candidates(self, soup: BeautifulSoup):
        # Abordagem Resiliente: Pega TODOS os links da página
        return soup.find_all("a")

    def _parse_candidate(self, candidate) -> Optional[Dict[str, str]]:
        title = candidate.text.strip()
        link_href = candidate.get("href", "")

        # Filtro básico para ignorar links de navegação do site (menus, etc)
        if not title or "concursos" not in link_href:
            return None

        # Pega o bloco pai (geralmente uma <li> ou <div>) para ler o contexto todo
        parent_block = candidate.parent
        if not parent_block:
            return None

        block_text = parent_block.text.strip()

        state_ok = self.is_in_target_state(block_text) if self.apply_state_filter else True
        if not (state_ok and self.is_relevant(block_text)):
            return None

        full_link = link_href if link_href.startswith("http") else f"https://www.pciconcursos.com.br{link_href}"
        return {"title": f"{self.title_prefix} {title}", "link": full_link, "pub_date": today_str()}


def build_pci_scrapers() -> List[PCIListScraper]:
    return [
        PCIListScraper(
            url="https://www.pciconcursos.com.br/concursos/sudeste/",
            title_prefix="[PCI]",
            apply_state_filter=True,
        ),
        PCIListScraper(
            # Substituto do RSS: página focada em Saúde geral do PCI.
            url="https://www.pciconcursos.com.br/concursos/saude/",
            title_prefix="[PCI Saúde]",
            apply_state_filter=True,
        ),
        PCIListScraper(
            # A URL já é escopada para o Estado do RJ inteiro — não reaplicamos
            # o filtro de cidade aqui, que derrubava vagas válidas de municípios
            # fora da lista curta de state_filters (ex: Niterói, São Gonçalo,
            # Duque de Caxias, Nova Iguaçu, Petrópolis).
            url="https://www.pciconcursos.com.br/concursos/rj/",
            title_prefix="[PCI RJ]",
            apply_state_filter=False,
        ),
    ]
