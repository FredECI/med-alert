"""Scrapers da família PCI Concursos (Sudeste, Saúde e RJ estadual)."""
import logging
from typing import Dict, List

from bs4 import BeautifulSoup

from medalert.scrapers.base import BaseScraper
from medalert.timeutil import today_str


class PCIScraper(BaseScraper):
    """Scraper genérico do PCI Concursos para a região Sudeste."""

    def __init__(self):
        super().__init__()
        self.url = "https://www.pciconcursos.com.br/concursos/sudeste/"

    def scrape(self) -> List[Dict[str, str]]:
        html_content = self.fetch_html(self.url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        found_jobs = []

        # Abordagem Resiliente: Pega TODOS os links da página
        all_links = soup.find_all("a")

        for link_element in all_links:
            title = link_element.text.strip()
            link_href = link_element.get("href", "")

            # Filtro básico para ignorar links de navegação do site (menus, etc)
            if not title or "concursos" not in link_href:
                continue

            # Pega o bloco pai (geralmente uma <li> ou <div>) para ler o contexto todo
            parent_block = link_element.parent
            if not parent_block:
                continue

            block_text = parent_block.text.strip()

            if self.is_in_target_state(block_text) and self.is_relevant(block_text):
                found_jobs.append({
                    "title": f"[PCI] {title}",
                    "link": link_href if link_href.startswith("http") else f"https://www.pciconcursos.com.br{link_href}",
                    "pub_date": today_str(),
                })

        # Remove duplicatas baseadas no link (caso o mesmo link apareça 2x no HTML)
        unique_jobs = {job["link"]: job for job in found_jobs}.values()

        logging.info(f"[PCIScraper] Found {len(unique_jobs)} relevant medical jobs.")
        return list(unique_jobs)


class PCISaudeScraper(BaseScraper):
    """Substituto do RSS. Scraper focado na página de Saúde geral do PCI."""

    def __init__(self):
        super().__init__()
        self.url = "https://www.pciconcursos.com.br/concursos/saude/"

    def scrape(self) -> List[Dict[str, str]]:
        html_content = self.fetch_html(self.url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        found_jobs = []

        all_links = soup.find_all("a")

        for link_element in all_links:
            title = link_element.text.strip()
            link_href = link_element.get("href", "")

            if not title or "concursos" not in link_href:
                continue

            parent_block = link_element.parent
            if not parent_block:
                continue

            block_text = parent_block.text.strip()

            # Na aba de saúde, focamos fortemente no filtro regional e de palavras
            if self.is_in_target_state(block_text) and self.is_relevant(block_text):
                found_jobs.append({
                    "title": f"[PCI Saúde] {title}",
                    "link": link_href if link_href.startswith("http") else f"https://www.pciconcursos.com.br{link_href}",
                    "pub_date": today_str(),
                })

        unique_jobs = {job["link"]: job for job in found_jobs}.values()
        logging.info(f"[PCISaude] Found {len(unique_jobs)} relevant medical jobs.")
        return list(unique_jobs)


class PCIEstadualScraper(BaseScraper):
    """Focado exclusivamente na listagem completa do Estado do Rio de Janeiro."""

    def __init__(self):
        super().__init__()
        self.url = "https://www.pciconcursos.com.br/concursos/rj/"

    def scrape(self) -> List[Dict[str, str]]:
        html_content = self.fetch_html(self.url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        found_jobs = []

        all_links = soup.find_all("a")

        for link_element in all_links:
            title = link_element.text.strip()
            link_href = link_element.get("href", "")

            if not title or "concursos" not in link_href:
                continue

            parent_block = link_element.parent
            if not parent_block:
                continue

            block_text = parent_block.text.strip()

            # A URL já é escopada para o Estado do RJ inteiro — não reaplicamos o
            # filtro de cidade aqui, que derrubava vagas válidas de municípios fora
            # da lista curta de state_filters (ex: Niterói, São Gonçalo, Duque de
            # Caxias, Nova Iguaçu, Petrópolis).
            if self.is_relevant(block_text):
                found_jobs.append({
                    "title": f"[PCI RJ] {title}",
                    "link": link_href if link_href.startswith("http") else f"https://www.pciconcursos.com.br{link_href}",
                    "pub_date": today_str(),
                })

        unique_jobs = {job["link"]: job for job in found_jobs}.values()
        logging.info(f"[PCIEstadual] Found {len(unique_jobs)} relevant medical jobs.")
        return list(unique_jobs)
