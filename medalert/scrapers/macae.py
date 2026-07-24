"""Scraper institucional para o diário de notícias da Prefeitura de Macaé."""
import logging
from typing import Dict, List

from bs4 import BeautifulSoup

from medalert.scrapers.base import BaseScraper
from medalert.timeutil import today_str


class MacaeGovScraper(BaseScraper):
    """Scraper institucional para o diário de notícias da Prefeitura de Macaé."""

    def __init__(self):
        super().__init__()
        self.url = "https://www.macae.rj.gov.br/noticias"
        # Palavras de gatilho para editais
        self.gov_triggers = ["concurso", "processo seletivo", "vaga", "inscrição", "inscricao", "edital"]

    def scrape(self) -> List[Dict[str, str]]:
        html_content = self.fetch_html(self.url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        found_jobs = []

        news_links = soup.find_all("a", href=True)

        for link in news_links:
            link_href = link.get("href", "")
            title = link.text.strip().lower()

            if "noticia" not in link_href:
                continue

            # A notícia deve falar sobre contratação E ser da área médica/saúde
            has_trigger = any(trigger in title for trigger in self.gov_triggers)
            is_medical = "saúde" in title or "saude" in title or self.is_relevant(title)

            if has_trigger and is_medical:
                full_link = link_href if link_href.startswith("http") else f"https://www.macae.rj.gov.br{link_href}"

                found_jobs.append({
                    "title": f"[Pref. Macaé] {link.text.strip()}",
                    "link": full_link,
                    "pub_date": today_str(),
                })

        unique_jobs = {job["link"]: job for job in found_jobs}.values()
        logging.info(f"[MacaeGov] Found {len(unique_jobs)} relevant governmental news/jobs.")
        return list(unique_jobs)
