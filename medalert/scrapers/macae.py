"""Scraper institucional para o diário de notícias da Prefeitura de Macaé."""
from typing import Dict, Optional

from bs4 import BeautifulSoup

from medalert.scrapers.base import BaseScraper
from medalert.timeutil import today_str

# Palavras de gatilho para editais
GOV_TRIGGERS = ["concurso", "processo seletivo", "vaga", "inscrição", "inscricao", "edital"]


class MacaeGovScraper(BaseScraper):
    """Scraper institucional para o diário de notícias da Prefeitura de Macaé."""
    url = "https://www.macae.rj.gov.br/noticias"

    def _find_candidates(self, soup: BeautifulSoup):
        return soup.find_all("a", href=True)

    def _parse_candidate(self, candidate) -> Optional[Dict[str, str]]:
        link_href = candidate.get("href", "")
        title_lower = candidate.text.strip().lower()

        if "noticia" not in link_href:
            return None

        # A notícia deve falar sobre contratação E ser da área médica/saúde
        has_trigger = any(trigger in title_lower for trigger in GOV_TRIGGERS)
        is_medical = "saúde" in title_lower or "saude" in title_lower or self.is_relevant(title_lower)

        if not (has_trigger and is_medical):
            return None

        full_link = link_href if link_href.startswith("http") else f"https://www.macae.rj.gov.br{link_href}"
        return {"title": f"[Pref. Macaé] {candidate.text.strip()}", "link": full_link, "pub_date": today_str()}
