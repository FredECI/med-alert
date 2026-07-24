"""Classe base compartilhada por todos os scrapers: sessão HTTP e filtros de relevância."""
import logging
from typing import Dict, List, Optional

import cloudscraper

from medalert.filtering import is_in_target_state, is_relevant


class BaseScraper:
    def __init__(self):
        # Cloudscraper já gerencia os headers e simula um navegador real automaticamente
        self.scraper = cloudscraper.create_scraper(browser={
            "browser": "chrome",
            "platform": "windows",
            "desktop": True,
        })

    def fetch_html(self, url: str) -> Optional[str]:
        try:
            logging.info(f"Fetching data from: {url}")
            response = self.scraper.get(url, timeout=15)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logging.error(f"Failed to fetch {url}. Error: {e}")
            return None

    def is_relevant(self, text: str) -> bool:
        return is_relevant(text)

    def is_in_target_state(self, text: str) -> bool:
        return is_in_target_state(text)

    def scrape(self) -> List[Dict[str, str]]:
        raise NotImplementedError("Subclasses must implement the scrape() method.")
