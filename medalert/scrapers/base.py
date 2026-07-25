"""Classe base compartilhada por todos os scrapers: sessão HTTP, retry e o
template method que a maioria dos scrapers usa para ir de HTML a lista de vagas.
"""
import logging
from typing import Dict, Iterable, List, Optional

import cloudscraper
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from medalert.filtering import has_job_signal, is_in_target_state, is_relevant

_RETRY = Retry(
    total=2,  # até 2 novas tentativas (3 no total) só para falhas transitórias
    backoff_factor=2,  # espera ~2s, depois ~4s entre tentativas
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET"],
)


class BaseScraper:
    #: URL única buscada por scrapers que usam o template method scrape() abaixo.
    #: Scrapers com formato genuinamente diferente (ex: TrabalhaBrasilScraper,
    #: que itera várias URLs por cidade) sobrescrevem scrape() por completo e
    #: ignoram este atributo.
    url: str = ""

    def __init__(self):
        # Cloudscraper já gerencia os headers e simula um navegador real automaticamente
        self.scraper = cloudscraper.create_scraper(browser={
            "browser": "chrome",
            "platform": "windows",
            "desktop": True,
        })
        adapter = HTTPAdapter(max_retries=_RETRY)
        self.scraper.mount("https://", adapter)
        self.scraper.mount("http://", adapter)

    @property
    def label(self) -> str:
        """Nome usado nos logs. Sobrescrito por scrapers instanciados várias
        vezes com parâmetros diferentes (ex: PCIListScraper), onde o nome da
        classe sozinho não distingue as instâncias."""
        return self.__class__.__name__

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

    def has_job_signal(self, text: str) -> bool:
        return has_job_signal(text)

    def scrape(self) -> List[Dict[str, str]]:
        """Template method: busca self.url, itera candidatos via
        _find_candidates() e converte cada um em um job (ou descarta) via
        _parse_candidate(). Cobre a maioria dos scrapers — os que têm um
        formato genuinamente diferente sobrescrevem scrape() diretamente."""
        html_content = self.fetch_html(self.url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        found_jobs = [
            job
            for job in (self._parse_candidate(candidate) for candidate in self._find_candidates(soup))
            if job is not None
        ]

        unique_jobs = {job["link"]: job for job in found_jobs}.values()
        logging.info(f"[{self.label}] Found {len(unique_jobs)} relevant medical jobs.")
        return list(unique_jobs)

    def _find_candidates(self, soup: BeautifulSoup) -> Iterable:
        raise NotImplementedError("Subclasses que usam o template method devem implementar _find_candidates().")

    def _parse_candidate(self, candidate) -> Optional[Dict[str, str]]:
        raise NotImplementedError("Subclasses que usam o template method devem implementar _parse_candidate().")
