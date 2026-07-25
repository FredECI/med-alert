"""Scrapers de agregadores de notícias (Google News, G1, Bing News)."""
from typing import Dict, Optional

from bs4 import BeautifulSoup

from medalert.scrapers.base import BaseScraper
from medalert.timeutil import today_str


class GoogleNewsScraper(BaseScraper):
    """
    Scraper focado no Google News, buscando por termos específicos.
    Isso engloba Folha Dirigida, Estratégia Concursos, Sanar, etc.
    """
    # A URL já traz a busca "concurso medico" filtrada para os últimos 7 dias (when:7d)
    url = "https://news.google.com/search?q=concurso%20medico%20rj%20OR%20rio%20de%20janeiro%20OR%20macae%20when%3A7d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419"

    def _find_candidates(self, soup: BeautifulSoup):
        # No Google News, as notícias costumam estar em tags <article>
        return soup.find_all("article")

    def _parse_candidate(self, candidate) -> Optional[Dict[str, str]]:
        link_element = candidate.find("a")
        if not link_element:
            return None

        title = link_element.text.strip()
        # Os links do Google News começam com "./articles/...", precisamos consertar isso
        raw_link = link_element.get("href", "")
        link_href = f"https://news.google.com{raw_link[1:]}" if raw_link.startswith("./") else raw_link

        # A URL de busca já é filtrada, mas isso sozinho deixava passar
        # matéria que só menciona saúde de passagem (ex: conteúdo
        # institucional/patrocinado) sem ser realmente sobre uma vaga —
        # por isso exigimos também um termo de concurso/processo seletivo.
        article_text = candidate.text.strip()
        if not (self.is_relevant(article_text) and self.has_job_signal(article_text)):
            return None

        return {"title": f"[Notícia/Radar] {title}", "link": link_href, "pub_date": today_str()}


class G1Scraper(BaseScraper):
    """Scraper focado na editoria de concursos do G1 (Nacional e Sudeste)."""
    url = "https://g1.globo.com/trabalho-e-carreira/concursos/"

    def _find_candidates(self, soup: BeautifulSoup):
        # O G1 usa a classe 'feed-post-link' para os títulos das notícias na home
        return soup.find_all("a", class_="feed-post-link")

    def _parse_candidate(self, candidate) -> Optional[Dict[str, str]]:
        title = candidate.text.strip()
        link_href = candidate.get("href", "")

        # Precisamos verificar o título da notícia para saber se é do nosso interesse
        if not (self.is_in_target_state(title) and self.is_relevant(title) and self.has_job_signal(title)):
            return None

        return {"title": f"[G1] {title}", "link": link_href, "pub_date": today_str()}


class BingNewsScraper(BaseScraper):
    """Busca em blogs médicos, portais de prefeituras e jornais locais através do Bing News."""
    # Query já foca nas cidades-alvo e na carreira (últimos 7 dias)
    url = "https://www.bing.com/news/search?q=concurso+medico+rio+de+janeiro+OR+macae+OR+campos&qft=interval%3d%227%22"

    def _find_candidates(self, soup: BeautifulSoup):
        # O Bing agrupa as notícias em div.news-card
        return soup.find_all("div", class_="news-card")

    def _parse_candidate(self, candidate) -> Optional[Dict[str, str]]:
        link_element = candidate.find("a", class_="title")
        if not link_element:
            return None

        title = link_element.text.strip()
        link_href = link_element.get("href", "")

        # Aqui, analisamos tanto o título quanto o snippet da notícia
        snippet_element = candidate.find("div", class_="snippet")
        snippet = snippet_element.text.strip() if snippet_element else ""
        full_text = f"{title} {snippet}"

        if not (self.is_relevant(full_text) and self.has_job_signal(full_text)):
            return None

        return {"title": f"[Radar/News] {title}", "link": link_href, "pub_date": today_str()}
