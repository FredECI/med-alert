"""Scrapers de agregadores de notícias (Google News, G1, Bing News)."""
import logging
from typing import Dict, List

from bs4 import BeautifulSoup

from medalert.scrapers.base import BaseScraper
from medalert.timeutil import today_str


class GoogleNewsScraper(BaseScraper):
    """
    Scraper focado no Google News, buscando por termos específicos.
    Isso engloba Folha Dirigida, Estratégia Concursos, Sanar, etc.
    """

    def __init__(self):
        super().__init__()
        # A URL já traz a busca "concurso medico" filtrada para os últimos 7 dias (when:7d)
        self.url = "https://news.google.com/search?q=concurso%20medico%20rj%20OR%20rio%20de%20janeiro%20OR%20macae%20when%3A7d&hl=pt-BR&gl=BR&ceid=BR%3Apt-419"

    def scrape(self) -> List[Dict[str, str]]:
        html_content = self.fetch_html(self.url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        found_jobs = []

        # No Google News, as notícias costumam estar em tags <article>
        articles = soup.find_all("article")

        for article in articles:
            link_element = article.find("a")
            if not link_element:
                continue

            title = link_element.text.strip()
            # Os links do Google News começam com "./articles/...", precisamos consertar isso
            raw_link = link_element.get("href", "")
            if raw_link.startswith("./"):
                link_href = f"https://news.google.com{raw_link[1:]}"
            else:
                link_href = raw_link

            article_text = article.text.strip()

            # Como a própria URL de busca já é filtrada, aqui somos um pouco mais
            # flexíveis, mas ainda garantimos que tenha a ver com a área médica.
            if self.is_relevant(article_text):
                found_jobs.append({
                    "title": f"[Notícia/Radar] {title}",
                    "link": link_href,
                    "pub_date": today_str(),
                })

        unique_jobs = {job["link"]: job for job in found_jobs}.values()
        logging.info(f"[GoogleNews] Found {len(unique_jobs)} relevant medical news/jobs.")
        return list(unique_jobs)


class G1Scraper(BaseScraper):
    """Scraper focado na editoria de concursos do G1 (Nacional e Sudeste)."""

    def __init__(self):
        super().__init__()
        self.url = "https://g1.globo.com/trabalho-e-carreira/concursos/"

    def scrape(self) -> List[Dict[str, str]]:
        html_content = self.fetch_html(self.url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        found_jobs = []

        # O G1 usa a classe 'feed-post-link' para os títulos das notícias na home
        links = soup.find_all("a", class_="feed-post-link")

        for link_element in links:
            title = link_element.text.strip()
            link_href = link_element.get("href", "")

            # Precisamos verificar o título da notícia para saber se é do nosso interesse
            if self.is_in_target_state(title) and self.is_relevant(title):
                found_jobs.append({
                    "title": f"[G1] {title}",
                    "link": link_href,
                    "pub_date": today_str(),
                })

        unique_jobs = {job["link"]: job for job in found_jobs}.values()
        logging.info(f"[G1Scraper] Found {len(unique_jobs)} relevant medical jobs.")
        return list(unique_jobs)


class BingNewsScraper(BaseScraper):
    """Busca em blogs médicos, portais de prefeituras e jornais locais através do Bing News."""

    def __init__(self):
        super().__init__()
        # Query já foca nas cidades-alvo e na carreira (últimos 7 dias)
        self.url = "https://www.bing.com/news/search?q=concurso+medico+rio+de+janeiro+OR+macae+OR+campos&qft=interval%3d%227%22"

    def scrape(self) -> List[Dict[str, str]]:
        html_content = self.fetch_html(self.url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        found_jobs = []

        # O Bing agrupa as notícias em div.news-card
        cards = soup.find_all("div", class_="news-card")

        for card in cards:
            link_element = card.find("a", class_="title")
            if not link_element:
                continue

            title = link_element.text.strip()
            link_href = link_element.get("href", "")

            # Aqui, analisamos tanto o título quanto o snippet da notícia
            snippet_element = card.find("div", class_="snippet")
            snippet = snippet_element.text.strip() if snippet_element else ""

            full_text = f"{title} {snippet}"

            if self.is_relevant(full_text):
                found_jobs.append({
                    "title": f"[Radar/News] {title}",
                    "link": link_href,
                    "pub_date": today_str(),
                })

        unique_jobs = {job["link"]: job for job in found_jobs}.values()
        logging.info(f"[BingNews] Found {len(unique_jobs)} relevant medical jobs.")
        return list(unique_jobs)
