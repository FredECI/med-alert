"""Scrapers de portais de emprego (JC Concursos, Trabalha Brasil, PandaPé/Unimed)."""
import logging
import time
from typing import Dict, List

from bs4 import BeautifulSoup

from medalert.scrapers.base import BaseScraper
from medalert.textutils import sanitize_title
from medalert.timeutil import today_str


class JCConcursosScraper(BaseScraper):
    """Scraper para o portal JC Concursos, buscando na página geral do RJ."""

    def __init__(self):
        super().__init__()
        self.url = "https://jcconcursos.com.br/concursos/rj"

    def scrape(self) -> List[Dict[str, str]]:
        html_content = self.fetch_html(self.url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        found_jobs = []

        cards = soup.find_all("a", href=True)

        for card in cards:
            title = card.text.strip()
            link_href = card.get("href")

            if "/concursos/" not in link_href and "/noticia/" not in link_href:
                continue

            if len(title) < 15:  # Evita capturar menus curtos
                continue

            if self.is_relevant(title):
                full_link = link_href if link_href.startswith("http") else f"https://jcconcursos.com.br{link_href}"

                found_jobs.append({
                    "title": f"[JC Concursos] {title}",
                    "link": full_link,
                    "pub_date": today_str(),
                })

        unique_jobs = {job["link"]: job for job in found_jobs}.values()
        logging.info(f"[JCConcursos] Found {len(unique_jobs)} relevant medical jobs.")
        return list(unique_jobs)


class TrabalhaBrasilScraper(BaseScraper):
    """Scraper paramétrico para o Trabalha Brasil, iterando sobre múltiplas cidades."""

    def __init__(self, cities: List[str]):
        super().__init__()
        self.cities = cities
        # Trabalha Brasil tem um antibot agressivo, garantimos que o cloudscraper esteja mascarado
        self.scraper.headers.update({"Referer": "https://www.google.com/"})

    def scrape(self) -> List[Dict[str, str]]:
        found_jobs = []

        for city in self.cities:
            # Constrói a URL dinamicamente substituindo espaços por hifens (ex: rio-das-ostras)
            formatted_city = city.lower().replace(" ", "-")
            url = f"https://www.trabalhabrasil.com.br/vagas-de-emprego-em-{formatted_city}-rj/medico"

            html_content = self.fetch_html(url)
            if not html_content:
                continue

            soup = BeautifulSoup(html_content, "html.parser")

            # Busca todos os links da página que possuam '/medico/' na URL (padrão das vagas deles)
            all_links = soup.find_all("a", href=True)

            for link in all_links:
                link_href = link.get("href", "")
                title = link.text.strip()

                # Ignora links vazios ou de navegação
                if "/medico/" not in link_href.lower() or len(title) < 5:
                    continue

                # O site injeta "vagas similares" de outras cidades/estados nos
                # resultados de uma busca por cidade específica — sem checar o
                # slug de estado na própria URL, vagas de fora do RJ (ex: "-es/")
                # vazavam para o site focado no Rio de Janeiro.
                if self.is_relevant(title) and self.is_in_target_state(link_href):
                    full_link = link_href if link_href.startswith("http") else f"https://www.trabalhabrasil.com.br{link_href}"

                    # Sanitiza (colapsa espaços/quebras) ANTES de truncar — se
                    # truncar primeiro, boa parte do limite de 60 caracteres é
                    # gasta em whitespace vindo do HTML, sobrando pouco texto útil.
                    clean_title = sanitize_title(title)
                    found_jobs.append({
                        "title": f"[Trabalha Brasil - {city.title()}] {clean_title[:60]}...",
                        "link": full_link,
                        "pub_date": today_str(),
                    })

            # Respiro de 2 segundos para não tomar block do site por varrer muitas cidades rápido
            time.sleep(2)

        unique_jobs = {job["link"]: job for job in found_jobs}.values()
        logging.info(f"[TrabalhaBrasil] Found {len(unique_jobs)} relevant medical jobs.")
        return list(unique_jobs)


class PandaPeUnimedScraper(BaseScraper):
    """Scraper para o portal InfoJobs/PandaPé da Unimed Costa do Sol."""

    def __init__(self):
        super().__init__()
        self.url = "https://unimedcostadosol.pandape.infojobs.com.br"

    def scrape(self) -> List[Dict[str, str]]:
        html_content = self.fetch_html(self.url)
        if not html_content:
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        found_jobs = []

        # PandaPé geralmente agrupa vagas em links com a classe 'job-link' ou dentro de listas '<li>'
        job_links = soup.find_all("a", href=True)

        for link in job_links:
            link_href = link.get("href", "")
            title = link.text.strip()

            # Filtro para ignorar o menu do site e pegar apenas páginas de detalhe de vaga (/Detail/)
            if "/Detail/" not in link_href and "vaga" not in link_href.lower():
                continue

            if self.is_relevant(title):
                full_link = link_href if link_href.startswith("http") else f"{self.url}{link_href}"

                found_jobs.append({
                    "title": f"[Unimed Costa do Sol] {title}",
                    "link": full_link,
                    "pub_date": today_str(),
                })

        unique_jobs = {job["link"]: job for job in found_jobs}.values()
        logging.info(f"[PandaPeUnimed] Found {len(unique_jobs)} relevant medical jobs.")
        return list(unique_jobs)
