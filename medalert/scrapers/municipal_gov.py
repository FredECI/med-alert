"""Scrapers institucionais de prefeituras/portais municipais da Região dos Lagos e
Norte Fluminense: Araruama, Cabo Frio, Rio das Ostras, Saquarema e Casimiro de Abreu.

Todas essas cidades já são RJ por definição (a URL em si já restringe o município),
então is_in_target_state() não é aplicado aqui — mesmo raciocínio que fez
PCIEstadualScraper abandonar esse filtro quando a URL já é escopada por estado.
"""
import json
import re
from typing import Dict, Optional

from bs4 import BeautifulSoup

from medalert.scrapers.base import BaseScraper
from medalert.taxonomy import NORTE_FLUMINENSE, NOTICIA, REGIAO_DOS_LAGOS
from medalert.textutils import sanitize_title
from medalert.timeutil import today_str

_LEADING_DATE_RE = re.compile(r"^\d{2}/\d{2}/\d{4}\s*-\s*")


class AraruamaGovScraper(BaseScraper):
    """Feed institucional já pré-escopado para 'Concurso Público' pela Prefeitura de
    Araruama. Cobre todas as carreiras (educação, guarda, etc), não só saúde — por
    isso ainda filtramos por is_relevant(). has_job_signal() é dispensável: a própria
    categoria da página já garante que todo item é sobre concurso/edital."""
    region = REGIAO_DOS_LAGOS
    url = "https://www.araruama.rj.gov.br/publicacoes/atos-oficiais-concurso-publico"

    def _find_candidates(self, soup: BeautifulSoup):
        return soup.find_all("a", href=True)

    def _parse_candidate(self, candidate) -> Optional[Dict[str, str]]:
        link_href = candidate.get("href", "")
        if "atos-oficiais-concurso-publico/" not in link_href:
            return None

        title_tag = candidate.find("h6")
        if not title_tag:
            return None

        raw_title = title_tag.get_text(" ", strip=True)
        # O h6 embute a data de publicação antes do título ("02/10/2025 - RESULTADO...");
        # removemos esse prefixo pois a data de descoberta já vai em pub_date.
        title = _LEADING_DATE_RE.sub("", raw_title).strip()
        if not title or not self.is_relevant(title):
            return None

        full_link = link_href if link_href.startswith("http") else f"https://www.araruama.rj.gov.br{link_href}"
        return {"title": f"[Araruama] {sanitize_title(title)}", "link": full_link, "pub_date": today_str()}


class CaboFrioSaudeScraper(BaseScraper):
    """Categoria 'Saúde' do portal de notícias de Cabo Frio — notícias gerais da
    área, não uma listagem de vagas. Precisa de has_job_signal() para separar
    concurso/processo seletivo real de notícia institucional (vacinação, mutirão,
    fórum etc). is_relevant() também é aplicado: é barato e a categoria por si só
    não garante que o item mencione especificamente termos médicos.

    Os cards de notícia (Elementor "Happy Addons" ha-card) não usam <a href> — o
    link vive num atributo data-ha-element-link com um JSON tipo
    {"url": "https:\\/\\/...", ...} num <div class="... ha-card"> que embrulha o
    <h2 class="ha-card-title">. Por isso este scraper não usa o padrão
    find_all("a", href=True) dos outros — precisa localizar os cards e decodificar
    o JSON do atributo.
    """
    job_type = NOTICIA
    region = REGIAO_DOS_LAGOS
    url = "https://noticias.cabofrio.rj.gov.br/category/saude/"

    def _find_candidates(self, soup: BeautifulSoup):
        return soup.select("div.ha-card[data-ha-element-link]")

    def _parse_candidate(self, candidate) -> Optional[Dict[str, str]]:
        title_tag = candidate.find("h2", class_="ha-card-title")
        if not title_tag:
            return None
        title = sanitize_title(title_tag.get_text())

        raw_link_data = candidate.get("data-ha-element-link", "")
        try:
            link_data = json.loads(raw_link_data)
        except (TypeError, ValueError):
            return None

        link = link_data.get("url", "")
        if not link:
            return None

        if not (self.is_relevant(title) and self.has_job_signal(title)):
            return None

        return {"title": f"[Cabo Frio] {title}", "link": link, "pub_date": today_str()}


class RioDasOstrasNoticiasScraper(BaseScraper):
    """Feed geral de notícias de Rio das Ostras (não é escopado a concurso), então
    precisa tanto de has_job_signal() quanto de is_relevant() para não deixar passar
    notícia institucional de saúde sem vaga real."""
    job_type = NOTICIA
    region = NORTE_FLUMINENSE
    url = "https://www.riodasostras.rj.gov.br/noticias/"

    def _find_candidates(self, soup: BeautifulSoup):
        return soup.find_all("a", class_="noticia-card-link", href=True)

    def _parse_candidate(self, candidate) -> Optional[Dict[str, str]]:
        title_tag = candidate.find("h6", class_="titulo-noticia")
        if not title_tag:
            return None

        title = sanitize_title(title_tag.get_text())
        if not (self.is_relevant(title) and self.has_job_signal(title)):
            return None

        link_href = candidate.get("href", "")
        full_link = link_href if link_href.startswith("http") else f"https://www.riodasostras.rj.gov.br{link_href}"
        return {"title": f"[Rio das Ostras] {title}", "link": full_link, "pub_date": today_str()}


class RioDasOstrasConcursoScraper(BaseScraper):
    """Página institucional dedicada a concursos públicos de Rio das Ostras. Cobre
    concursos de vários órgãos (Município, SAAE, Fundação de Cultura) e várias
    carreiras — os títulos listados são só identificadores tipo "Município de Rio
    das Ostras/RJ - 01/2020", sem cargo. is_relevant() ainda é aplicado (mesmo
    raciocínio do Araruama/Saquarema): quando um edital específico de saúde entrar
    na lista, o título deve mencionar isso para ser capturado. Alguns cards da
    página (ex: "2ª Prova Prática") não têm link algum — ficam de fora naturalmente
    por não bater no find_all("a", href=True)."""
    region = NORTE_FLUMINENSE
    url = "https://www.riodasostras.rj.gov.br/concursopublico/"

    def _find_candidates(self, soup: BeautifulSoup):
        return soup.find_all("a", href=True)

    def _parse_candidate(self, candidate) -> Optional[Dict[str, str]]:
        link_href = candidate.get("href", "")
        if "concursopublico-" not in link_href:
            return None

        title_tag = candidate.find("h5")
        if not title_tag:
            return None

        title = sanitize_title(title_tag.get_text())
        if not title or not self.is_relevant(title):
            return None

        full_link = link_href if link_href.startswith("http") else f"https://www.riodasostras.rj.gov.br{link_href}"
        return {"title": f"[Rio das Ostras] {title}", "link": full_link, "pub_date": today_str()}


class SaquaremaGovScraper(BaseScraper):
    """Categoria 'Concurso Público' do site institucional de Saquarema — pré-escopada
    a concurso pela própria prefeitura, mas cobre todas as carreiras (educação,
    guarda-vidas etc), então ainda filtramos por is_relevant()."""
    region = REGIAO_DOS_LAGOS
    url = "https://www.saquarema.rj.gov.br/category/concurso-publico/"

    def _find_candidates(self, soup: BeautifulSoup):
        return soup.find_all("h2", class_="article-title")

    def _parse_candidate(self, candidate) -> Optional[Dict[str, str]]:
        link_tag = candidate.find("a", href=True)
        if not link_tag:
            return None

        title = sanitize_title(link_tag.get_text())
        if not title or not self.is_relevant(title):
            return None

        link_href = link_tag["href"]
        full_link = link_href if link_href.startswith("http") else f"https://www.saquarema.rj.gov.br{link_href}"
        return {"title": f"[Saquarema] {title}", "link": full_link, "pub_date": today_str()}


class CasimiroDeAbreuGovScraper(BaseScraper):
    """Portal da transparência de Casimiro de Abreu, página de concursos públicos.
    A tabela principal (aba "CONCURSOS") lista um concurso por linha, com um botão
    de link para o grupo de arquivos (concursopublico.php?grup=N). Cobre todos os
    órgãos/carreiras, então is_relevant() ainda é necessário.

    A aba "DECLARAÇÕES" lista anos sem concurso ("não houve concursos públicos
    para o exercício de 2026") como cards separados (div.list-group-item), fora da
    tabela — não são <tr> e não têm o botão de link, então o parser baseado em
    linhas de tabela simplesmente não os enxerga como candidatos, sem precisar de
    tratamento especial para não quebrar."""
    region = NORTE_FLUMINENSE
    url = "https://transparencia.casimirodeabreu.rj.gov.br/concursopublico.php"

    def _find_candidates(self, soup: BeautifulSoup):
        return soup.select("table.table tbody tr")

    def _parse_candidate(self, candidate) -> Optional[Dict[str, str]]:
        cells = candidate.find_all("td")
        if len(cells) < 4:
            return None

        description = sanitize_title(cells[0].get_text())
        if not description or not self.is_relevant(description):
            return None

        link_tag = candidate.find("a", href=True)
        if not link_tag:
            return None

        link_href = link_tag["href"]
        full_link = (
            link_href
            if link_href.startswith("http")
            else f"https://transparencia.casimirodeabreu.rj.gov.br/{link_href}"
        )
        return {"title": f"[Casimiro de Abreu] {description}", "link": full_link, "pub_date": today_str()}
