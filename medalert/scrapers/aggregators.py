"""Scrapers de fontes agregadoras genéricas (não focadas em saúde e/ou não
focadas no RJ) — InfoJobs, Concursos no Brasil e o Diário Oficial do
Município do Rio de Janeiro (DOM-RJ). As três precisam de filtragem de
verdade porque a fonte em si mistura vagas/concursos de qualquer área.
"""
import json
import logging
import re
from datetime import timedelta
from typing import Dict, List, Optional
from urllib.parse import quote

from bs4 import BeautifulSoup

from medalert.scrapers.base import BaseScraper
from medalert.textutils import sanitize_title
from medalert.timeutil import now_brt, today_str

_TAG_RE = re.compile(r"</?strong>")


class InfoJobsScraper(BaseScraper):
    """InfoJobs, já filtrado por URL para vagas de "médico" em Rio de Janeiro (RJ capital).

    A busca do próprio InfoJobs (embutida na URL) é por substring, não por
    palavra inteira — ela também traz vagas como "Analista de Contas
    Médicas", "Vendedora Interno Medicamentos" ou "Repasse Médico", que não
    são vagas para médicos. Reaplicamos is_relevant() (que casa por palavra
    inteira) sobre o título de cada vaga como filtro defensivo.
    """

    url = "https://www.infojobs.com.br/vagas-de-medico-em-rio-janeiro,-rj.aspx"

    def _find_candidates(self, soup: BeautifulSoup):
        # Cada card de vaga tem um <a class="text-decoration-none" href="/vaga-de-...-em-rio-janeiro__ID.aspx">
        # envolvendo o <h2> com o título. Links de perfil de empresa usam uma
        # classe parecida ("text-body text-decoration-none"), por isso o
        # filtro real de "é uma vaga mesmo" é o padrão da URL em
        # _parse_candidate, não o seletor de classe aqui.
        return soup.find_all("a", class_="text-decoration-none", href=True)

    def _parse_candidate(self, candidate) -> Optional[Dict[str, str]]:
        link_href = candidate.get("href", "")
        if "/vaga-de-" not in link_href:
            return None

        title = sanitize_title(candidate.get_text())
        if not title or not self.is_relevant(title):
            return None

        full_link = link_href if link_href.startswith("http") else f"https://www.infojobs.com.br{link_href}"
        return {"title": f"[InfoJobs] {title}", "link": full_link, "pub_date": today_str()}


class ConcursosNoBrasilScraper(BaseScraper):
    """Tabela geral de concursos abertos no Rio de Janeiro (todas as áreas).

    A URL já é escopada para o Estado do RJ, mas a tabela lista concursos de
    qualquer área (educação, segurança, administrativo etc) — precisa do
    filtro de relevância de saúde. O texto visível do link é só o nome do
    órgão (ex: "Prefeitura de Volta Redonda"); o contexto rico que diz do que
    se trata (ex: "...abre 355 vagas na Saúde") fica no atributo title do
    próprio link, então o filtro roda sobre ele.
    """

    url = "https://concursosnobrasil.com/concursos/rj/"

    def _find_candidates(self, soup: BeautifulSoup):
        return soup.find_all("tr")

    def _parse_candidate(self, candidate) -> Optional[Dict[str, str]]:
        link = candidate.find("a", href=True)
        if not link:
            return None  # linha de cabeçalho da tabela (<th>), sem <a>

        headline = sanitize_title(link.get("title", ""))
        org_name = sanitize_title(link.get_text())
        context_text = f"{headline} {org_name}".strip()
        if not context_text or not self.is_relevant(context_text):
            return None

        display_title = headline or org_name
        return {
            "title": f"[Concursos no Brasil] {display_title}",
            "link": link["href"],
            "pub_date": today_str(),
        }


class DomRjScraper(BaseScraper):
    """Busca por palavra-chave no Diário Oficial do Município do Rio de Janeiro.

    A home tem um formulário Angular ("Busca por Palavra") que dá POST em
    /buscanova com um token — mas esse POST só devolve o shell estático da
    SPA; os resultados de verdade são buscados depois, client-side, via JS.
    Reverse-engenheirando o bundle da aplicação (função `$scope.search` do
    `SearchCtrl`, em assets/javascripts/application.*.js), o endpoint real é
    um GET simples para o backend Elasticsearch por trás do site, sem token
    nem cookie de sessão:

        GET /busca/busca/buscar/query/{pagina}/di:{AAAA-MM-DD}/df:{AAAA-MM-DD}/?1=1&q={termo}

    Isso devolve JSON cru do Elasticsearch (confirmado testando direto: 200
    OK, sem precisar do token "_Token[fields]" do form nem de cookies) — por
    isso conseguimos usar fetch_html (GET) normalmente, só que sobrescrevendo
    scrape() porque a resposta é JSON, não HTML (o template method da base
    espera montar um BeautifulSoup a partir do conteúdo).

    Como isso é busca full-text em TODO o conteúdo do Diário (não uma
    listagem de vagas dedicada — a maioria dos resultados é sobre qualquer
    outro assunto que cite a palavra buscada, ex: atos de pessoal, editais de
    outras áreas, avisos administrativos), aplicamos is_relevant() E
    has_job_signal() sobre os trechos destacados (highlight) de cada
    resultado antes de aceitar, igual às fontes de notícia geral.

    Limitação assumida conscientemente: só a primeira página (10 resultados,
    ordenados por relevância/data pelo próprio Elasticsearch) de uma janela
    de LOOKBACK_DAYS dias é buscada a cada execução — suficiente para um
    robô que roda diariamente e já deduplica por link.
    """

    SEARCH_TERM = "saúde"
    LOOKBACK_DAYS = 7

    def __init__(self):
        super().__init__()
        end_date = now_brt().date()
        start_date = end_date - timedelta(days=self.LOOKBACK_DAYS)
        self.url = (
            "https://doweb.rio.rj.gov.br/busca/busca/buscar/query/0/"
            f"di:{start_date.isoformat()}/df:{end_date.isoformat()}/"
            f"?1=1&q={quote(self.SEARCH_TERM)}"
        )

    def scrape(self) -> List[Dict[str, str]]:
        raw = self.fetch_html(self.url)
        if not raw:
            return []

        try:
            data = json.loads(raw)
        except ValueError as e:
            logging.error(f"[{self.label}] Resposta do DOM-RJ não é JSON válido: {e}")
            return []

        hits = data.get("hits", {}).get("hits", [])
        found_jobs = [job for job in (self._parse_hit(hit) for hit in hits) if job is not None]

        unique_jobs = {job["link"]: job for job in found_jobs}.values()
        logging.info(f"[{self.label}] Found {len(unique_jobs)} relevant medical jobs.")
        return list(unique_jobs)

    def _parse_hit(self, hit: Dict) -> Optional[Dict[str, str]]:
        source = hit.get("_source", {})
        snippets = hit.get("highlight", {}).get("conteudo", [])
        clean_snippets = [sanitize_title(_TAG_RE.sub("", s)) for s in snippets if s]
        excerpt = " (...) ".join(s for s in clean_snippets if s)
        if not excerpt:
            return None

        if not (self.is_relevant(excerpt) and self.has_job_signal(excerpt)):
            return None

        diario_id = source.get("diario_id")
        pagina = source.get("pagina")
        if diario_id is None or pagina is None:
            return None

        link = f"https://doweb.rio.rj.gov.br/ver/{diario_id}/{pagina}/{quote(self.SEARCH_TERM)}"
        title_text = excerpt if len(excerpt) <= 200 else f"{excerpt[:200]}..."
        return {"title": f"[DOM-RJ] {title_text}", "link": link, "pub_date": today_str()}
