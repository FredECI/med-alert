"""Scrapers de instituições de saúde e do IBAM (RioSaúde, Fiotec, IBAM).

RioSaúde e Fiotec são fontes já escopadas por natureza (empresa pública de
saúde do Rio e fundação que administra as chamadas públicas dos hospitais
federais no Rio), então não reaplicamos is_relevant() palavra por palavra
neles — ver docstring de cada classe para o raciocínio específico. O IBAM,
por outro lado, lista concursos de qualquer área em qualquer estado do
país, então precisa dos dois filtros (estado-alvo E área de saúde).
"""
import logging
import re
from collections import OrderedDict
from typing import Dict, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from medalert.scrapers.base import BaseScraper
from medalert.taxonomy import (
    CAPITAL_METROPOLITANA,
    PROCESSO_SELETIVO,
    classify_specialties,
)
from medalert.textutils import sanitize_title
from medalert.timeutil import now_brt, today_str

# Editais da RioSaúde/hospitais municipais sempre abrem o parágrafo com
# "Edital NNN/2026 (...)" — usamos \b para não confundir com o cabeçalho
# genérico "Editais abertos (2026)" (plural, sem o mesmo prefixo de parágrafo).
_EDITAL_PARAGRAPH_RE = re.compile(r"^\s*edital\b", re.IGNORECASE)

# A Fiotec administra chamadas públicas de vários projetos da Fiocruz, nem
# todos hospitalares. Muitos títulos de vaga hospitalar (ex: "Banco de
# currículos | Diversos cargos - INC/RJ") não têm nenhuma palavra-chave
# médica literal, só o nome da instituição — por isso, além de is_relevant(),
# aceitamos também o nome de institutos/hospitais federais atendidos no Rio.
#
# Os acrônimos são casados com CAIXA SENSÍVEL de propósito: em minúsculas,
# "inc" apareceria dentro de razão social estrangeira ("... Inc.") e "into" é
# palavra comum em inglês, o que abriria brecha para falso positivo. Como
# acrônimo institucional eles sempre vêm em maiúsculas no título.
_FIOTEC_ACRONYM_RE = re.compile(r"\b(INTO|INCA|INC)\b")
_FIOTEC_HOSPITAL_RE = re.compile(r"\bhospital\b", re.IGNORECASE)


def _is_fiotec_health_institution(title: str) -> bool:
    return bool(_FIOTEC_ACRONYM_RE.search(title) or _FIOTEC_HOSPITAL_RE.search(title))


class RioSaudeScraper(BaseScraper):
    """Scraper institucional da RioSaúde (Empresa Pública de Saúde do Rio de Janeiro).

    A página inteira é a central de processos seletivos/editais abertos da
    empresa pública de saúde municipal — não é um feed genérico, então não
    exigimos is_relevant()/has_job_signal() por item: confiamos no escopo da
    própria fonte. Cada edital é publicado como um parágrafo `<p>` iniciado
    por "Edital NNN/2026 (...)" dentro de `div.entry-content`, contendo o(s)
    cargo(s) e um link para o PDF do edital ("Acesse o edital").
    """
    job_type = PROCESSO_SELETIVO
    region = CAPITAL_METROPOLITANA
    url = "https://riosaude.prefeitura.rio/processos-seletivos-editais-abertos-2025/"

    def _find_candidates(self, soup: BeautifulSoup):
        content = soup.find("div", class_="entry-content")
        if not content:
            return []
        return content.find_all("p")

    def _parse_candidate(self, candidate) -> Optional[Dict[str, str]]:
        text = candidate.get_text(separator=" ", strip=True)
        if not _EDITAL_PARAGRAPH_RE.match(text):
            return None

        link_tag = candidate.find("a", href=True)
        if not link_tag:
            return None

        full_link = urljoin(self.url, link_tag["href"])
        return {
            "title": f"[RioSaúde] {sanitize_title(text)}",
            "link": full_link,
            "source_url": self.url,
            "pub_date": today_str(),
        }


class FiotecScraper(BaseScraper):
    """Scraper das Chamadas Públicas da Fiotec (fundação que administra a
    contratação para hospitais/institutos federais no Rio: INTO, INCA, INC,
    Hospital Federal da Lagoa).

    Usamos a listagem "todas" em vez de apenas "chamadas-publicas": ela é um
    superconjunto e inclui também os processos seletivos simplificados, que
    ficavam de fora antes.

    Como a Fiotec presta esse serviço para projetos não hospitalares de
    outros estados (ex: bolsista da Conab em MG), mantemos um filtro:
    aceitamos o item se ele bate em is_relevant() (palavra médica/saúde) OU
    menciona um dos institutos/hospitais federais atendidos no Rio.
    """
    job_type = PROCESSO_SELETIVO
    url = "https://www.fiotec.fiocruz.br/pt/vagas-projetos/todas"

    def _find_candidates(self, soup: BeautifulSoup):
        return soup.select("div.list-blog-item-titulo h2 a")

    def _parse_candidate(self, candidate) -> Optional[Dict[str, str]]:
        title = candidate.get_text(strip=True)
        link_href = candidate.get("href", "")
        if not title or not link_href:
            return None

        if not (self.is_relevant(title) or _is_fiotec_health_institution(title)):
            return None

        full_link = urljoin(self.url, link_href)
        return {
            "title": f"[Fiotec] {sanitize_title(title)}",
            "link": full_link,
            "pub_date": today_str(),
        }


class RioSaudePssScraper(BaseScraper):
    """Portal de Processos Seletivos Simplificados da RioSaúde.

    O site é uma SPA React: o HTML servido é só `<div id="root">`, então não
    há o que raspar nele. Os dados, porém, estão embutidos no próprio bundle
    JavaScript como uma lista de objetos literais
    ({edital, cargo, arquivoEdital, unidades, ...}) — é isso que este scraper
    lê. O nome do bundle carrega um hash que muda a cada deploy, por isso ele
    é descoberto a partir do HTML em vez de ficar fixo no código.

    Duas decisões de recorte:

    * Um edital abre dezenas de cargos (o 003/2026 sozinho tem 23 cargos
      médicos). Notificar cargo a cargo viraria uma enxurrada, então
      agrupamos por edital e resumimos as especialidades numa entrada só.
    * A página guarda todo o histórico, incluindo 2025. Só editais do ano
      corrente entram: os anteriores já se encerraram e só poluiriam o radar.
    """
    job_type = PROCESSO_SELETIVO
    region = CAPITAL_METROPOLITANA

    url = "https://pss.riosaude.rio.br"

    #: Cada objeto no bundle descreve um cargo dentro de um edital.
    _RECORD_RE = re.compile(
        r'\{edital:"(?P<edital>[^"]*)",cargo:"(?P<cargo>[^"]*)".*?arquivoEdital:"(?P<arquivo>[^"]*)"'
    )
    _BUNDLE_RE = re.compile(r'src="(?P<path>/assets/index-[^"]+\.js)"')

    #: Quantas especialidades aparecem no título antes de virar "(+N)".
    _MAX_CARGOS_NO_TITULO = 3

    def scrape(self) -> List[Dict[str, str]]:
        page = self.fetch_html(self.url)
        if not page:
            return []

        bundle_match = self._BUNDLE_RE.search(page)
        if not bundle_match:
            logging.error(f"[{self.label}] Não encontrei o bundle JS na página — layout mudou?")
            return []

        bundle = self.fetch_html(f"{self.url}{bundle_match.group('path')}")
        if not bundle:
            return []

        ano_corrente = str(now_brt().year)
        por_edital: "OrderedDict[tuple, List[str]]" = OrderedDict()

        for record in self._RECORD_RE.finditer(bundle):
            edital = record.group("edital")
            cargo = sanitize_title(record.group("cargo"))
            if not edital.endswith(f"/{ano_corrente}") or not self.is_relevant(cargo):
                continue
            por_edital.setdefault((edital, record.group("arquivo")), []).append(cargo)

        found_jobs = [
            self._build_job(edital, arquivo, cargos)
            for (edital, arquivo), cargos in por_edital.items()
        ]

        unique_jobs = {job["link"]: job for job in found_jobs}.values()
        logging.info(f"[{self.label}] Found {len(unique_jobs)} relevant medical jobs.")
        return list(unique_jobs)

    def _build_job(self, edital: str, arquivo: str, cargos: List[str]) -> Dict[str, str]:
        mostrados = cargos[:self._MAX_CARGOS_NO_TITULO]
        resumo = ", ".join(mostrados)
        restantes = len(cargos) - len(mostrados)
        if restantes > 0:
            resumo += f" (+{restantes})"

        # O PDF do edital fica na raiz do site (testado: /001-2026.pdf
        # devolve application/pdf, enquanto outros caminhos caem no HTML
        # da SPA). Sem arquivo, o link cai para a home — ainda único por
        # causa do fragmento com o número do edital.
        link = f"{self.url}/{arquivo}" if arquivo else f"{self.url}/#{edital.replace('/', '-')}"

        return {
            "title": f"[RioSaúde PSS] Edital {edital} — {resumo}",
            "link": link,
            "source_url": self.url,
            "pub_date": today_str(),
            # Classificado sobre a lista COMPLETA de cargos, não sobre o
            # resumo do título: o edital 003/2026 abre 23 cargos médicos e o
            # título mostra 3. Sem isto, vinte especialidades reais ficariam
            # invisíveis para quem filtra por elas.
            "specialties": classify_specialties(" ".join(cargos)),
        }


class IbamScraper(BaseScraper):
    """Scraper do site de concursos do IBAM (Instituto Brasileiro de
    Administração Municipal), agregador nacional de concursos municipais.

    Esta fonte lista concursos de QUALQUER área em QUALQUER estado (não é
    pré-filtrada por saúde nem por RJ), então exigimos os dois filtros:
    is_in_target_state() sobre o nome do município/UF E is_relevant() sobre
    o cargo descrito — sem os dois, a maior parte dos resultados seria de
    concursos de outros estados/áreas (ex: procurador em Blumenau/SC,
    guarda-vidas em Saquarema/RJ que, apesar de RJ, não é vaga de saúde).
    """
    url = "https://www.ibam-concursos.org.br/default.asp"

    def _find_candidates(self, soup: BeautifulSoup):
        return soup.select("div.concurso-card")

    def _parse_candidate(self, candidate) -> Optional[Dict[str, str]]:
        heading = candidate.find("h4")
        if not heading:
            return None
        municipio = heading.get_text(strip=True)

        descricao_tag = candidate.select_one("div.fs-16")
        descricao = descricao_tag.get_text(strip=True) if descricao_tag else ""

        full_text = f"{municipio} {descricao}"
        if not (self.is_in_target_state(full_text) and self.is_relevant(full_text)):
            return None

        # Link para o edital em PDF (dentro do bloco de documentos que
        # acompanha o card); se não achar nenhum documento, cai para a
        # própria página de listagem.
        doc_link_tag = candidate.select_one("div[id^='docs-'] a[href]")
        link = urljoin(self.url, doc_link_tag["href"]) if doc_link_tag else self.url

        title = f"{municipio} - {descricao}" if descricao else municipio
        return {
            "title": f"[IBAM] {sanitize_title(title)}",
            "link": link,
            "source_url": self.url,
            "pub_date": today_str(),
        }
