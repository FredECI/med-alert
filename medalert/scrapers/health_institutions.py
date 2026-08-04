"""Scrapers de instituições de saúde e do IBAM (RioSaúde, Fiotec, IBAM).

RioSaúde e Fiotec são fontes já escopadas por natureza (empresa pública de
saúde do Rio e fundação que administra as chamadas públicas dos hospitais
federais no Rio), então não reaplicamos is_relevant() palavra por palavra
neles — ver docstring de cada classe para o raciocínio específico. O IBAM,
por outro lado, lista concursos de qualquer área em qualquer estado do
país, então precisa dos dois filtros (estado-alvo E área de saúde).
"""
import re
from typing import Dict, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from medalert.scrapers.base import BaseScraper
from medalert.textutils import sanitize_title
from medalert.timeutil import today_str

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
            "pub_date": today_str(),
        }


class FiotecScraper(BaseScraper):
    """Scraper das Chamadas Públicas da Fiotec (fundação que administra a
    contratação para hospitais/institutos federais no Rio: INTO, INCA, INC,
    Hospital Federal da Lagoa).

    A URL já é escopada para a seção "Chamadas Públicas", mas a Fiotec
    também presta esse serviço para projetos não hospitalares de outros
    estados (ex: bolsista da Conab em MG) — por isso mantemos um filtro:
    aceitamos o item se ele bate em is_relevant() (palavra médica/saúde) OU
    menciona um dos institutos/hospitais federais atendidos no Rio.
    """
    url = "https://www.fiotec.fiocruz.br/pt/vagas-projetos/todas/chamadas-publicas"

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
        return {"title": f"[IBAM] {sanitize_title(title)}", "link": link, "pub_date": today_str()}
