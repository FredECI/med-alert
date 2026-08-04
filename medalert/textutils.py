"""Utilitários de limpeza de texto compartilhados pelos scrapers e pelo gerador de relatórios."""
import re
import unicodedata
from typing import Tuple
from urllib.parse import urlparse

_SOURCE_PREFIX_RE = re.compile(r"^\[(?P<source>[^\]]+)\]\s*(?P<rest>.*)$")

# Vagas capturadas antes da padronização do prefixo "[Fonte]" não têm tag no
# título — hoje são ~46% do histórico e apareciam no site sem identificação
# nenhuma. Em vez de reescrever o banco, o domínio do link resolve na hora de
# publicar: vale para o histórico e ainda serve de rede de segurança se um
# scraper novo esquecer o prefixo.
_SOURCE_BY_DOMAIN = {
    "pciconcursos.com.br": "PCI",
    "trabalhabrasil.com.br": "Trabalha Brasil",
    "jcconcursos.com.br": "JC Concursos",
    "news.google.com": "Notícia/Radar",
    "g1.globo.com": "G1",
    "bing.com": "Radar/News",
    "macae.rj.gov.br": "Pref. Macaé",
    "riosaude.prefeitura.rio": "RioSaúde",
    "fiotec.fiocruz.br": "Fiotec",
    "ibam-concursos.org.br": "IBAM",
    "infojobs.com.br": "InfoJobs",
    "concursosnobrasil.com": "Concursos no Brasil",
    "araruama.rj.gov.br": "Araruama",
    "cabofrio.rj.gov.br": "Cabo Frio",
    "riodasostras.rj.gov.br": "Rio das Ostras",
    "saquarema.rj.gov.br": "Saquarema",
    "casimirodeabreu.rj.gov.br": "Casimiro de Abreu",
}


def strip_accents(text: str) -> str:
    """Remove acentos para o casamento não depender de 'macaé' vs 'macae'.

    Resolve na origem a duplicação manual de variantes acentuadas que as
    listas de filtro ainda carregam.
    """
    decomposed = unicodedata.normalize("NFD", str(text))
    return "".join(char for char in decomposed if unicodedata.category(char) != "Mn")


def sanitize_title(raw_title) -> str:
    """Remove quebras de linha e colapsa espaços redundantes de títulos raspados de HTML."""
    title = str(raw_title).replace("\n", " ").replace("\r", " ").replace("|", "-")
    return " ".join(title.split())


def split_source_and_title(full_title: str) -> Tuple[str, str]:
    """Separa o prefixo de fonte entre colchetes (ex: '[PCI RJ] Concurso...')
    do resto do título. Todos os scrapers prefixam o título dessa forma, então
    isso permite mostrar a fonte separada do texto no site em vez de deixar o
    colchete embutido no título toda vez. Sem prefixo reconhecível, devolve
    source vazio e o título original."""
    match = _SOURCE_PREFIX_RE.match(str(full_title).strip())
    if not match:
        return "", str(full_title).strip()
    return match.group("source"), match.group("rest")


def infer_source_from_link(link: str) -> str:
    """Nome da fonte deduzido do domínio do link, ou "" se desconhecido.

    Usado só como fallback para títulos sem o prefixo "[Fonte]".
    """
    host = urlparse(str(link)).netloc.lower()
    if host.startswith("www."):
        host = host[4:]

    for domain, source in _SOURCE_BY_DOMAIN.items():
        if host == domain or host.endswith(f".{domain}"):
            return source

    return ""
