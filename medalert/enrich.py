"""Busca o documento de uma vaga e extrai dele o que dá para saber.

Três regras que valem para tudo aqui:

1. **O texto nunca é guardado no banco.** O SQLite é versionado no Git; 80
   editais de ~55 mil caracteres cada inflariam o repositório para sempre.
   Extrai, aproveita, descarta. O cache existe só em disco local e está no
   .gitignore.
2. **Nada aqui pode derrubar a rodada.** PDF corrompido, site fora do ar,
   layout novo, arquivo escaneado — tudo devolve None e a vaga entra sem o
   campo extra, exatamente como entrava antes. O enriquecimento é um bônus,
   não um pré-requisito.
3. **Só busca o que precisa.** Uma vaga é lida uma vez; nas rodadas seguintes
   o resultado vem do cache. Num dia normal isso significa nenhum ou poucos
   downloads.
"""
import io
import logging
import os
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

from medalert.deadline import extract_deadline

_TIMEOUT = 40

#: Acima disto o download é abortado. Editais têm centenas de KB; um arquivo
#: de dezenas de MB é sinal de que o link aponta para outra coisa, e baixá-lo
#: gastaria o tempo da rodada inteira.
_TAMANHO_MAXIMO = 12 * 1024 * 1024

#: Abaixo disto o PDF é um escaneamento sem camada de texto. Não é erro: é uma
#: resposta legítima ("não há o que ler"), e 2 dos 34 editais do conjunto de
#: referência são assim.
_MINIMO_DE_TEXTO = 500

#: Fora do repositório versionado — ver a regra 1 acima.
CACHE_DIR = Path(os.environ.get("MEDALERT_CACHE_DIR", ".cache/editais"))


def _caminho_no_cache(url: str) -> Path:
    import hashlib

    return CACHE_DIR / (hashlib.sha1(url.encode("utf-8")).hexdigest() + ".txt")


def _ler_pdf(conteudo: bytes) -> str:
    from pypdf import PdfReader

    paginas = PdfReader(io.BytesIO(conteudo)).pages
    return "\n".join((pagina.extract_text() or "") for pagina in paginas)


def _ler_html(conteudo: bytes) -> str:
    return BeautifulSoup(conteudo, "html.parser").get_text(separator=" ")


def fetch_edital_text(url: str, session) -> Optional[str]:
    """Texto do edital, ou None quando não há o que ler.

    `session` é injetada (na prática a do scraper, que já tem cloudscraper e
    retry configurados) para não abrir uma segunda pilha de HTTP no projeto.
    """
    cache = _caminho_no_cache(url)
    if cache.exists():
        texto = cache.read_text(encoding="utf-8")
        return texto if len(texto) >= _MINIMO_DE_TEXTO else None

    try:
        resposta = session.get(url, timeout=_TIMEOUT)
        resposta.raise_for_status()
        conteudo = resposta.content
        if len(conteudo) > _TAMANHO_MAXIMO:
            logging.info(f"📄 Documento grande demais, ignorado: {url}")
            return None

        eh_pdf = url.lower().endswith(".pdf") or conteudo[:5] == b"%PDF-"
        texto = _ler_pdf(conteudo) if eh_pdf else _ler_html(conteudo)
    except Exception as e:
        # De propósito amplo: pypdf levanta exceções próprias para arquivo
        # corrompido, e nenhuma delas justifica interromper a coleta.
        logging.info(f"📄 Não consegui ler {url}: {e}")
        return None

    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(texto, encoding="utf-8")
    return texto if len(texto) >= _MINIMO_DE_TEXTO else None


def read_deadline(url: str, session) -> Optional[str]:
    """Prazo de inscrição do edital em `url`, ou None ao abster.

    Junta as duas metades — buscar e interpretar — para quem chama não ter de
    conhecer nenhuma delas.
    """
    texto = fetch_edital_text(url, session)
    return extract_deadline(texto) if texto else None
