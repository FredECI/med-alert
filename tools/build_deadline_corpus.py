"""Baixa um edital, extrai o texto e o prepara para entrar no conjunto de
referência de prazos (tests/fixtures/prazos.json).

Isto é meia ferramenta de propósito: ela produz o TEXTO, nunca a anotação. O
campo `encerramento` continua sendo preenchido à mão, depois de abrir o
edital e conferir o texto normativo contra o cronograma do anexo — se uma
máquina anotasse, o conjunto só provaria que o extrator concorda consigo
mesmo, e deixaria de ser uma referência.

Uso:
    python tools/build_deadline_corpus.py <url-do-edital> <apelido>

Depois de rodar, acrescente a entrada correspondente em prazos.json com o
prazo que VOCÊ leu, e a nota explicando o que torna aquele edital
interessante (formato incomum, armadilha, inconsistência).

Precisa de pypdf, declarado em requirements.txt.
"""
import gzip
import io
import sys
from pathlib import Path

import cloudscraper
from pypdf import PdfReader

DESTINO = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "editais"

#: Abaixo disso o PDF é um escaneamento sem camada de texto. Vale guardar
#: assim mesmo: "não dá para ler" é um caso legítimo do conjunto, e o extrator
#: precisa abster nele em vez de quebrar.
MINIMO_DE_TEXTO = 500


def extrair_texto(url: str) -> str:
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "desktop": True}
    )
    resposta = scraper.get(url, timeout=40)
    resposta.raise_for_status()
    paginas = PdfReader(io.BytesIO(resposta.content)).pages
    return "\n".join((pagina.extract_text() or "") for pagina in paginas)


def main(url: str, apelido: str) -> int:
    texto = extrair_texto(url)
    DESTINO.mkdir(parents=True, exist_ok=True)
    caminho = DESTINO / f"{apelido}.txt.gz"

    with gzip.open(caminho, "wt", encoding="utf-8", compresslevel=9) as arquivo:
        arquivo.write(texto)

    print(f"gravado: {caminho.relative_to(Path.cwd())} ({len(texto)} caracteres)")
    if len(texto) < MINIMO_DE_TEXTO:
        print("  ⚠ quase sem texto — provavelmente um PDF escaneado.")
        print('    Anote encerramento: null, nota: "PDF escaneado...".')
    else:
        print("  Agora leia o edital e acrescente a entrada em tests/fixtures/prazos.json.")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__)
        raise SystemExit(2)
    raise SystemExit(main(sys.argv[1], sys.argv[2]))
