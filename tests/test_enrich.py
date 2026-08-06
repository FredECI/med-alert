"""Testes da busca do documento de uma vaga.

O foco é a degradação: quase todo teste aqui verifica que uma falha vira None
em vez de exceção. Enriquecimento é bônus — se ele puder derrubar a coleta,
uma mudança de layout num único site tira o radar inteiro do ar.
"""
import pytest

from medalert import enrich


class _Resposta:
    def __init__(self, content=b"", erro=None):
        self.content = content
        self._erro = erro

    def raise_for_status(self):
        if self._erro:
            raise self._erro


class _Sessao:
    """Sessão falsa que conta as buscas — é assim que se prova o cache."""

    def __init__(self, resposta=None, erro=None):
        self.resposta = resposta if resposta is not None else _Resposta(b"<p>ok</p>")
        self.erro = erro
        self.buscas = []

    def get(self, url, timeout=None):
        self.buscas.append(url)
        if self.erro:
            raise self.erro
        return self.resposta


@pytest.fixture(autouse=True)
def _cache_isolado(tmp_path, monkeypatch):
    """Cada teste com seu próprio cache: sem isso um teste veria o arquivo
    gravado por outro e passaria por engano."""
    monkeypatch.setattr(enrich, "CACHE_DIR", tmp_path / "editais")


def _html_longo(texto="inscricoes ") -> bytes:
    return ("<html><body>" + texto * 200 + "</body></html>").encode("utf-8")


def test_text_is_extracted_from_html():
    sessao = _Sessao(_Resposta(_html_longo()))

    assert "inscricoes" in enrich.fetch_edital_text("https://x/pagina", sessao)


def test_a_network_failure_returns_none_instead_of_raising():
    sessao = _Sessao(erro=RuntimeError("site fora do ar"))

    assert enrich.fetch_edital_text("https://x/e.pdf", sessao) is None


def test_a_corrupt_pdf_returns_none_instead_of_raising():
    """pypdf levanta exceções próprias para arquivo truncado, e nenhuma delas
    justifica interromper a coleta."""
    sessao = _Sessao(_Resposta(b"%PDF-1.4 lixo truncado"))

    assert enrich.fetch_edital_text("https://x/e.pdf", sessao) is None


def test_a_scanned_pdf_reads_as_nothing_to_read():
    """2 dos 38 editais do conjunto de referência são imagem pura. Devolver o
    punhado de caracteres soltos seria pior que devolver nada: daria ao
    extrator a impressão de ter um documento para ler."""
    sessao = _Sessao(_Resposta(b"<html>oi</html>"))

    assert enrich.fetch_edital_text("https://x/pagina", sessao) is None


def test_an_oversized_document_is_not_read():
    """Editais têm centenas de KB. Dezenas de MB é sinal de que o link aponta
    para outra coisa, e baixá-lo gastaria o tempo da rodada inteira."""
    sessao = _Sessao(_Resposta(b"x" * (enrich._TAMANHO_MAXIMO + 1)))

    assert enrich.fetch_edital_text("https://x/e.pdf", sessao) is None


def test_the_same_document_is_fetched_only_once():
    sessao = _Sessao(_Resposta(_html_longo()))

    enrich.fetch_edital_text("https://x/pagina", sessao)
    enrich.fetch_edital_text("https://x/pagina", sessao)

    assert len(sessao.buscas) == 1


def test_different_documents_do_not_share_a_cache_entry():
    sessao = _Sessao(_Resposta(_html_longo()))

    enrich.fetch_edital_text("https://x/um", sessao)
    enrich.fetch_edital_text("https://x/dois", sessao)

    assert len(sessao.buscas) == 2


def test_read_deadline_abstains_when_there_is_no_document():
    sessao = _Sessao(erro=RuntimeError("404"))

    assert enrich.read_deadline("https://x/e.pdf", sessao) is None


def test_read_deadline_finds_the_date_in_the_document():
    corpo = ("regras para a inscricao: o candidato devera acessar, entre os dias "
             "13/05/2026 a 20/05/2026, para preenchimento do formulario. ") * 12
    sessao = _Sessao(_Resposta(f"<html>{corpo}</html>".encode("utf-8")))

    assert enrich.read_deadline("https://x/pagina", sessao) == "2026-05-20"
