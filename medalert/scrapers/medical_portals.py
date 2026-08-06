"""Portais de concursos médicos e bancas: MedGrupo, Estratégia (Saúde) e FGV."""
import json
import logging
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

from medalert.scrapers.base import BaseScraper
from medalert.taxonomy import (
    DESCONHECIDO,
    ENCERRADO,
    FONTE,
    NOTICIA,
    RESIDENCIA,
    SPECIALTIES,
    classify_specialties,
)
from medalert.textutils import sanitize_title
from medalert.timeutil import now_brt, today_str

#: Código do estado do Rio de Janeiro no filtro do MedGrupo (lido do
#: checkbox "Rio de Janeiro" em #lstEstados).
_MEDGRUPO_RJ = "1"
_MEDGRUPO_FILTER_URL = "https://concursos.medgrupo.com.br/Home/Filtrar"

#: Valor da opção "todas as especialidades" no formulário — não é uma
#: especialidade e precisa ficar de fora do catálogo.
_MEDGRUPO_TODAS = "-1"

#: Única palavra da coluna "inscrição" que afirma alguma coisa. Medido na
#: listagem real do RJ: de 83 concursos, 44 vinham como "encerradas", 28 como
#: "ver edital(is)" e 11 vazios.
_MEDGRUPO_ENCERRADA = "encerradas"


class MedGrupoScraper(BaseScraper):
    """Plantão de Concursos do MedGrupo — concursos e residências médicas.

    A listagem não vem no HTML da página: ela é carregada por AJAX. O
    formulário faz `POST Home/Filtrar` com um corpo JSON
    {estados, especialidades, tipos, ano} e recebe de volta um FRAGMENTO DE
    HTML (não JSON), que é injetado na página. Por isso este scraper
    sobrescreve scrape(): o template method da base só sabe fazer GET.

    Detalhe descoberto testando: `ano` é obrigatório — mandar null devolve
    500 ("null entry for parameter 'ano' of non-nullable type Int32").

    A fonte é exclusivamente médica e o filtro de estado já é aplicado pelo
    próprio site, então não reaplicamos is_relevant()/is_in_target_state().
    """
    job_type = RESIDENCIA

    url = "https://concursos.medgrupo.com.br/"

    def scrape(self) -> List[Dict[str, str]]:
        soup = self._buscar(None)
        if soup is None:
            return []

        found_jobs = [
            job for job in (self._parse_candidate(row) for row in soup.find_all("tr")) if job is not None
        ]

        unique_jobs = list({job["link"]: job for job in found_jobs}.values())

        por_link = self._especialidades_por_link()
        for job in unique_jobs:
            familias = por_link.get(job["link"])
            if familias:
                job["specialties"] = familias

        com_especialidade = sum(1 for job in unique_jobs if job.get("specialties"))
        logging.info(
            f"[{self.label}] Found {len(unique_jobs)} relevant medical jobs "
            f"({com_especialidade} com especialidade declarada pela fonte)."
        )
        return unique_jobs

    def _especialidades_por_link(self) -> Dict[str, List[str]]:
        """Link do edital -> famílias de especialidade que ele oferece.

        Perguntado ao próprio MedGrupo, não deduzido do texto. O site mantém um
        catálogo de 143 especialidades e filtra a listagem por elas; refazemos a
        busca uma vez por família e vemos quais concursos sobrevivem ao filtro.
        É caro (uma chamada por família) e vale a pena: o título de um concurso
        aqui é só o nome do hospital, e o corpo do edital cita especialidade em
        bibliografia, tabela de salário e pré-requisito — 219 ocorrências
        irrelevantes contra ~80 de vaga real quando medido. Ler ali marcaria
        quase todo edital com quase toda família.

        Falha em silêncio: sem o catálogo, as vagas saem sem especialidade e o
        resto da coleta segue igual.
        """
        catalogo = self._catalogo_de_especialidades()
        if not catalogo:
            return {}

        # Conjunto por link, não lista: um mesmo concurso ocupa várias linhas
        # da tabela (edital, retificação, anexos), então o link volta repetido
        # dentro de uma única busca.
        por_link: Dict[str, set] = {}
        for familia, ids in catalogo.items():
            for link in self._links_da_busca(ids):
                por_link.setdefault(link, set()).add(familia)

        # Reordenado pela taxonomia para a saída não depender da ordem em que
        # as buscas voltaram.
        return {
            link: [f for f in SPECIALTIES if f in familias]
            for link, familias in por_link.items()
        }

    def _catalogo_de_especialidades(self) -> Dict[str, List[str]]:
        """Família -> códigos do MedGrupo, montado classificando o catálogo
        deles com o nosso vocabulário. Assim uma especialidade nova no site
        entra sozinha, sem ninguém precisar editar uma tabela aqui."""
        html = self.fetch_html(self.url)
        if not html:
            return {}

        lista = BeautifulSoup(html, "html.parser").find(id="lstEspecialidades")
        if not lista:
            logging.error(f"[{self.label}] Catálogo de especialidades não encontrado — layout mudou?")
            return {}

        catalogo: Dict[str, List[str]] = {}
        for opcao in lista.select("input[value], option[value]"):
            codigo = opcao.get("value")
            rotulo = (opcao.get("data-label") or opcao.parent.get_text(" ", strip=True)).strip()
            if not codigo or codigo == _MEDGRUPO_TODAS or not rotulo:
                continue
            for familia in classify_specialties(rotulo):
                catalogo.setdefault(familia, []).append(codigo)
        return catalogo

    def _links_da_busca(self, especialidades: Optional[List[str]]) -> List[str]:
        """Links de edital devolvidos pela listagem com o filtro aplicado."""
        soup = self._buscar(especialidades)
        if soup is None:
            return []
        links = (self._extract_edital_link(linha) for linha in soup.find_all("tr"))
        return [link for link in links if link]

    def _buscar(self, especialidades: Optional[List[str]]) -> Optional[BeautifulSoup]:
        payload = json.dumps({
            "estados": [_MEDGRUPO_RJ],
            "especialidades": especialidades,
            "tipos": None,
            "ano": now_brt().year,
        })
        try:
            response = self.scraper.post(
                _MEDGRUPO_FILTER_URL,
                data=payload,
                headers={
                    "Content-Type": "application/json; charset=utf-8",
                    "X-Requested-With": "XMLHttpRequest",
                },
                timeout=25,
            )
            response.raise_for_status()
        except Exception as e:
            logging.error(f"Failed to fetch {_MEDGRUPO_FILTER_URL}. Error: {e}")
            return None
        return BeautifulSoup(response.text, "html.parser")

    def _parse_candidate(self, candidate) -> Optional[Dict[str, str]]:
        instituicao_tag = candidate.select_one("a.concurso-relacao")
        if not instituicao_tag:
            return None

        # O texto visível é a sigla ("AFAMCI"); o nome por extenso fica no
        # tooltip, que é bem mais informativo no alerta do Telegram.
        nome = sanitize_title(
            instituicao_tag.get("data-original-title") or instituicao_tag.get_text()
        )
        if not nome:
            return None

        link = self._extract_edital_link(candidate)
        if not link:
            return None

        return {
            "title": f"[MedGrupo] {nome}",
            "link": link,
            "source_url": self.url,
            "pub_date": today_str(),
            **self._read_status(candidate),
        }

    @staticmethod
    def _read_status(row) -> Dict[str, str]:
        """Situação das inscrições, declarada pela própria fonte.

        A coluna "inscrição" da listagem é assimétrica: ela afirma quando o
        prazo acabou, mas NÃO afirma o contrário. "ver editais" e a célula
        vazia significam "consulte o edital", não "está aberto" — por isso só
        `encerradas` vira uma conclusão, e o resto fica desconhecido.

        Ler daqui é o oposto de inferir: não há regex sobre PDF nem palpite
        sobre qual das dezenas de datas do edital é o prazo. É a fonte
        respondendo, então esta é a única origem em que confiamos a ponto de
        deixar de mandar o alerta.
        """
        celula = row.select_one("td.col-5")
        texto = celula.get_text(strip=True).lower() if celula else ""
        if texto == _MEDGRUPO_ENCERRADA:
            return {"status": ENCERRADO, "status_source": FONTE}
        return {"status": DESCONHECIDO}

    @staticmethod
    def _extract_edital_link(row) -> Optional[str]:
        """O link do edital não é um href da linha: ele vem dentro de um
        pedaço de HTML escapado no atributo data-original-title (o tooltip
        que lista os arquivos do edital)."""
        for tag in row.select("[data-original-title]"):
            tooltip = tag.get("data-original-title") or ""
            if "href" not in tooltip:
                continue
            inner = BeautifulSoup(tooltip, "html.parser").find("a", href=True)
            if inner:
                return inner["href"]
        return None


class EstrategiaSaudeScraper(BaseScraper):
    """Painel "Concursos Saúde" do blog do Estratégia Concursos.

    A página inteira já é da área da saúde, mas tem alcance NACIONAL: cada
    concurso é um `h3` com link para a matéria específica. Por isso aqui o
    filtro necessário é o geográfico, e não o de relevância médica — exigir
    palavra-chave médica descartaria títulos legítimos e enxutos como
    "Concurso SES RJ", que não repetem "saúde" no nome.
    """
    job_type = NOTICIA

    url = "https://www.estrategiaconcursos.com.br/blog/concursos-area-da-saude/"

    def _find_candidates(self, soup: BeautifulSoup):
        return soup.select("h3 a[href]")

    def _parse_candidate(self, candidate) -> Optional[Dict[str, str]]:
        title = sanitize_title(candidate.get_text())
        link = candidate.get("href", "")
        if not title or not link:
            return None

        if not self.is_in_target_state(title):
            return None

        return {"title": f"[Estratégia Saúde] {title}", "link": link, "pub_date": today_str()}


class FgvConcursosScraper(BaseScraper):
    """Concursos organizados pela FGV.

    Lista nacional e de todas as áreas (de tribunais a secretarias de saúde),
    então exige relevância médica. A listagem é PAGINADA (`?page=N`) e o item
    mais importante para nós não está na primeira página — o concurso da
    EBSERH aparece só na segunda.

    Sobre o recorte geográfico: o normal é exigir que o título cite o RJ, mas
    alguns empregadores nacionais da saúde contratam para unidades no Rio sem
    dizer isso no nome do certame. A EBSERH é o caso claro — administra os
    hospitais universitários federais, incluindo os do Rio —, então esses
    empregadores entram por uma lista curta e explícita em vez de afrouxar o
    filtro geográfico para todo mundo.
    """

    url = "https://conhecimento.fgv.br/concursos"

    #: Empregadores nacionais da saúde que contratam para unidades no Rio.
    NATIONAL_HEALTH_EMPLOYERS = ["ebserh", "ministério da saúde", "ministerio da saude", "fiocruz"]

    #: Páginas varridas por execução. A listagem mistura certames em
    #: andamento e encerrados sem ordenação útil para nós — a EBSERH está na
    #: 2ª página e o concurso de Saúde da PMERJ mais adiante, daí varrer 5.
    PAGES = 5

    def scrape(self) -> List[Dict[str, str]]:
        found_jobs: List[Dict[str, str]] = []

        for page in range(self.PAGES):
            page_url = self.url if page == 0 else f"{self.url}?page={page}"
            html = self.fetch_html(page_url)
            if not html:
                continue

            soup = BeautifulSoup(html, "html.parser")
            found_jobs.extend(
                job
                for job in (self._parse_candidate(c) for c in self._find_candidates(soup))
                if job is not None
            )

        unique_jobs = {job["link"]: job for job in found_jobs}.values()
        logging.info(f"[{self.label}] Found {len(unique_jobs)} relevant medical jobs.")
        return list(unique_jobs)

    def _find_candidates(self, soup: BeautifulSoup):
        return soup.select('a[href^="/concursos/"]')

    def _parse_candidate(self, candidate) -> Optional[Dict[str, str]]:
        title = sanitize_title(candidate.get_text())
        href = candidate.get("href", "")
        if not title or not href:
            return None

        if not self.is_relevant(title):
            return None

        title_lower = title.lower()
        national_health = any(emp in title_lower for emp in self.NATIONAL_HEALTH_EMPLOYERS)
        if not (self.is_in_target_state(title) or national_health):
            return None

        full_link = href if href.startswith("http") else f"https://conhecimento.fgv.br{href}"
        return {"title": f"[FGV] {title}", "link": full_link, "pub_date": today_str()}
