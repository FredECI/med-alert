"""Montagem das mensagens enviadas no Telegram.

Lógica pura, sem rede: recebe uma vaga e devolve o texto. É o que permite
testar o formato de verdade, sem depender do Telegram.

Formato HTML, e não Markdown, por robustez. Os títulos vêm de páginas
raspadas e chegam cheios de `[`, `]`, `_` e `*` — no Markdown do Telegram
qualquer um desses desbalanceado faz a API recusar a mensagem com HTTP 400 e
o alerta se perde. No modo HTML só três caracteres precisam de escape
(`<`, `&`, `>`), o que é muito mais difícil de quebrar.
"""
from typing import Optional

from medalert.models import Job
from medalert.taxonomy import (
    CONCURSO,
    EMPREGO,
    NOTICIA,
    PROCESSO_SELETIVO,
    RESIDENCIA,
    job_type_label,
    region_label,
)

#: Cabeçalho por tipo. O de notícia é diferente de propósito: uma matéria de
#: jornal sobre um concurso que "deve sair" não é uma vaga aberta, e anunciá-la
#: como "Nova oportunidade" promete algo que o link não entrega.
_HEADERS = {
    CONCURSO: "📋 <b>Novo concurso público</b>",
    PROCESSO_SELETIVO: "📄 <b>Novo processo seletivo</b>",
    RESIDENCIA: "🎓 <b>Nova residência médica</b>",
    EMPREGO: "💼 <b>Nova vaga de emprego</b>",
    NOTICIA: "📰 <b>Radar: possível oportunidade</b>",
}
_DEFAULT_HEADER = "🚨 <b>Nova oportunidade encontrada</b>"

#: Rótulo do link principal, também por tipo — "Acessar edital" está errado
#: para uma notícia ou para uma vaga CLT.
_LINK_LABELS = {
    CONCURSO: "Acessar edital",
    PROCESSO_SELETIVO: "Acessar edital",
    RESIDENCIA: "Acessar edital",
    EMPREGO: "Ver vaga",
    NOTICIA: "Ler notícia",
}
_DEFAULT_LINK_LABEL = "Acessar"

_AVISO_NOTICIA = (
    "<i>Isto é uma notícia sobre um processo, não uma vaga confirmada. "
    "Vale acompanhar.</i>"
)


def escape_html(text) -> str:
    """Escapa o que o Telegram interpreta como marcação no modo HTML."""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _link(url: str, label: str) -> str:
    return f'<a href="{escape_html(url)}">{escape_html(label)}</a>'


def build_job_message(job: Job) -> str:
    """Monta o alerta de uma vaga."""
    header = _HEADERS.get(job.job_type or "", _DEFAULT_HEADER)
    link_label = _LINK_LABELS.get(job.job_type or "", _DEFAULT_LINK_LABEL)

    linhas = [
        header,
        "",
        f"🏥 <b>{escape_html(job.title)}</b>",
        f"📍 {escape_html(region_label(job.region))}"
        f" · 🏷 {escape_html(job_type_label(job.job_type))}",
        # "Descoberta em" e não "Data": nenhum scraper lê a data real de
        # publicação da fonte, então prometer isso seria impreciso.
        f"📅 Descoberta em {escape_html(job.discovered_at)}",
        "",
        f"🔗 {_link(job.link, link_label)}",
    ]

    # A página de origem costuma ter anexos, cronograma e retificações que o
    # PDF do edital sozinho não traz. Só aparece quando é de fato outro
    # endereço — repetir o mesmo link duas vezes só confundiria.
    if job.source_url:
        linhas.append(f"🌐 {_link(job.source_url, 'Página de origem')}")

    if job.job_type == NOTICIA:
        linhas.extend(["", _AVISO_NOTICIA])

    return "\n".join(linhas)


def build_failure_alert(total_scrapers: int, failures: list) -> str:
    """Aviso de operação, endereçado a quem mantém o robô."""
    detalhe = "\n".join(f"• {escape_html(f)}" for f in failures)
    return (
        "🔴 <b>MedAlert: falha total na execução</b>\n\n"
        f"Todos os {total_scrapers} scrapers falharam nesta rodada:\n\n"
        f"{detalhe}"
    )


def build_job_message_from_parts(
    title: str,
    discovered_at: str,
    link: str,
    job_type: Optional[str] = None,
    region: Optional[str] = None,
    source_url: Optional[str] = None,
) -> str:
    """Atalho para quem tem os campos soltos em vez de um Job."""
    return build_job_message(
        Job(
            title=title,
            link=link,
            discovered_at=discovered_at,
            job_type=job_type,
            region=region,
            source_url=source_url,
        )
    )
