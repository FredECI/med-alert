"""Gera os artefatos públicos a partir do banco de dados: a planilha CSV e os
dados que o Jekyll consome para montar o site (_data/jobs.json e
_data/robot_status.json).

O site (index.md + _includes/) não é mais escrito por aqui — ele é um
template estático que lê site.data.jobs no build do Jekyll. Isso separa o
conteúdo/apresentação (Liquid/HTML) da lógica de coleta (Python).
"""
import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from medalert.models import Job
from medalert.storage import DatabaseManager
from medalert.textutils import sanitize_title, split_source_and_title
from medalert.timeutil import now_brt

CSV_HEADER = ["Data de Captura", "Título do Processo Seletivo", "Link de Acesso"]


def _format_display_timestamp(iso_str: Optional[str]) -> Optional[str]:
    """Converte um timestamp ISO (como o gravado por storage.py) para o
    formato exibido no site. Formatar aqui, em Python testável, em vez de
    num filtro de data do Liquid que não dá pra testar localmente."""
    if not iso_str:
        return None
    try:
        parsed = datetime.fromisoformat(iso_str)
    except ValueError:
        return None
    return parsed.strftime("%d/%m/%Y às %H:%M")


def _write_json(filename: str, payload) -> None:
    path = Path(filename)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, mode="w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


class ReportGenerator:
    """Gera relatórios consolidados a partir do banco de dados SQLite."""

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    def fetch_jobs(self) -> List[Job]:
        """Busca todas as vagas já vistas, ordenadas pelas mais recentes."""
        return self.db.fetch_all_jobs()

    def generate_csv(self, filename: str = "vagas_abertas.csv") -> None:
        """Exporta as vagas para uma planilha CSV."""
        jobs = self.fetch_jobs()

        with open(filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file, delimiter=";")  # Ponto e vírgula é melhor para o Excel em português
            writer.writerow(CSV_HEADER)

            for job in jobs:
                writer.writerow([job.discovered_at, sanitize_title(job.title), job.link])

        logging.info(f"📊 Relatório CSV gerado: {filename} com {len(jobs)} vagas.")

    def generate_jobs_data(self, filename: str = "_data/jobs.json") -> None:
        """Escreve as vagas como JSON para o Jekyll ler via site.data.jobs.

        Separa a fonte (ex: "PCI RJ") do título — todo scraper prefixa o
        título com "[Fonte] ...", então isso evita que o colchete apareça
        cru no site e permite mostrar a fonte como um elemento próprio.
        """
        jobs = self.fetch_jobs()
        payload = []
        seen_signatures = set()

        for job in jobs:
            # Mesmo edital vindo de duas fontes tem links diferentes e por isso
            # vira duas linhas no banco; no site mostramos só uma. Vagas sem
            # assinatura confiável (dedup_key None) nunca são agrupadas.
            if job.dedup_key:
                if job.dedup_key in seen_signatures:
                    continue
                seen_signatures.add(job.dedup_key)

            source, rest = split_source_and_title(job.title)
            payload.append({
                "title": sanitize_title(rest),
                "source": sanitize_title(source),
                "link": job.link,
                "discovered_at": job.discovered_at,
                "last_seen_at": _format_display_timestamp(job.last_seen_at),
            })

        _write_json(filename, payload)
        logging.info(f"🗂️ Dados gerados: {filename} com {len(payload)} vagas.")


def write_robot_status(
    scrapers_total: int,
    failed_labels: List[str],
    filename: str = "_data/robot_status.json",
) -> None:
    """Registra a saúde da última execução para o painel do site.

    Ao contrário de generate_csv()/generate_jobs_data() (só regenerados
    quando há vaga nova), isto é escrito em TODA execução — "o robô rodou e
    está saudável" é informação nova mesmo sem vaga nova nenhuma. Só os
    nomes dos scrapers que falharam vão pro arquivo público; o erro
    detalhado de cada um continua só no log e no alerta do Telegram.
    """
    payload = {
        "last_run_at": now_brt().strftime("%d/%m/%Y às %H:%M"),
        "scrapers_total": scrapers_total,
        "scrapers_ok": scrapers_total - len(failed_labels),
        "scrapers_failed": failed_labels,
    }
    _write_json(filename, payload)
    logging.info(f"🩺 Status do robô gerado: {filename}.")
