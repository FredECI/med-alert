"""Gera os artefatos públicos (CSV e index.md) a partir do banco de dados."""
import csv
import logging
from typing import List

from medalert.models import Job
from medalert.storage import DatabaseManager
from medalert.textutils import sanitize_title
from medalert.timeutil import now_brt

SITE_TITLE = "MedAlert RJ"
SITE_HEADING = "# 🩺 MedAlert: Radar de Oportunidades\n\n"
SITE_DESCRIPTION = (
    "Painel atualizado automaticamente com editais e processos seletivos abertos, "
    "com foco especial em Macaé, capital e regiões próximas.\n\n"
)
TABLE_HEADER = "| Data de Descoberta | Título do Processo Seletivo | Link Oficial |\n"
TABLE_DIVIDER = "| :--- | :--- | :--- |\n"
LINK_LABEL = "Acessar Edital"
CSV_HEADER = ["Data de Captura", "Título do Processo Seletivo", "Link de Acesso"]


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

    def generate_markdown(self, filename: str = "index.md") -> None:
        """Exporta as vagas para um arquivo index.md (Página inicial do site)."""
        jobs = self.fetch_jobs()

        with open(filename, mode="w", encoding="utf-8") as file:
            # Esse cabeçalho (Frontmatter) diz ao GitHub para usar um layout legal
            file.write("---\n")
            file.write("layout: default\n")
            file.write(f"title: {SITE_TITLE}\n")
            file.write("---\n\n")

            file.write(SITE_HEADING)
            file.write(SITE_DESCRIPTION)

            data_hora_atual = now_brt().strftime("%d/%m/%Y às %H:%M")
            file.write(f"**Última atualização do robô:** {data_hora_atual}\n\n")

            file.write(TABLE_HEADER)
            file.write(TABLE_DIVIDER)

            for job in jobs:
                title = sanitize_title(job.title)
                file.write(f"| {job.discovered_at} | **{title}** | [{LINK_LABEL}]({job.link}) |\n")

        logging.info(f"📝 Site gerado: {filename} com {len(jobs)} vagas.")
