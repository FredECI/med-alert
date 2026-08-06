"""Lê o edital das vagas que já estão no banco e preenche o prazo delas.

Existe porque a rodada normal só lê o edital de vaga NOVA — o acervo anterior
ficaria para sempre sem prazo. É uma tarefa de manutenção, rodada à mão uma
vez depois de estrear a leitura de editais (e de novo se o extrator melhorar
muito), não algo do caminho de execução.

Uso:
    python tools/backfill_deadlines.py [--db ARQUIVO] [--limite N] [--aplicar]

Sem --aplicar ele só mostra o que faria. Rode assim primeiro: o banco é
versionado no Git, então uma escrita errada vira commit.
"""
import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import cloudscraper  # noqa: E402

from medalert.enrich import read_deadline  # noqa: E402
from medalert.storage import DatabaseManager  # noqa: E402
from medalert.taxonomy import CRONOGRAMA, status_from_deadline  # noqa: E402
from medalert.timeutil import today_str  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="med_alerts.db")
    parser.add_argument("--limite", type=int, default=200)
    parser.add_argument("--aplicar", action="store_true", help="grava no banco")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)
    db = DatabaseManager(db_name=args.db)
    session = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "desktop": True}
    )

    # Só PDF e só quem ainda não tem prazo nem situação declarada pela fonte —
    # o mesmo recorte da rodada normal, para o retroativo não prometer mais do
    # que a coleta contínua entrega.
    pendentes = [
        job for job in db.fetch_all_jobs()
        if job.link.lower().endswith(".pdf") and not job.deadline and not job.status_source
    ][:args.limite]

    print(f"{len(pendentes)} vaga(s) a examinar em {args.db}\n")
    hoje = today_str()
    lidos = encerrados = 0

    try:
        for job in pendentes:
            prazo = read_deadline(job.link, session)
            if not prazo:
                continue
            lidos += 1
            situacao = status_from_deadline(prazo, hoje)
            encerrados += situacao == "encerrado"
            print(f"  {prazo}  {situacao:11} {job.title[:58]}")
            if args.aplicar:
                db.touch_seen(job.link, status=situacao, deadline=prazo,
                              status_source=CRONOGRAMA)
    finally:
        db.close()

    print(f"\nprazo lido em {lidos} de {len(pendentes)} · {encerrados} já encerrada(s)")
    if not args.aplicar:
        print("Nada foi gravado. Repita com --aplicar depois de conferir a lista acima.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
