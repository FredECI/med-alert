# MedAlert RJ

Radar automático de processos seletivos e concursos na área da saúde, com foco em Macaé, capital e Região dos Lagos (RJ). Roda sem supervisão via GitHub Actions (`.github/workflows/scraper.yml`), 2x ao dia: raspa 25 fontes, deduplica contra um banco SQLite, notifica novidades no Telegram e publica um site estático (Jekyll/GitHub Pages) com todas as vagas já descobertas.

## Estrutura

```
medalert/
  filtering.py       # Relevância médica, região (RJ) e "isto é vaga mesmo?"
  storage.py         # SQLite: schema, dedupe por link, rastreio de "última vez visto"
  notify.py          # Notificações via Telegram
  report.py          # Gera vagas_abertas.csv, _data/jobs.json e _data/robot_status.json
  scrapers/          # Um scraper por fonte, todos herdando de BaseScraper
  orchestrator.py    # Roda tudo: scrapers -> banco -> notificações -> artefatos
main.py              # Ponto de entrada (usado pelo GitHub Actions)
tests/               # pytest — filtros, dedupe, fuso, sanitização, um teste por scraper
```

O site é um template Jekyll: `index.md` + `_includes/` iteram sobre `site.data.jobs`
(vindo de `_data/jobs.json`). O Python **não** escreve HTML/Markdown — ele só produz
dados, e a apresentação vive nos templates.

## Filtragem

Três checagens independentes, combinadas conforme o quanto cada fonte já vem
pré-filtrada (ver a docstring de cada scraper para o critério aplicado):

- `is_relevant()` — o texto é da área médica/saúde?
- `is_in_target_state()` — é do RJ / de uma cidade-alvo?
- `has_job_signal()` — é contratação de verdade, e não um fórum/curso que
  "abre inscrições"? Termos fortes (`concurso`, `edital`, `processo seletivo`)
  valem sozinhos; termos fracos (`vaga`, `inscrições`) só contam fora de
  contexto de evento.

Medicina veterinária é descartada por **subtração**: o trecho veterinário é
apagado do texto e a relevância reavaliada no que sobrou. Assim "Médico
Veterinário" sai do radar, mas um concurso que abre vagas para "Médico
Clínico Geral e Médico Veterinário" continua entrando pela vaga humana.

## Rodando localmente

```bash
pip install -r requirements-dev.txt
pytest
```

Para rodar o robô de verdade (atualiza `med_alerts.db`, `vagas_abertas.csv` e `_data/*.json` no diretório atual):

```bash
pip install -r requirements.txt
python main.py
```

As variáveis `TELEGRAM_BOT_TOKEN` e `TELEGRAM_CHAT_IDS` (lista separada por vírgula) são opcionais — sem elas, o robô roda normalmente só sem enviar notificações.

## Dados

`med_alerts.db` é versionado no próprio repositório (é o banco de produção real). Ao rodar localmente para testar mudanças, prefira copiar o arquivo para fora do repo em vez de rodar direto contra ele.
