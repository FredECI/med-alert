# MedAlert RJ

Radar automático de processos seletivos e concursos na área da saúde, com foco em Macaé, capital e Região dos Lagos (RJ). Roda sem supervisão via GitHub Actions (`.github/workflows/scraper.yml`), 2x ao dia: raspa ~10 fontes, deduplica contra um banco SQLite, notifica novidades no Telegram e publica um site estático (Jekyll/GitHub Pages) com todas as vagas já descobertas.

## Estrutura

```
medalert/
  storage.py         # SQLite: schema, dedupe por link, rastreio de "última vez visto"
  filtering.py         # Filtros de relevância (área médica) e regionalização (RJ)
  notify.py              # Notificações via Telegram
  report.py                # Geração de vagas_abertas.csv e index.md
  scrapers/                  # Um scraper por fonte, todos herdando de BaseScraper
  orchestrator.py              # Roda tudo: scrapers -> banco -> notificações -> relatórios
main.py                         # Ponto de entrada (usado pelo GitHub Actions)
tests/                           # pytest — dedupe, filtros, fuso horário, sanitização
```

## Rodando localmente

```bash
pip install -r requirements-dev.txt
pytest
```

Para rodar o robô de verdade (cria/atualiza `med_alerts.db`, `vagas_abertas.csv` e `index.md` no diretório atual):

```bash
pip install -r requirements.txt
python main.py
```

As variáveis `TELEGRAM_BOT_TOKEN` e `TELEGRAM_CHAT_IDS` (lista separada por vírgula) são opcionais — sem elas, o robô roda normalmente só sem enviar notificações.

## Dados

`med_alerts.db` é versionado no próprio repositório (é o banco de produção real). Ao rodar localmente para testar mudanças, prefira copiar o arquivo para fora do repo em vez de rodar direto contra ele.
