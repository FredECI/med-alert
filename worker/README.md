# Worker do bot (Cloudflare)

Atende o bot do Telegram: recebe `/start`, guarda as preferências de cada
assinante e responde na hora. O robô de vagas continua no GitHub Actions e só
**lê** a lista daqui.

Por que separado: o Telegram exige um endereço sempre no ar para entregar as
mensagens (webhook), e o GitHub Actions só existe durante a execução do cron.
O envio dos alertas fica do lado do Actions porque o Worker gratuito permite
no máximo 50 chamadas externas por requisição — não daria para disparar
dezenas de alertas de uma vez.

Nenhum dado de assinante entra no repositório: fica tudo no KV da Cloudflare.

## Passo a passo do deploy

Tudo abaixo roda uma única vez.

> **Requer Node 22 ou superior** — o wrangler recusa versões anteriores. Os
> testes deste diretório rodam em qualquer versão; só o deploy exige a 22.
> Confira com `node --version` antes de começar.

### 1. Conta na Cloudflare e login

Crie a conta gratuita em <https://dash.cloudflare.com/sign-up> (não pede
cartão) e depois autentique o terminal — abre o navegador para confirmar:

```bash
cd worker
npx wrangler login
```

### 2. Criar o namespace do KV

```bash
npx wrangler kv namespace create SUBSCRIBERS
```

O comando imprime um `id`. Copie e cole em `wrangler.toml`, no lugar de
`PREENCHER_APOS_CRIAR_O_NAMESPACE`.

### 3. Gerar os dois segredos novos

Um prova que o update veio mesmo do Telegram; o outro autoriza o robô a ler a
lista de assinantes. Gere valores aleatórios:

```bash
python -c "import secrets; print('WEBHOOK:', secrets.token_urlsafe(32)); print('SYNC:', secrets.token_urlsafe(32))"
```

Guarde os dois — serão usados aqui e, o `SYNC`, também no GitHub na Fase 4.

### 4. Enviar os segredos para o Worker

Cada comando pede o valor de forma interativa (não fica no histórico):

```bash
npx wrangler secret put TELEGRAM_BOT_TOKEN       # o mesmo token já usado no GitHub
npx wrangler secret put TELEGRAM_WEBHOOK_SECRET  # o WEBHOOK gerado acima
npx wrangler secret put SYNC_TOKEN               # o SYNC gerado acima
```

### 5. Publicar

```bash
npx wrangler deploy
```

Anote a URL impressa no fim, algo como
`https://medalert-bot.SEU-SUBDOMINIO.workers.dev`.

### 6. Registrar o webhook no Telegram

Troque os três valores e rode:

```bash
curl -X POST "https://api.telegram.org/bot<TOKEN_DO_BOT>/setWebhook" \
  -H "Content-Type: application/json" \
  -d '{"url":"https://<SUA_URL_DO_WORKER>/webhook","secret_token":"<WEBHOOK_GERADO>"}'
```

Resposta esperada: `{"ok":true,...}`.

> A partir daqui o bot passa a usar webhook, e o método antigo `getUpdates`
> deixa de funcionar para ele. É reversível a qualquer momento com
> `.../deleteWebhook`.

### 7. Testar

Mande `/start` para o bot no Telegram. Deve responder na hora, com os botões
de região. Comandos disponíveis: `/minhas`, `/regioes`, `/tipos`, `/parar`.

Para conferir a lista de assinantes:

```bash
curl -H "Authorization: Bearer <SYNC_GERADO>" https://<SUA_URL_DO_WORKER>/subscribers
```

## Se algo der errado

```bash
npx wrangler tail            # logs ao vivo do Worker
```

Verificar como o Telegram enxerga o webhook (mostra o último erro de entrega):

```bash
curl "https://api.telegram.org/bot<TOKEN_DO_BOT>/getWebhookInfo"
```

- **O bot não responde nada:** confira em `getWebhookInfo` se a URL está certa
  e se `last_error_message` diz algo. `403` costuma ser
  `TELEGRAM_WEBHOOK_SECRET` diferente do que foi passado no `setWebhook`.
- **`/subscribers` devolve 401:** o `SYNC_TOKEN` do Worker não bate com o
  enviado no header.

## Testes

```bash
npm test     # ou: node --test
```

Cobrem a lógica pura (marcação de opções, montagem dos teclados, padrões e o
limite de 64 bytes do `callback_data`). Rodam no CI junto com os testes Python.

A correspondência entre as chaves daqui e as de `medalert/taxonomy.py` é
verificada por `tests/test_taxonomy_parity.py`, no lado Python — renomear uma
chave em só um dos lados quebraria o filtro em silêncio.
