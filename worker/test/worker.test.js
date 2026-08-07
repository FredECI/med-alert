// Testes do roteamento de comandos, exercitando o Worker pela porta de
// entrada real (o fetch do webhook) com KV e Telegram falsos.
//
// Existem por causa de um bug que passou por tudo: `SITE_TEXT` era usado sem
// ter sido importado. `node --check` valida sintaxe, não identificador
// indefinido; os testes do wizard só cobrem lógica pura; e o try/catch do
// webhook engole a exceção e devolve 200, então o Telegram não reclama e o
// usuário simplesmente não recebe resposta. Um comando quebrado ficava
// indistinguível de um comando que não existe.
import assert from "node:assert/strict";
import test from "node:test";

import worker from "../src/worker.js";
import { SITE_URL } from "../src/wizard.js";

const SEGREDO = "segredo-de-teste";

function kvFalso(inicial = {}) {
  const dados = new Map(Object.entries(inicial).map(([k, v]) => [k, JSON.stringify(v)]));
  return {
    get: async (chave, opcoes) => {
      const bruto = dados.get(chave);
      if (bruto === undefined) return null;
      return opcoes && opcoes.type === "json" ? JSON.parse(bruto) : bruto;
    },
    put: async (chave, valor) => { dados.set(chave, valor); },
    delete: async (chave) => { dados.delete(chave); },
    list: async () => ({
      keys: [...dados.keys()].map((name) => ({ name })),
      list_complete: true,
    }),
  };
}

/** Substitui o fetch global e devolve a lista de chamadas feitas ao Telegram. */
function interceptarTelegram(t) {
  const chamadas = [];
  const original = globalThis.fetch;
  globalThis.fetch = async (url, init) => {
    chamadas.push({ metodo: String(url).split("/").pop(), corpo: JSON.parse(init.body) });
    return { ok: true, text: async () => "" };
  };
  t.after(() => { globalThis.fetch = original; });
  return chamadas;
}

function ambiente(assinantes = {}) {
  return {
    SUBSCRIBERS: kvFalso(assinantes),
    TELEGRAM_BOT_TOKEN: "token",
    TELEGRAM_WEBHOOK_SECRET: SEGREDO,
    SYNC_TOKEN: "sync",
  };
}

function comando(texto, chatId = 42) {
  return new Request("https://worker.test/webhook", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Telegram-Bot-Api-Secret-Token": SEGREDO,
    },
    body: JSON.stringify({ message: { chat: { id: chatId }, text: texto } }),
  });
}

const INSCRITO = {
  "sub:42": { chat_id: "42", regioes: ["outras_rj"], tipos: ["concurso"], ativo: true },
};

test("/site responde com o endereço do painel", async (t) => {
  const chamadas = interceptarTelegram(t);

  await worker.fetch(comando("/site"), ambiente(INSCRITO));

  assert.equal(chamadas.length, 1, "o comando precisa produzir exatamente uma resposta");
  assert.ok(chamadas[0].corpo.text.includes(SITE_URL));
});

test("/site liga a prévia do link", async (t) => {
  const chamadas = interceptarTelegram(t);

  await worker.fetch(comando("/site"), ambiente(INSCRITO));

  assert.equal(chamadas[0].corpo.disable_web_page_preview, false);
});

test("/site funciona para quem não está inscrito", async (t) => {
  // O painel é público; quem nunca se inscreveu ou saiu com /parar também usa.
  const chamadas = interceptarTelegram(t);

  await worker.fetch(comando("/site"), ambiente());

  assert.ok(chamadas[0].corpo.text.includes(SITE_URL));
});

test("um comando quebrado não pode se parecer com um comando inexistente", async (t) => {
  // O coração da regressão: se o handler estourar, o catch devolve 200 e o
  // usuário não recebe NADA — igualzinho a um comando que não existe. Este
  // teste distingue os dois casos exigindo uma resposta.
  const chamadas = interceptarTelegram(t);

  for (const cmd of ["/site", "/minhas", "/regioes", "/tipos", "/especialidades"]) {
    chamadas.length = 0;
    await worker.fetch(comando(cmd), ambiente(INSCRITO));
    assert.equal(chamadas.length, 1, `${cmd} não respondeu nada`);
    assert.ok(chamadas[0].corpo.text, `${cmd} respondeu vazio`);
  }
});

test("comando desconhecido lista os comandos disponíveis", async (t) => {
  const chamadas = interceptarTelegram(t);

  await worker.fetch(comando("/naoexiste"), ambiente(INSCRITO));

  assert.match(chamadas[0].corpo.text, /\/site/);
  assert.match(chamadas[0].corpo.text, /\/minhas/);
});

test("update sem o header secreto é recusado", async (t) => {
  // O endereço do Worker é público; sem esta checagem qualquer um forjaria
  // mensagens em nome do Telegram.
  const chamadas = interceptarTelegram(t);
  const req = new Request("https://worker.test/webhook", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ message: { chat: { id: 1 }, text: "/site" } }),
  });

  const resposta = await worker.fetch(req, ambiente(INSCRITO));

  assert.equal(resposta.status, 403);
  assert.equal(chamadas.length, 0);
});
