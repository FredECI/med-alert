// Testes da lógica pura do assistente. Usam o runner embutido do Node
// (`node --test`), sem nenhuma dependência a instalar.
import assert from "node:assert/strict";
import test from "node:test";

import {
  isSubscriptionUseless,
  regionKeyboard,
  summaryText,
  toggle,
  typeKeyboard,
} from "../src/wizard.js";
import { DEFAULT_JOB_TYPES, DEFAULT_REGIONS, REGION_KEYS } from "../src/taxonomy.js";
import { newSubscriber } from "../src/store.js";

test("toggle marca e desmarca sem alterar a lista original", () => {
  const original = ["a"];
  assert.deepEqual(toggle(original, "b"), ["a", "b"]);
  assert.deepEqual(toggle(original, "a"), []);
  assert.deepEqual(original, ["a"], "a lista de entrada não pode ser mutada");
});

test("novo assinante começa com tudo menos notícia/radar", () => {
  const sub = newSubscriber(123);
  assert.deepEqual(sub.regioes, DEFAULT_REGIONS);
  assert.ok(!sub.tipos.includes("noticia"), "notícia é ruidosa e vem desmarcada");
  assert.ok(sub.tipos.includes("concurso"));
  assert.equal(sub.chat_id, "123", "chat_id guardado como texto, igual ao banco");
});

test("chat_id é sempre string, venha número ou texto", () => {
  assert.equal(newSubscriber(123).chat_id, newSubscriber("123").chat_id);
});

test("o teclado mostra o estado atual de cada opção", () => {
  const teclado = regionKeyboard(["capital_metropolitana"]);
  const linhas = teclado.inline_keyboard.map((linha) => linha[0].text);

  assert.ok(linhas.some((t) => t.startsWith("✅") && t.includes("Capital")));
  assert.ok(linhas.some((t) => t.startsWith("▫️") && t.includes("Lagos")));
});

test("o teclado de regiões oferece uma opção por região e o avanço", () => {
  const teclado = regionKeyboard([]);
  assert.equal(teclado.inline_keyboard.length, REGION_KEYS.length + 1);
  assert.equal(teclado.inline_keyboard.at(-1)[0].callback_data, "go|tipos");
});

test("o teclado de tipos permite voltar e concluir", () => {
  const acoes = typeKeyboard(DEFAULT_JOB_TYPES).inline_keyboard.at(-1);
  assert.deepEqual(acoes.map((b) => b.callback_data), ["go|regioes", "go|fim"]);
});

test("callback_data cabe no limite de 64 bytes do Telegram", () => {
  const todos = [...regionKeyboard([]).inline_keyboard, ...typeKeyboard([]).inline_keyboard];
  for (const linha of todos) {
    for (const botao of linha) {
      const bytes = new TextEncoder().encode(botao.callback_data).length;
      assert.ok(bytes <= 64, `${botao.callback_data} tem ${bytes} bytes`);
    }
  }
});

test("o resumo lista as escolhas em português", () => {
  const texto = summaryText({ regioes: ["regiao_dos_lagos"], tipos: ["residencia"] });
  assert.match(texto, /Região dos Lagos/);
  assert.match(texto, /Residência médica/);
});

test("o resumo avisa quando a lista ficou vazia", () => {
  const texto = summaryText({ regioes: [], tipos: ["residencia"] });
  assert.match(texto, /nenhuma/);
});

test("assinatura sem região ou sem tipo nunca casaria com nada", () => {
  assert.ok(isSubscriptionUseless({ regioes: [], tipos: ["concurso"] }));
  assert.ok(isSubscriptionUseless({ regioes: ["outras_rj"], tipos: [] }));
  assert.ok(!isSubscriptionUseless({ regioes: ["outras_rj"], tipos: ["concurso"] }));
});
