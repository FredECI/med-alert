// Testes da lógica pura do assistente. Usam o runner embutido do Node
// (`node --test`), sem nenhuma dependência a instalar.
import assert from "node:assert/strict";
import test from "node:test";

import {
  SITE_TEXT,
  SITE_URL,
  especialidadesDe,
  isSubscriptionUseless,
  regionKeyboard,
  specialtyKeyboard,
  summaryText,
  toggle,
  typeKeyboard,
} from "../src/wizard.js";
import {
  DEFAULT_JOB_TYPES,
  DEFAULT_REGIONS,
  DEFAULT_SPECIALTIES,
  REGION_KEYS,
  SPECIALTY_KEYS,
} from "../src/taxonomy.js";
import { newSubscriber } from "../src/store.js";
import { buildMessagePayload } from "../src/telegram.js";

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

test("o teclado de tipos permite voltar e avançar para especialidades", () => {
  const acoes = typeKeyboard(DEFAULT_JOB_TYPES).inline_keyboard.at(-1);
  assert.deepEqual(acoes.map((b) => b.callback_data), ["go|regioes", "go|especialidades"]);
});

test("o teclado de especialidades encerra o assistente", () => {
  const teclado = specialtyKeyboard(DEFAULT_SPECIALTIES);
  assert.equal(teclado.inline_keyboard.length, SPECIALTY_KEYS.length + 1);
  assert.deepEqual(
    teclado.inline_keyboard.at(-1).map((b) => b.callback_data),
    ["go|tipos", "go|fim"],
  );
});

test("novo assinante recebe todas as especialidades marcadas", () => {
  // Inclusive "sem especialidade identificada": a maioria das vagas não diz a
  // especialidade, e estrear com ela desmarcada esconderia quase tudo de quem
  // acabou de se inscrever.
  const sub = newSubscriber(123);
  assert.deepEqual(sub.especialidades, SPECIALTY_KEYS);
});

test("callback_data cabe no limite de 64 bytes do Telegram", () => {
  const todos = [
    ...regionKeyboard([]).inline_keyboard,
    ...typeKeyboard([]).inline_keyboard,
    ...specialtyKeyboard([]).inline_keyboard,
  ];
  for (const linha of todos) {
    for (const botao of linha) {
      const bytes = new TextEncoder().encode(botao.callback_data).length;
      assert.ok(bytes <= 64, `${botao.callback_data} tem ${bytes} bytes`);
    }
  }
});

test("o resumo lista as escolhas em português", () => {
  const texto = summaryText({
    regioes: ["regiao_dos_lagos"],
    tipos: ["residencia"],
    especialidades: ["pediatria"],
  });
  assert.match(texto, /Região dos Lagos/);
  assert.match(texto, /Residência médica/);
  assert.match(texto, /Pediatria e neonatologia/);
});

test("o resumo avisa quando a lista ficou vazia", () => {
  const texto = summaryText({ regioes: [], tipos: ["residencia"], especialidades: [] });
  assert.match(texto, /nenhuma/);
});

test("campo de especialidade ausente vale como TODAS, nunca como nenhuma", () => {
  // Quem se inscreveu antes deste passo existir não tem o campo no KV. Tratar
  // isso como lista vazia silenciaria a pessoa sem aviso — e, antes disso,
  // derrubaria o /minhas dela.
  const antigo = { regioes: ["outras_rj"], tipos: ["concurso"] };

  assert.deepEqual(especialidadesDe(antigo), SPECIALTY_KEYS);
  assert.ok(!isSubscriptionUseless(antigo));
  assert.match(summaryText(antigo), /Clínica médica/);
});

test("o texto do painel leva o endereço do site", () => {
  assert.equal(SITE_URL, "https://fredeci.github.io/med-alert/");
  assert.ok(SITE_TEXT.includes(SITE_URL), "o endereço precisa estar no corpo da mensagem");
});

test("a prévia de link vem desligada por padrão", () => {
  // Num alerta de vaga a prévia mostraria o cabeçalho de um PDF de edital:
  // ocupa espaço e não informa nada.
  const payload = buildMessagePayload("1", "texto");
  assert.equal(payload.disable_web_page_preview, true);
});

test("a prévia pode ser ligada quando o link é o assunto da mensagem", () => {
  const payload = buildMessagePayload("1", SITE_TEXT, undefined, { preview: true });
  assert.equal(payload.disable_web_page_preview, false);
});

test("o teclado continua sendo enviado junto quando existe", () => {
  const comTeclado = buildMessagePayload("1", "t", regionKeyboard([]));
  const semTeclado = buildMessagePayload("1", "t");

  assert.ok(comTeclado.reply_markup, "o teclado precisa chegar ao Telegram");
  assert.ok(!("reply_markup" in semTeclado), "sem teclado, o campo nem é enviado");
});

test("o resumo aponta para o painel", () => {
  // O alerta entrega só o que passa no filtro da pessoa; o painel é onde ela
  // acha o que ficou de fora e o que já passou.
  const texto = summaryText({ regioes: ["outras_rj"], tipos: ["concurso"] });
  assert.match(texto, /\/site/);
});

test("assinatura com qualquer das três listas vazia nunca casaria com nada", () => {
  const cheio = { regioes: ["outras_rj"], tipos: ["concurso"], especialidades: ["pediatria"] };
  assert.ok(isSubscriptionUseless({ ...cheio, regioes: [] }));
  assert.ok(isSubscriptionUseless({ ...cheio, tipos: [] }));
  assert.ok(isSubscriptionUseless({ ...cheio, especialidades: [] }));
  assert.ok(!isSubscriptionUseless(cheio));
});
