import { JOB_TYPES, REGIONS, labelFor } from "./taxonomy.js";

// Lógica pura do assistente de preferências: sem rede, sem KV. É o que dá
// para testar de verdade sem subir nada (ver worker/test/wizard.test.js).

export function toggle(selected, key) {
  return selected.includes(key)
    ? selected.filter((item) => item !== key)
    : [...selected, key];
}

function checkbox(marcado) {
  return marcado ? "✅" : "▫️";
}

function optionRows(options, selected, prefixo) {
  return options.map((opt) => [{
    text: `${checkbox(selected.includes(opt.key))} ${opt.label}`,
    callback_data: `${prefixo}|${opt.key}`,
  }]);
}

export function regionKeyboard(selected) {
  return {
    inline_keyboard: [
      ...optionRows(REGIONS, selected, "r"),
      [{ text: "Continuar ➡️", callback_data: "go|tipos" }],
    ],
  };
}

export function typeKeyboard(selected) {
  return {
    inline_keyboard: [
      ...optionRows(JOB_TYPES, selected, "t"),
      [
        { text: "⬅️ Voltar", callback_data: "go|regioes" },
        { text: "Concluir ✅", callback_data: "go|fim" },
      ],
    ],
  };
}

export const REGION_STEP_TEXT =
  "*Passo 1 de 2 — Onde você quer receber vagas?*\n\n" +
  "Toque para marcar ou desmarcar. Já deixei tudo marcado; " +
  "desmarque o que não interessa e toque em *Continuar*.";

export const TYPE_STEP_TEXT =
  "*Passo 2 de 2 — Que tipo de oportunidade?*\n\n" +
  '"Notícia / radar" são matérias sobre concursos que ainda vão sair — ' +
  "úteis como aviso antecipado, mas mais ruidosas. Por isso vêm desmarcadas.";

function listOrNone(keys, catalogo) {
  if (!keys.length) return "_nenhuma — você não receberá alertas_";
  return keys.map((k) => `• ${labelFor(catalogo, k)}`).join("\n");
}

export function summaryText(sub) {
  return (
    "*Suas preferências*\n\n" +
    `*Regiões:*\n${listOrNone(sub.regioes, REGIONS)}\n\n` +
    `*Tipos:*\n${listOrNone(sub.tipos, JOB_TYPES)}\n\n` +
    "Para mudar: /regioes ou /tipos. Para sair: /parar"
  );
}

export function isSubscriptionUseless(sub) {
  // Sem região ou sem tipo, nenhum alerta jamais casaria — melhor avisar do
  // que deixar a pessoa esperando por mensagens que nunca chegariam.
  return sub.regioes.length === 0 || sub.tipos.length === 0;
}
