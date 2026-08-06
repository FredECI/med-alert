import { JOB_TYPES, REGIONS, SPECIALTIES, SPECIALTY_KEYS, labelFor } from "./taxonomy.js";

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

export function specialtyKeyboard(selected) {
  return {
    inline_keyboard: [
      ...optionRows(SPECIALTIES, selected, "e"),
      [
        { text: "⬅️ Voltar", callback_data: "go|tipos" },
        { text: "Concluir ✅", callback_data: "go|fim" },
      ],
    ],
  };
}

export function typeKeyboard(selected) {
  return {
    inline_keyboard: [
      ...optionRows(JOB_TYPES, selected, "t"),
      [
        { text: "⬅️ Voltar", callback_data: "go|regioes" },
        { text: "Continuar ➡️", callback_data: "go|especialidades" },
      ],
    ],
  };
}

export const REGION_STEP_TEXT =
  "*Passo 1 de 3 — Onde você quer receber vagas?*\n\n" +
  "Toque para marcar ou desmarcar. Já deixei tudo marcado; " +
  "desmarque o que não interessa e toque em *Continuar*.";

export const TYPE_STEP_TEXT =
  "*Passo 2 de 3 — Que tipo de oportunidade?*\n\n" +
  '"Notícia / radar" são matérias sobre concursos que ainda vão sair — ' +
  "úteis como aviso antecipado, mas mais ruidosas. Por isso vêm desmarcadas.";

export const SPECIALTY_STEP_TEXT =
  "*Passo 3 de 3 — Quais especialidades?*\n\n" +
  "Atenção ao último item: a maioria dos editais não diz a especialidade em " +
  "lugar nenhum legível. Desmarcar *Sem especialidade identificada* some com " +
  "boa parte das vagas — inclusive algumas que são da sua área e simplesmente " +
  "não dizem isso no título.";

function listOrNone(keys, catalogo) {
  if (!keys.length) return "_nenhuma — você não receberá alertas_";
  return keys.map((k) => `• ${labelFor(catalogo, k)}`).join("\n");
}

// Quem se inscreveu antes das especialidades existirem não tem o campo no KV.
// AUSENTE significa TODAS, nunca nenhuma: a pessoa recebia tudo e continua
// recebendo tudo até decidir o contrário. Tratar como vazio a silenciaria sem
// aviso e ainda derrubaria o /minhas dela.
export const especialidadesDe = (sub) => sub.especialidades || [...SPECIALTY_KEYS];

export function summaryText(sub) {
  return (
    "*Suas preferências*\n\n" +
    `*Regiões:*\n${listOrNone(sub.regioes, REGIONS)}\n\n` +
    `*Tipos:*\n${listOrNone(sub.tipos, JOB_TYPES)}\n\n` +
    `*Especialidades:*\n${listOrNone(especialidadesDe(sub), SPECIALTIES)}\n\n` +
    "Para mudar: /regioes, /tipos ou /especialidades. Para sair: /parar"
  );
}

export function isSubscriptionUseless(sub) {
  // Qualquer uma das três listas vazia faz a interseção ser vazia, e nenhum
  // alerta jamais casaria — melhor avisar do que deixar a pessoa esperando
  // por mensagens que nunca chegariam.
  return (
    sub.regioes.length === 0 ||
    sub.tipos.length === 0 ||
    especialidadesDe(sub).length === 0
  );
}
