import { DEFAULT_JOB_TYPES, DEFAULT_REGIONS } from "./taxonomy.js";

// Uma chave por assinante, e não um blob único com a lista toda.
// O KV gratuito permite apenas 1 escrita por segundo NA MESMA CHAVE: com um
// blob compartilhado, dois usuários mexendo nas preferências ao mesmo tempo
// disputariam a mesma chave e uma das alterações se perderia no
// leia-modifique-escreva.
const PREFIX = "sub:";

const keyFor = (chatId) => `${PREFIX}${chatId}`;

export function newSubscriber(chatId) {
  return {
    chat_id: String(chatId),
    regioes: [...DEFAULT_REGIONS],
    tipos: [...DEFAULT_JOB_TYPES],
    criado_em: new Date().toISOString(),
    ativo: true,
  };
}

export async function getSubscriber(env, chatId) {
  return await env.SUBSCRIBERS.get(keyFor(chatId), { type: "json" });
}

export async function putSubscriber(env, sub) {
  await env.SUBSCRIBERS.put(keyFor(sub.chat_id), JSON.stringify(sub));
}

export async function deleteSubscriber(env, chatId) {
  // Apagar de verdade, não marcar como inativo: o `/parar` precisa remover o
  // dado pessoal de quem pediu para sair.
  await env.SUBSCRIBERS.delete(keyFor(chatId));
}

export async function listSubscribers(env) {
  const subs = [];
  let cursor;

  // O list() do KV pagina; sem seguir o cursor, assinantes além da primeira
  // página deixariam de receber alerta sem nenhum sinal de erro.
  do {
    const page = await env.SUBSCRIBERS.list({ prefix: PREFIX, cursor });
    for (const key of page.keys) {
      const sub = await env.SUBSCRIBERS.get(key.name, { type: "json" });
      if (sub && sub.ativo) subs.push(sub);
    }
    cursor = page.list_complete ? undefined : page.cursor;
  } while (cursor);

  return subs;
}
