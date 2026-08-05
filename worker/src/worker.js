import { answerCallback, editMessage, sendMessage } from "./telegram.js";
import {
  deleteSubscriber,
  getSubscriber,
  listSubscribers,
  newSubscriber,
  putSubscriber,
} from "./store.js";
import {
  REGION_STEP_TEXT,
  TYPE_STEP_TEXT,
  isSubscriptionUseless,
  regionKeyboard,
  summaryText,
  toggle,
  typeKeyboard,
} from "./wizard.js";

const BOAS_VINDAS =
  "🩺 *MedAlert RJ*\n\n" +
  "Eu aviso quando aparece concurso, processo seletivo ou vaga de médico " +
  "no Rio de Janeiro.\n\n" +
  "Vamos ajustar o que você quer receber:";

const AVISO_SEM_FILTRO =
  "⚠️ Você desmarcou tudo em uma das listas, então nenhum alerta se encaixa " +
  "no seu filtro. Use /regioes ou /tipos para escolher ao menos uma opção.";

// ==========================================
// Autenticação das rotas usadas pelo robô
// ==========================================
function isSyncAuthorized(request, env) {
  return request.headers.get("Authorization") === `Bearer ${env.SYNC_TOKEN}`;
}

// O endereço do Worker é público (o repositório é aberto), então qualquer um
// poderia forjar um update. O Telegram devolve este header em toda chamada
// justamente para provar que a requisição veio dele.
function isTelegramAuthentic(request, env) {
  return (
    request.headers.get("X-Telegram-Bot-Api-Secret-Token") ===
    env.TELEGRAM_WEBHOOK_SECRET
  );
}

// ==========================================
// Comandos de texto
// ==========================================
async function handleCommand(env, chatId, text) {
  const comando = text.trim().split(/\s+/)[0].toLowerCase().replace(/@.*$/, "");
  let sub = await getSubscriber(env, chatId);

  if (comando === "/start") {
    if (!sub) {
      sub = newSubscriber(chatId);
      await putSubscriber(env, sub);
    }
    await sendMessage(env, chatId, BOAS_VINDAS);
    return sendMessage(env, chatId, REGION_STEP_TEXT, regionKeyboard(sub.regioes));
  }

  if (!sub) {
    return sendMessage(env, chatId, "Você ainda não está inscrito. Envie /start para começar.");
  }

  if (comando === "/regioes") {
    return sendMessage(env, chatId, REGION_STEP_TEXT, regionKeyboard(sub.regioes));
  }
  if (comando === "/tipos") {
    return sendMessage(env, chatId, TYPE_STEP_TEXT, typeKeyboard(sub.tipos));
  }
  if (comando === "/minhas") {
    return sendMessage(env, chatId, summaryText(sub));
  }
  if (comando === "/parar") {
    await deleteSubscriber(env, chatId);
    return sendMessage(
      env,
      chatId,
      "Pronto, removi seu cadastro e suas preferências. Se mudar de ideia, é só mandar /start. 👋",
    );
  }

  return sendMessage(
    env,
    chatId,
    "Comandos: /minhas, /regioes, /tipos, /parar",
  );
}

// ==========================================
// Toques nos botões
// ==========================================
async function handleCallback(env, callback) {
  const chatId = callback.message.chat.id;
  const messageId = callback.message.message_id;
  const [tipo, valor] = (callback.data || "").split("|");

  const sub = await getSubscriber(env, chatId);
  if (!sub) {
    await answerCallback(env, callback.id);
    return sendMessage(env, chatId, "Sua inscrição expirou. Envie /start para recomeçar.");
  }

  if (tipo === "r") {
    sub.regioes = toggle(sub.regioes, valor);
    await putSubscriber(env, sub);
    await answerCallback(env, callback.id);
    return editMessage(env, chatId, messageId, REGION_STEP_TEXT, regionKeyboard(sub.regioes));
  }

  if (tipo === "t") {
    sub.tipos = toggle(sub.tipos, valor);
    await putSubscriber(env, sub);
    await answerCallback(env, callback.id);
    return editMessage(env, chatId, messageId, TYPE_STEP_TEXT, typeKeyboard(sub.tipos));
  }

  if (tipo === "go") {
    if (valor === "regioes") {
      await answerCallback(env, callback.id);
      return editMessage(env, chatId, messageId, REGION_STEP_TEXT, regionKeyboard(sub.regioes));
    }
    if (valor === "tipos") {
      await answerCallback(env, callback.id);
      return editMessage(env, chatId, messageId, TYPE_STEP_TEXT, typeKeyboard(sub.tipos));
    }
    if (valor === "fim") {
      await answerCallback(env, callback.id, "Preferências salvas!");
      await editMessage(env, chatId, messageId, summaryText(sub));
      if (isSubscriptionUseless(sub)) {
        return sendMessage(env, chatId, AVISO_SEM_FILTRO);
      }
      return true;
    }
  }

  return answerCallback(env, callback.id);
}

async function handleUpdate(env, update) {
  if (update.callback_query) {
    return handleCallback(env, update.callback_query);
  }

  const message = update.message;
  if (message && message.text) {
    return handleCommand(env, message.chat.id, message.text);
  }
  return true;
}

export default {
  async fetch(request, env) {
    const { pathname } = new URL(request.url);

    // --- Webhook do Telegram ---
    if (pathname === "/webhook" && request.method === "POST") {
      if (!isTelegramAuthentic(request, env)) {
        return new Response("forbidden", { status: 403 });
      }
      try {
        await handleUpdate(env, await request.json());
      } catch (erro) {
        // Sempre 200: um erro devolvido faz o Telegram reenviar o mesmo update
        // repetidamente, e um bug de formatação viraria uma tempestade de
        // retentativas. O erro fica no log do Worker.
        console.error("Falha ao processar update:", erro);
      }
      return new Response("ok");
    }

    // --- Lidas pelo robô no GitHub Actions ---
    if (pathname === "/subscribers" && request.method === "GET") {
      if (!isSyncAuthorized(request, env)) {
        return new Response("unauthorized", { status: 401 });
      }
      const subscribers = await listSubscribers(env);
      return Response.json({ subscribers });
    }

    // Chamado quando o envio falha com 403 (usuário bloqueou o bot): sem isso,
    // o robô tentaria entregar para sempre a alguém que não recebe mais.
    if (pathname === "/subscribers/prune" && request.method === "POST") {
      if (!isSyncAuthorized(request, env)) {
        return new Response("unauthorized", { status: 401 });
      }
      const { chat_ids: chatIds = [] } = await request.json();
      // Conta remoções REAIS, não o tamanho da lista recebida: o robô usa
      // este número para saber quantos assinantes de fato saíram, e devolver
      // o total enviado faria uma limpeza vazia parecer bem-sucedida.
      let removed = 0;
      for (const chatId of chatIds) {
        if (await getSubscriber(env, chatId)) {
          await deleteSubscriber(env, chatId);
          removed += 1;
        }
      }
      return Response.json({ removed });
    }

    return new Response("not found", { status: 404 });
  },
};
