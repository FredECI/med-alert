async function callTelegram(env, method, payload) {
  const response = await fetch(
    `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/${method}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    },
  );

  if (!response.ok) {
    console.error(`Telegram ${method} falhou: ${response.status} ${await response.text()}`);
  }
  return response.ok;
}

// A prévia de link fica DESLIGADA por padrão, e ligada só quando quem chama
// pede. Num alerta de vaga ela mostraria o cabeçalho de um PDF de edital —
// ocupa espaço e não informa nada. Já numa mensagem cujo assunto É o link, a
// prévia diz o título e a descrição da página, que é o que confirma para a
// pessoa que o endereço é o certo antes de ela tocar nele.
export function buildMessagePayload(chatId, text, replyMarkup, { preview = false } = {}) {
  return {
    chat_id: chatId,
    text,
    parse_mode: "Markdown",
    disable_web_page_preview: !preview,
    ...(replyMarkup ? { reply_markup: replyMarkup } : {}),
  };
}

export function sendMessage(env, chatId, text, replyMarkup, options) {
  return callTelegram(env, "sendMessage", buildMessagePayload(chatId, text, replyMarkup, options));
}

export function editMessage(env, chatId, messageId, text, replyMarkup) {
  return callTelegram(env, "editMessageText", {
    chat_id: chatId,
    message_id: messageId,
    text,
    parse_mode: "Markdown",
    disable_web_page_preview: true,
    ...(replyMarkup ? { reply_markup: replyMarkup } : {}),
  });
}

// Sem isso o botão fica com a ampulheta girando no app do usuário, mesmo
// tendo funcionado.
export function answerCallback(env, callbackId, text) {
  return callTelegram(env, "answerCallbackQuery", {
    callback_query_id: callbackId,
    ...(text ? { text } : {}),
  });
}
