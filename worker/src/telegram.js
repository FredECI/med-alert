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

export function sendMessage(env, chatId, text, replyMarkup) {
  return callTelegram(env, "sendMessage", {
    chat_id: chatId,
    text,
    parse_mode: "Markdown",
    disable_web_page_preview: true,
    ...(replyMarkup ? { reply_markup: replyMarkup } : {}),
  });
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
