// SIN import de dotenv — Railway inyecta las variables directo
import express from "express";
import axios from "axios";

const app = express();
app.use(express.json({ limit: "25mb" }));

// ── Variables de entorno ──
const API_KEY = process.env.ANTHROPIC_API_KEY;
const AGENT_ID = process.env.ANTHROPIC_AGENT_ID || "agent_011CaqQvYwUshE1kV1L5Dqfj";
const ENVIRONMENT_ID = process.env.ANTHROPIC_ENVIRONMENT_ID || "env_01HpwrYj8eQfY9xBZtdDh4iM";
const WAAPI_TOKEN = process.env.WAAPI_TOKEN;
const WAAPI_INSTANCE_ID = process.env.WAAPI_INSTANCE_ID || "91610";

console.log("API KEY EXISTS:", !!API_KEY);
console.log("API KEY LENGTH:", API_KEY?.length);

const ANTHROPIC_HEADERS = {
  "x-api-key": API_KEY,
  "anthropic-version": "2023-06-01",
  "anthropic-beta": "managed-agents-2026-04-01",
  "content-type": "application/json",
};

const sessions = {};

async function createClaudeSession() {
  const response = await axios.post(
    "https://api.anthropic.com/v1/sessions",
    {
      agent: { type: "agent", id: AGENT_ID },
      environment_id: ENVIRONMENT_ID,
    },
    { headers: ANTHROPIC_HEADERS }
  );
  console.log("✅ Sesión creada:", response.data.id);
  return response.data.id;
}

async function sendMessageToClaude(sessionId, text) {
  const streamPromise = fetch(
    `https://api.anthropic.com/v1/sessions/${sessionId}/events/stream`,
    { headers: ANTHROPIC_HEADERS }
  );

  await axios.post(
    `https://api.anthropic.com/v1/sessions/${sessionId}/events`,
    {
      events: [
        { type: "user.message", content: [{ type: "text", text }] },
      ],
    },
    { headers: ANTHROPIC_HEADERS }
  );

  const stream = await streamPromise;
  const reader = stream.body.getReader();
  const decoder = new TextDecoder();

  let fullResponse = "";
  let buffer = "";
  const TIMEOUT_MS = 5 * 60 * 1000;
  const startTime = Date.now();

  while (true) {
    if (Date.now() - startTime > TIMEOUT_MS) {
      reader.cancel();
      break;
    }

    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() || "";

    for (const line of lines) {
      if (!line.startsWith("data: ")) continue;
      try {
        const json = JSON.parse(line.slice(6));

        if (json.type === "agent.message") {
          for (const block of json.content || []) {
            if (block.type === "text") fullResponse += block.text;
          }
        }

        if (json.type === "session.status_idle" && json.stop_reason?.type !== "requires_action") {
          reader.cancel();
          return fullResponse || "Sin respuesta del agente.";
        }

        if (json.type === "session.error") {
          reader.cancel();
          return "Error procesando tu consulta.";
        }
      } catch {}
    }
  }

  return fullResponse || "Sin respuesta.";
}

async function sendWhatsAppMessage(chatId, message) {
  await axios.post(
    `https://waapi.app/api/v1/instances/${WAAPI_INSTANCE_ID}/client/action/send-message`,
    { chatId, message },
    { headers: { Authorization: `Bearer ${WAAPI_TOKEN}`, "Content-Type": "application/json" } }
  );
}

app.get("/debug", (req, res) => {
  res.json({
    total_env_vars: Object.keys(process.env).length,
    anthropic_keys: Object.keys(process.env).filter(k => k.startsWith("ANTHROPIC")),
    waapi_keys: Object.keys(process.env).filter(k => k.startsWith("WAAPI")),
    api_key_exists: !!API_KEY,
    api_key_length: API_KEY?.length,
    test_var: process.env.TEST_VAR,
  });
});

app.post("/webhook/waapi", async (req, res) => {
  res.sendStatus(200);

  try {
    console.log("Webhook recibido");

    const message = req.body?.data?.message;
    if (!message) return;

    const chatId = message.from;
    const text = message.body;
    if (!text) return;

    console.log("Mensaje:", text, "de:", chatId);

    if (!API_KEY) {
      console.error("❌ No hay API key");
      return;
    }

    if (!sessions[chatId]) {
      console.log("Creando nueva sesión Claude...");
      sessions[chatId] = await createClaudeSession();
    }

    const response = await sendMessageToClaude(sessions[chatId], text);
    console.log("Respuesta Claude:", response.slice(0, 100) + "...");

    await sendWhatsAppMessage(chatId, response);
  } catch (error) {
    console.error("ERROR:", error.response?.data || error.message || error);
    const chatId = req.body?.data?.message?.from;
    if (chatId && sessions[chatId]) delete sessions[chatId];
  }
});

app.get("/", (req, res) => {
  res.send("Bot WAAPI + Claude funcionando ✅");
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Servidor funcionando en puerto ${PORT}`);
});