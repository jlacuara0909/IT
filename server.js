import express from "express";
import axios from "axios";

const app = express();
app.use(express.json({ limit: "25mb" }));

const API_KEY = process.env.ANTHROPIC_API_KEY;
const AGENT_ID = process.env.ANTHROPIC_AGENT_ID || "agent_011CaqQvYwUshE1kV1L5Dqfj";
const ENVIRONMENT_ID = process.env.ANTHROPIC_ENVIRONMENT_ID || "env_01HpwrYj8eQfY9xBZtdDh4iM";
const VAULT_ID = process.env.ANTHROPIC_VAULT_ID || "vlt_011CaqPXcEfFfTL8DEwYpWsy";
const WAAPI_TOKEN = process.env.WAAPI_TOKEN;
const WAAPI_INSTANCE_ID = process.env.WAAPI_INSTANCE_ID || "91610";

console.log("API KEY EXISTS:", !!API_KEY);

const ANTHROPIC_HEADERS = {
  "x-api-key": API_KEY,
  "anthropic-version": "2023-06-01",
  "anthropic-beta": "managed-agents-2026-04-01",
  "content-type": "application/json",
};

const sessions = {};

async function waitForSessionReady(sessionId, maxWaitMs = 60000) {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    try {
      const res = await axios.get(
        `https://api.anthropic.com/v1/sessions/${sessionId}`,
        { headers: ANTHROPIC_HEADERS }
      );
      const status = res.data.status;
      console.log("📋 Estado sesión:", status);
      if (status === "idle" || status === "running") return true;
      if (status === "terminated") return false;
    } catch (err) {
      console.error("Error checkeando sesión:", err.message);
    }
    await new Promise(r => setTimeout(r, 2000));
  }
  return false;
}

async function createClaudeSession() {
  const response = await axios.post(
    "https://api.anthropic.com/v1/sessions",
    {
      agent: { type: "agent", id: AGENT_ID },
      environment_id: ENVIRONMENT_ID,
      vault_ids: [VAULT_ID],
    },
    { headers: ANTHROPIC_HEADERS }
  );
  const sessionId = response.data.id;
  console.log("✅ Sesión creada:", sessionId);

  const ready = await waitForSessionReady(sessionId);
  if (!ready) throw new Error("Sesión no se pudo inicializar");
  console.log("✅ Sesión lista");
  return sessionId;
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
  console.log("📤 Mensaje enviado a Claude");

  const stream = await streamPromise;
  if (!stream.ok) {
    const errorText = await stream.text();
    console.error("❌ Stream error:", stream.status, errorText);
    return "Error conectando con el agente.";
  }

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
        console.log("📨 Evento:", json.type);

        if (json.type === "agent.message") {
          for (const block of json.content || []) {
            if (block.type === "text") fullResponse += block.text;
          }
        }

        if (json.type === "session.status_idle" && json.stop_reason?.type !== "requires_action") {
          console.log("✅ Respuesta completa");
          reader.cancel();
          return fullResponse || "Sin respuesta del agente.";
        }

        if (json.type === "session.error") {
          console.error("❌ Error sesión:", JSON.stringify(json));
          reader.cancel();
          return "Hubo un error. Intentá de nuevo.";
        }
      } catch {}
    }
  }

  return fullResponse || "Sin respuesta.";
}

async function sendWhatsAppMessage(chatId, message) {
  try {
    await axios.post(
      `https://waapi.app/api/v1/instances/${WAAPI_INSTANCE_ID}/client/action/send-message`,
      { chatId, message },
      { headers: { Authorization: `Bearer ${WAAPI_TOKEN}`, "Content-Type": "application/json" } }
    );
    console.log("✅ WhatsApp enviado");
  } catch (err) {
    console.error("❌ Error WhatsApp:", err.response?.data?.message || err.message);
  }
}

app.post("/webhook/waapi", async (req, res) => {
  res.sendStatus(200);

  try {
    const message = req.body?.data?.message;
    if (!message) return;

    const chatId = message.from;
    const text = message.body;
    if (!text) return;
    if (message.fromMe) return;
    if (chatId === "status@broadcast") return;

    console.log("─────────────────────────────");
    console.log("Mensaje:", text, "de:", chatId);

    if (!API_KEY) {
      console.error("❌ No hay API key");
      return;
    }

    if (!sessions[chatId]) {
      console.log("Creando nueva sesión Claude...");
      try {
        sessions[chatId] = await createClaudeSession();
      } catch (err) {
        console.error("❌ Error creando sesión:", err.response?.data || err.message);
        await sendWhatsAppMessage(chatId, "Error iniciando sesión. Intentá de nuevo.");
        return;
      }
    }

    const response = await sendMessageToClaude(sessions[chatId], text);
    console.log("Respuesta:", response.slice(0, 100) + "...");

    await sendWhatsAppMessage(chatId, response);
  } catch (error) {
    console.error("ERROR:", error.response?.data || error.message || error);
    const chatId = req.body?.data?.message?.from;
    if (chatId && sessions[chatId]) delete sessions[chatId];
  }
});

app.get("/debug", (req, res) => {
  res.json({
    api_key_exists: !!API_KEY,
    vault_id: VAULT_ID,
    active_sessions: Object.keys(sessions).length,
    session_ids: sessions,
  });
});

app.get("/", (req, res) => {
  res.send("Bot WAAPI + Claude funcionando ✅");
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Servidor funcionando en puerto ${PORT}`);
});