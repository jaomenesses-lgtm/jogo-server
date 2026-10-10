// Servidor multiplayer simples (uma sala só).
// Cada jogador manda sua posição; o servidor repassa para os outros.
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT, maxPayload: 1024 });

let proximoId = 1;
const jogadores = new Map(); // ws -> { id, x, y, z, ry, anim }

function enviar(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function paraTodosMenos(origem, obj) {
  const texto = JSON.stringify(obj);
  for (const ws of jogadores.keys()) {
    if (ws !== origem && ws.readyState === 1) ws.send(texto);
  }
}

function num(v) {
  return typeof v === "number" && isFinite(v) ? v : 0;
}

wss.on("connection", (ws) => {
  const eu = { id: proximoId++, x: 0, y: 0, z: 0, ry: 0, anim: 0 };
  jogadores.set(ws, eu);
  ws.isAlive = true;

  // diz ao novo jogador quem ele é e quem já está na sala
  enviar(ws, { t: "welcome", id: eu.id, outros: [...jogadores.values()].filter((j) => j.id !== eu.id) });
  paraTodosMenos(ws, { t: "join", id: eu.id });

  ws.on("pong", () => (ws.isAlive = true));

  ws.on("message", (dados) => {
    let m;
    try {
      m = JSON.parse(dados.toString());
    } catch (e) {
      return;
    }
    if (m.t !== "s") return;
    eu.x = num(m.x);
    eu.y = num(m.y);
    eu.z = num(m.z);
    eu.ry = num(m.ry);
    eu.anim = num(m.anim);
    paraTodosMenos(ws, { t: "s", id: eu.id, x: eu.x, y: eu.y, z: eu.z, ry: eu.ry, anim: eu.anim });
  });

  ws.on("close", () => {
    jogadores.delete(ws);
    paraTodosMenos(ws, { t: "leave", id: eu.id });
  });
});

// derruba conexões mortas (celular que perdeu sinal)
setInterval(() => {
  for (const ws of jogadores.keys()) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 20000);

console.log("Servidor ouvindo na porta " + PORT);
