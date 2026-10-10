// Servidor multiplayer (uma sala só).
//
// Protocolo (JSON, uma mensagem por frame de texto):
//   cliente -> servidor
//     {"t":"hi","n":"Nome"}                          (opcional, logo depois de conectar)
//     {"t":"s","x":0,"y":0,"z":0,"r":0,"k":0,"u":0,"d":0}   estado do jogador
//   servidor -> cliente
//     {"t":"welcome","id":7,"outros":[{"i":3,"n":"Ana","x":..,"y":..,"z":..,"r":..,"k":..,"u":..,"d":..}]}
//     {"t":"join","id":9,"n":"Beto"}
//     {"t":"leave","id":9}
//     {"t":"w","p":[[id,x,y,z,r,k,u,d], ...]}      mundo: só quem mudou, ~15x por segundo
//
// O servidor NÃO manda uma mensagem por jogador: ele junta tudo num único pacote
// por "tick". Isso gasta muito menos CPU/banda e é o que deixa o plano grátis aguentar.

const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const MAX_JOGADORES = Number(process.env.MAX_JOGADORES) || 60;
const TICK_MS = 66; // ~15 pacotes por segundo
const MIN_INTERVALO_MS = 30; // ignora quem manda estado mais rápido que isso
const LIMITE_MUNDO = 10000; // coordenada máxima aceita

// HTTP simples: o Render precisa de uma porta HTTP respondendo, e dá pra
// usar essa URL num "pinger" (UptimeRobot) pra o serviço grátis não dormir.
const servidor = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
  res.end(JSON.stringify({ ok: true, jogadores: jogadores.size }));
});

const wss = new WebSocketServer({ server: servidor, maxPayload: 1024, perMessageDeflate: false });

let proximoId = 1;
const jogadores = new Map(); // ws -> jogador

function num(v) {
  if (typeof v !== "number" || !isFinite(v)) return 0;
  if (v > LIMITE_MUNDO) return LIMITE_MUNDO;
  if (v < -LIMITE_MUNDO) return -LIMITE_MUNDO;
  return v;
}

function r2(v) {
  return Math.round(v * 100) / 100;
}

function nomeLimpo(n, id) {
  if (typeof n !== "string") return "Jogador" + id;
  n = n.replace(/[^A-Za-z0-9 _\-.]/g, "").trim().slice(0, 16);
  return n.length ? n : "Jogador" + id;
}

function enviar(ws, texto) {
  if (ws.readyState === 1) ws.send(texto);
}

function paraTodos(texto, menos) {
  for (const ws of jogadores.keys()) {
    if (ws !== menos) enviar(ws, texto);
  }
}

function linhaJogador(j) {
  return { i: j.id, n: j.nome, x: j.x, y: j.y, z: j.z, r: j.r, k: j.k, u: j.u, d: j.d };
}

wss.on("connection", (ws) => {
  if (jogadores.size >= MAX_JOGADORES) {
    ws.close(1013, "sala cheia");
    return;
  }
  const id = proximoId++;
  const eu = { id, nome: "Jogador" + id, x: 0, y: 0, z: 0, r: 0, k: 0, u: 0, d: 0, tem: false, sujo: false, ultimo: 0 };
  jogadores.set(ws, eu);
  ws.isAlive = true;

  const outros = [];
  for (const j of jogadores.values()) if (j !== eu && j.tem) outros.push(linhaJogador(j));
  enviar(ws, JSON.stringify({ t: "welcome", id, outros }));

  ws.on("pong", () => (ws.isAlive = true));

  ws.on("message", (dados) => {
    ws.isAlive = true;
    let m;
    try {
      m = JSON.parse(dados.toString());
    } catch (e) {
      return;
    }
    if (!m || typeof m !== "object") return;

    if (m.t === "hi") {
      eu.nome = nomeLimpo(m.n, id);
      if (eu.tem) paraTodos(JSON.stringify({ t: "join", id, n: eu.nome }), ws);
      return;
    }
    if (m.t !== "s") return;

    const agora = Date.now();
    if (agora - eu.ultimo < MIN_INTERVALO_MS) return;
    eu.ultimo = agora;

    eu.x = r2(num(m.x));
    eu.y = r2(num(m.y));
    eu.z = r2(num(m.z));
    eu.r = r2(num(m.r));
    eu.k = Math.max(0, Math.min(15, Math.round(num(m.k))));
    eu.u = r2(num(m.u));
    eu.d = Math.max(-1, Math.min(1, Math.round(num(m.d))));
    eu.sujo = true;

    if (!eu.tem) {
      // primeira posição: agora ele aparece pra todo mundo
      eu.tem = true;
      paraTodos(JSON.stringify({ t: "join", id, n: eu.nome }), ws);
    }
  });

  ws.on("close", () => {
    jogadores.delete(ws);
    if (eu.tem) paraTodos(JSON.stringify({ t: "leave", id }), ws);
  });

  ws.on("error", () => {
    try {
      ws.terminate();
    } catch (e) {}
  });
});

// um pacote só com todo mundo que mudou desde o último tick
setInterval(() => {
  const p = [];
  for (const j of jogadores.values()) {
    if (!j.sujo) continue;
    j.sujo = false;
    p.push([j.id, j.x, j.y, j.z, j.r, j.k, j.u, j.d]);
  }
  if (p.length === 0) return;
  paraTodos(JSON.stringify({ t: "w", p }), null);
}, TICK_MS);

// derruba conexões mortas (celular que perdeu sinal)
setInterval(() => {
  for (const ws of jogadores.keys()) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    try {
      ws.ping();
    } catch (e) {}
  }
}, 20000);

servidor.listen(PORT, () => console.log("Servidor ouvindo na porta " + PORT));

module.exports = { servidor, wss };
