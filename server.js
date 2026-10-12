'use strict';

const http = require('http');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 10000);
const MAX_PLAYERS_PER_ROOM = Number(process.env.MAX_PLAYERS_PER_ROOM || 12);
const MAX_MESSAGE_BYTES = Number(process.env.MAX_MESSAGE_BYTES || 8 * 1024 * 1024);
const PENDING_TIMEOUT_MS = 2 * 60 * 1000;
const IDLE_TIMEOUT_MS = 90 * 1000;
const PROTOCOL_VERSION = 1;

// roomId -> { host, players: Map(clientId -> peer), createdAt }
const rooms = new Map();

function safeId(value, max = 128) {
  if (typeof value !== 'string') return '';
  const s = value.trim();
  if (!s || s.length > max) return '';
  if (!/^[A-Za-z0-9._~-]+$/.test(s)) return '';
  return s;
}

function newId(bytes = 18) {
  return crypto.randomBytes(bytes).toString('base64url');
}

function send(ws, msg) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(msg));
    return true;
  }
  return false;
}

function error(ws, code, message) {
  send(ws, { type: 'error', code, message });
}

function removeRoom(roomId, reason = 'host_disconnected') {
  const room = rooms.get(roomId);
  if (!room) return;
  for (const peer of room.players.values()) {
    send(peer.ws, { type: 'room_closed', reason });
    try { peer.ws.close(4001, reason); } catch {}
  }
  rooms.delete(roomId);
}

function removePlayer(room, clientId, reason = 'disconnected') {
  const peer = room.players.get(clientId);
  if (!peer) return;
  room.players.delete(clientId);
  send(room.host.ws, {
    type: 'player_left',
    clientId,
    reason,
    admitted: !!peer.admitted
  });
}

function createRoom(ws, msg) {
  const requestedRoom = safeId(msg.roomId, 160);
  const clientId = safeId(msg.clientId, 128) || newId(12);
  const roomId = requestedRoom || newId(18);

  if (rooms.has(roomId)) {
    return error(ws, 'ROOM_EXISTS', 'That room already exists.');
  }

  const room = {
    roomId,
    host: { ws, clientId },
    players: new Map(),
    createdAt: Date.now()
  };
  rooms.set(roomId, room);

  ws.meta = { role: 'host', roomId, clientId };
  send(ws, {
    type: 'room_created',
    protocol: PROTOCOL_VERSION,
    roomId,
    clientId,
    maxPlayers: MAX_PLAYERS_PER_ROOM
  });
}

function requestJoin(ws, msg) {
  const roomId = safeId(msg.roomId, 160);
  const room = rooms.get(roomId);
  if (!room) return error(ws, 'ROOM_NOT_FOUND', 'Room not found.');
  if (room.players.size >= MAX_PLAYERS_PER_ROOM) {
    return error(ws, 'ROOM_FULL', 'Room is full.');
  }

  let clientId = safeId(msg.clientId, 128) || newId(12);
  while (room.players.has(clientId) || room.host.clientId === clientId) {
    clientId = newId(12);
  }

  const peer = {
    ws,
    clientId,
    admitted: false,
    connectedAt: Date.now(),
    lastSeen: Date.now()
  };
  room.players.set(clientId, peer);
  ws.meta = { role: 'player', roomId, clientId };

  send(ws, {
    type: 'join_pending',
    protocol: PROTOCOL_VERSION,
    roomId,
    clientId
  });

  // Opaque application payload. The relay neither parses nor validates it.
  send(room.host.ws, {
    type: 'join_request',
    clientId,
    payload: msg.payload ?? null
  });
}

function admitPlayer(ws, msg) {
  const meta = ws.meta;
  if (!meta || meta.role !== 'host') return error(ws, 'NOT_HOST', 'Host only.');
  const room = rooms.get(meta.roomId);
  if (!room) return error(ws, 'ROOM_NOT_FOUND', 'Room not found.');

  const clientId = safeId(msg.clientId, 128);
  const peer = room.players.get(clientId);
  if (!peer) return error(ws, 'PLAYER_NOT_FOUND', 'Player not found.');

  if (msg.accept === false) {
    send(peer.ws, { type: 'join_rejected', payload: msg.payload ?? null });
    try { peer.ws.close(4003, 'join_rejected'); } catch {}
    room.players.delete(clientId);
    return;
  }

  peer.admitted = true;
  send(peer.ws, { type: 'join_accepted', payload: msg.payload ?? null });
  send(ws, { type: 'player_admitted', clientId });
}

function playerToHost(ws, msg) {
  const meta = ws.meta;
  if (!meta || meta.role !== 'player') return error(ws, 'NOT_PLAYER', 'Player only.');
  const room = rooms.get(meta.roomId);
  const peer = room?.players.get(meta.clientId);
  if (!room || !peer) return error(ws, 'ROOM_NOT_FOUND', 'Room not found.');
  if (!peer.admitted) return error(ws, 'NOT_ADMITTED', 'Host has not admitted this player yet.');

  peer.lastSeen = Date.now();
  send(room.host.ws, {
    type: 'from_player',
    clientId: meta.clientId,
    payload: msg.payload ?? null
  });
}

function hostToPlayer(ws, msg) {
  const meta = ws.meta;
  if (!meta || meta.role !== 'host') return error(ws, 'NOT_HOST', 'Host only.');
  const room = rooms.get(meta.roomId);
  if (!room) return error(ws, 'ROOM_NOT_FOUND', 'Room not found.');

  const clientId = safeId(msg.clientId, 128);
  const peer = room.players.get(clientId);
  if (!peer || !peer.admitted) return error(ws, 'PLAYER_NOT_FOUND', 'Admitted player not found.');

  send(peer.ws, { type: 'from_host', payload: msg.payload ?? null });
}

function hostBroadcast(ws, msg) {
  const meta = ws.meta;
  if (!meta || meta.role !== 'host') return error(ws, 'NOT_HOST', 'Host only.');
  const room = rooms.get(meta.roomId);
  if (!room) return error(ws, 'ROOM_NOT_FOUND', 'Room not found.');

  for (const peer of room.players.values()) {
    if (peer.admitted) send(peer.ws, { type: 'from_host', payload: msg.payload ?? null });
  }
}

function kickPlayer(ws, msg) {
  const meta = ws.meta;
  if (!meta || meta.role !== 'host') return error(ws, 'NOT_HOST', 'Host only.');
  const room = rooms.get(meta.roomId);
  if (!room) return error(ws, 'ROOM_NOT_FOUND', 'Room not found.');

  const clientId = safeId(msg.clientId, 128);
  const peer = room.players.get(clientId);
  if (!peer) return;

  send(peer.ws, { type: 'kicked', payload: msg.payload ?? null });
  try { peer.ws.close(4004, 'kicked'); } catch {}
  room.players.delete(clientId);
}

function handleMessage(ws, raw) {
  if (Buffer.byteLength(raw) > MAX_MESSAGE_BYTES) {
    error(ws, 'MESSAGE_TOO_LARGE', 'Message exceeds relay limit.');
    return ws.close(1009, 'message_too_large');
  }

  let msg;
  try { msg = JSON.parse(raw.toString()); }
  catch { return error(ws, 'BAD_JSON', 'Invalid JSON.'); }

  if (!msg || typeof msg.type !== 'string') {
    return error(ws, 'BAD_MESSAGE', 'Missing message type.');
  }

  switch (msg.type) {
    case 'create_room': return createRoom(ws, msg);
    case 'join_room': return requestJoin(ws, msg);
    case 'admit_player': return admitPlayer(ws, msg);
    case 'to_host': return playerToHost(ws, msg);
    case 'to_player': return hostToPlayer(ws, msg);
    case 'broadcast': return hostBroadcast(ws, msg);
    case 'kick_player': return kickPlayer(ws, msg);
    case 'ping': return send(ws, { type: 'pong', t: Date.now() });
    default: return error(ws, 'UNKNOWN_TYPE', 'Unknown relay message type.');
  }
}

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    return res.end(JSON.stringify({
      ok: true,
      service: 'whiteboard-relay',
      protocol: PROTOCOL_VERSION,
      rooms: rooms.size,
      uptimeSeconds: Math.floor(process.uptime())
    }));
  }

  res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
  res.end(`<!doctype html><meta charset="utf-8"><title>Whiteboard Relay</title>
  <style>body{font-family:system-ui;background:#111;color:#eee;max-width:720px;margin:60px auto;padding:0 20px}code{background:#222;padding:2px 6px;border-radius:5px}</style>
  <h1>Whiteboard Relay is online</h1>
  <p>This server only forwards room messages. It does not contain the Whiteboard client.</p>
  <p>WebSocket endpoint: <code>/ws</code></p>
  <p>Health endpoint: <code>/health</code></p>`);
});

const wss = new WebSocketServer({
  server,
  path: '/ws',
  maxPayload: MAX_MESSAGE_BYTES,
  perMessageDeflate: false
});

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (data) => handleMessage(ws, data));
  ws.on('error', () => {});

  ws.on('close', () => {
    const meta = ws.meta;
    if (!meta) return;
    const room = rooms.get(meta.roomId);
    if (!room) return;

    if (meta.role === 'host') {
      removeRoom(meta.roomId, 'host_disconnected');
    } else {
      removePlayer(room, meta.clientId, 'disconnected');
    }
  });
});

const heartbeat = setInterval(() => {
  const now = Date.now();

  for (const [roomId, room] of rooms) {
    if (!room.host.ws || room.host.ws.readyState !== WebSocket.OPEN) {
      removeRoom(roomId, 'host_disconnected');
      continue;
    }

    for (const [clientId, peer] of room.players) {
      if (!peer.admitted && now - peer.connectedAt > PENDING_TIMEOUT_MS) {
        send(peer.ws, { type: 'join_expired' });
        try { peer.ws.close(4008, 'join_expired'); } catch {}
        room.players.delete(clientId);
      }
    }
  }

  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      try { ws.terminate(); } catch {}
      continue;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
}, 30000);

wss.on('close', () => clearInterval(heartbeat));

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Whiteboard relay listening on port ${PORT}`);
});
