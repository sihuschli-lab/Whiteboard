'use strict';

const http = require('http');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 10000);
const MAX_PLAYERS_PER_ROOM = Number(process.env.MAX_PLAYERS_PER_ROOM || 10);
const MAX_MESSAGE_BYTES = Number(process.env.MAX_MESSAGE_BYTES || 8 * 1024 * 1024);
const PENDING_TIMEOUT_MS = Number(process.env.PENDING_TIMEOUT_MS || 2 * 60 * 1000);
const HOST_GRACE_MS = Number(process.env.HOST_GRACE_MS || 10 * 60 * 1000);
const PLAYER_GRACE_MS = Number(process.env.PLAYER_GRACE_MS || 10 * 60 * 1000);
const PROTOCOL_VERSION = 2;

// boardId -> {
//   boardId, sessionId, masterHash, host, players, revokedPlayers, createdAt
// }
// App payloads remain opaque. The relay only authenticates reconnect identities and routes messages.
const rooms = new Map();

function safeId(value, max = 160) {
  if (typeof value !== 'string') return '';
  const s = value.trim();
  if (!s || s.length > max) return '';
  if (!/^[A-Za-z0-9._~-]+$/.test(s)) return '';
  return s;
}

function safeToken(value, max = 512) {
  if (typeof value !== 'string') return '';
  const s = value.trim();
  if (s.length < 16 || s.length > max) return '';
  if (!/^[A-Za-z0-9._~-]+$/.test(s)) return '';
  return s;
}

function newId(bytes = 18) {
  return crypto.randomBytes(bytes).toString('base64url');
}

function sha256(value) {
  return crypto.createHash('sha256').update(String(value)).digest();
}

function sameBuffer(a, b) {
  return Buffer.isBuffer(a) && Buffer.isBuffer(b) && a.length === b.length && crypto.timingSafeEqual(a, b);
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

function isOpen(ws) {
  return !!ws && ws.readyState === WebSocket.OPEN;
}

function connectedPlayers(room) {
  return [...room.players.values()].filter(p => p.admitted && isOpen(p.ws));
}

function admittedCount(room) {
  let n = 0;
  for (const p of room.players.values()) if (p.admitted) n++;
  return n;
}

function closeSocket(ws, code, reason) {
  try { ws?.close(code, reason); } catch {}
}

function removeRoom(boardId, reason = 'session_ended') {
  const room = rooms.get(boardId);
  if (!room) return;
  if (isOpen(room.host.ws)) send(room.host.ws, { type: 'room_closed', reason });
  for (const peer of room.players.values()) {
    if (isOpen(peer.ws)) {
      send(peer.ws, { type: 'room_closed', reason });
      closeSocket(peer.ws, 4001, reason);
    }
  }
  rooms.delete(boardId);
}

function resumePayload(room, playerId) {
  const body = Buffer.from(JSON.stringify({
    v: 1,
    b: room.boardId,
    s: room.sessionId,
    p: playerId,
    n: newId(12)
  })).toString('base64url');
  const sig = crypto.createHmac('sha256', room.masterHash).update(body).digest('base64url');
  return `${body}.${sig}`;
}

function verifyResumeToken(room, playerId, token) {
  if (typeof token !== 'string' || token.length > 1000) return false;
  const dot = token.lastIndexOf('.');
  if (dot < 1) return false;
  const body = token.slice(0, dot);
  const suppliedSig = token.slice(dot + 1);
  let expectedSig;
  try { expectedSig = crypto.createHmac('sha256', room.masterHash).update(body).digest('base64url'); }
  catch { return false; }
  const a = Buffer.from(suppliedSig);
  const b = Buffer.from(expectedSig);
  if (!sameBuffer(a, b)) return false;
  try {
    const data = JSON.parse(Buffer.from(body, 'base64url').toString('utf8'));
    return data?.v === 1 && data.b === room.boardId && data.s === room.sessionId && data.p === playerId;
  } catch {
    return false;
  }
}

function makeRoom(boardId, sessionId, masterToken, ws, clientId) {
  const room = {
    boardId,
    sessionId,
    masterHash: sha256(masterToken),
    host: { ws, clientId, disconnectedAt: null },
    players: new Map(),
    revokedPlayers: new Set(),
    createdAt: Date.now()
  };
  rooms.set(boardId, room);
  ws.meta = { role: 'host', boardId, clientId };
  return room;
}

function roomPlayerList(room) {
  return [...room.players.values()]
    .filter(p => p.admitted)
    .map(p => ({ clientId: p.clientId, connected: isOpen(p.ws) }));
}

function notifyHostOfPending(room) {
  if (!isOpen(room.host.ws)) return;
  for (const p of room.players.values()) {
    if (!p.admitted && isOpen(p.ws) && p.pendingPayload !== undefined) {
      send(room.host.ws, { type: 'join_request', clientId: p.clientId, payload: p.pendingPayload ?? null });
    }
  }
}

function claimHost(ws, msg) {
  const boardId = safeId(msg.boardId);
  const sessionId = safeId(msg.sessionId, 200);
  const masterToken = safeToken(msg.masterToken);
  const clientId = safeId(msg.clientId, 128) || newId(12);
  if (!boardId || !sessionId || !masterToken) return error(ws, 'BAD_HOST_CLAIM', 'Invalid host identity.');

  let room = rooms.get(boardId);
  let resumed = false;

  if (!room) {
    room = makeRoom(boardId, sessionId, masterToken, ws, clientId);
  } else {
    const masterHash = sha256(masterToken);
    if (!sameBuffer(masterHash, room.masterHash)) return error(ws, 'MASTER_AUTH_FAILED', 'Master identity is not valid for this board.');

    if (room.sessionId !== sessionId) {
      // Same master intentionally starts a different session. Old reconnect credentials become invalid.
      removeRoom(boardId, 'session_replaced');
      room = makeRoom(boardId, sessionId, masterToken, ws, clientId);
    } else {
      resumed = true;
      if (isOpen(room.host.ws) && room.host.ws !== ws) {
        send(room.host.ws, { type: 'host_replaced' });
        closeSocket(room.host.ws, 4009, 'host_replaced');
      }
      room.host.ws = ws;
      room.host.clientId = clientId;
      room.host.disconnectedAt = null;
      ws.meta = { role: 'host', boardId, clientId };
    }
  }

  send(ws, {
    type: 'room_claimed',
    protocol: PROTOCOL_VERSION,
    boardId,
    sessionId,
    clientId,
    resumed,
    maxPlayers: MAX_PLAYERS_PER_ROOM,
    players: roomPlayerList(room),
    hostGraceSeconds: Math.floor(HOST_GRACE_MS / 1000),
    playerGraceSeconds: Math.floor(PLAYER_GRACE_MS / 1000)
  });

  for (const peer of room.players.values()) {
    if (peer.admitted && isOpen(peer.ws)) send(peer.ws, { type: 'host_online' });
  }
  notifyHostOfPending(room);
}

function requestJoin(ws, msg) {
  const boardId = safeId(msg.boardId);
  const room = rooms.get(boardId);
  if (!room) return error(ws, 'ROOM_NOT_FOUND', 'Der Meister hat noch keine Spielrunde eröffnet.');
  if (admittedCount(room) >= MAX_PLAYERS_PER_ROOM) return error(ws, 'ROOM_FULL', 'Die Spielrunde ist voll.');

  let clientId = safeId(msg.clientId, 128) || newId(12);
  while (room.players.has(clientId) || room.host.clientId === clientId) clientId = newId(12);

  const peer = {
    ws,
    clientId,
    admitted: false,
    connectedAt: Date.now(),
    lastSeen: Date.now(),
    disconnectedAt: null,
    pendingPayload: msg.payload ?? null
  };
  room.players.set(clientId, peer);
  ws.meta = { role: 'player', boardId, clientId };

  send(ws, {
    type: 'join_pending',
    protocol: PROTOCOL_VERSION,
    boardId,
    sessionId: room.sessionId,
    clientId,
    hostOnline: isOpen(room.host.ws)
  });

  if (isOpen(room.host.ws)) {
    send(room.host.ws, { type: 'join_request', clientId, payload: peer.pendingPayload });
  } else {
    send(ws, { type: 'host_offline', graceSeconds: Math.floor(HOST_GRACE_MS / 1000) });
  }
}

function admitPlayer(ws, msg) {
  const meta = ws.meta;
  if (!meta || meta.role !== 'host') return error(ws, 'NOT_HOST', 'Host only.');
  const room = rooms.get(meta.boardId);
  if (!room) return error(ws, 'ROOM_NOT_FOUND', 'Room not found.');

  const clientId = safeId(msg.clientId, 128);
  const peer = room.players.get(clientId);
  if (!peer) return error(ws, 'PLAYER_NOT_FOUND', 'Player not found.');

  if (msg.accept === false) {
    send(peer.ws, { type: 'join_rejected', payload: msg.payload ?? null });
    closeSocket(peer.ws, 4003, 'join_rejected');
    room.players.delete(clientId);
    room.revokedPlayers.add(clientId);
    return;
  }

  if (admittedCount(room) >= MAX_PLAYERS_PER_ROOM && !peer.admitted) {
    return error(ws, 'ROOM_FULL', 'The room is full.');
  }

  peer.admitted = true;
  peer.pendingPayload = undefined;
  peer.disconnectedAt = null;
  room.revokedPlayers.delete(clientId);
  const resumeToken = resumePayload(room, clientId);
  send(peer.ws, {
    type: 'join_accepted',
    payload: msg.payload ?? null,
    clientId,
    sessionId: room.sessionId,
    resumeToken,
    playerGraceSeconds: Math.floor(PLAYER_GRACE_MS / 1000)
  });
  send(ws, { type: 'player_admitted', clientId });
}

function resumePlayer(ws, msg) {
  const boardId = safeId(msg.boardId);
  const clientId = safeId(msg.clientId, 128);
  const resumeToken = typeof msg.resumeToken === 'string' ? msg.resumeToken : '';
  const room = rooms.get(boardId);
  if (!room) return error(ws, 'ROOM_NOT_FOUND', 'Der Meister ist noch nicht wieder verbunden.');
  if (!clientId || room.revokedPlayers.has(clientId)) return error(ws, 'RESUME_REJECTED', 'Diese Spieleranmeldung ist nicht mehr gültig.');
  if (!verifyResumeToken(room, clientId, resumeToken)) return error(ws, 'RESUME_REJECTED', 'Die Wiederanmeldung ist ungültig oder gehört zu einer anderen Spielrunde.');

  let peer = room.players.get(clientId);
  if (!peer) {
    if (admittedCount(room) >= MAX_PLAYERS_PER_ROOM) return error(ws, 'ROOM_FULL', 'Die Spielrunde ist voll.');
    peer = { ws: null, clientId, admitted: true, connectedAt: Date.now(), lastSeen: Date.now(), disconnectedAt: null, pendingPayload: undefined };
    room.players.set(clientId, peer);
  }
  if (!peer.admitted) return error(ws, 'RESUME_REJECTED', 'Diese Spieleranmeldung ist nicht freigeschaltet.');

  if (isOpen(peer.ws) && peer.ws !== ws) {
    send(peer.ws, { type: 'player_replaced' });
    closeSocket(peer.ws, 4010, 'player_replaced');
  }

  peer.ws = ws;
  peer.lastSeen = Date.now();
  peer.disconnectedAt = null;
  ws.meta = { role: 'player', boardId, clientId };

  send(ws, {
    type: 'resume_accepted',
    protocol: PROTOCOL_VERSION,
    boardId,
    sessionId: room.sessionId,
    clientId,
    hostOnline: isOpen(room.host.ws),
    playerGraceSeconds: Math.floor(PLAYER_GRACE_MS / 1000)
  });
  if (isOpen(room.host.ws)) send(room.host.ws, { type: 'player_resumed', clientId });
  else send(ws, { type: 'host_offline', graceSeconds: Math.floor(HOST_GRACE_MS / 1000) });
}

function playerToHost(ws, msg) {
  const meta = ws.meta;
  if (!meta || meta.role !== 'player') return error(ws, 'NOT_PLAYER', 'Player only.');
  const room = rooms.get(meta.boardId);
  const peer = room?.players.get(meta.clientId);
  if (!room || !peer || !peer.admitted) return error(ws, 'NOT_ADMITTED', 'Player is not admitted.');
  peer.lastSeen = Date.now();
  if (!isOpen(room.host.ws)) return error(ws, 'HOST_OFFLINE', 'Der Meister ist vorübergehend nicht verbunden.');
  send(room.host.ws, { type: 'from_player', clientId: meta.clientId, payload: msg.payload ?? null });
}

function hostToPlayer(ws, msg) {
  const meta = ws.meta;
  if (!meta || meta.role !== 'host') return error(ws, 'NOT_HOST', 'Host only.');
  const room = rooms.get(meta.boardId);
  if (!room) return error(ws, 'ROOM_NOT_FOUND', 'Room not found.');
  const clientId = safeId(msg.clientId, 128);
  const peer = room.players.get(clientId);
  if (!peer || !peer.admitted) return error(ws, 'PLAYER_NOT_FOUND', 'Admitted player not found.');
  if (!isOpen(peer.ws)) return error(ws, 'PLAYER_OFFLINE', 'Player is temporarily offline.');
  send(peer.ws, { type: 'from_host', payload: msg.payload ?? null });
}

function hostBroadcast(ws, msg) {
  const meta = ws.meta;
  if (!meta || meta.role !== 'host') return error(ws, 'NOT_HOST', 'Host only.');
  const room = rooms.get(meta.boardId);
  if (!room) return error(ws, 'ROOM_NOT_FOUND', 'Room not found.');
  for (const peer of room.players.values()) if (peer.admitted && isOpen(peer.ws)) send(peer.ws, { type: 'from_host', payload: msg.payload ?? null });
}

function kickPlayer(ws, msg) {
  const meta = ws.meta;
  if (!meta || meta.role !== 'host') return error(ws, 'NOT_HOST', 'Host only.');
  const room = rooms.get(meta.boardId);
  if (!room) return error(ws, 'ROOM_NOT_FOUND', 'Room not found.');
  const clientId = safeId(msg.clientId, 128);
  const peer = room.players.get(clientId);
  if (!peer) return;
  room.revokedPlayers.add(clientId);
  send(peer.ws, { type: 'kicked', payload: msg.payload ?? null });
  closeSocket(peer.ws, 4004, 'kicked');
  room.players.delete(clientId);
}

function leaveRoom(ws) {
  const meta = ws.meta;
  if (!meta || meta.role !== 'player') return error(ws, 'NOT_PLAYER', 'Player only.');
  const room = rooms.get(meta.boardId);
  if (!room) return;
  const peer = room.players.get(meta.clientId);
  if (!peer) return;
  room.revokedPlayers.add(meta.clientId);
  room.players.delete(meta.clientId);
  if (isOpen(room.host.ws)) send(room.host.ws, { type: 'player_left', clientId: meta.clientId, reason: 'left', admitted: !!peer.admitted });
  send(ws, { type: 'left_room' });
}

function closeRoom(ws) {
  const meta = ws.meta;
  if (!meta || meta.role !== 'host') return error(ws, 'NOT_HOST', 'Host only.');
  removeRoom(meta.boardId, 'session_ended');
}

function handleMessage(ws, raw) {
  if (Buffer.byteLength(raw) > MAX_MESSAGE_BYTES) {
    error(ws, 'MESSAGE_TOO_LARGE', 'Message exceeds relay limit.');
    return closeSocket(ws, 1009, 'message_too_large');
  }
  let msg;
  try { msg = JSON.parse(raw.toString()); }
  catch { return error(ws, 'BAD_JSON', 'Invalid JSON.'); }
  if (!msg || typeof msg.type !== 'string') return error(ws, 'BAD_MESSAGE', 'Missing message type.');

  switch (msg.type) {
    case 'claim_host': return claimHost(ws, msg);
    case 'join_room': return requestJoin(ws, msg);
    case 'resume_player': return resumePlayer(ws, msg);
    case 'admit_player': return admitPlayer(ws, msg);
    case 'to_host': return playerToHost(ws, msg);
    case 'to_player': return hostToPlayer(ws, msg);
    case 'broadcast': return hostBroadcast(ws, msg);
    case 'kick_player': return kickPlayer(ws, msg);
    case 'leave_room': return leaveRoom(ws);
    case 'close_room': return closeRoom(ws);
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
      maxPlayers: MAX_PLAYERS_PER_ROOM,
      reconnectGraceSeconds: Math.floor(HOST_GRACE_MS / 1000),
      uptimeSeconds: Math.floor(process.uptime())
    }));
  }
  res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
  res.end(`<!doctype html><meta charset="utf-8"><title>Whiteboard Relay</title>
  <style>body{font-family:system-ui;background:#111;color:#eee;max-width:720px;margin:60px auto;padding:0 20px}code{background:#222;padding:2px 6px;border-radius:5px}</style>
  <h1>Whiteboard Relay is online</h1>
  <p>Protocol <code>${PROTOCOL_VERSION}</code> • reconnect-enabled • opaque application payloads.</p>
  <p>WebSocket endpoint: <code>/ws</code></p>
  <p>Health endpoint: <code>/health</code></p>`);
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: MAX_MESSAGE_BYTES, perMessageDeflate: false });

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', data => handleMessage(ws, data));
  ws.on('error', () => {});
  ws.on('close', () => {
    const meta = ws.meta;
    if (!meta) return;
    const room = rooms.get(meta.boardId);
    if (!room) return;
    const now = Date.now();

    if (meta.role === 'host') {
      if (room.host.ws === ws) {
        room.host.ws = null;
        room.host.disconnectedAt = now;
        for (const peer of room.players.values()) {
          if (peer.admitted && isOpen(peer.ws)) send(peer.ws, { type: 'host_offline', graceSeconds: Math.floor(HOST_GRACE_MS / 1000) });
        }
      }
      return;
    }

    const peer = room.players.get(meta.clientId);
    if (!peer || peer.ws !== ws) return;
    if (!peer.admitted) {
      room.players.delete(meta.clientId);
      if (isOpen(room.host.ws)) send(room.host.ws, { type: 'player_left', clientId: meta.clientId, reason: 'disconnected', admitted: false });
      return;
    }
    peer.ws = null;
    peer.disconnectedAt = now;
    if (isOpen(room.host.ws)) send(room.host.ws, { type: 'player_disconnected', clientId: meta.clientId, graceSeconds: Math.floor(PLAYER_GRACE_MS / 1000) });
  });
});

const heartbeat = setInterval(() => {
  const now = Date.now();
  for (const [boardId, room] of rooms) {
    if (!isOpen(room.host.ws) && room.host.disconnectedAt && now - room.host.disconnectedAt > HOST_GRACE_MS) {
      removeRoom(boardId, 'host_timeout');
      continue;
    }
    for (const [clientId, peer] of room.players) {
      if (!peer.admitted && now - peer.connectedAt > PENDING_TIMEOUT_MS) {
        send(peer.ws, { type: 'join_expired' });
        closeSocket(peer.ws, 4008, 'join_expired');
        room.players.delete(clientId);
        continue;
      }
      if (peer.admitted && !isOpen(peer.ws) && peer.disconnectedAt && now - peer.disconnectedAt > PLAYER_GRACE_MS) {
        room.players.delete(clientId);
        if (isOpen(room.host.ws)) send(room.host.ws, { type: 'player_expired', clientId });
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
server.listen(PORT, '0.0.0.0', () => console.log(`Whiteboard relay protocol ${PROTOCOL_VERSION} listening on port ${PORT}`));
