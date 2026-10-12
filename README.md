# Whiteboard Relay

Minimal WebSocket relay for the Whiteboard client.

The relay deliberately does **not** implement whiteboard features. The browser client remains authoritative for board state, images, drawings, text, dice, Focus/Complete, undo/redo, and permissions.

## Endpoints

- `GET /` — simple status page
- `GET /health` — JSON health check
- `WSS /ws` — WebSocket relay endpoint

After deployment on Render, a client connects to:

```text
wss://YOUR-RENDER-HOST.onrender.com/ws
```

## Stable relay protocol v1

Client -> relay:

- `create_room`
- `join_room`
- `admit_player`
- `to_host`
- `to_player`
- `broadcast`
- `kick_player`
- `ping`

The application-specific data lives inside `payload`. The relay forwards that payload without interpreting it.

### Host creates a room

```json
{"type":"create_room","roomId":"OPTIONAL_RANDOM_ROOM_ID","clientId":"OPTIONAL_CLIENT_ID"}
```

### Player requests to join

```json
{"type":"join_room","roomId":"ROOM_ID","clientId":"OPTIONAL_CLIENT_ID","payload":{"anything":"client-defined"}}
```

The host receives:

```json
{"type":"join_request","clientId":"...","payload":{"anything":"client-defined"}}
```

### Host accepts or rejects

```json
{"type":"admit_player","clientId":"...","accept":true,"payload":{"anything":"client-defined"}}
```

### Player -> host

```json
{"type":"to_host","payload":{"anything":"client-defined"}}
```

Host receives:

```json
{"type":"from_player","clientId":"...","payload":{"anything":"client-defined"}}
```

### Host -> one player

```json
{"type":"to_player","clientId":"...","payload":{"anything":"client-defined"}}
```

### Host -> all admitted players

```json
{"type":"broadcast","payload":{"anything":"client-defined"}}
```

Players receive host data as:

```json
{"type":"from_host","payload":{"anything":"client-defined"}}
```

## Render

Use:

- Runtime: Node
- Build command: `npm install`
- Start command: `npm start`
- Health check: `/health`

No database is required. Rooms exist only in RAM and disappear when the host disconnects or the Render instance restarts.
