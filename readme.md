# snub-ws

WebSocket server middleware for [snub](https://github.com/cokeeffekt/snub).

Built on [uWebSockets.js](https://github.com/uNetworking/uWebSockets.js). Requires Redis.

---

## Install

```
npm install snub snub-ws
```

### Node support

| Node | Supported |
|---|---|
| 18, 20 | ✅ (EOL — see below) |
| 22 | ✅ recommended |
| 23 | ✅ |
| 24, 26 | ✅ |
| 21, 25 | ❌ |

uWebSockets.js ships prebuilt binaries tied to Node's ABI and cannot build from source, and no
single release covers this whole range. snub-ws therefore installs two pinned builds
(`uws-modern` v20.69.0 for Node 22/24/26, `uws-legacy` v20.51.0 for Node 18/20/22/23) and loads
whichever matches the running Node. Nothing is required of you — but it does mean a large
`node_modules`, see below.

Node 18 and 20 are past end-of-life and `uws-legacy` is pinned to the last release supporting
them, so it will receive no further upstream fixes. Support for them will be dropped in a future
major; move to Node 22+ when you can.

### Reclaiming disk space

The two builds ship a binary for every platform/arch/ABI combination — about 218 MB, of which any
one machine can load a single ~7 MB file. In a container build you can drop the binaries your
platform can never use:

```dockerfile
RUN npm ci --omit=dev && npx snub-ws-prune
```

That leaves every ABI for the current platform and arch, so switching Node versions still works
without reinstalling. Around 170 MB is freed. Skip it if the same `node_modules` is reused across
operating systems or CPU architectures. Add `--dry-run` to preview.

---

## Quick start

```js
const Snub = require('snub');
const SnubWS = require('snub-ws');

const snub = new Snub({ host: 'localhost' });

snub.use(SnubWS({ port: 8585, auth: false }));

snub.on('ws:hello', function (event, reply) {
  console.log('message from', event.from.username, ':', event.payload);
  reply('hi back');
});
```

A WebSocket client connects to `ws://localhost:8585`, sends `["hello", "world"]`, and the server logs `message from null : world` and replies `["hello:reply", "hi back"]`.

---

## Config

```js
SnubWS({
  port: 8585,

  // Authentication — see Auth section
  auth: false,

  // Log verbose internal info
  debug: false,

  // Allow the same username to have multiple simultaneous connections
  multiLogin: true,

  // Milliseconds before an unauthenticated client is kicked (AUTH_TIMEOUT)
  authTimeout: 3000,

  // Rate limiting: [maxMessages, windowMs] — false to disable
  // e.g. [50, 5000] = max 50 messages per 5 seconds
  throttle: [50, 5000],

  // Milliseconds of inactivity before a client is kicked (IDLE_TIMEOUT)
  // Minimum: 5 minutes. Maximum: 960 seconds (uWS v20 limit).
  idleTimeout: 960000,

  // Milliseconds between websocket ping frames sent to every open socket, so
  // a proxy or load balancer in front does not reap quiet connections. Set it
  // below the shortest idle timeout on the path. Not client activity: it never
  // moves lastMsgTime or postpones IDLE_TIMEOUT. Minimum 1000, 0 = off.
  keepaliveInterval: 60000,

  // Restrict WebSocket upgrades to listed origins. null = allow all.
  // e.g. ['https://example.com', 'https://app.example.com']
  // Also scopes CORS on the HTTP offload route: when set, only these origins
  // may read offloaded message bodies; when null the route answers `*`.
  allowedOrigins: null,

  // Maximum simultaneous connections. 0 = unlimited.
  maxConnections: 0,

  // Maximum inbound message size in bytes (default 16 MB)
  maxPayloadLength: 16777216,

  // Maximum outbound buffer in bytes before backpressure kicks in (default 1 MB)
  maxBackpressure: 1048576,

  // Maximum queued messages per client under backpressure.
  // When exceeded the queue is cleared and the client is kicked (QUEUE_OVERFLOW).
  maxQueueSize: 100,

  // Messages larger than this (bytes) are offloaded to HTTP. 0 = disabled.
  // Default: 0.5 MB
  offloadToHttpSize: 524288,

  // Include the raw message string in the snub payload for debugging.
  // true = all events, or pass an array of specific event names.
  includeRaw: false,

  // Additional event names that clients are blocked from sending. Matched on
  // the first ':' segment, so 'admin' also blocks 'admin:anything'.
  internalWsEvents: [],

  // Cluster membership. Each instance heartbeats into Redis every
  // heartbeatInterval ms and is considered gone after instanceTtl ms without
  // one. Liveness is judged on Redis' clock, so host clocks need not agree.
  heartbeatInterval: 5000,
  instanceTtl: 15000,

  // Install SIGINT/SIGTERM/SIGUSR2 handlers that call close() and then exit.
  // Set false to own shutdown yourself — see Graceful shutdown.
  handleSignals: true,
})
```

---

## Auth

### No auth

```js
snub.use(SnubWS({ auth: false }));
```

All clients are accepted immediately on connect. `username` will be `null`.

### Function

```js
snub.use(SnubWS({
  auth: function (authPayload, accept) {
    if (authPayload.password === 'secret')
      return accept(true);
    accept(false);
  }
}));
```

`accept` can be called with:
- `true` — accept, `username` is taken from `authPayload.username`
- `false` — deny (client is kicked with `AUTH_FAIL`)
- `object` — accept and merge into the `_acceptAuth` reply sent to client

The `authPayload` argument is the object the client sent in its `_auth` message, merged with the current client state (so `authPayload.remoteAddress` etc. are available).

### Snub event

Delegate auth to any listener in your app:

```js
snub.use(SnubWS({ auth: 'authenticate-client' }));

snub.on('ws:authenticate-client', function (authPayload, reply) {
  // authPayload includes username, remoteAddress, etc.
  if (authPayload.username === 'admin')
    return reply({ role: 'admin' }); // merged into _acceptAuth
  reply(false);
});
```

### HTTP Basic Auth

If the WebSocket upgrade request includes an `Authorization: Basic …` header, it is decoded and used as the auth payload automatically — no `_auth` message required.

---

## Client protocol

Messages are JSON arrays: `[eventName, payload?, replyId?]`

### Authenticate

Send this as the first message after connecting (required unless `auth: false`):

```json
["_auth", { "username": "alice", "password": "secret" }]
```

On success the server responds:

```json
["_acceptAuth", { "_id": "connectionId" }]
```

Additional keys from the `accept(object)` call are included in this response.

On failure the client is kicked with reason `AUTH_FAIL`.

### Send a message

```json
["event-name", { "any": "payload" }]
```

With a reply ID (the server will reply to this ID):

```json
["event-name", { "any": "payload" }, "my-reply-id"]
```

The server replies with `["my-reply-id", replyData]`, or `["my-reply-id:error", { error: "..." }]` if nothing was listening.

### Built-in client events

| Event | Sent by | Description |
|-------|---------|-------------|
| `_auth` | client | Authenticate with the server |
| `_ping` | client | Ping the server — server responds with `_pong` |
| `_pong` | client | Response to server-initiated `_ping` — ignored |

Clients may only send the three events above from the `_` namespace; any other
`_`-prefixed name is dropped rather than forwarded to the bus. Events sent before
authentication completes are also dropped — only `_auth`, `_ping` and `_pong` are
accepted on an unauthenticated socket. A client can never reach snub-ws' own
control events (`send:…`, `kick:…`, `get-clients:…` and the rest); those names,
the `client-*` lifecycle events and a string `auth` event are reserved on the
first `:` segment. Turn on `debug` to log the reason whenever an inbound event is
dropped.

### Built-in server events

| Event | Sent by | Description |
|-------|---------|-------------|
| `_acceptAuth` | server | Authentication accepted |
| `_kickConnection` | server | Server is about to close the connection — includes reason string |
| `_offload` | server | Message too large; fetch from HTTP — see Large messages |
| `_ping` | server | Sent shortly before a quiet client reaches `idleTimeout` |
| `_pong` | server | Response to client `_ping` |

The transport keepalive (`keepaliveInterval`) is not in this table because it is
not a message: it is a websocket ping control frame, answered by the browser or
websocket library itself and never delivered to `onmessage`.

---

## Client → Server (receiving messages)

Inbound client messages are forwarded to snub with the `ws:` prefix.

```js
snub.on('ws:my-event', function (event, reply) {
  console.log(event.from);     // client state object
  console.log(event.payload);  // the payload the client sent
  console.log(event._ts);      // server receive timestamp

  reply({ ok: true });         // sends ["replyId", { ok: true }] back to client
                               // (only if the client included a replyId)
});
```

The `event.from` object:

```js
{
  id: 'instanceId;key_uid',   // unique connection ID
  username: 'alice',          // from auth payload (null if auth: false)
  channels: ['room1'],
  authenticated: true,
  connectTime: 1710000000000,
  remoteAddress: '127.0.0.1',
  lastMsgTime: 1710000001234,
  meta: {}                    // arbitrary key/value — see Meta
}
```

---

## Server → Client (sending messages)

### Send to specific clients

Target by username or connection ID. Comma-separate to target multiple.

```js
// by username
snub.poly('ws:send:alice', ['event-name', payload]).send();

// by connection ID
snub.poly('ws:send:' + connectionId, ['event-name', payload]).send();

// multiple targets
snub.poly('ws:send:alice,bob', ['event-name', payload]).send();
```

### Send to all clients

```js
snub.poly('ws:send-all', ['event-name', payload]).send();

// optionally filter to specific usernames/IDs
snub.poly('ws:send-all', ['event-name', payload, ['alice', 'bob']]).send();
```

### Send to a channel

```js
// via event name suffix
snub.poly('ws:send-channel:room1', ['event-name', payload]).send();

// multiple channels in suffix
snub.poly('ws:send-channel:room1,room2', ['event-name', payload]).send();

// or pass channel list as payload element
snub.poly('ws:send-channel', ['event-name', payload, ['room1', 'room2']]).send();
```

---

## Channels

Channels are sets of string tags on a client. Use them to group clients for targeted broadcasts.

```js
// Add channels to a client (by username or ID)
snub.poly('ws:add-channel:alice', ['room1', 'room2']).send();

// Remove specific channels
snub.poly('ws:del-channel:alice', ['room2']).send();

// Replace the entire channel set
snub.poly('ws:set-channel:alice', ['room1']).send();
```

---

## Kick

```js
// by username or ID
snub.poly('ws:kick:alice', 'reason string').send();

// multiple targets
snub.poly('ws:kick:alice,bob', 'reason string').send();

// with a custom WebSocket close code (default 1000)
snub.poly('ws:kick', ['alice', 'reason string', 1008]).send();

// kick everyone
snub.poly('ws:kick-all', 'reason string').send();
```

Before closing, the server sends `["_kickConnection", "reason string"]` to the client so it can handle the reason before the socket closes.

Automatic kick reasons:

| Reason | Cause |
|--------|-------|
| `AUTH_TIMEOUT` | Client did not authenticate within `authTimeout` ms |
| `AUTH_FAIL` | Auth check returned false |
| `DUPE_LOGIN` | Second connection from same username when `multiLogin: false` |
| `IDLE_TIMEOUT` | No messages received within `idleTimeout` ms |
| `THROTTLE_LIMIT` | Client exceeded the rate limit |
| `QUEUE_OVERFLOW` | Outbound queue exceeded `maxQueueSize` |
| `SERVER_SHUTDOWN` | `close()` was called, or the server received SIGINT / SIGTERM / SIGUSR2 with `handleSignals: true` |

---

## Meta

Arbitrary key/value data attached to a client. Included in all `from` payloads and query results. Updated values are broadcast via `ws:client-updated`.

```js
// set by username or ID
snub.poly('ws:set-meta:alice', { role: 'admin', plan: 'pro' }).send();

// set for multiple clients via payload list
snub.poly('ws:set-meta', [{ role: 'guest' }, ['alice', 'bob']]).send();
```

Allowed value types: string, number, boolean, or array of string/number/boolean.
- Strings/numbers: max 128 characters
- Arrays: max 64 items, each item max 64 characters
- Other types are silently dropped

---

## Query

All query events use `snub.mono(...).awaitReply()` since they need a response.

```js
// Get clients by username or connection ID
const clients = await snub.mono('ws:get-clients:alice').awaitReply();
const clients = await snub.mono('ws:get-clients:alice,bob').awaitReply();

// Pass IDs/usernames as payload instead of suffix
const clients = await snub.mono('ws:get-clients', ['alice', 'bob']).awaitReply();

// Get all connected clients across all instances
const all = await snub.mono('ws:connected-clients').awaitReply();

// Filter connected-clients to specific usernames/IDs
const some = await snub.mono('ws:connected-clients', ['alice']).awaitReply();

// Get all clients subscribed to one or more channels
const inRoom = await snub.mono('ws:channel-clients', ['room1']).awaitReply();
const inRooms = await snub.mono('ws:channel-clients', ['room1', 'room2']).awaitReply();
```

All queries return an array of client state objects (see shape above) for every
**authenticated** client on every live instance. Each instance mirrors its clients
into Redis, so a query is a Redis read — it never waits on other instances to
answer, and a crashed instance's clients drop out within `instanceTtl`. `meta`
and `channels` are mirrored as they change; `lastMsgTime` is refreshed once per
heartbeat.

When you need to know *which* instances an answer covers — to tell "offline"
from "an instance is missing" — use `cluster-clients`:

```js
const { clients, instances } = await snub.mono('ws:cluster-clients').awaitReply();
// clients:   same shape as above
// instances: ids of every instance that was alive when the answer was built

// Optional filter, same semantics as the queries above
await snub.mono('ws:cluster-clients', { ids: ['alice'] }).awaitReply();
await snub.mono('ws:cluster-clients', { channels: ['room1'] }).awaitReply();
```

---

## Server lifecycle events

These are emitted by snub-ws itself — listen with `snub.on(...)`.

```js
snub.on('ws:client-authenticated', function (state) {
  console.log('connected:', state.username, state.id);
});

snub.on('ws:client-disconnected', function (state) {
  console.log('disconnected:', state.username);
});

snub.on('ws:client-updated', function (state) {
  // fired when meta or channels change
  console.log('updated:', state.username, state.meta);
});

snub.on('ws:client-failedauth', function (state) {
  console.log('auth failed from', state.remoteAddress);
});
```

---

## Large message offloading

When `offloadToHttpSize` is set and an outbound message exceeds that size, the payload is stored in Redis (under the snub prefix, `_snubws_offload:<id>`) with a 30-second TTL and the client receives a redirect instead:

```json
["_offload", "a3f9...hex32chars"]
```

The client fetches the full payload over HTTP:

```
GET http://hostname:port/?offload=a3f9...hex32chars
```

The server responds with the original JSON message payload (`Content-Type: application/json`). The ID is 128-bit cryptographically random.

---

## Multi-instance

Run one `snub-ws` per process, as many processes as you like, all pointed at
the same Redis and snub `prefix`. Instances need each other for three things,
and all three go through Redis rather than through the bus:

- **Membership.** Each instance heartbeats into the sorted set
  `<prefix>_snubws_instance` every `heartbeatInterval` and is swept after
  `instanceTtl` without one. Scores come from Redis `TIME`, so a host whose
  clock is off is neither evicted nor kept alive by mistake. A crashed instance
  disappears within `instanceTtl`; a signalled or `close()`d one removes itself
  immediately.
- **Client state.** Each instance mirrors its authenticated clients into
  `<prefix>_snubws_clients:<instanceId>`, refreshed with the heartbeat and
  expiring with it. The query events read those hashes — see Query.
- **Single login.** With `multiLogin: false`, an accepted login claims
  `<prefix>_snubws_login:<username>` atomically and learns who held it before;
  that session is kicked with `DUPE_LOGIN` wherever it is connected. Two
  simultaneous logins on different instances therefore always leave exactly one.

Message delivery does not touch any of this: every `send`, `kick`, channel and
meta event is broadcast on the bus and each instance acts on the clients it
holds.

Every key is namespaced by the snub `prefix`, so separate deployments sharing a
Redis database never see each other. Each instance is identified by
`config.instanceId` (defaults to PID + random suffix).

---

## Graceful shutdown

`snub.use()` returns a handle (snub ≥ 5.1.0), and the middleware itself
exposes the same `close()`:

```js
const ws = SnubWs({ port: 8585, handleSignals: false });
const handle = await snub.use(ws);

// later
await handle.close();   // or: await ws.close()
```

`close()`:

1. Stops accepting connections and releases the port
2. Kicks every client with `SERVER_SHUTDOWN`
3. Waits 500 ms for close handshakes to complete
4. Removes the instance, its client mirror and its login claims from Redis

It resolves once that is done and is safe to call more than once. It does not
touch the snub instance — that is the app's to close.

With `handleSignals: true` (the default) `SIGINT`, `SIGTERM` and `SIGUSR2` run
`close()` and then `process.exit(0)`. Set it to `false` if your app owns
shutdown and wants to drain other work first.
