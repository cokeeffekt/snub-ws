# snub-ws

WebSocket server middleware for [snub](https://github.com/cokeeffekt/snub).

---

## Install

```
npm install snub snub-ws
```

Redis must be running and accessible.

---

## Basic usage

```js
const Snub = require('snub');
const SnubWS = require('snub-ws');

const snub = new Snub({ host: 'localhost' });

snub.use(SnubWS({ port: 8585, auth: false }));
```

---

## Config

```js
SnubWS({
  port: 8585,               // WebSocket server port
  auth: false,              // Auth handler — see Auth section below
  debug: false,             // Log verbose info
  multiLogin: true,         // Allow same username to connect more than once
  authTimeout: 3000,        // Ms before unauthenticated client is kicked
  throttle: [50, 5000],     // [maxMessages, windowMs] — false to disable
  idleTimeout: 960000,      // Ms before idle client is kicked (min 5min, max 960s)
  allowedOrigins: null,     // Array of allowed origins e.g. ['https://example.com'], null = allow all
  maxConnections: 0,        // Max simultaneous connections, 0 = unlimited
  maxPayloadLength: 16777216,   // Max inbound message size in bytes (16 MB)
  maxQueueSize: 100,        // Max queued messages under backpressure before kicking
  maxBackpressure: 1048576, // Max outbound buffer in bytes (1 MB)
  offloadToHttpSize: 524288,// Messages larger than this (bytes) are offloaded to HTTP (0.5 MB)
  includeRaw: false,        // Include raw message string in snub payload (true or array of event names)
  internalWsEvents: [],     // Extra event names clients are blocked from sending
});
```

---

## Auth

### No auth

```js
SnubWS({ auth: false })
```

All clients are accepted automatically.

### Function

```js
SnubWS({
  auth: function (authPayload, accept) {
    if (authPayload.username && authPayload.password === 'secret')
      return accept(true);
    accept(false);
  }
})
```

`accept` can be called with:
- `true` — accept with no extra data
- `false` — deny (client is kicked)
- `object` — accept and merge into the `_acceptAuth` reply sent to client

### Snub event

```js
SnubWS({ auth: 'authenticate-client' })

snub.on('ws:authenticate-client', function (authPayload, reply) {
  if (authPayload.username === 'admin') return reply(true);
  reply(false);
});
```

---

## Client protocol

Messages are JSON-encoded arrays: `[eventName, payload?, replyId?]`

### Authenticate

The first message a client should send after connecting (required when `auth` is not `false`):

```json
["_auth", { "username": "alice", "password": "secret" }]
```

On success the server responds with:

```json
["_acceptAuth", { "_id": "connectionId" }]
```

### Send a message

```json
["event-name", { "any": "payload" }, "optional-reply-id"]
```

---

## Server → Client events (snub API)

### Send to specific clients

```js
// by username or connection ID (comma-separate for multiple)
snub.poly('ws:send:alice', ['event-name', payload]).send();
snub.poly('ws:send:alice,bob', ['event-name', payload]).send();
snub.poly('ws:send:' + connectionId, ['event-name', payload]).send();
```

### Send to all clients

```js
snub.poly('ws:send-all', ['event-name', payload]).send();

// filter to specific usernames/IDs
snub.poly('ws:send-all', ['event-name', payload, ['alice', 'bob']]).send();
```

### Send to a channel

```js
snub.poly('ws:send-channel:my-channel', ['event-name', payload]).send();

// multiple channels
snub.poly('ws:send-channel:ch1,ch2', ['event-name', payload]).send();

// inline channel list
snub.poly('ws:send-channel', ['event-name', payload, ['ch1', 'ch2']]).send();
```

### Kick clients

```js
// by username or ID
snub.poly('ws:kick:alice', 'reason').send();
snub.poly('ws:kick:alice,bob', 'reason').send();

// with WebSocket close code
snub.poly('ws:kick', ['alice', 'reason', 1008]).send();

// all connected clients
snub.poly('ws:kick-all', 'reason').send();
```

---

## Channels

```js
snub.poly('ws:add-channel:alice', ['room1', 'room2']).send(); // add channels
snub.poly('ws:del-channel:alice', ['room1']).send();          // remove channels
snub.poly('ws:set-channel:alice', ['room1']).send();          // replace all channels
```

---

## Meta

Arbitrary key/value data attached to a client, included in all `from` payloads.

```js
// set by username or ID
snub.poly('ws:set-meta:alice', { role: 'admin', plan: 'pro' }).send();

// set for multiple clients by list
snub.poly('ws:set-meta', [{ role: 'guest' }, ['alice', 'bob']]).send();
```

---

## Query

```js
// get client states by username/ID
const clients = await snub.mono('ws:get-clients:alice,bob').awaitReply();
const clients = await snub.mono('ws:get-clients', ['alice', 'bob']).awaitReply();

// get all connected clients (optionally filtered)
const all = await snub.mono('ws:connected-clients').awaitReply();
const some = await snub.mono('ws:connected-clients', ['alice']).awaitReply();

// get clients in a channel
const inRoom = await snub.mono('ws:channel-clients', ['room1']).awaitReply();
```

Each client state has this shape:

```js
{
  id: 'connectionId',
  username: 'alice',
  channels: ['room1'],
  authenticated: true,
  connectTime: 1710000000000,
  remoteAddress: '127.0.0.1',
  lastMsgTime: 1710000001234,
  meta: {}
}
```

---

## Client → Server events (snub listeners)

Inbound messages from clients are forwarded to snub with the `ws:` prefix.

```js
snub.on('ws:my-event', function (event, reply) {
  console.log(event.from);    // client state
  console.log(event.payload); // message payload
  reply('response');          // optional reply back to client
});
```

---

## Server-emitted snub events

| Event | Type | Description |
|-------|------|-------------|
| `ws:client-authenticated` | mono | Fired when a client successfully authenticates |
| `ws:client-disconnected` | mono | Fired when a client disconnects |
| `ws:client-updated` | mono | Fired when a client's state changes (meta, channels) |

---

## Large message offloading

When `offloadToHttpSize` is set and a message exceeds that size, the server stores the payload in Redis (30s TTL) and sends the client a redirect:

```json
["_offload", "offloadId"]
```

The client should then fetch:

```
GET http://hostname:port/?offload=<offloadId>
```

The response is the full JSON message payload.
