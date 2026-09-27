# Changelog

## 5.2.0 — 2026-09-23

### Instance tracking rewritten

Instances need each other for three things: answering "who is connected" across
the cluster, enforcing `multiLogin: false`, and cleaning up after one dies. All
three went through a registry scored on each host's wall clock and a
count-the-replies fan-out. Reviewed against a real multi-host deployment that
had the following problems, all fixed here:

| was | now |
|---|---|
| Registry scores were the writer's `Date.now()`, compared against the reader's. A host 30 s behind was swept by everyone else and never counted. | Scores and the sweep threshold both come from Redis `TIME`, inside one Lua script. Host clocks are irrelevant and the read + sweep is atomic. |
| `multiLogin: false` compared `connectTime` across hosts, so a newer login on a slow-clock host looked older and **both sessions survived**. The dedupe broadcast also had no acknowledgement and a 100 ms race. | An accepted login atomically claims `<prefix>_snubws_login:<username>` and learns the previous holder, which is kicked by id wherever it is. Two simultaneous logins on different instances leave exactly one. |
| Queries did `poly(...).awaitReply(1000, instances.length)`: a dead instance made every query wait the full second for up to 30 s, and a short answer was returned as if complete with only a `console.warn`. | Each instance mirrors its authenticated clients into `<prefix>_snubws_clients:<instanceId>` (refreshed and expiring with the heartbeat). Queries are a Redis read: no reply window, no partial answers, and a crashed instance's clients drop out within `instanceTtl`. |
| `_snubws_instance` and `_snubws_offload:*` were written **unprefixed**, so deployments sharing a Redis db inflated each other's instance counts. | Every key lives under the snub `prefix`. |
| Only a signal removed an instance; a SIGKILL/OOM left it registered for 30 s. The heartbeat `zadd` had no error handler. | Heartbeat every `heartbeatInterval` (5 s), swept after `instanceTtl` (15 s), both configurable; every key carries a TTL; errors are logged. |
| No way to shut down: the signal handler called `process.exit(0)` itself and the uWS listen socket was never exposed. | `close()` — on the handle `snub.use()` returns (snub ≥ 5.1.0) and on the middleware itself — releases the port, kicks with `SERVER_SHUTDOWN`, drains, and deregisters. `handleSignals: false` lets the app own shutdown. |

New: `ws:cluster-clients` replies `{ clients, instances }` so a caller can tell
"offline" from "an instance is missing". New config: `heartbeatInterval`,
`instanceTtl`, `handleSignals`.

**Behaviour changes to know about**

- Only **authenticated** clients appear in query results. Previously a socket
  inside its `authTimeout` window was listed with `authenticated: false`.
- `lastMsgTime` in query results for clients on *other* instances is as of the
  last heartbeat, not the last frame. Own-instance clients are always fresh.
- Keys are now prefixed. During a rolling upgrade each version only sees its
  own registry; message delivery is unaffected because it never used it.
- The `ws_internal:*` events are gone. They were never public.
- Requires Redis with Lua scripting (any supported version) and works best
  with snub ≥ 5.1.0, which exposes `prefix`; on older snub the default `snub:`
  prefix is assumed with a warning.

Covered by `snub-smoke/scenarios/ws/35-ws-instance-registry.js`: prefixed keys
with TTLs, skewed-clock instances staying registered, SIGTERM deregistering,
SIGKILL expiring within `instanceTtl` with no stalled query, `cluster-clients`,
`close()` releasing the port while the process lives, and cross-instance single
login under skew and under a simultaneous race.

## 5.1.0 — 2026-08-17

### Security

**S9 — clients could address snub-ws' own control events.** Client messages are re-emitted onto
the bus as `ws:<event>` with the client choosing `<event>`, and snub-ws' control events live in
that same namespace. The reserved-name guard was the only separation between the two, and it
compared the *whole* event name against a set populated with *first segments* —
`registerWsSnubEvent('kick:*', …)` reserved `kick` but listened on `ws:kick:*`. `kick` was blocked;
`kick:victim` was not.

Every parameterised control event was therefore callable by any connected client:

| client frame | effect |
|---|---|
| `["get-clients:victim", null, "r1"]` | the victim's full state returned to the caller's socket — username, id, channels, meta and `remoteAddress` — aggregated across every instance in the cluster. Comma lists (`get-clients:a,b,c`) made it a bulk enumeration primitive. |
| `["kick:victim", "x"]` | any client could disconnect any other client, or a comma-separated list of them |
| `["set-meta:victim", …]` | writes into another client's meta and fires a spurious `ws:client-updated` |
| `["send:victim", […]]` | not exploitable, but only because `ws:send:*` array-destructures its payload and the inbound wrapper is not iterable. The same applied to `send-channel:*` and `add`/`del`/`set-channel:*`. |

Three further holes in the same class:

- **Inbound events were not gated on authentication.** `onMessage` never checked
  `authenticated` — only outbound `send()` did — so the side-effecting events above worked from an
  unauthenticated socket inside the `authTimeout` window, and every app-level `snub.on('ws:…')`
  handler was reachable pre-auth.
- **A string `auth` event was client-sendable.** With `auth: '<event>'`, `#validateAuth` emits
  `ws:<event>` directly rather than through `registerWsSnubEvent`, so it was never reserved. A
  client could invoke the app's auth handler with an arbitrary body and read the verdict off a
  `replyId`, bypassing the deny path, the kick and the auth timeout entirely — a credential and
  username oracle.
- **Lifecycle events were forgeable.** `client-authenticated`, `client-disconnected`,
  `client-updated` and `client-failedauth` are emitted by snub-ws but not registered through the
  helper, so a client could publish them onto the bus and drive app handlers that assume they came
  from the server.

Fixed by making the reserved set uniformly first-segment, reserving the lifecycle events and the
configured auth event, rejecting `_`-prefixed and non-string event names, and gating the emit on
`authenticated`.

### Behaviour changes

- **Client messages sent before authentication completes are now dropped** rather than forwarded to
  the bus with `from.authenticated === false`. With `auth: false` clients are authenticated in the
  constructor, so this only affects the in-flight window of `auth` as a string or function. This is
  the change most likely to surprise: an app that acted on pre-auth events will simply stop seeing
  them.
- **`_`-prefixed events from clients no longer reach the bus.** `_auth`, `_ping` and `_pong` are
  handled as before; anything else beginning with `_` is dropped instead of being emitted as
  `ws:_<name>`.
- **Non-string event names are ignored** instead of being coerced (`['ws:' + 123]`).
- **`internalWsEvents` entries are normalised to their first segment.** An entry of `'foo:bar'` now
  reserves all of `foo:*`, not just the exact name — previously an entry containing a colon could
  never match at all.

### Diagnostics

- A dropped inbound event now logs a reason under `config.debug`. A dropped event is otherwise
  invisible from both ends — the client gets no error and the bus never sees it — which makes it
  the hardest kind of upgrade break to diagnose.

### Tests

- Six cases in `snub-ws.test.js` (tagged `S9`) covering reserved control events, forged lifecycle
  and `_` frames, pre-auth reachability, non-string event names, the configured auth event, and a
  guard that app events containing a colon are not over-blocked
- `snub-smoke/scenarios/ws/34-ws-event-namespace.js` — 10 checks against a live Redis and forked
  snub-ws instances. The `send:*` and channel checks assert that *nothing was delivered* rather
  than that something threw: those paths are currently protected only by the payload wrapper being
  non-iterable, so unwrapping it in a future refactor would silently re-arm client-to-client
  message spoofing.

## 5.0.0 — 2026-07-29

### Node version support

Previously snub-ws depended on a single pinned uWebSockets.js build (`v20.51.0`), which ships
prebuilt binaries only for Node 18, 20, 22 and 23. On Node 24 or 26 `require('snub-ws')` threw
`Cannot find module './uws_<platform>_<arch>_137.node'` at load — `npm install` succeeded, so the
failure only appeared at startup. uWebSockets.js has no source-build fallback, and each release
supports a sliding window of roughly four ABIs, so no single pin can cover both old and new Node.

snub-ws now installs two pinned builds under aliases and selects at load time:

| Alias | uWS version | Node |
|---|---|---|
| `uws-modern` | v20.69.0 | 22, 24, 26 |
| `uws-legacy` | v20.51.0 | 18, 20, 22, 23 |

Modern is preferred where both apply (Node 22). If neither loads, the thrown error names the
running Node version, ABI, platform and both underlying failures instead of uWS's misleading
single-version message. Added `engines: { node: ">=18" }`. Node 21 and 25 are not supported by
either build.

`uws-legacy` is pinned to the last release supporting Node 18/20 and will receive no further
upstream fixes; it is a transition mechanism and will be dropped once users have moved to Node 22+.

### Disk usage

The two builds together ship 39 prebuilt binaries (~218 MB) of which one machine loads exactly
one. New optional `snub-ws-prune` bin (`npm run prune-uws`, or `npx snub-ws-prune` for consumers)
deletes binaries for other platforms, freeing ~170 MB while keeping every ABI for the current
platform so Node version switches still work. It is opt-in, never run automatically, and supports
`--dry-run`.

### Bug fixes

- **B1** Remove hardcoded `config.debug = true` that overrode user config and forced verbose logging on all instances
- **B2** Fix `normalizeStringArray` declared as implicit global — would crash in strict mode
- **B3** Fix `idleTimeout` clamp: was capping at 959 s (~16 min) but documented as 1 hour default; now correctly clamps to uWS v20 max of 960 s and defaults to 960 s
- **B4** Fix outbound dedup: was comparing against `lastMsgTime` (inbound timestamp), silently dropping messages to active clients; now uses a dedicated `lastSendTime`
- **B5** Fix `kick:*` event: was passing `payload.message` instead of `payload` as the kick reason, so kick reason was always `undefined`
- **B6** Fix `#authTimeout` not cleared in `onClose`: timer could fire after disconnect, attempting to kick an already-closed connection
- **B8** Implement `ws:set-channel:*` event (atomically replaces a client's channel set); previously documented but not implemented
- **B9** Handle `SIGTERM` and `SIGUSR2` in addition to `SIGINT` for clean shutdown in containers and process managers
- **B10** Await `zremrangebyscore` in `aliveInstances`; wrap in try/catch so Redis errors degrade gracefully to an empty instance list instead of propagating or swallowing unhandled rejections
- **B11** Expose `maxPayloadLength` as a config option (was hardcoded at 16 MB)
- **B12** Fall back to `res.getRemoteAddressAsText()` for `remoteAddress` when `x-forwarded-for` / `x-real-ip` headers are absent
- **B13** Bound `#messageQueue` with configurable `maxQueueSize` (default 100): when the limit is reached the queue is cleared and the client is kicked with `QUEUE_OVERFLOW`
- **B15** `ClientMap.kick()` now accepts and forwards a `code` parameter to `WsClient.kick()`
- **B16** Gate `onDrain` log behind `config.debug`
- **B17** Fix `maxConnections` never applying: the guard read `.length` on `clients()`, which returns a `ClientMap extends Map`, so it was always `undefined` and the comparison always false. Now uses a new `WsClients.count` getter, which also avoids rebuilding a `ClientMap` over every connected client on each upgrade
- **B18** Fix the offload route returning **200** for a miss: `writeHeader()` preceded `writeStatus()` and uWS ignores a status written after a header, so a 404 went out as a 200 with `404: Route not found` as the body — a client that trusted the status handed that string to `JSON.parse`
- **B19** Fix offloaded messages being unreadable from any browser: the route set no CORS headers, but the client always fetches it cross-origin (it derives the URL from the ws url, and an origin includes the port), so Chrome blocked the read and the message was dropped with no retry. `Access-Control-Allow-Origin` is now set on both the hit and the 404, and `OPTIONS` is answered for preflight. When `allowedOrigins` is configured the allowed origin is echoed (with `Vary: Origin`) instead of `*`, and a disallowed origin gets no header
- **B20** Set `res.aborted` in the offload route's `onAborted` handler — the existing `if (res.aborted) return` guards were never true, so a client disconnecting mid-lookup was still written to. Response writes are also corked

### Security

- **S1** `remoteAddress` now falls back to the real socket IP when proxy headers are absent (see B12)
- **S2** Add `allowedOrigins` config option (array of strings, `null` = allow all): upgrades from unlisted origins are rejected with 403
- **S3** Add `maxConnections` config option (integer, `0` = unlimited): upgrades beyond the limit are rejected with 503 (the guard as first written never fired — see B17)
- **S5** Replace `Math.random()`-based offload IDs (guessable, ~2.1B keyspace) with `crypto.randomBytes(16)` (128-bit, cryptographically random)

### Performance

- **M9** Replace O(n) throttle array scan (allocating a new array per message) with a fixed-size circular buffer: O(1), zero allocations per message

### New config options

| Option | Default | Description |
|--------|---------|-------------|
| `allowedOrigins` | `null` | Allowed WebSocket upgrade origins |
| `maxConnections` | `0` | Max simultaneous connections |
| `maxPayloadLength` | `16777216` | Max inbound message size in bytes |
| `maxQueueSize` | `100` | Max messages queued under backpressure |

### Graceful shutdown

On `SIGINT`, `SIGTERM`, or `SIGUSR2`: all connected clients are kicked with `SERVER_SHUTDOWN`, a 500 ms drain window is allowed for close handshakes, then the instance is removed from Redis and the process exits.

### TypeScript types

Added `index.d.ts` covering `SnubWsConfig`, `ClientState`, and `AuthCallback`.

### Tests

- Added Redis mock (`jest.mock('ioredis')` with `ioredis-mock`) — tests no longer require a live Redis server
- Replaced `justWait()` timing delays with event-driven `waitFor()` polling to eliminate flakiness
- Added regression test for B4 (outbound dedup)
- Added regression test for B5 (kick reason)
- Added throttle kick test (M17)
- Added offload-to-HTTP test (M19)
- Added multi-instance cross-snub messaging test (M20)
