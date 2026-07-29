# Changelog

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
