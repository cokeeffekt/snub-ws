# Changelog

## Unreleased

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

### Security

- **S1** `remoteAddress` now falls back to the real socket IP when proxy headers are absent (see B12)
- **S2** Add `allowedOrigins` config option (array of strings, `null` = allow all): upgrades from unlisted origins are rejected with 403
- **S3** Add `maxConnections` config option (integer, `0` = unlimited): upgrades beyond the limit are rejected with 503
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
