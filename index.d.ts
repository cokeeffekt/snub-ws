export interface SnubWsConfig {
  /** Port to listen on. Default: 8585 */
  port?: number;

  /**
   * Authentication handler.
   * - `false` — no auth required (default)
   * - `string` — snub event name to send auth payload to; handler must reply with `true`, `false`, or an object
   * - `function` — called with `(authObj, callback)` where callback is `(validAuthOrObj, err?) => void`
   */
  auth?: false | string | ((authObj: ClientState & Record<string, unknown>, callback: AuthCallback) => void);

  /** Enable debug logging. Default: false */
  debug?: boolean;

  /** Allow the same username to be connected more than once. Default: true */
  multiLogin?: boolean;

  /** Ms to wait for auth before kicking unauthenticated client. Default: 3000 */
  authTimeout?: number;

  /**
   * Rate limit as `[maxMessages, windowMs]`.
   * e.g. `[50, 5000]` = max 50 messages per 5 seconds. Default: [50, 5000]
   * Set to `false` to disable.
   */
  throttle?: [number, number] | false;

  /**
   * Idle disconnect timeout in ms. Min 5 minutes, max 960 seconds (uWS v20 limit).
   * Default: 960000 (960 seconds)
   */
  idleTimeout?: number;

  /**
   * Ms between websocket ping frames sent to every open socket, so a proxy or
   * load balancer with a shorter idle timeout than `idleTimeout` does not reap
   * quiet connections. Does not count as client activity: `lastMsgTime` and
   * `IDLE_TIMEOUT` are unaffected. Min 1000, `0` disables. Default: 60000
   */
  keepaliveInterval?: number;

  /**
   * Allowed WebSocket upgrade origins. `null` allows all origins.
   * e.g. `['https://example.com']`. Default: null
   */
  allowedOrigins?: string[] | null;

  /** Max simultaneous connections. 0 = unlimited. Default: 0 */
  maxConnections?: number;

  /** Instance identifier prefix. Default: process.pid */
  instanceId?: string | number;

  /**
   * Include the raw message string in the snub event payload.
   * `true` includes all events, or pass an array of event names.
   * Default: false
   */
  includeRaw?: boolean | string[];

  /** Error handler. Default: no-op */
  error?: (err: unknown) => void;

  /**
   * Additional event names clients are blocked from sending. Matched on the
   * first `:`-separated segment, so `'admin'` also blocks `admin:anything`.
   * snub-ws' own control events, the `client-*` lifecycle events, a string
   * `auth` event and every `_`-prefixed name are reserved automatically.
   * Default: []
   */
  internalWsEvents?: string[];

  /** Max incoming message size in bytes. Default: 16777216 (16 MB) */
  maxPayloadLength?: number;

  /** Max queued messages under backpressure before kicking client. Default: 100 */
  maxQueueSize?: number;

  /** Max outbound buffer before uWS drops the connection. Default: 1048576 (1 MB) */
  maxBackpressure?: number;

  /**
   * Offload messages larger than this many bytes to HTTP instead of WebSocket.
   * Set to 0 or negative to disable. Default: 524288 (0.5 MB)
   */
  offloadToHttpSize?: number;

  /** uWS compression setting. Default: uWS.SHARED_COMPRESSOR */
  compression?: string;

  /** Ms between registry heartbeats. Default: 5000 */
  heartbeatInterval?: number;

  /**
   * Ms without a heartbeat before an instance is considered gone (judged on
   * Redis' clock). Clamped to at least 2 × heartbeatInterval. Default: 15000
   */
  instanceTtl?: number;

  /**
   * Install SIGINT/SIGTERM/SIGUSR2 handlers that call close() and then
   * process.exit(0). Default: true
   */
  handleSignals?: boolean;
}

/** Returned by `snub.use(SnubWs(config))` (snub >= 5.1.0). */
export interface SnubWsHandle {
  /**
   * Stop listening, kick every client with SERVER_SHUTDOWN, drain for 500 ms,
   * then remove this instance from Redis. Idempotent.
   */
  close(): Promise<void>;
  readonly instanceId: string;
  readonly port: number;
  readonly closed: boolean;
}

/** Reply shape of the `ws:cluster-clients` query. */
export interface ClusterClientsReply {
  clients: ClientState[];
  /** Ids of every instance alive when the answer was assembled. */
  instances: string[];
}

export interface SnubWsMiddleware {
  (snubInstance: unknown): SnubWsHandle;
  /** Same as the handle's close(); rejects if the middleware was never registered. */
  close(): Promise<void>;
}

export type AuthCallback = (validAuthOrObj: true | false | Record<string, unknown>, err?: unknown) => void;

export interface ClientState {
  /** Unique connection ID */
  id: string;
  /** Username from auth payload */
  username: string | undefined;
  /** Channel memberships */
  channels: string[];
  /** Whether the client has successfully authenticated */
  authenticated: boolean;
  /** Unix timestamp of connection open */
  connectTime: number;
  /** Remote IP address */
  remoteAddress: string;
  /** Unix timestamp of last inbound message */
  lastMsgTime: number;
  /** Arbitrary metadata set via ws:set-meta */
  meta: Record<string, unknown>;
}

/**
 * snub-ws middleware factory.
 *
 * @example
 * const snub = new Snub();
 * snub.use(SnubWs({ port: 8585, auth: false }));
 */
declare function SnubWs(config?: SnubWsConfig): SnubWsMiddleware;

export = SnubWs;
