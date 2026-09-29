// uWebSockets.js ships prebuilt binaries keyed on Node's ABI (process.versions.modules) and
// has no source-build fallback, so a release can only load on the Node versions it was built
// for. No single release spans the range we support, so two pinned builds are installed under
// aliases and we take whichever one loads:
//   uws-modern  v20.69.0 -> Node 22, 24, 26
//   uws-legacy  v20.51.0 -> Node 18, 20, 22, 23
// Modern is tried first so Node 22, which both cover, gets the newer build.
const uWS = (() => {
  const errors = [];
  for (const name of ['uws-modern', 'uws-legacy']) {
    try {
      return require(name);
    } catch (err) {
      errors.push(
        '  ' + name + ': ' + String(err && err.message).split('\n')[0]
      );
    }
  }
  throw new Error(
    'snub-ws: no usable uWebSockets.js build for Node ' +
      process.version +
      ' (ABI ' +
      process.versions.modules +
      ', ' +
      process.platform +
      ' ' +
      process.arch +
      ').\nSupported: Node 18, 20, 22, 23, 24, 26 on glibc Linux, macOS and Windows.\n' +
      errors.join('\n')
  );
})();
const { randomBytes } = require('crypto');

const DEFAULT_CONFIG = {
  port: 8585,
  auth: false,
  debug: false,
  multiLogin: true, // can the same user be connected more than once
  authTimeout: 3000,
  throttle: [50, 5000], // X number of messages per Y milliseconds.
  idleTimeout: 960 * 1000, // disconnect if nothing has come from client in x ms (uWS v20 max: 960 seconds)
  keepaliveInterval: 60 * 1000, // ms between transport pings to every socket, 0 = off
  allowedOrigins: null, // array of allowed origins e.g. ['https://example.com'], null = allow all
  maxConnections: 0, // max simultaneous connections, 0 = unlimited
  instanceId: process.pid,
  includeRaw: false, // for including raw client messages, debug purposes only.
  error: (_) => {},
  internalWsEvents: [],
  maxPayloadLength: 16 * 1024 * 1024, // max incoming message size in bytes
  maxQueueSize: 100, // max messages to queue when client is under backpressure before kicking
  maxBackpressure: 1 * 1024 * 1024, // memory limit for backpressure
  offloadToHttpSize: 0.5 * 1024 * 1024, // if message is larger than this, offload to http
  heartbeatInterval: 5 * 1000, // ms between registry heartbeats
  instanceTtl: 15 * 1000, // ms without a heartbeat before an instance is considered gone
  handleSignals: true, // install SIGINT/SIGTERM/SIGUSR2 handlers that close() then exit
};

let snub;
let warnedNoPrefix = false;
module.exports = function (config) {
  config = {
    ...DEFAULT_CONFIG,
    ...config,
  };
  config.idleTimeout = Math.max(config.idleTimeout, 1000 * 60 * 5); // min 5 minutes
  config.idleTimeout = Math.min(960 * 1000, config.idleTimeout); // uWS v20 max 960 seconds
  // Separate from idleTimeout on purpose: that one decides when a quiet client
  // is away, this one only keeps bytes moving so a proxy or balancer in front
  // does not reap the connection first.
  config.keepaliveInterval =
    config.keepaliveInterval > 0
      ? Math.max(1000, Number(config.keepaliveInterval))
      : 0;

  config.offloadToHttpSize =
    config.offloadToHttpSize < 1 ? null : config.offloadToHttpSize;
  // An instance must survive at least two missed beats before it is swept.
  config.heartbeatInterval = Math.max(100, config.heartbeatInterval);
  config.instanceTtl = Math.max(
    config.instanceTtl,
    config.heartbeatInterval * 2
  );

  if (config.debug) console.log('Snub-ws Init', config);

  const middleware = function (snubInstance) {
    snub = snubInstance; // hoist snub instance
    config.instanceId += '_' + snub.generateUID();

    // Create a set of internal events to prevent socket clients from sending
    // them. Everything in here is a *first segment*: a control event is
    // addressed as `<name>:<targets>`, so reserving the name reserves every
    // target. User-supplied entries are normalised the same way, otherwise an
    // entry like 'my-event:sub' would never match the first-segment lookup in
    // WsClient.onMessage.
    config.internalWsEvents = config.internalWsEvents.map((eventName) => {
      return eventName.replace(/^ws:/, '').split(':')[0]; // ensure no ws: prefix
    });
    config.internalWsEvents = new Set(config.internalWsEvents);

    // Lifecycle events snub-ws emits *about* a client. They never originate
    // from a client, so a client must not be able to forge one.
    for (const eventName of [
      'client-authenticated',
      'client-disconnected',
      'client-updated',
      'client-failedauth',
    ])
      config.internalWsEvents.add(eventName);

    // When auth is delegated to a bus event, #validateAuth emits it directly
    // rather than through registerWsSnubEvent, so it needs reserving too --
    // otherwise a client can invoke the app's auth handler with an arbitrary
    // body and read the verdict back off a replyId, bypassing #denyAuth and
    // the kick path entirely.
    if (typeof config.auth === 'string')
      config.internalWsEvents.add(
        config.auth.replace(/^ws:/, '').split(':')[0]
      );

    function registerWsSnubEvent(eventName, handler) {
      config.internalWsEvents.add(eventName.split(':')[0]);
      snub.on('ws:' + eventName, (payload, reply, channel) => {
        if (eventName.includes(':*'))
          channel = channel.split(':').at(-1).split(',');
        handler(payload, reply, channel);
      });
    }

    // --- redis keys ---------------------------------------------------------
    // Everything snub-ws writes lives under the snub prefix, so two deployments
    // sharing one redis db never see each other's instances or clients.
    if (typeof snub.prefix !== 'string' && !warnedNoPrefix) {
      warnedNoPrefix = true;
      console.warn(
        'Snub-Ws: this snub does not expose `prefix` (needs snub >= 5.1.0); assuming "snub:" for registry keys'
      );
    }
    const prefix = typeof snub.prefix === 'string' ? snub.prefix : 'snub:';
    const keys = {
      registry: prefix + '_snubws_instance',
      clients: (instanceId) => prefix + '_snubws_clients:' + instanceId,
      login: (username) => prefix + '_snubws_login:' + username,
      offload: (id) => prefix + '_snubws_offload:' + id,
    };
    const ownClientsKey = keys.clients(config.instanceId);
    let closed = false;

    // Liveness is decided by redis' own clock: scores are redis TIME in ms and
    // the sweep threshold is redis TIME minus the ttl, both inside one script.
    // Host clock skew can therefore neither evict a healthy instance nor keep a
    // dead one, and the read + sweep pair is atomic. Every key carries a TTL so
    // a cluster that dies outright leaves nothing behind.
    const HEARTBEAT_LUA = `
      local t = redis.call('TIME')
      local now = t[1] * 1000 + math.floor(t[2] / 1000)
      local ttl = tonumber(ARGV[2])
      redis.call('ZADD', KEYS[1], string.format('%.0f', now), ARGV[1])
      redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', string.format('%.0f', now - ttl))
      redis.call('PEXPIRE', KEYS[1], ttl * 2)
      for i = 2, #KEYS do redis.call('PEXPIRE', KEYS[i], ttl) end
      return string.format('%.0f', now)`;
    const ALIVE_LUA = `
      local t = redis.call('TIME')
      local now = t[1] * 1000 + math.floor(t[2] / 1000)
      redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', string.format('%.0f', now - tonumber(ARGV[1])))
      return redis.call('ZRANGE', KEYS[1], 0, -1)`;
    // Single login as compare-and-set: the claimant learns who held the
    // username before it, and the holder only releases a key it still owns.
    const CLAIM_LOGIN_LUA = `
      local prev = redis.call('GET', KEYS[1])
      redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
      return prev`;
    const RELEASE_LOGIN_LUA = `
      if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
      return 0`;

    // Each instance mirrors its authenticated clients into a hash so cluster
    // queries are a read, not a fan-out. Structural changes (auth, meta,
    // channels) are written immediately; lastMsgTime moves on every inbound
    // frame and is flushed by the heartbeat instead.
    const store = {
      offloadKey: keys.offload,
      persist(client) {
        if (closed || !client.state.authenticated) return;
        client.dirty = false;
        return snub.redis
          .pipeline([
            [
              'hset',
              ownClientsKey,
              client.state.id,
              snub.stringifyJson(client.state),
            ],
            ['pexpire', ownClientsKey, config.instanceTtl],
          ])
          .exec()
          .catch((error) =>
            console.error('Snub-Ws: Redis error persisting client state', error)
          );
      },
      remove(client) {
        return snub.redis
          .hdel(ownClientsKey, client.state.id)
          .catch((error) =>
            console.error('Snub-Ws: Redis error removing client state', error)
          );
      },
      claimLogin(client) {
        return snub.redis.eval(
          CLAIM_LOGIN_LUA,
          1,
          keys.login(client.state.username),
          client.state.id,
          config.instanceTtl
        );
      },
      releaseLogin(client) {
        return snub.redis
          .eval(
            RELEASE_LOGIN_LUA,
            1,
            keys.login(client.state.username),
            client.state.id
          )
          .catch((error) =>
            console.error('Snub-Ws: Redis error releasing single login', error)
          );
      },
    };

    const wsClients = new WsClients(config, store);

    // --- instance registry ---------------------------------------------------
    async function heartbeat() {
      if (closed) return;
      try {
        const expireKeys = [ownClientsKey];
        const dirty = [];
        for (const client of wsClients.clients().values()) {
          if (!client.state.authenticated) continue;
          if (!config.multiLogin && client.state.username)
            expireKeys.push(keys.login(client.state.username));
          if (client.dirty) {
            client.dirty = false;
            dirty.push([
              'hset',
              ownClientsKey,
              client.state.id,
              snub.stringifyJson(client.state),
            ]);
          }
        }
        if (dirty.length) {
          const results = await snub.redis.pipeline(dirty).exec();
          const failed = results.find(([err]) => err);
          if (failed) throw failed[0];
        }
        await snub.redis.eval(
          HEARTBEAT_LUA,
          1 + expireKeys.length,
          keys.registry,
          ...expireKeys,
          config.instanceId,
          config.instanceTtl
        );
      } catch (error) {
        console.error('Snub-Ws: Redis error in heartbeat', error);
      }
    }

    async function aliveInstances() {
      try {
        return await snub.redis.eval(
          ALIVE_LUA,
          1,
          keys.registry,
          config.instanceTtl
        );
      } catch (error) {
        console.error('Snub-Ws: Redis error in aliveInstances', error);
        return [];
      }
    }

    heartbeat();
    const heartbeatTimer = setInterval(heartbeat, config.heartbeatInterval);
    heartbeatTimer.unref();

    const idleTimer = setInterval(() => {
      wsClients.clients().forEach((client) => {
        // send ping to connection that will soon idle out
        const idleOutTime = client.state.lastMsgTime + config.idleTimeout;
        if (idleOutTime < Date.now() + 1000 * 30)
          client.send('_ping', Date.now());

        // time to idle this connection out
        if (idleOutTime < Date.now()) {
          client.kick('IDLE_TIMEOUT', 1000);
        }
      });
    }, 1000 * 10);
    idleTimer.unref();

    const keepaliveTimer = config.keepaliveInterval
      ? setInterval(() => {
          wsClients.clients().forEach((client) => client.keepalive());
        }, config.keepaliveInterval)
      : null;
    if (keepaliveTimer) keepaliveTimer.unref();

    // Stop listening, drain the clients, then deregister. Safe to call more
    // than once. Leaves the snub instance alone -- that belongs to the app.
    let listenSocket = null;
    async function close() {
      if (closed) return;
      closed = true;
      clearInterval(heartbeatTimer);
      clearInterval(idleTimer);
      clearInterval(keepaliveTimer);
      if (config.handleSignals)
        for (const signal of SIGNALS) process.off(signal, onSignal);
      // Release the port first so a balancer stops sending new connections
      // here while the existing ones are told to leave.
      if (listenSocket) {
        uWS.us_listen_socket_close(listenSocket);
        listenSocket = null;
      }
      const clients = wsClients.clients();
      clients.forEach((client) => client.kick('SERVER_SHUTDOWN'));
      await justWait(500);
      try {
        const ops = [
          snub.redis.zrem(keys.registry, config.instanceId),
          snub.redis.del(ownClientsKey),
        ];
        if (!config.multiLogin)
          for (const client of clients.values())
            if (client.state.username)
              ops.push(
                snub.redis.eval(
                  RELEASE_LOGIN_LUA,
                  1,
                  keys.login(client.state.username),
                  client.state.id
                )
              );
        await Promise.all(ops);
      } catch (error) {
        console.error('Snub-Ws: Redis error during close', error);
      }
    }

    const SIGNALS = ['SIGINT', 'SIGTERM', 'SIGUSR2'];
    async function onSignal() {
      await close();
      process.exit(0);
    }
    if (config.handleSignals)
      for (const signal of SIGNALS) process.on(signal, onSignal);

    // A browser always fetches offload bodies cross-origin: the client derives
    // the URL from the ws url, and an origin includes the port, so a page on
    // :3000 reading from a socket on :8585 is cross-origin by construction.
    // Without these headers Chrome blocks the read and the offloaded message is
    // dropped with no retry. Must be called after writeStatus() — uWS emits
    // headers in call order and ignores a status written after one.
    function writeCorsHeaders(res, origin) {
      if (!config.allowedOrigins) {
        res.writeHeader('Access-Control-Allow-Origin', '*');
        return;
      }
      // Origins are already restricted for the socket itself, so mirror that
      // here rather than widening the offload route to every origin. An origin
      // that is not allowed simply gets no header, and the browser blocks it.
      if (origin && config.allowedOrigins.includes(origin)) {
        res.writeHeader('Access-Control-Allow-Origin', origin);
        res.writeHeader('Vary', 'Origin');
      }
    }

    const socketServer = uWS
      .App()
      .options('/*', (res, req) => {
        // The client's offload fetch is a simple GET and is not preflighted,
        // but anything that adds a header would be.
        const origin = req.getHeader('origin');
        res.onAborted(() => {
          res.aborted = true;
        });
        // corked: uWS warns once a response makes several uncorked writes, and
        // these responses are now several headers deep
        res.cork(() => {
          res.writeStatus('204 No Content');
          writeCorsHeaders(res, origin);
          res.writeHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
          res.writeHeader('Access-Control-Allow-Headers', 'Content-Type');
          res.writeHeader('Access-Control-Max-Age', '86400');
          res.end();
        });
      })
      .get('/*', (res, req) => {
        res.onAborted(() => {
          // The guards below already assume this flag; without it they never
          // fire and a client that disconnects mid-lookup gets written to.
          res.aborted = true;
          console.log('Request aborted');
        });
        // Retrieve the raw query string
        const query = req.getQuery(); // For example: "offload=1233"
        // `req` is invalid once this handler yields, so anything the async
        // continuation needs has to be read now.
        const origin = req.getHeader('origin');

        // Parse the query string into a usable object
        const params = Object.fromEntries(new URLSearchParams(query));
        // return res.end(JSON.stringify(params));

        function fail() {
          if (res.aborted) return;
          res.cork(() => {
            res.writeStatus('404 Not Found');
            writeCorsHeaders(res, origin);
            res.writeHeader('Content-Type', 'text/plain');
            res.end('404: Route not found');
          });
        }

        if (params.offload) {
          const offloadId = params.offload;
          snub.redis
            .get(keys.offload(offloadId))
            .then((offloadData) => {
              if (res.aborted) return;
              if (offloadData) {
                res.cork(() => {
                  writeCorsHeaders(res, origin);
                  res.writeHeader('Content-Type', 'application/json');
                  res.end(offloadData);
                });
                return;
              }
              fail();
            })
            .catch((error) => {
              fail();
            });
        } else {
          fail();
        }
      })
      .ws('/*', {
        /* Options */
        compression: config.compression
          ? uWS[config.compression]
          : uWS.SHARED_COMPRESSOR,
        maxPayloadLength: config.maxPayloadLength,
        idleTimeout: config.idleTimeout / 1000,
        maxBackpressure: config.maxBackpressure || 1 * 1024 * 1024,
        //: 2 * 1024 * 1024,
        /* Handlers */
        upgrade: (res, req, ctx) => {
          const origin = req.getHeader('origin');

          if (
            config.allowedOrigins &&
            !config.allowedOrigins.includes(origin)
          ) {
            res.writeStatus('403 Forbidden').end('Forbidden');
            return;
          }

          if (
            config.maxConnections > 0 &&
            wsClients.count >= config.maxConnections
          ) {
            res
              .writeStatus('503 Service Unavailable')
              .end('Too many connections');
            return;
          }

          let basicAuth = false;
          try {
            basicAuth = Buffer.from(
              req.getHeader('authorization').split(' ')[1],
              'base64'
            )
              .toString()
              .split(':');
            basicAuth = {
              username: basicAuth[0],
              password: basicAuth[1],
            };
          } catch (error) {}

          const remoteAddress =
            req.getHeader('x-forwarded-for') ||
            req.getHeader('x-real-ip') ||
            Buffer.from(res.getRemoteAddressAsText()).toString();

          var obj = {
            key: req.getHeader('sec-websocket-key'),
            url: req.getUrl(),
            basicAuth: basicAuth,
            wsMeta: {
              url: req.getUrl(),
              origin,
              host: req.getHeader('host'),
              remoteAddress,
              cookie: req.getHeader('cookie'),
            },
          };
          res.upgrade(
            obj,
            req.getHeader('sec-websocket-key'),
            req.getHeader('sec-websocket-protocol'),
            req.getHeader('sec-websocket-extensions'),
            ctx
          );
        },
        open: (ws) => {
          return (ws.wsClient = wsClients.createClient(ws, config));
        },
        message: (ws, message, isBinary) => {
          return ws.wsClient.onMessage(message);
        },
        drain: (ws) => {
          ws.wsClient.onDrain();
          // need to work out if this is useful for anything.
          // console.log('WebSocket back pressure: ' + ws.getBufferedAmount());
        },
        close: async (ws, code, message) => {
          return ws.wsClient.onClose(code, message);
        },
      })
      .any('/*', (res, req) => {
        res.end('Nothing to see here!');
      })
      .listen(config.port, (token) => {
        if (token) {
          listenSocket = token;
          console.log(
            `Snub WS server listening ${config.instanceId} on port ${config.port}, MultiLogin: ${config.multiLogin}`
          );
        } else {
          console.error(
            'Snub WS server FAILED listening on port ' + config.port
          );
        }
      });

    registerWsSnubEvent('send-all', (ipayload) => {
      const [event, payload, idsOrUsernames] = ipayload;
      const clients = wsClients.clients(idsOrUsernames);
      clients.send(event, payload);
    });

    registerWsSnubEvent('send:*', (ipayload, reply, idsOrUsernames) => {
      const [event, payload] = ipayload;
      const clients = wsClients.clients(idsOrUsernames);
      clients.send(event, payload);
    });

    registerWsSnubEvent('send-channel:*', (ipayload, reply, idsOrUsernames) => {
      const [event, payload] = ipayload;
      const clients = wsClients.channelClients(idsOrUsernames);
      clients.send(event, payload);
    });

    registerWsSnubEvent('send-channel', (ipayload) => {
      const [event, payload, idsOrUsernames] = ipayload;
      const clients = wsClients.channelClients(idsOrUsernames);
      clients.send(event, payload);
    });

    registerWsSnubEvent('set-meta:*', (metaObj, reply, idsOrUsernames) => {
      const clients = wsClients.clients(idsOrUsernames);
      clients.setMeta(metaObj);
      reply(clients.states);
    });

    registerWsSnubEvent('set-meta', (payload, reply) => {
      const [metaObj, idsOrUsernames] = payload;
      const clients = wsClients.clients(idsOrUsernames);
      clients.setMeta(metaObj);
      reply(clients.states);
    });

    registerWsSnubEvent(
      'add-channel:*',
      (arrayOfChannels, reply, idsOrUsernames) => {
        const clients = wsClients.clients(idsOrUsernames);
        clients.addChannel(arrayOfChannels);
      }
    );

    registerWsSnubEvent(
      'del-channel:*',
      (arrayOfChannels, reply, idsOrUsernames) => {
        const clients = wsClients.clients(idsOrUsernames);
        clients.delChannel(arrayOfChannels);
      }
    );

    registerWsSnubEvent(
      'set-channel:*',
      (arrayOfChannels, reply, idsOrUsernames) => {
        const clients = wsClients.clients(idsOrUsernames);
        clients.setChannel(arrayOfChannels);
      }
    );

    registerWsSnubEvent('kick-all', (message, reply, idsOrUsernames) => {
      const clients = wsClients.clients(idsOrUsernames);
      clients.kick(message);
    });

    registerWsSnubEvent('kick', (payload, reply) => {
      const [idsOrUsernames, reason, code] = payload;
      const clients = wsClients.clients(idsOrUsernames);
      clients.kick(reason, code);
    });

    registerWsSnubEvent('kick:*', (payload, reply, idsOrUsernames) => {
      const clients = wsClients.clients(idsOrUsernames);
      clients.kick(payload);
    });

    // --- cluster-wide client state ------------------------------------------
    // Our own clients come from memory (fresher); every other live instance's
    // from the hash it maintains. No reply counting, no waiting on a quiet
    // period, and a dead instance's clients vanish with its hash.
    async function clusterClients() {
      const instances = await aliveInstances();
      const clients = wsClients
        .clients()
        .states.filter((state) => state.authenticated);
      const others = instances.filter((id) => id !== config.instanceId);
      if (others.length) {
        try {
          const results = await snub.redis
            .pipeline(others.map((id) => ['hvals', keys.clients(id)]))
            .exec();
          results.forEach(([err, values], i) => {
            if (err)
              return console.error(
                'Snub-Ws: could not read clients of instance ' + others[i],
                err
              );
            for (const raw of values) {
              try {
                clients.push(snub.parseJson(raw));
              } catch (error) {
                console.error(
                  'Snub-Ws: bad client record from instance ' + others[i],
                  error
                );
              }
            }
          });
        } catch (error) {
          console.error('Snub-Ws: Redis error reading cluster clients', error);
        }
      }
      return { clients, instances };
    }

    const byIdOrUsername = (idsOrUsernames) => {
      const list = normalizeStringArray(idsOrUsernames);
      return (state) =>
        list === undefined ||
        list.includes(state.id) ||
        list.includes(state.username);
    };
    const byChannel = (channels) => {
      const list = normalizeStringArray(channels) || [];
      return (state) =>
        list.some((channel) => state.channels.includes(channel));
    };

    async function getAllConnectedClientStates(idsOrUsernames) {
      try {
        const { clients } = await clusterClients();
        return clients.filter(byIdOrUsername(idsOrUsernames));
      } catch (err) {
        console.error(err);
        return [];
      }
    }

    registerWsSnubEvent('get-clients:*', async (_, reply, idsOrUsernames) => {
      reply(await getAllConnectedClientStates(idsOrUsernames));
    });

    registerWsSnubEvent('get-clients', async (idsOrUsernames, reply) => {
      reply(await getAllConnectedClientStates(idsOrUsernames));
    });

    registerWsSnubEvent('connected-clients', async (idsOrUsernames, reply) => {
      reply(await getAllConnectedClientStates(idsOrUsernames));
    });

    registerWsSnubEvent('channel-clients', async (channels, reply) => {
      try {
        const { clients } = await clusterClients();
        reply(clients.filter(byChannel(channels)));
      } catch (err) {
        console.error(err);
        reply([]);
      }
    });

    // The honest form of the queries above: says which instances were alive
    // when the answer was assembled, so a caller can tell "offline" from
    // "an instance is missing". Optional filter: { ids, channels }.
    registerWsSnubEvent('cluster-clients', async (filter, reply) => {
      try {
        const { clients, instances } = await clusterClients();
        let out = clients;
        if (filter && filter.ids !== undefined)
          out = out.filter(byIdOrUsername(filter.ids));
        if (filter && filter.channels !== undefined)
          out = out.filter(byChannel(filter.channels));
        reply({ clients: out, instances });
      } catch (err) {
        console.error(err);
        reply({ clients: [], instances: [] });
      }
    });

    const handle = {
      close,
      get instanceId() {
        return config.instanceId;
      },
      get port() {
        return config.port;
      },
      get closed() {
        return closed;
      },
    };
    middleware.close = close;
    return handle;
  };

  middleware.close = () =>
    Promise.reject(
      new Error('snub-ws: not registered yet -- pass it to snub.use() first')
    );
  return middleware;
};

function hashString(str) {
  let hash = 0;
  let i;
  let chr;
  if (str.length === 0) return hash;
  for (i = 0; i < str.length; i++) {
    chr = str.charCodeAt(i);
    hash = (hash << 5) - hash + chr;
    hash |= 0; // Convert to 32bit integer
  }
  return hash;
}

function justWait(ms = 1000) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

class ClientMap extends Map {
  get states() {
    return Array.from(this.values()).map((client) => client.state);
  }

  send(event, payload) {
    this.forEach((ws) => {
      ws.send(event, payload);
    });
  }
  setMeta(metaObj) {
    this.forEach((ws) => {
      ws.setMeta(metaObj);
    });
  }
  addChannel(arrayOfChannels) {
    this.forEach((ws) => {
      ws.addChannel(arrayOfChannels);
    });
  }
  delChannel(arrayOfChannels) {
    this.forEach((ws) => {
      ws.delChannel(arrayOfChannels);
    });
  }
  setChannel(arrayOfChannels) {
    this.forEach((ws) => {
      ws.setChannel(arrayOfChannels);
    });
  }
  kick(reason, code) {
    this.forEach((ws) => {
      ws.kick(reason, code);
    });
  }
}

class WsClients {
  #config;
  #clients;
  #store;

  constructor(config, store) {
    this.#config = config;
    this.#clients = new Map();
    this.#store = store;
  }

  get count() {
    return this.#clients.size;
  }

  clients(idsOrUsernames) {
    idsOrUsernames = normalizeStringArray(idsOrUsernames);
    const clientMap = new ClientMap();
    this.#clients.forEach((client) => {
      if (
        idsOrUsernames === undefined ||
        idsOrUsernames.includes(client.state.id) ||
        idsOrUsernames.includes(client.state.username)
      )
        clientMap.set(client.state.id, client);
    });
    return clientMap;
  }

  channelClients(channels) {
    channels = normalizeStringArray(channels);
    const clientMap = new ClientMap();
    this.#clients.forEach((client) => {
      if (channels.some((channel) => client.state.channels.includes(channel)))
        clientMap.set(client.state.id, client);
    });
    return clientMap;
  }

  createClient(ws) {
    const client = new WsClient(ws, this.#config, this.#clients, this.#store);
    return client;
  }
}

class WsClient {
  #ws;
  #config;
  #clients;
  #store;
  #dirty = false;
  #internal = {
    id: null,
    connectTime: Date.now(),
    lastMsgTime: Date.now(),
    lastSendTime: 0,
    lastMsgHash: null,
    channels: new Set(),
    metaObj: {},
    auth: {},
    authenticated: false,
    recent: null,
    recentIdx: 0,
    closing: false,
    wsMeta: {},
  };
  #authTimeout;
  #messageQueue = [];

  constructor(ws, config, clients, store) {
    this.#ws = ws;
    this.#config = config;
    this.#clients = clients;
    this.#store = store;

    this.#internal.id =
      config.instanceId +
      ';' +
      ws.key.replace(/[^a-z]/gim, '') +
      '_' +
      snub.generateUID();
    clients.set(this.#internal.id, this);

    this.#internal.wsMeta = ws.wsMeta;
    if (config.throttle)
      this.#internal.recent = new Array(config.throttle[0]).fill(0);

    if (ws.basicAuth) {
      this.#validateAuth(ws.basicAuth);
      ws.basicAuth = false;
    }

    if (config.auth === false) {
      this.#validateAuth(false);
    }
    this.#authTimeout = setTimeout(() => {
      if (!this.#internal.authenticated) this.kick('AUTH_TIMEOUT', 1000);
    }, config.authTimeout);
  }

  get state() {
    return {
      id: this.#internal.id,
      username: this.#internal.auth.username,
      channels: [...this.#internal.channels],
      authenticated: this.#internal.authenticated,
      connectTime: this.#internal.connectTime,
      remoteAddress: this.#internal.wsMeta.remoteAddress,
      lastMsgTime: this.#internal.lastMsgTime,
      meta: this.#internal.metaObj,
    };
  }

  // Set when state changed in a way that is not worth a redis write of its
  // own (lastMsgTime); the heartbeat flushes it.
  get dirty() {
    return this.#dirty;
  }
  set dirty(value) {
    this.#dirty = value;
  }

  // An inbound event that never reaches the bus is silent on both sides, which
  // makes it the hardest kind of upgrade break to diagnose. Costs nothing when
  // debug is off.
  #debugDrop(event, why) {
    if (!this.#config.debug) return;
    console.warn(
      `Snub-Ws: dropped inbound event "${event}" from ${
        this.#internal.auth?.username || this.#internal.id
      } — ${why}`
    );
  }

  onMessage(message) {
    let stringMessage;
    try {
      stringMessage = Buffer.from(message).toString();
      message = snub.parseJson(stringMessage);
    } catch (error) {
      return;
    }
    if (!Array.isArray(message)) return;
    const [event, payload, reply] = message;
    // Everything below assumes a string; event.split() would throw uncaught
    // inside the uWS message callback, as the try/catch here only wraps the emit.
    if (typeof event !== 'string') return;

    this.#internal.lastMsgTime = Date.now();
    this.#dirty = true;

    // Compare the first segment, not the whole name: control events are
    // registered as e.g. 'kick:*' but reserved as 'kick', so an exact-match
    // lookup let 'kick:someone-else' through to the control handler.
    if (this.#config.internalWsEvents.has(event.split(':')[0])) {
      // Dropped events are otherwise invisible from both ends -- the client
      // gets no error and the bus never sees the event -- so an app that named
      // one of its own events after a control event has nothing to go on.
      this.#debugDrop(event, 'it is a reserved control event');
      return;
    }

    if (event === '_auth') return this.#validateAuth(payload);
    if (event === '_ping') return this.send('_pong', payload);
    if (event === '_pong') return;
    if (event.startsWith('_')) {
      this.#debugDrop(event, '_ prefixed events are reserved for the protocol');
      return;
    }

    if (this.#config.throttle) {
      const oldest = this.#internal.recent[this.#internal.recentIdx];
      if (oldest > Date.now() - this.#config.throttle[1]) {
        return this.kick('THROTTLE_LIMIT');
      }
      this.#internal.recent[this.#internal.recentIdx] = Date.now();
      this.#internal.recentIdx =
        (this.#internal.recentIdx + 1) % this.#config.throttle[0];
    }

    // App handlers all assume `from` is a real, authenticated client. Gated
    // after the throttle so an unauthenticated flood still trips THROTTLE_LIMIT
    // and gets kicked, rather than being dropped for free until authTimeout.
    if (!this.#internal.authenticated) {
      this.#debugDrop(event, 'the client has not authenticated yet');
      return;
    }

    const includeRaw =
      this.#config.includeRaw === true ||
      (Array.isArray(this.#config.includeRaw) &&
        this.#config.includeRaw.includes(event));

    try {
      snub
        .mono('ws:' + event, {
          from: this.state,
          payload,
          _raw: includeRaw ? stringMessage : undefined,
          _ts: Date.now(),
        })
        .replyAt(
          reply
            ? (data) => {
                this.send(reply, data);
              }
            : undefined
        )
        .send((c) => {
          if (c < 1 && reply) {
            this.send(reply + ':error', {
              error: `Nothing was listening to this event [ws:${event}] `,
            });
          }
        });
    } catch (error) {
      console.error('Error sending event', event, error, message);
    }
  }

  // A websocket ping frame rather than a message: the peer's websocket stack
  // answers it below the application, so nothing reaches onMessage and
  // lastMsgTime is left alone. A client kept alive this way still idles out.
  keepalive() {
    if (this.#internal.closing) return;
    try {
      this.#ws.ping();
    } catch (error) {
      // uWS throws on a socket that closed under us; onClose tidies up
    }
  }

  onDrain() {
    if (this.#config.debug) console.log('Snub-Ws: Drain');
    while (this.#messageQueue.length > 0) {
      const message = this.#messageQueue.shift();
      if (!this.#ws.send(message)) {
        console.warn(
          'Snub-WS: Backpressure detected again. Re-queuing message.'
        );
        this.#messageQueue.unshift(message);
        break; // Stop trying to send if backpressure returns
      }
    }
  }

  onClose(code, message) {
    this.#internal.closing = true;
    clearTimeout(this.#authTimeout);
    message = Buffer.from(message).toString();
    this.#clients.delete(this.#internal.id);
    if (this.#internal.authenticated) {
      this.#store.remove(this);
      if (!this.#config.multiLogin && this.state.username)
        this.#store.releaseLogin(this);
    }
    snub.mono('ws:client-disconnected', this.state).send();
  }

  send(event, payload) {
    if (this.#internal.authenticated === false) return;
    const sendString = snub.stringifyJson([event, payload]);

    const msgHash = hashString(sendString);
    // dont send the same message twice in a row within 3 seconds
    if (
      msgHash === this.#internal.lastMsgHash &&
      Date.now() - this.#internal.lastSendTime < 3000
    ) {
      return;
    }

    this.#internal.lastMsgHash = msgHash;
    this.#internal.lastSendTime = Date.now();
    if (this.#internal.closing) return;

    if (
      this.#config.offloadToHttpSize &&
      Buffer.byteLength(sendString, 'utf8') > this.#config.offloadToHttpSize
    ) {
      if (this.#config.debug)
        console.log(
          'Snub-Ws: Offloading to HTTP',
          event,
          Buffer.byteLength(sendString, 'utf8'),
          sendString.length
        );
      const offloadId = randomBytes(16).toString('hex');
      snub.redis.set(this.#store.offloadKey(offloadId), sendString, 'EX', 30);
      this.#ws.send(snub.stringifyJson(['_offload', offloadId]));
      return;
    }

    if (!this.#ws.send(sendString)) {
      if (this.#messageQueue.length >= this.#config.maxQueueSize) {
        this.#messageQueue = [];
        return this.kick('QUEUE_OVERFLOW');
      }
      if (this.#config.debug)
        console.warn(
          'Snub-Ws: Backpressure detected',
          event,
          this.#ws.getBufferedAmount()
        );
      this.#messageQueue.push(sendString);
    }
  }

  setMeta(metaObj) {
    // metaObj should be an object with string number bool array values only
    // array values should be strings or numbers only
    // strings should be less than 128 characters
    // numbers should be less than 128 characters
    // arrays should be less than 64 items
    // everything else will be dropped with no warning.

    metaObj = { ...this.#internal.metaObj, ...metaObj };
    const newMeta = {};
    Object.keys(metaObj).forEach((k) => {
      const value = metaObj[k];

      if (Array.isArray(value)) {
        // Handle array values
        newMeta[k] = value
          .filter(
            (i) =>
              ['number', 'string', 'boolean'].includes(typeof i) &&
              String(i).length < 64
          )
          .slice(0, 64);
      } else if (
        ['number', 'string', 'boolean'].includes(typeof value) &&
        (typeof value === 'boolean' || String(value).length <= 128)
      ) {
        // Handle strings, numbers, or booleans (with length check for strings/numbers)
        newMeta[k] = value;
      }
    });
    this.#internal.metaObj = newMeta;
    this.#store.persist(this);
    snub.mono('ws:client-updated', this.state).send();
  }

  addChannel(arrayOfChannels) {
    if (typeof arrayOfChannels === 'string')
      arrayOfChannels = [arrayOfChannels];
    this.#internal.channels = new Set([
      ...this.#internal.channels,
      ...arrayOfChannels,
    ]);
    this.#store.persist(this);
  }

  delChannel(arrayOfChannels) {
    if (typeof arrayOfChannels === 'string')
      arrayOfChannels = [arrayOfChannels];
    arrayOfChannels.forEach((channel) => {
      this.#internal.channels.delete(channel);
    });
    this.#store.persist(this);
  }

  setChannel(arrayOfChannels) {
    if (typeof arrayOfChannels === 'string')
      arrayOfChannels = [arrayOfChannels];
    this.#internal.channels = new Set(arrayOfChannels);
    this.#store.persist(this);
  }

  kick(reason, code = 1000) {
    this.send('_kickConnection', reason);
    setTimeout((_) => {
      try {
        this.#ws.end(code, reason);
      } catch (error) {
        // already closed
      }
    }, 100);
  }

  #validateAuth(authPayload) {
    if (this.#config.auth === false) return this.#acceptAuth(authPayload);
    if (typeof authPayload !== 'object') return this.#denyAuth();
    if (!authPayload.username && !this.#config.multiLogin) {
      authPayload.username = this.state.id;
      console.warn(
        'Multilogin is set to false and no username provided, using client id as username, this will defeat the purpose of multilogin'
      );
    }

    const authObj = { ...this.state, ...authPayload };

    const authCheck = (validAuthOrObj, err) => {
      if (err) {
        console.error('Snub-WS: AuthCheck Error', err);
      }
      if (validAuthOrObj === false) return this.#denyAuth();
      if (validAuthOrObj === true) return this.#acceptAuth(authPayload);
      if (typeof validAuthOrObj === 'object') {
        return this.#acceptAuth(authPayload, validAuthOrObj);
      }
      return this.#denyAuth();
    };

    if (typeof this.#config.auth === 'string') {
      snub
        .mono('ws:' + this.#config.auth, authObj)
        .replyAt(authCheck)
        .send((received) => {
          if (!received) {
            console.error(
              `Snub-Ws:Auth event provided was not listening. ${
                this.#config.auth
              }`,
              received
            );
            this.#denyAuth();
          }
        });
    }

    if (typeof this.#config.auth === 'function') {
      this.#config.auth(authObj, authCheck);
    }
  }

  async #acceptAuth(authPayload, validAuthOrObj = {}) {
    this.#internal.authenticated = true;
    this.#internal.auth = authPayload;

    if (!this.#config.multiLogin && this.state.username) {
      // Claim the username. Whoever held it before -- on this instance or any
      // other -- is kicked by id, and the claim itself is what orders two
      // near-simultaneous logins, not a comparison of host clocks.
      try {
        const previous = await this.#store.claimLogin(this);
        if (previous && previous !== this.state.id)
          snub.poly('ws:kick', [previous, 'DUPE_LOGIN', 3000]).send();
      } catch (error) {
        console.error(
          'Snub-Ws: Redis error claiming single login; allowing the login',
          error
        );
      }
    } else {
      await justWait(0);
    }

    // Kicked or closed while the claim was in flight.
    if (!this.state.authenticated || this.#internal.closing) return;
    this.send('_acceptAuth', { _id: this.state.id, ...validAuthOrObj });
    this.#store.persist(this);
    snub.mono('ws:client-authenticated', this.state).send();
  }

  #denyAuth() {
    clearTimeout(this.#authTimeout);
    this.#internal.authenticated = false;
    this.kick('AUTH_FAIL', 3000);
    snub.mono('ws:client-failedauth', this.state).send();
  }
}

const normalizeStringArray = (strOrArray) => {
  if (!strOrArray) return strOrArray;
  if (typeof strOrArray === 'string') return [strOrArray];
  if (Array.isArray(strOrArray)) return strOrArray;
  throw new Error('Invalid input: expected string or array of strings');
};
