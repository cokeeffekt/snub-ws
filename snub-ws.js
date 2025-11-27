const uWS = require('uWebSockets.js');

const DEFAULT_CONFIG = {
  port: 8585,
  auth: false,
  debug: false,
  multiLogin: true, // can the same user be connected more than once
  authTimeout: 3000,
  throttle: [50, 5000], // X number of messages per Y milliseconds.
  idleTimeout: 1000 * 60 * 60, // disconnect if nothing has come from client in x ms 1 hour default
  instanceId: process.pid,
  includeRaw: false, // for including raw client messages, debug purposes only.
  error: (_) => {},
  internalWsEvents: [],
  maxBackpressure: 1 * 1024 * 1024, // memory limit for backpressure
  offloadToHttpSize: 0.5 * 1024 * 1024, // if message is larger than this, offload to http
};

let snub;
module.exports = function (config) {
  config = {
    ...DEFAULT_CONFIG,
    ...config,
  };
  config.idleTimeout = Math.max(config.idleTimeout, 1000 * 60 * 5); // min 5 minute on idle timeout
  config.idleTimeout = Math.min(959000, config.idleTimeout);

  config.offloadToHttpSize =
    config.offloadToHttpSize < 1 ? null : config.offloadToHttpSize;

  if (config.debug) console.log('Snub-ws Init', config);

  return function (snubInstance) {
    snub = snubInstance; // hoist snub instance
    config.instanceId += '_' + snub.generateUID();

    // create a set of internal events to prevent socket clients from sending them
    config.internalWsEvents = config.internalWsEvents.map((eventName) => {
      return eventName.replace(/^ws:/, ''); // ensure no ws: prefix
    });
    config.internalWsEvents = new Set(config.internalWsEvents);
    function registerWsSnubEvent(eventName, handler) {
      config.internalWsEvents.add(eventName.split(':')[0]);
      snub.on('ws:' + eventName, (payload, reply, channel) => {
        if (eventName.includes(':*'))
          channel = channel.split(':').at(-1).split(',');
        handler(payload, reply, channel);
      });
    }

    // New stuff
    const wsClients = new WsClients(config);

    // ten second interval to keep instance alive
    instanceAlive();
    setInterval(() => {
      instanceAlive();

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

    function instanceAlive() {
      snub.redis.zadd(
        '_snubws_instance',
        Date.now() + 30 * 1000,
        config.instanceId
      );
    }

    async function aliveInstances() {
      const now = Date.now();
      const instances = await snub.redis.zrangebyscore(
        '_snubws_instance',
        now,
        '+inf'
      );
      snub.redis.zremrangebyscore('_snubws_instance', '-inf', now);
      return instances;
    }

    process.on('SIGINT', async (_) => {
      await snub.redis.zrem('_snubws_instance', config.instanceId);
      process.exit(0);
    });

    const socketServer = uWS
      .App()
      .get('/*', (res, req) => {
        res.onAborted(() => {
          console.log('Request aborted');
        });
        // Retrieve the raw query string
        const query = req.getQuery(); // For example: "offload=1233"

        // Parse the query string into a usable object
        const params = Object.fromEntries(new URLSearchParams(query));
        // return res.end(JSON.stringify(params));

        function fail() {
          if (res.aborted) return;
          res.writeHeader('Content-Type', 'text/plain');
          res.writeStatus('404 Not Found');
          res.end('404: Route not found');
        }

        if (params.offload) {
          const offloadId = params.offload;
          snub.redis
            .get('_snubws_offload:' + offloadId)
            .then((offloadData) => {
              if (res.aborted) return;
              if (offloadData) {
                res.writeHeader('Content-Type', 'application/json');
                res.end(offloadData);
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
        maxPayloadLength: 16 * 1024 * 1024,
        idleTimeout: config.idleTimeout / 1000,
        maxBackpressure: config.maxBackpressure || 1 * 1024 * 1024,
        //: 2 * 1024 * 1024,
        /* Handlers */
        upgrade: (res, req, ctx) => {
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
          var obj = {
            key: req.getHeader('sec-websocket-key'),
            url: req.getUrl(),
            basicAuth: basicAuth,
            wsMeta: {
              url: req.getUrl(),
              origin: req.getHeader('origin'),
              host: req.getHeader('host'),
              remoteAddress:
                req.getHeader('x-forwarded-for') || req.getHeader('x-real-ip'),
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
      clients.kick(payload.message);
    });

    async function getAllConnectedClientStates(idsOrUsernames) {
      try {
        var instances = await aliveInstances();
        const instancesClients = await snub
          .poly('ws_internal:connected-clients', idsOrUsernames)
          .awaitReply(1000, instances.length);

        if (instancesClients.length < instances.length)
          console.warn(
            'Snub-Ws: Not all instances replied to ws_internal:connected-clients',
            instancesClients.length,
            instances.length
          );
        const clients = [];
        instancesClients.forEach((instance) => {
          clients.push(...instance[1]);
        });
        return clients;
      }
      catch (err) {
        console.error(err)
        return []
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
        var instances = await aliveInstances();
        const instancesClients = await snub
          .poly('ws_internal:channel-clients', channels)
          .awaitReply(1000, instances.length);
        const clients = [];
        instancesClients.forEach((instance) => {
          clients.push(...instance[1]);
        });
        reply(clients);
      }
      catch (err) {
        console.error(err)
        reply([])
      }
    });

    snub.on('ws_internal:dedupe-client-check', async (state) => {
      if (config.multiLogin) return;
      const clients = wsClients.clients(state.username);
      clients.forEach((client) => {
        if (
          client.state.id !== state.id &&
          client.state.connectTime <= state.connectTime
        ) {
          if (client.state.connectTime === state.connectTime)
            if (client.state.id > state.id) return;
          client.kick('DUPE_LOGIN', 3000);
        }
      });
    });

    snub.on('ws_internal:connected-clients', function (idsOrUsernames, reply) {
      // if(Math.random() > .5) return;
      reply([config.instanceId, wsClients.clients(idsOrUsernames).states]);
    });

    snub.on('ws_internal:channel-clients', function (channels, reply) {
      reply([config.instanceId, wsClients.channelClients(channels).states]);
    });
  };
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
  kick(reason) {
    this.forEach((ws) => {
      ws.kick(reason);
    });
  }
}

class WsClients {
  #config;
  #clients;

  constructor(config) {
    this.#config = config;
    this.#clients = new Map();
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
    const client = new WsClient(ws, this.#config, this.#clients);
    return client;
  }
}

class WsClient {
  #ws;
  #config;
  #clients;
  #internal = {
    id: null,
    connectTime: Date.now(),
    lastMsgTime: Date.now(),
    channels: new Set(),
    metaObj: {},
    auth: {},
    authenticated: false,
    recent: [],
    closing: false,
    wsMeta: {},
  };
  #authTimeout;
  #messageQueue = [];

  constructor(ws, config, clients) {
    this.#ws = ws;
    this.#config = config;
    this.#clients = clients;

    this.#internal.id =
      config.instanceId +
      ';' +
      ws.key.replace(/[^a-z]/gim, '') +
      '_' +
      snub.generateUID();
    clients.set(this.#internal.id, this);

    this.#internal.wsMeta = ws.wsMeta;

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

    this.#internal.lastMsgTime = Date.now();

    if (this.#config.internalWsEvents.has(event)) return; // block internal events

    if (event === '_auth') return this.#validateAuth(payload);
    if (event === '_ping') return this.send('_pong', payload);
    if (event === '_pong') return;

    //@todo prevent client from sending internal events

    if (this.#config.throttle) {
      this.#internal.recent = this.#internal.recent.filter(
        (ts) => ts > Date.now() - this.#config.throttle[1]
      );
      if (this.#internal.recent.length > this.#config.throttle[0]) {
        return this.kick('THROTTLE_LIMIT');
      }
      this.#internal.recent.push(Date.now());
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

  onDrain() {
    console.log('Snub-Ws: Drain');
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
    message = Buffer.from(message).toString();
    this.#clients.delete(this.#internal.id);
    snub.mono('ws:client-disconnected', this.state).send();
  }

  send(event, payload) {
    if (this.#internal.authenticated === false) return;
    const sendString = snub.stringifyJson([event, payload]);

    const msgHash = hashString(sendString);
    // dont send the same message twice in a row within 3 seconds
    if (
      msgHash === this.#internal.lastMsgHash &&
      Date.now() - this.#internal.lastMsgTime < 3000
    ) {
      return;
    }

    this.#internal.lastMsgHash = msgHash;
    if (this.#internal.closing) return;

    if (
      this.#config.offloadToHttpSize &&
      Buffer.byteLength(sendString, 'utf8') > this.#config.offloadToHttpSize
    ) {
      console.log('Snub-Ws: Offloading to HTTP', event, sendString.length);
      const offloadId = snub.generateUID();
      snub.redis.set('_snubws_offload:' + offloadId, sendString, 'EX', 30);
      this.#ws.send(snub.stringifyJson(['_offload', offloadId]));
      return;
    }

    if (!this.#ws.send(sendString)) {
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
    snub.mono('ws:client-updated', this.state).send();
  }

  addChannel(arrayOfChannels) {
    if (typeof arrayOfChannels === 'string')
      arrayOfChannels = [arrayOfChannels];
    this.#internal.channels = new Set([
      ...this.#internal.channels,
      ...arrayOfChannels,
    ]);
  }

  delChannel(arrayOfChannels) {
    if (typeof arrayOfChannels === 'string')
      arrayOfChannels = [arrayOfChannels];
    arrayOfChannels.forEach((channel) => {
      this.#internal.channels.delete(channel);
    });
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

  #acceptAuth(authPayload, validAuthOrObj = {}) {
    this.#internal.authenticated = true;
    this.#internal.auth = authPayload;
    if (!this.#config.multiLogin && this.state.username)
      snub.poly('ws_internal:dedupe-client-check', this.state).send();

    setTimeout(
      () => {
        if (this.state.authenticated) {
          this.send('_acceptAuth', { _id: this.state.id, ...validAuthOrObj });
          snub.mono('ws:client-authenticated', this.state).send();
        }
      },
      this.#config.multiLogin ? 0 : 100
    );
  }

  #denyAuth() {
    clearTimeout(this.#authTimeout);
    this.#internal.authenticated = false;
    this.kick('AUTH_FAIL', 3000);
    snub.mono('ws:client-failedauth', this.state).send();
  }
}

normalizeStringArray = (strOrArray) => {
  if (!strOrArray) return strOrArray;
  if (typeof strOrArray === 'string') return [strOrArray];
  if (Array.isArray(strOrArray)) return strOrArray;
  throw new Error('Invalid input: expected string or array of strings');
};
