const uWS = require('uws.js');
const path = require('path');
var cleanUpFns = [];

process.stdin.resume(); // so the program will not close instantly

function exitHandler(options, exitCode) {
  cleanUpFns.forEach((fn) => {
    fn();
  });
  process.exit();
}

// do something when app is closing
process.on('exit', exitHandler.bind(null, { exit: true }));
process.on('SIGINT', exitHandler.bind(null, { exit: true }));
process.on('SIGUSR1', exitHandler.bind(null, { exit: true }));
process.on('SIGUSR2', exitHandler.bind(null, { exit: true }));
process.on('uncaughtException', exitHandler.bind(null, { exit: true }));

module.exports = function (config) {
  config = Object.assign(
    {
      port: 8585,
      auth: false,
      debug: false,
      multiLogin: true, // can the same user be connected more than once
      authTimeout: 3000,
      throttle: [50, 5000], // X number of messages per Y milliseconds.
      idleTimeout: 1000 * 60 * 60, // disconnect if nothing has come from client in x ms 1 hour default
      instanceId: process.pid + '_' + Date.now(),
      error: (_) => {},
    },
    config || {}
  );

  if (config.debug) console.log('Snub-ws Init', config);

  return function (snub) {
    var socketClients = [];
    var trackedClients = [];
    var trackedInstances = new Set();

    uWS
      .App()
      .ws('/*', {
        /* Options */
        compression: config.compression
          ? uWS[config.compression]
          : uWS.SHARED_COMPRESSOR,
        maxPayloadLength: 16 * 1024 * 1024,
        idleTimeout: config.idleTimeout,
        /* Handlers */
        upgrade: (res, req, ctx) => {
          var obj = {
            key: req.getHeader('sec-websocket-key'),
            url: req.getUrl(),
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
          Object.assign(ws, {
            id:
              config.instanceId +
              '_' +
              ws.key.replace(/[^a-z]/gim, '') +
              '_' +
              snub.generateUID(),
            auth: {},
            channels: [],
            authenticated: false,
            connectTime: Date.now(),
            recent: [],
            lastMsgTime: Date.now(),
            metaObj: {},
          });
          Object.defineProperty(ws, 'state', {
            get: function () {
              return {
                id: ws.id,
                username: ws.auth.username,
                channels: ws.channels,
                authenticated: ws.authenticated,
                connectTime: ws.connectTime,
                remoteAddress: ws.wsMeta.remoteAddress,
                meta: ws.metaObj,
              };
            },
          });

          socketClients.push(ws);

          if (config.auth)
            setTimeout((_) => {
              if (!ws.authenticated) wsKick(ws, 'AUTH_TIMEOUT');
            }, config.authTimeout);
        },
        message: (ws, message, isBinary) => {
          var [event, payload, reply] = JSON.parse(
            Buffer.from(message).toString()
          );
          if (
            [
              'send-all',
              'kick-all',
              'connected-clients',
              'client-authenticated',
              'client-failedauth',
            ].includes(event)
          )
            return false;
          if (
            event.match(
              /^(send|kick|send-channel|set-meta|add-channel|set-channel|del-channel):/
            )
          )
            return false;

          if (event === '_auth') return wsValidateAuth(ws, payload);
          if (event === '_ping') return wsSend(ws, '_pong', payload);

          if (config.throttle) {
            ws.recent = ws.recent.filter(
              (ts) => ts > Date.now() - config.throttle[1]
            );
            if (ws.recent.length > config.throttle[0]) {
              wsKick(ws, 'THROTTLE_LIMIT');
            }
            ws.recent.push(Date.now());
          }

          snub
            .mono('ws:' + event, {
              from: ws.state,
              payload,
              _ts: Date.now(),
            })
            .replyAt(
              reply
                ? (data) => {
                    wsSend(ws, reply, data);
                  }
                : undefined
            )
            .send((c) => {
              if (c < 1 && reply) {
                wsSend(ws, reply + ':error', {
                  error: 'Nothing was listening to this event',
                });
              }
            });
        },
        drain: (ws) => {
          // need to work out if this is useful for anything.
          // console.log('WebSocket back pressure: ' + ws.getBufferedAmount());
        },
        close: async (ws, code, message) => {
          ws.dead = true;
          ws.authenticated = false;
          snub.mono('ws:client-disconnected', ws.state).send();
          snub.poly('ws_internal:tracked-client-remove', ws.id).send();
          // wait 15 seconds before cleaning up the socket from the client list
          await justWait(15000);
          var idx = socketClients.findIndex((i) => i === ws);
          socketClients.splice(idx, 1);
        },
      })
      .any('/*', (res, req) => {
        res.end('Nothing to see here!');
      })
      .listen(config.port, (token) => {
        if (token) {
          console.log('Snub WS server listening on port ' + config.port);
        } else {
          console.error(
            'Snub WS server FAILED listening on port ' + config.port
          );
        }
      });

    trackedInstances.add(config.instanceId);
    snub.on('ws_internal:tracked-instance', (i) => {
      if (i.online) return trackedInstances.add(i.instanceId);
      trackedInstances.delete(i.instanceId);
    });
    snub
      .poly('ws_internal:tracked-instance', {
        instanceId: config.instanceId,
        online: true,
      })
      .send();

    cleanUpFns.push((_) => {
      if (config.debug) console.log('Snub WS Cleanup');
      snub
        .poly(
          'ws_internal:tracked-client-remove',
          socketClients.map((c) => c.state.id)
        )
        .send();
      snub
        .poly('ws_internal:tracked-instance', {
          instanceId: config.instanceId,
          online: false,
        })
        .send();
    });

    snub.on('ws_internal:tracked-client-upsert', upsertTrackedClients);

    snub.on('ws_internal:tracked-client-remove', function (clientIds) {
      if (!Array.isArray(clientIds)) clientIds = [clientIds];
      var clients = [];
      clientIds.forEach((clientId) => {
        var idx = trackedClients.findIndex((tc) => tc.id === clientId);
        if (idx > -1) {
          var rClient = trackedClients.splice(idx, 1)[0];
          clients.push(rClient);
        }
      });
      if (clients.length)
        snub
          .poly(
            'ws:connected-clients-offline',
            JSON.parse(JSON.stringify(clients))
          )
          .send();

      console.log('trackedClients', trackedClients.length);
    });

    // on launch get list of tracked clients from other instances
    snub.poly('ws_internal:tracked-client-fetch').send();

    snub.on('ws_internal:tracked-client-fetch', (_) => {
      var clientStates = socketClients
        .filter((client) => {
          return client.state.authenticated;
        })
        .map((client) => client.state);
      if (clientStates.length)
        snub.poly('ws_internal:tracked-client-upsert', clientStates).send();
    });

    Object.defineProperty(snub, 'wsConnectedClients', {
      get: function () {
        return JSON.parse(JSON.stringify(trackedClients));
      },
    });

    snub.on('ws:send-all', function (payload) {
      socketClients.forEach((ws) => {
        if (ws.authenticated) wsSend(ws, ...payload);
      });
    });

    snub.on('ws:send:*', function (payload, n1, channel) {
      var sendTo = channel.split(':').pop().split(',');
      var [event, ePayload] = payload;
      socketClients.forEach((ws) => {
        if (
          sendTo.includes(ws.state.id) ||
          sendTo.includes(ws.state.username)
        ) {
          wsSend(ws, event, ePayload);
        }
      });
    });

    snub.on('ws:send-channel:*', function (payload, n1, channel) {
      var channels = channel.split(':').pop().split(',');
      var [event, ePayload] = payload;
      socketClients.forEach((ws) => {
        if (channels.some((channel) => ws.state.channels.includes(channel)))
          wsSend(ws, event, ePayload);
      });
    });

    snub.on('ws:set-meta:*', function (metaObj, reply, channel) {
      Object.keys(metaObj).forEach((k) => {
        if (Array.isArray(metaObj[k])) {
          return (metaObj[k] = metaObj[k]
            .filter((i) => {
              return (
                ['number', 'string'].includes(typeof i) && String(i).length < 64
              );
            })
            .slice(0, 64));
        }
        if (
          ['number', 'string'].includes(typeof metaObj[k]) &&
          String(metaObj[k]).length > 128
        ) {
          return delete metaObj[k];
        }
        if (typeof metaObj[k] === 'object') {
          return delete metaObj[k];
        }
      });

      var clients = channel.split(':').pop().split(',');
      clients = socketClients
        .filter((ws) => {
          if (
            !clients.includes(ws.state.id) &&
            !clients.includes(ws.state.username)
          )
            return false;
          Object.assign(ws.metaObj, metaObj);
          return true;
        })
        .map((ws) => ws.state);
      snub.mono('ws_internal:tracked-client-upsert', clients).send();
      if (clients.length > 0 && reply) {
        reply(clients);
      }
    });

    // add, set and delete channels for a client
    snub.on('ws:add-channel:*', function (arrayOfChannels, reply, channel) {
      if (typeof arrayOfChannels === 'string')
        arrayOfChannels = [arrayOfChannels];
      var clients = channel.split(':').pop().split(',');
      clients = socketClients
        .filter((ws) => {
          if (
            !clients.includes(ws.state.id) &&
            !clients.includes(ws.state.username)
          )
            return false;
          ws.channels = [].concat(ws.channels, arrayOfChannels);
          return true;
        })
        .map((ws) => ws.state);
      snub.mono('ws_internal:tracked-client-upsert', clients).send();
      if (clients.length > 0 && reply) {
        reply(clients);
      }
    });

    snub.on('ws:set-channel:*', function (arrayOfChannels, reply, channel) {
      if (typeof arrayOfChannels === 'string')
        arrayOfChannels = [arrayOfChannels];
      var clients = channel.split(':').pop().split(',');
      clients = socketClients
        .filter((ws) => {
          if (
            !clients.includes(ws.state.id) &&
            !clients.includes(ws.state.username)
          )
            return false;
          ws.channels = arrayOfChannels;
          return true;
        })
        .map((ws) => ws.state);
      snub.mono('ws_internal:tracked-client-upsert', clients).send();
      if (clients.length > 0 && reply) {
        reply(clients);
      }
    });

    snub.on('ws:del-channel:*', function (arrayOfChannels, reply, channel) {
      if (typeof arrayOfChannels === 'string')
        arrayOfChannels = [arrayOfChannels];
      var clients = channel.split(':').pop().split(',');
      clients = socketClients
        .filter((ws) => {
          if (
            !clients.includes(ws.state.id) &&
            !clients.includes(ws.state.username)
          )
            return false;
          ws.channels = ws.state.channels.filter(
            (channel) => !(arrayOfChannels.indexOf(channel) > -1)
          );
          return true;
        })
        .map((ws) => ws.state);
      snub.mono('ws_internal:tracked-client-upsert', clients).send();
      if (clients.length > 0 && reply) {
        reply(clients);
      }
    });

    snub.on('ws:kick:*', function (message, n2, channel) {
      var sendTo = channel.split(':').pop().split(',');
      socketClients.forEach((ws) => {
        if (
          !sendTo.includes(ws.state.id) &&
          !sendTo.includes(ws.state.username)
        )
          return;
        wsKick(ws, message);
        ws.authenticated = false;
      });
    });
    snub.on('ws:kick-all', function (message = 'kick') {
      socketClients.forEach((ws) => {
        wsKick(ws, message);
        ws.authenticated = false;
      });
    });

    snub.on('ws_internal:client-authenticated', function (wsState) {
      if (config.multiLogin === false) {
        socketClients.forEach((ws) => {
          if (
            ws.state.username === wsState.username &&
            ws.state.id !== wsState.id &&
            ws.connectTime < wsState.connectTime
          ) {
            return wsKick(ws, 'DUPE_LOGIN');
          }
          return false;
        });
      }
    });

    snub.on('ws:connected-clients', function (clientIds, reply) {
      if (!clientIds) return reply(snub.wsConnectedClients);
      reply(
        snub.wsConnectedClients.filter(
          (c) => clientIds.includes(c.id) || clientIds.includes(c.username)
        )
      );
    });

    function upsertTrackedClients(clients) {
      if (!Array.isArray(clients)) clients = [clients];
      clients.forEach((c) => {
        if (!c.authenticated) return;
        var existing = trackedClients.find((tc) => tc.id === c.id);
        if (existing) {
          return Object.assign(existing, c);
        }
        trackedClients.push(c);
      });
      snub
        .poly(
          'ws:connected-clients-update',
          JSON.parse(JSON.stringify(clients))
        )
        .send();
    }

    function wsValidateAuth(ws, authPayload) {
      if (config.debug) console.log('Snub-ws wsAcceptAuth', authPayload);
      if (config.auth === false) {
        return wsAcceptAuth(ws, authPayload);
      }

      if (typeof config.auth === 'string') {
        snub
          .mono(
            'ws:' + config.auth,
            Object.assign({}, authPayload, ws.wsMeta, { id: ws.id })
          )
          .replyAt((payload) => {
            if (payload === true) {
              return wsAcceptAuth(ws, authPayload);
            }
            wsDenyAuth(ws);
          })
          .send((received) => {
            if (!received) {
              wsDenyAuth(ws);
            }
          });
      }
      if (typeof config.auth === 'function') {
        config.auth(
          Object.assign({}, authPayload, ws.wsMeta, { id: this.id }),
          (res) => {
            if (res === true) return wsAcceptAuth(ws, authPayload);
            return wsDenyAuth(ws);
          }
        );
      }
    }

    function wsAcceptAuth(ws, authPayload) {
      ws.authenticated = true;
      ws.auth = authPayload;
      snub.mono('ws:client-authenticated', ws.state).send();
      snub.poly('ws_internal:client-authenticated', ws.state).send();
      if (config.debug)
        console.log('Snub WS Client Authenticated => ' + ws.state.id);

      // we want to add a delay here to allow a small window of time to kick dupe users
      setTimeout(
        (_) => {
          if (ws.state.authenticated) {
            wsSend(ws, '_acceptAuth', ws.state.id);
          }
        },
        config.multiLogin ? 0 : 200
      );
      snub.mono('ws_internal:tracked-client-upsert', ws.state).send();

      if (config.debug) console.log('Snub-ws wsAcceptAuth', ws);
    }
    function wsDenyAuth(ws) {
      ws.authenticated = false;
      wsKick(ws, 'AUTH_FAIL');
      snub.mono('ws:client-failedauth', ws.state).send();
      if (config.debug) console.log('Snub-ws wsDenyAuth');
    }

    function wsKick(ws, reason = null) {
      if (ws.dead) return;
      wsSend(ws, '_kickConnection', reason);
      ws.end(1000, reason);
      if (config.debug) console.log('Snub-ws wsKick');
    }

    function wsSend(ws, event, payload) {
      if (ws.dead) return;
      var sendString = JSON.stringify([event, payload]);
      const msgHash = hashString(sendString);
      if (msgHash === ws.lastMsgHash) return;
      ws.lastMsgHash = msgHash;
      ws.send(sendString);
    }
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
