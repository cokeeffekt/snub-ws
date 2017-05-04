const querystring = require('querystring');
const url = require('url');
const uws = require('uws');

module.exports = function (config) {
  config = Object.assign({
    port: 8585,
    auth: function (auth, accept, deny) {
      accept();
    },
    debug: false,
    mutliLogin: true,
    authTimeout: 3000
  }, config || {});

  return function (snub) {

    var WebSocketServer = uws.Server;
    var wss = new WebSocketServer({
      port: config.port
    }, () => {
      if (config.debug)
        console.log('Snub WS server listening on port ' + config.port);
    });
    var socketClients = [];

    wss.on('connection', (ws) => {
      var clientConn = new clientConnection(ws, config.auth);
      socketClients.push(clientConn);
      if (config.debug)
        console.log('Snub WS Client Connected => ' + clientConn.id);
      snub.mono('ws:client-connected', clientConn.state).send();
      snub.poly('ws_internal:client-connected', clientConn.state).send();
    });

    snub.on('ws:send-all', function (payload) {
      socketClients.forEach(client => {
        client.send(...payload);
      });
    });

    // get client info
    snub.on('ws:get-client', function (arrayOfClients, reply) {
      if (typeof arrayOfClients == 'string') arrayOfClients = [arrayOfClients];
      arrayOfClients = socketClients.filter(client => {
        if (arrayOfClients.includes(client.state.id) || arrayOfClients.includes(client.state.username))
          return true;
        return false;
      }).map(client => client.state);
      if (arrayOfClients.length > 0)
        reply(arrayOfClients);
    });

    snub.on('ws:send:*', function (payload, n1, channel) {
      var sendTo = channel.split(':').pop().split(',');
      var [event, ePayload] = payload;
      socketClients.filter(client => {
        if (sendTo.includes(client.state.id) || sendTo.includes(client.state.username))
          return true;
        return false;
      }).forEach(client => {
        client.send(event, ePayload);
      });
    });

    snub.on('ws:send-channel:*', function (payload, n1, channel) {
      var channels = channel.split(':').pop().split(',');
      var [event, ePayload] = payload;
      socketClients.filter(client => {
        return channels.some(channel => client.state.channels.includes(channel));
      }).forEach(client => {
        client.send(event, ePayload);
      });
    });

    // add, set and delet channels for a client
    snub.on('ws:add-channel:*', function (arrayOfChannels, reply, channel) {
      if (typeof arrayOfChannels == 'string') arrayOfChannels = [arrayOfChannels];
      var clients = channel.split(':').pop().split(',');
      clients = socketClients.filter(client => {
        if (clients.includes(client.state.id) || clients.includes(client.state.username)) {
          client.channels = [].concat(client.channels, arrayOfChannels);
          return true;
        }
        return false;
      }).map(client => client.state);
      if (clients.length > 0)
        reply(clients);
    });
    snub.on('ws:set-channel:*', function (arrayOfChannels, reply, channel) {
      if (typeof arrayOfChannels == 'string') arrayOfChannels = [arrayOfChannels];
      var clients = channel.split(':').pop().split(',');
      clients = socketClients.filter(client => {
        if (clients.includes(client.state.id) || clients.includes(client.state.username)) {
          client.channels = arrayOfChannels;
          return true;
        }
        return false;
      }).map(client => client.state);
      if (clients.length > 0)
        reply(clients);
    });
    snub.on('ws:del-channel:*', function (arrayOfChannels, reply, channel) {
      if (typeof arrayOfChannels == 'string') arrayOfChannels = [arrayOfChannels];
      var clients = channel.split(':').pop().split(',');
      clients = socketClients.filter(client => {
        if (clients.includes(client.state.id) || clients.includes(client.state.username)) {
          client.channels = client.state.channels.filter(channel => (arrayOfChannels.indexOf(channel) > -1 ? false : true));
          return true;
        }
        return false;
      }).map(client => client.state);
      if (clients.length > 0)
        reply(clients);
    });

    snub.on('ws:kick:*', function (message, n2, channel) {
      var sendTo = channel.split(':').pop().split(',');
      socketClients.filter(client => {
        if (sendTo.includes(client.state.id) || sendTo.includes(client.state.username))
          return true;
        return false;
      }).forEach(client => {
        client.kick(message);
        client.authenticated = false;
      });
    });

    snub.on('ws_internal:client-authenticated', function (connectedClient) {
      // if mutliLogin is on then we need to kick other clients with the same username.
      if (config.mutliLogin === false)
        socketClients.forEach(client => {
          if (client.state.username == connectedClient.username && client.state.id != connectedClient.id && client.connectTime < connectedClient.connectTime) {
            return client.kick('Duplicate Login');
          }
          return false;
        });
    });

    snub.on('ws_internal:client-disconnected', function (s) {
      var fi = socketClients.findIndex(c => c.id == s.id);
      if (fi > -1)
        socketClients.splice(fi, 1);
    });

    snub.on('ws:connected-clients', function (nil, reply, channel) {
      reply(socketClients.map(c => {
        return c.state;
      }));
    });

    function generateUID() {
      var firstPart = (Math.random() * 46656) | 0;
      var secondPart = (Math.random() * 46656) | 0;
      firstPart = ('000' + firstPart.toString(36)).slice(-3);
      secondPart = ('000' + secondPart.toString(36)).slice(-3);
      return firstPart + secondPart;
    }

    function clientConnection(ws, authFunction) {
      var upgradeUrl = url.parse(ws.upgradeReq.url);

      Object.assign(this, {
        id: process.pid + '-' + generateUID(),
        auth: {},
        socket: ws,
        channels: [],
        connected: true,
        authenticated: false,
        connectTime: Date.now()
      });

      Object.defineProperty(this, 'state', {
        get: function () {
          return {
            id: this.id,
            username: this.auth.username,
            channels: this.channels,
            connected: this.connected,
            authenticated: this.authenticated,
            connectTime: this.connectTime
          };
        }
      });

      this.send = function (event, payload) {
        if ((this.connected && this.authenticated) || event == '_kickConnection')
          ws.send(JSON.stringify([event, payload]));
      };

      this.close = function () {
        ws.close();
      };

      this.kick = (reason) => {
        this.authenticated = false;
        this.send('_kickConnection', reason || null);
        setTimeout(this.close, 100);
        if (config.debug)
          console.log('Snub WS Client Kicked [' + reason + '] => ' + this.state.id);
      };

      var authTimeout;
      var acceptAuth = () => {
        this.authenticated = true;

        snub.mono('ws:client-authenticated', this.state).send();
        snub.poly('ws_internal:client-authenticated', this.state).send();
        if (config.debug)
          console.log('Snub WS Client Authenticated => ' + this.state.id);
        clearTimeout(authTimeout);

        // we want to add a delay here to allow a small window of time to kick dupe users
        setTimeout(() => {
          if (this.state.authenticated)
            this.send('_acceptAuth', this.state.id);
        }, 200);

      };
      var denyAuth = () => {
        this.kick('Authenication failed');
        setTimeout(this.close, 100);
        snub.mono('ws:client-failedauth', this.state).send();
        if (config.debug)
          console.log('Snub WS Client Rejected Auth => ' + this.state.id);
        clearTimeout(authTimeout);
      };

      var libReserved = {
        _auth: (data) => {
          this.auth = data || {};
          authTimeout = setTimeout(() => {
            this.kick('Authenication timeout');
          }, config.authTimeout);

          if (typeof authFunction == 'string')
            snub.mono('ws:' + authFunction, this.auth).replyAt(payload => {
              if (payload === true)
                return acceptAuth();
              denyAuth();
            }).send(recieved => {
              if (!recieved)
                denyAuth();
            });
          if (typeof authFunction == 'function')
            authFunction(data, acceptAuth, denyAuth);
        }
      };

      ws.on('message', e => {
        try {
          var [event, data, reply] = JSON.parse(e);

          //block client messages
          if (['send-all', 'connected-clients', 'client-authenticated', 'client-failedauth'].includes(event) || event.match(/^(send|kick|client-attributes)\:/))
            return false;

          if (typeof libReserved[event] == 'function') {
            return libReserved[event](data);
          }
          if (!this.authenticated) return;
          snub.mono('ws:' + event, {
              from: this.state,
              payload: data
            })
            .replyAt((reply ? data => {
              this.send(reply, data);
            } : undefined))
            .send();
        } catch (err) {
          console.log(err);
        }
      });

      ws.on('close', () => {
        this.connected = false;
        this.authenticated = false;
        snub.mono('ws:client-disconnected', this.state).send();
        snub.poly('ws_internal:client-disconnected', this.state).send();
      });
    }
  };


};