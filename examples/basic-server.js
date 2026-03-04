'use strict';

/**
 * Basic snub-ws server example.
 *
 * Requires Redis running on localhost:6379.
 * Run with: node examples/basic-server.js
 *
 * Connect a WebSocket client to ws://localhost:8585 and send:
 *   ["_auth", { "username": "alice", "password": "secret" }]
 */

const Snub = require('snub');
const SnubWS = require('../snub-ws.js');

const snub = new Snub({ host: 'localhost' });

snub.use(
  SnubWS({
    port: 8585,
    auth: function (authPayload, accept) {
      if (authPayload.username && authPayload.password === 'secret')
        return accept(true);
      accept(false);
    },
    multiLogin: false,
    throttle: [50, 5000],
  })
);

// Listen for inbound client events
snub.on('ws:ping', function (event, reply) {
  console.log('ping from', event.from.username);
  reply('pong');
});

// Send a message to a specific user from anywhere in your app
function sendToUser(username, event, payload) {
  snub.poly('ws:send:' + username, [event, payload]).send();
}

// Log connect / disconnect
snub.on('ws:client-authenticated', function (state) {
  console.log('connected:', state.username, state.id);
});

snub.on('ws:client-disconnected', function (state) {
  console.log('disconnected:', state.username, state.id);
});

// Example: broadcast a message every 10 seconds
setInterval(() => {
  snub.poly('ws:send-all', ['heartbeat', Date.now()]).send();
}, 10000);

console.log('Server starting on port 8585...');
