jest.mock('ioredis', () => {
  const IORedisMock = require('ioredis-mock');
  class RedisMock extends IORedisMock {
    client() {
      return Promise.resolve('OK');
    }
  }
  return RedisMock;
});

const Snub = require('snub');
const SnubWS = require('./snub-ws.js');
const WebSocket = require('ws');
const http = require('http');
var snub = new Snub({
  host: 'localhost',
  password: '',
  db: 8,
  timeout: 10000,
  intercepter: async (payload, reply, listener, channel) => {
    if (listener === 'test-intercept-block') return false;
    if (listener === 'test-intercept-mono')
      payload.intercept = payload.intercept * 2;
    return true;
  },
});

function auth(auth, accept) {
  if (auth.username && auth.password === 'password') return accept({
    token: '123456',
  });
  return accept(false);
}

const snubws1 = new SnubWS({
  debug: false,
  port: 8686,
  multiLogin: false,
  idleTimeout: 100,
  auth: auth,
});
const snubws2 = new SnubWS({
  debug: false,
  port: 8787,
  multiLogin: false,
  idleTimeout: 100,
  auth: auth,
  includeRaw: true,
});
const snubws3 = new SnubWS({
  debug: false,
  port: 8888,
  multiLogin: false,
  idleTimeout: 100,
  auth: auth,
});

const snubwsNA = new SnubWS({
  debug: false,
  multiLogin: true,
  idleTimeout: 100,
  auth: false,
});

const ports = [8686, 8787, 8888];

let next = 0;
function nextPort() {
  const port = ports[next];
  next = (next + 1) % ports.length; // Wrap around when reaching the end of the array
  return port;
}
snub.use(snubws1);
snub.use(snubws2);
snub.use(snubws3);
snub.use(snubwsNA);

const snubwsThrottle = new SnubWS({
  debug: false,
  port: 9001,
  auth: auth,
  throttle: [3, 1000],
});
snub.use(snubwsThrottle);

const snubwsOffload = new SnubWS({
  debug: false,
  port: 9002,
  auth: false,
  offloadToHttpSize: 100,
});
snub.use(snubwsOffload);

// Dedicated servers for the S9 reserved-event-name tests. multiLogin is left on
// so an "attacker" and a "victim" client can be connected at the same time.
const snubwsSec = new SnubWS({
  debug: false,
  port: 9101,
  multiLogin: true,
  auth: auth,
});
snub.use(snubwsSec);

// auth delegated to a bus event rather than a function, so the event name itself
// has to be reserved.
const snubwsSecAuthEvent = new SnubWS({
  debug: false,
  port: 9102,
  multiLogin: true,
  auth: 'sec-auth-check',
});
snub.use(snubwsSecAuthEvent);

// debug on, so the drop diagnostics are actually emitted.
const snubwsSecDebug = new SnubWS({
  debug: true,
  port: 9103,
  multiLogin: true,
  auth: auth,
});
snub.use(snubwsSecDebug);

var snub2 = new Snub({
  host: 'localhost',
  password: '',
  db: 8,
  timeout: 10000,
});

test('Connect/Disconnect to snub-ws web socket server with basic auth', async function () {
  let didAuth = false;
  var socketClient = new Ws('ws://username:password@localhost:' + nextPort(), {
    onmessage: (e) => {
      try {
        var [key, value] = JSON.parse(e.data);
        if (key === '_acceptAuth') {
          didAuth = true;
          // console.log('Auth Accepted');
        }
      } catch (error) {
        console.error(error);
      }
    },
    onreconnect: (e) => console.log('Reconnecting...', e),
    onmaximum: (e) => console.log('Stop Attempting!', e),
    onclose: (e) => {
      console.log('socket closed');
    },
    onerror: (e) => console.warn('Error:', e),
  });
  await justWait(1000);
  expect(didAuth).toBe(true);
}, 10000);

test('Connect/Disconnect to snub-ws web socket server with socket auth', async function () {
  let didAuth = false;
  var socketClient = new Ws('ws://localhost:' + nextPort(), {
    onopen: (e) => {
      socketClient.json([
        '_auth',
        { username: 'username', password: 'password' },
      ]);
    },
    onmessage: (e) => {
      try {
        var [key, value] = JSON.parse(e.data);
        if (key === '_acceptAuth') {
          didAuth = true;
          // console.log('Auth Accepted');
        }
      } catch (error) {
        console.error(error);
      }
    },
    onreconnect: (e) => console.log('Reconnecting...', e),
    onmaximum: (e) => console.log('Stop Attempting!', e),
    onclose: (e) => {
      console.log('socket closed');
    },
    onerror: (e) => console.warn('Error:', e),
  });
  await justWait(1000);
  expect(didAuth).toBe(true);
}, 10000);

test('Connect/Disconnect to snub-ws web socket server with no auth', async function () {
  let didAuth = false;
  var socketClient = new Ws('ws://localhost:8585', {
    onmessage: (e) => {
      try {
        var [key, value] = JSON.parse(e.data);
        if (key === '_acceptAuth') {
          didAuth = true;
          // console.log('Auth Accepted');
        }
      } catch (error) {
        console.error(error);
      }
    },
    onreconnect: (e) => console.log('Reconnecting...', e),
    onmaximum: (e) => console.log('Stop Attempting!', e),
    onclose: (e) => {
      console.log('socket closed');
    },
    onerror: (e) => console.warn('Error:', e),
  });
  await justWait(1000);

  expect(didAuth).toBe(true);
}, 10000);

test('Bulk connections', async function () {
  let didAuth = 0;
  let sendAll = 0;
  let blockCheck = 0;
  let tokenCheck = '';

  let doubleMe = 0;

  snub.on('ws:double-me', async (event, reply) => {
    reply(event.payload * 2);
  });


  let disconnectCount = 0;
  snub.on('ws:client-disconnected', async (event, reply) => {
    disconnectCount++;
    // console.log('Client disconnected:', event.payload)
  }
  );
  let updateCount = 0;
  snub.on('ws:client-updated', async (event, reply) => {
    updateCount++;
  }
  );

  const starTrekCharacters = [
    'james-t-kirk',
    'spock',
    'leonard-mccoy',
    'nyota-uhura',
    'montgomery-scott',
    'hikaru-sulu',
    'pavel-chekov',
    'jean-luc-picard',
    'william-riker',
    'data',
    'geordi-laforge',
    'beverly-crusher',
    'deanna-troi',
    'tasha-yar',
    'wesley-crusher',
    'benjamin-sisko',
    'jadzia-dax',
    'kira-nerys',
    'jadzia-dax',
    'kira-nerys',
    'odo',
    'quark',
  ];

  const connections = new Map();

  function connectCharacter(character) {
    var socketClient = new Ws('ws://localhost:' + nextPort(), {
      onopen: (e) => {
        socketClient.json([
          '_auth',
          { username: character, password: 'password' },
        ]);
      },
      onmessage: (e) => {
        var [key, value] = JSON.parse(e.data);
        if (key === '_acceptAuth') {
          tokenCheck = value.token;
          didAuth++;
          socketClient.json(['double-me', 6, 'qwerty456']);
          return;
        }
        if (key === 'send-all-test') {
          sendAll++;
          return;
        }
        if (key === 'set-value') {
          socketClient.testValue = value;
          return;
        }
        if (key === 'blocked') {
          blockCheck++;
          return;
        }
        if (key === 'qwerty456') {
          doubleMe = value;
          return;
        }
        console.log('Message:', key, value);
      },
      onerror: (e) => console.warn('Error:', e),
    });
    connections.set(character, socketClient);
  }

  // Create the first 18 unique connections, then wait for them all to
  // authenticate before connecting the duplicates (jadzia-dax, kira-nerys).
  // This guarantees each 1st occurrence is in the authenticated map when its
  // 2nd occurrence fires the dedup check, eliminating the auth-ordering race.
  for (let i = 0; i < starTrekCharacters.length; i++) {
    if (i === 18) await waitFor(() => didAuth >= 18);
    connectCharacter(starTrekCharacters[i]);
  }

  // wait for all 22 to auth, then allow dedup kicks + close handshakes to settle
  await waitFor(() => didAuth >= 22);
  await justWait(500);

  expect(tokenCheck).toBe('123456');
  expect(doubleMe).toBe(12);

  // check blocked messages
  connections.get('james-t-kirk').json(['send-all', ['blocked', 'value']]);
  await justWait(200);
  expect(blockCheck).toBe(0);

  // checK if all connections are authenticated
  let trekClients = await snub
    .mono('ws:connected-clients', starTrekCharacters)
    .awaitReply();
  expect(trekClients.length).toBe(20);

  // kick some clients
  snub.poly('ws:kick:odo,quark', 'test-kick').send();
  await waitFor(() => disconnectCount >= 4);

  // check if some clients are kicked
  trekClients = await snub
    .mono('ws:connected-clients', starTrekCharacters)
    .awaitReply();
  expect(trekClients.length).toBe(18);

  // send all test
  snub.poly('ws:send-all', ['send-all-test', 'value']).send();
  await justWait(300);
  expect(sendAll).toBe(18);

  // test send
  snub.poly('ws:send:james-t-kirk,spock', ['set-value', 123]).send();
  await justWait(300);
  expect(connections.get('james-t-kirk').testValue).toBe(123);
  expect(connections.get('spock').testValue).toBe(123);

  snub
    .poly('ws:send-all', ['set-value', 456, ['leonard-mccoy', 'nyota-uhura']])
    .send();
  await justWait(300);
  expect(connections.get('leonard-mccoy').testValue).toBe(456);
  expect(connections.get('nyota-uhura').testValue).toBe(456);

  // test channels

  snub
    .poly('ws:add-channel:james-t-kirk,spock,leonard-mccoy,nyota-uhura', [
      'tos',
    ])
    .send();
  await justWait(300);
  snub.poly('ws:del-channel:nyota-uhura', ['tos']).send();
  snub.poly('ws:send-channel:tos', ['set-value', 789]).send();
  await justWait(300);
  expect(connections.get('leonard-mccoy').testValue).toBe(789);
  expect(connections.get('spock').testValue).toBe(789);
  expect(connections.get('james-t-kirk').testValue).toBe(789);
  expect(connections.get('nyota-uhura').testValue).toBe(456);

  const channelCheck = await snub
    .mono('ws:channel-clients', ['tos'])
    .awaitReply();
  expect(channelCheck.length).toBe(3);

  // test meta
  snub
    .poly('ws:set-meta:data,geordi-laforge,beverly-crusher,deanna-troi', {
      series: 'tng',
      starship: 'enterprise',
      likable: true,
      dead: undefined,
      episode: { total: 178, last: 176 },
    })
    .send();
  await justWait(300);

  const metaCheck = await snub
    .mono('ws:get-clients:geordi-laforge,beverly-crusher')
    .awaitReply();
  expect(metaCheck.length).toBe(2);
  expect(metaCheck[0].meta.series).toBe('tng');
  expect(metaCheck[1].meta.starship).toBe('enterprise');
  expect(metaCheck[0].meta.dead).toBe(undefined);
  expect(metaCheck[1].meta.likable).toBe(true);
  expect(metaCheck[0].meta.episode).toBe(undefined);

  // test meta new functions
  snub
    .poly('ws:set-meta', [
      {
        series: 'tos',
        starship: 'enterprise',
        likable: true,
        dead: undefined,
        episode: { total: 178, last: 176 },
      },
      [
        'james-t-kirk',
        'spock',
        'leonard-mccoy',
        'nyota-uhura',
        'montgomery-scott',
        'hikaru-sulu',
        'pavel-chekov',
      ],
    ])
    .send();
  await justWait(300);

  const metaCheck1 = await snub
    .mono('ws:get-clients', ['montgomery-scott', 'hikaru-sulu', 'pavel-chekov'])
    .awaitReply();
  expect(metaCheck1.length).toBe(3);
  expect(metaCheck1[0].meta.series).toBe('tos');
  expect(metaCheck1[1].meta.starship).toBe('enterprise');
  expect(metaCheck1[0].meta.dead).toBe(undefined);
  expect(metaCheck1[1].meta.likable).toBe(true);
  expect(metaCheck1[0].meta.episode).toBe(undefined);

 

  // close connections from client
  connections.get('william-riker').close();
  connections.get('data').close();
  await waitFor(() => disconnectCount >= 6);
  trekClients = await snub
    .mono('ws:connected-clients', starTrekCharacters)
    .awaitReply();
  expect(trekClients.length).toBe(16);

  expect(didAuth).toBe(22);

  expect(disconnectCount).toBe(6);
  expect(updateCount).toBe(11);
}, 15000);

test('kick:* passes kick reason to client (B5 regression)', async function () {
  let clientId = null;
  let kickReason = null;

  const client = new Ws('ws://localhost:8585', {
    onmessage: (e) => {
      const [key, value] = JSON.parse(e.data);
      if (key === '_acceptAuth') clientId = value._id;
      if (key === '_kickConnection') kickReason = value;
    },
  });

  await justWait(300);
  expect(clientId).not.toBeNull();

  snub.poly('ws:kick:' + clientId, 'kick-reason-test').send();
  await justWait(300);

  expect(kickReason).toBe('kick-reason-test');
  client.close();
}, 5000);

test('Outbound dedup suppresses identical messages within 3s, resets after (B4 regression)', async function () {
  let clientId = null;
  let received = 0;

  const client = new Ws('ws://localhost:8585', {
    onmessage: (e) => {
      const [key, value] = JSON.parse(e.data);
      if (key === '_acceptAuth') clientId = value._id;
      if (key === 'dedup-test') received++;
    },
  });

  await justWait(300);
  expect(clientId).not.toBeNull();

  // Send identical message twice back-to-back — second should be deduped
  snub.poly('ws:send:' + clientId, ['dedup-test', 'payload']).send();
  snub.poly('ws:send:' + clientId, ['dedup-test', 'payload']).send();
  await justWait(200);
  expect(received).toBe(1);

  // After the 3s window expires the same message should be delivered again
  await justWait(3000);
  snub.poly('ws:send:' + clientId, ['dedup-test', 'payload']).send();
  await justWait(200);
  expect(received).toBe(2);

  client.close();
}, 10000);

test('Throttle kicks client after exceeding message rate limit (M17)', async function () {
  let clientId = null;
  let kickReason = null;

  const client = new Ws('ws://localhost:9001', {
    onopen: () => client.json(['_auth', { username: 'throttle-test', password: 'password' }]),
    onmessage: (e) => {
      const [key, value] = JSON.parse(e.data);
      if (key === '_acceptAuth') clientId = value._id;
      if (key === '_kickConnection') kickReason = value;
    },
  });

  await waitFor(() => clientId !== null);

  // Send 4 messages rapidly — 4th exceeds throttle limit of 3 per 1000ms
  client.json(['msg', 1]);
  client.json(['msg', 2]);
  client.json(['msg', 3]);
  client.json(['msg', 4]);

  await waitFor(() => kickReason !== null);
  expect(kickReason).toBe('THROTTLE_LIMIT');
}, 5000);

test('Large messages are offloaded to HTTP and retrievable (M19)', async function () {
  // snubwsOffload uses auth:false so clients are auto-authenticated on connect
  let clientId = null;
  let offloadId = null;

  const client = new Ws('ws://localhost:9002', {
    onmessage: (e) => {
      const [key, value] = JSON.parse(e.data);
      if (key === '_acceptAuth') clientId = value._id;
      if (key === '_offload') offloadId = value;
    },
  });

  await waitFor(() => clientId !== null);

  // Send a message larger than offloadToHttpSize (50 bytes)
  const largePayload = 'x'.repeat(200);
  snub.poly('ws:send:' + clientId, ['big-event', largePayload]).send();

  await waitFor(() => offloadId !== null);

  const body = await new Promise((resolve, reject) => {
    http.get('http://localhost:9002/?offload=' + offloadId, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => resolve(data));
    }).on('error', reject);
  });

  const [event, payload] = JSON.parse(body);
  expect(event).toBe('big-event');
  expect(payload).toBe(largePayload);

  client.close();
}, 10000);

test('Messages sent from a second snub instance reach connected clients (M20)', async function () {
  let clientId = null;
  let received = null;

  const client = new Ws('ws://localhost:8585', {
    onmessage: (e) => {
      const [key, value] = JSON.parse(e.data);
      if (key === '_acceptAuth') clientId = value._id;
      if (key === 'cross-instance') received = value;
    },
  });

  await waitFor(() => clientId !== null);

  snub2.poly('ws:send:' + clientId, ['cross-instance', 'hello-from-snub2']).send();

  await waitFor(() => received !== null);
  expect(received).toBe('hello-from-snub2');

  client.close();
}, 5000);

// --- S9: clients must not be able to address snub-ws' own control events ---
//
// Control events are registered as e.g. 'kick:*' but reserved by first segment
// only ('kick'), while the inbound guard compared the whole event name.
// 'kick:someone-else' therefore missed the reserved set and was re-emitted as
// ws:kick:someone-else, straight into the control handler.

test('Client cannot reach parameterised control events (S9)', async function () {
  let victimId = null;
  let attackerId = null;
  let victimKicked = null;
  let victimGotEvil = false;
  let victimGotLegit = false;
  let victimGotChannelEvil = false;
  let victimGotChannelLegit = false;
  const attackerReplies = [];

  const victim = new Ws('ws://localhost:9101', {
    onopen: () =>
      victim.json(['_auth', { username: 'sec-victim', password: 'password' }]),
    onmessage: (e) => {
      const [key, value] = JSON.parse(e.data);
      if (key === '_acceptAuth') victimId = value._id;
      if (key === '_kickConnection') victimKicked = value;
      if (key === 'evil') victimGotEvil = true;
      if (key === 'legit') victimGotLegit = true;
      if (key === 'channel-evil') victimGotChannelEvil = true;
      if (key === 'channel-legit') victimGotChannelLegit = true;
    },
  });

  const attacker = new Ws('ws://localhost:9101', {
    onopen: () =>
      attacker.json([
        '_auth',
        { username: 'sec-attacker', password: 'password' },
      ]),
    onmessage: (e) => {
      const [key, value] = JSON.parse(e.data);
      if (key === '_acceptAuth') attackerId = value._id;
      if (key.startsWith('reply-')) attackerReplies.push([key, value]);
    },
  });

  await waitFor(() => victimId !== null && attackerId !== null);

  // Every one of these previously reached the control handler for sec-victim.
  attacker.json(['get-clients:sec-victim', null, 'reply-1']);
  attacker.json(['kick:sec-victim', 'pwned']);
  attacker.json(['set-meta:sec-victim', { injected: true }]);
  attacker.json(['send:sec-victim', ['evil', 'x']]);
  attacker.json(['send-channel:sec-secret', ['evil', 'x']]);
  attacker.json(['add-channel:sec-victim', ['sec-secret']]);
  attacker.json(['set-channel:sec-victim', ['sec-secret']]);
  attacker.json(['del-channel:sec-victim', ['sec-secret']]);
  // ...and self-targeted, which is the worse of the two: subscribing yourself
  // to a channel you were never granted.
  attacker.json(['add-channel:sec-attacker', ['sec-secret']]);

  await justWait(400);

  // No state disclosure came back, and the victim is untouched.
  expect(attackerReplies).toEqual([]);
  expect(victimKicked).toBeNull();
  expect(victimGotEvil).toBe(false);

  // Nobody joined sec-secret, so a broadcast to it reaches neither client.
  snub.poly('ws:send-channel:sec-secret', ['channel-evil', 1]).send();
  await justWait(300);
  expect(victimGotChannelEvil).toBe(false);

  // The same control events still work when they come from the bus.
  snub.poly('ws:send:sec-victim', ['legit', 1]).send();
  snub.poly('ws:add-channel:sec-victim', ['sec-allowed']).send();
  await justWait(300);
  snub.poly('ws:send-channel:sec-allowed', ['channel-legit', 1]).send();

  await waitFor(() => victimGotLegit && victimGotChannelLegit);
  expect(victimGotLegit).toBe(true);
  expect(victimGotChannelLegit).toBe(true);

  victim.close();
  attacker.close();
}, 15000);

test('Client cannot forge lifecycle or underscore-prefixed events (S9)', async function () {
  // These are mono, so a competing listener would win the lottery half the time
  // and make the counts below meaningless. Drop the earlier tests' handlers so
  // these are the only ones registered.
  snub.off('ws:client-updated');
  snub.off('ws:client-disconnected');

  const forged = [];
  // A genuine lifecycle payload is the client state itself; a forged one is the
  // inbound wrapper, so `from` is the tell.
  const catchForgery = (name) => (payload) => {
    if (payload && payload.from) forged.push(name);
  };
  snub.on('ws:client-authenticated.sectest', catchForgery('authenticated'));
  snub.on('ws:client-disconnected.sectest', catchForgery('disconnected'));
  snub.on('ws:client-updated.sectest', catchForgery('updated'));
  snub.on('ws:client-failedauth.sectest', catchForgery('failedauth'));

  let internalReached = 0;
  snub.on('ws:_kickConnection.sectest', () => internalReached++);
  snub.on('ws:_acceptAuth.sectest', () => internalReached++);

  let clientId = null;
  let pong = null;
  const client = new Ws('ws://localhost:9101', {
    onopen: () =>
      client.json(['_auth', { username: 'sec-forger', password: 'password' }]),
    onmessage: (e) => {
      const [key, value] = JSON.parse(e.data);
      if (key === '_acceptAuth') clientId = value._id;
      if (key === '_pong') pong = value;
    },
  });

  await waitFor(() => clientId !== null);

  client.json(['client-authenticated', { username: 'admin' }]);
  client.json(['client-disconnected', { username: 'admin' }]);
  client.json(['client-updated', { username: 'admin' }]);
  client.json(['client-failedauth', { username: 'admin' }]);
  client.json(['_kickConnection', 'nope']);
  client.json(['_acceptAuth', { _id: 'nope' }]);

  await justWait(400);

  expect(forged).toEqual([]);
  expect(internalReached).toBe(0);

  // The underscore block must not have taken the keepalive with it.
  client.json(['_ping', 12345]);
  await waitFor(() => pong !== null);
  expect(pong).toBe(12345);

  client.close();
  await justWait(200);

  snub.off('ws:client-authenticated.sectest');
  snub.off('ws:client-disconnected.sectest');
  snub.off('ws:client-updated.sectest');
  snub.off('ws:client-failedauth.sectest');
  snub.off('ws:_kickConnection.sectest');
  snub.off('ws:_acceptAuth.sectest');
}, 10000);

test('Unauthenticated clients cannot reach app handlers (S9)', async function () {
  const received = [];
  snub.on('ws:sec-app-event', (payload) => received.push(payload.payload));

  let clientId = null;
  const client = new Ws('ws://localhost:9101', {
    onmessage: (e) => {
      const [key, value] = JSON.parse(e.data);
      if (key === '_acceptAuth') clientId = value._id;
    },
  });

  await justWait(300);
  expect(clientId).toBeNull(); // no _auth sent yet

  client.json(['sec-app-event', 'before-auth']);
  await justWait(300);
  expect(received).toEqual([]);

  client.json(['_auth', { username: 'sec-late', password: 'password' }]);
  await waitFor(() => clientId !== null);

  client.json(['sec-app-event', 'after-auth']);
  await waitFor(() => received.length > 0);
  expect(received).toEqual(['after-auth']);

  client.close();
  snub.off('ws:sec-app-event');
}, 10000);

test('Non-string event names are ignored without killing the socket (S9)', async function () {
  const received = [];
  snub.on('ws:sec-shape-event', (payload) => received.push(payload.payload));

  let clientId = null;
  const client = new Ws('ws://localhost:9101', {
    onopen: () =>
      client.json(['_auth', { username: 'sec-shape', password: 'password' }]),
    onmessage: (e) => {
      const [key, value] = JSON.parse(e.data);
      if (key === '_acceptAuth') clientId = value._id;
    },
  });

  await waitFor(() => clientId !== null);

  // event.split() would throw uncaught inside the uWS callback without the guard
  client.json([123, 'x']);
  client.json([{ toString: 'nope' }, 'x']);
  client.json([null, 'x']);
  client.json([['nested'], 'x']);

  await justWait(300);
  expect(received).toEqual([]);

  // The connection survived all of that.
  client.json(['sec-shape-event', 'still-alive']);
  await waitFor(() => received.length > 0);
  expect(received).toEqual(['still-alive']);

  client.close();
  snub.off('ws:sec-shape-event');
}, 10000);

test('App events containing a colon are not over-blocked (S9)', async function () {
  // The guard reserves first segments, so an app event whose first segment is
  // not a control name must still get through with its segments intact.
  const received = [];
  snub.on('ws:sec-room:*', (payload, reply, channel) =>
    received.push([channel.split(':').at(-1), payload.payload])
  );

  let clientId = null;
  const client = new Ws('ws://localhost:9101', {
    onopen: () =>
      client.json(['_auth', { username: 'sec-rooms', password: 'password' }]),
    onmessage: (e) => {
      const [key, value] = JSON.parse(e.data);
      if (key === '_acceptAuth') clientId = value._id;
    },
  });

  await waitFor(() => clientId !== null);

  client.json(['sec-room:lobby', 'hello']);
  await waitFor(() => received.length > 0);
  expect(received).toEqual([['lobby', 'hello']]);

  client.close();
  snub.off('ws:sec-room:*');
}, 10000);

test('Configured auth event is not client-sendable (S9)', async function () {
  let authCalls = 0;
  snub.on('ws:sec-auth-check', (authObj, reply) => {
    authCalls++;
    reply(authObj.password === 'password' ? { token: 'sec-token' } : false);
  });

  let clientId = null;
  const replies = [];
  const client = new Ws('ws://localhost:9102', {
    onopen: () =>
      client.json(['_auth', { username: 'sec-authed', password: 'password' }]),
    onmessage: (e) => {
      const [key, value] = JSON.parse(e.data);
      if (key === '_acceptAuth') clientId = value._id;
      if (key.startsWith('reply-')) replies.push([key, value]);
    },
  });

  await waitFor(() => clientId !== null);
  expect(authCalls).toBe(1);

  // Calling the app's auth handler directly would be an oracle: arbitrary body,
  // verdict returned on the replyId, none of #denyAuth's kick or timeout.
  client.json([
    'sec-auth-check',
    { username: 'admin', password: 'guess' },
    'reply-a',
  ]);
  client.json([
    'sec-auth-check',
    { username: 'admin', password: 'password' },
    'reply-b',
  ]);

  await justWait(400);
  expect(authCalls).toBe(1);
  expect(replies).toEqual([]);

  client.close();
  snub.off('ws:sec-auth-check');
}, 10000);

test('Dropped inbound events are diagnosable under config.debug (S9)', async function () {
  // A drop is silent on both sides, so without this an app that named one of
  // its events after a control event has nothing to bisect against.
  const warnings = [];
  const realWarn = console.warn;
  console.warn = (...args) => warnings.push(args.join(' '));

  try {
    let clientId = null;
    const client = new Ws('ws://localhost:9103', {
      onmessage: (e) => {
        const [key, value] = JSON.parse(e.data);
        if (key === '_acceptAuth') clientId = value._id;
      },
    });

    // Pre-auth first, while the connection is still unauthenticated.
    await justWait(300);
    client.json(['sec-debug-event', 1]);
    await justWait(200);

    client.json(['_auth', { username: 'sec-debug', password: 'password' }]);
    await waitFor(() => clientId !== null);

    client.json(['kick:someone-else', 'x']);
    client.json(['_kickConnection', 'x']);
    await justWait(300);

    const dropped = warnings.filter((w) => w.includes('dropped inbound event'));
    expect(dropped.length).toBe(3);
    expect(
      dropped.some(
        (w) => w.includes('sec-debug-event') && w.includes('not authenticated')
      )
    ).toBe(true);
    expect(
      dropped.some(
        (w) => w.includes('kick:someone-else') && w.includes('reserved control')
      )
    ).toBe(true);
    expect(
      dropped.some((w) => w.includes('_kickConnection') && w.includes('_ prefixed'))
    ).toBe(true);

    client.close();
  } finally {
    console.warn = realWarn;
  }
}, 10000);

// helper functions

function Ws(url, opts) {
  opts = {
    autoConnect: true,
    onopen: noop,
    onmessage: noop,
    onreconnect: noop,
    onmaximum: noop,
    onclose: noop,
    onerror: noop,
    ...opts,
  };

  var ws;
  var num = 0;
  var $ = {
    hash: Math.random(),
  };
  var max = opts.maxAttempts || Infinity;
  $.open = function () {
    try {
      ws.close(1000);
      ws = undefined;
    } catch (error) {}
    ws = new WebSocket(url, opts.protocols || []);
    $.ws = ws;

    ws.onmessage = opts.onmessage || noop;

    ws.onopen = function (e) {
      // console.log('ws-open');
      (opts.onopen || noop)(e);
      num = 0;
    };

    ws.onclose = function (e) {
      if (e.code === 3000) return; // unauthorized
      if (e.code === 1005) return;
      // https://github.com/Luka967/websocket-close-codes
      // https://developer.mozilla.org/en-US/docs/Web/API/CloseEvent
      e.code === 1000 || e.code === 1001 || $.reconnect(e);
      if (e.code === 1000 && e.reason === 'IDLE_TIMEOUT') $.reconnect(e);
      (opts.onclose || noop)(e);
    };

    ws.onerror = function (e) {
      e && e.code === 'ECONNREFUSED'
        ? $.reconnect(e)
        : (opts.onerror || noop)(e);
    };
  };

  $.reconnect = function (e) {
    console.log('Reconnecting...', e);
    if (num++ < max) {
      setTimeout(
        function () {
          (opts.onreconnect || noop)(e);
          $.open();
        },
        num === 1 ? 1 : (opts.timeout || 500) * (num - 1)
      );
    } else {
      (opts.onmaximum || noop)(e);
    }
  };

  $.readyState = function () {
    return ws.readyState;
  };

  $.json = function (x) {
    ws.send(JSON.stringify(x));
  };

  $.send = function (x) {
    ws.send(x);
  };

  $.close = function (x, y) {
    ws.close(x || 1e3, y);
    ws.onmessage = noop;
    ws.onopen = noop;
    ws.onerror = noop;
  };

  if (opts.autoConnect) $.open(); // init

  return $;
  function noop() {}
}

function justWait(ms = 1000) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function waitFor(condition, timeout = 5000) {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const check = () => {
      if (condition()) return resolve();
      if (Date.now() - start > timeout)
        return reject(new Error('waitFor timeout: ' + condition.toString()));
      setTimeout(check, 50);
    };
    check();
  });
}
