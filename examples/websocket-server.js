const Snub = require('snub');
const snub = new Snub({
  debug: false
});
const SnubWS = require('../snub-ws.js');

const snubws = new SnubWS({
  debug: true,
  mutliLogin: true,
  auth: 'authenticate-client' || function (auth, accept, deny) {
    if (auth.username == 'username') return accept();
    deny();
  }
});

snub.use(snubws);

// snub.on('ws:*', function () {
//   console.log(arguments);
// });

snub.on('ws:authenticate-client', function (auth, reply) {
  console.log(auth);
  if (auth.username == 'username') return reply(true);
  reply(false);
});

snub.on('ws:client-authenticated', function (payload) {
  // we can send a message to a single user.
  // this will work with polly but every snub instance will send the message to the client, not alway desired.
  console.log('got Authed user data=> ' + process.pid + ' - ' + JSON.stringify(payload));
  snub.poly('ws:send:' + payload.id, ['hello', 'yay! you authenticated']).send();
});

snub.on('ws:do-math', function (event, reply) {
  console.log('domath');
  // if the event for the client is expecting a reply we can do so.
  if (typeof reply === 'function')
    reply(process.pid + '=' + event.payload.reduce((p, c) => {
      return (p + c);
    }, 0));
});

snub.on('ws:broadcast', function (event) {
  snub.poly('ws:get-client', event.from.id).replyAt(userInfo => {
    console.log(userInfo);
  }).send();

  snub.poly('ws:add-channel:' + event.from.id, 'channel1').replyAt(user => {
    console.log('ADD USER CHANNEL=>', user);

    snub.poly('ws:set-channel:' + event.from.id, ['channel2', 'channel3']).replyAt(user => {
      console.log('SET USER CHANNEL=>', user);

      snub.poly('ws:del-channel:' + event.from.id, 'channel2').replyAt(user => {
        console.log('DEL USER CHANNEL=>', user);

        snub.poly('ws:send-channel:channel3', ['channel3', 'channel3 message']).send();
      }).send();
    }).send();
  }).send();

  snub.poly('ws:send-all', ['hull0', 'derka?']).send();
});

snub.on('ws:whos-online', function (event, reply) {
  var c = [];
  // when c is fully populated we are going to need to send it
  var sendReply = () => {
    var r = [].concat(...c).map(cu => cu.id);
    reply(r);
  };

  // force sendtimeout
  var sendTimeout = setTimeout(sendReply, 4500);

  // set a instance count higher than will ever be required
  var count = 10;
  snub.poly('ws:connected-clients').replyAt(connected => {
    c.push(connected);
    if (count === c.length) {
      clearTimeout(sendTimeout);
      sendReply();
    }
  }, 6000).send(listenCount => {
    count = listenCount;
  });
});

// create a basic http server to serve up client files.
const http = require('http');
const fs = require('fs');
const path = require('path');

http.createServer(function (request, response) {
  switch (request.url) {
    case '/':
      fs.readFile(path.dirname(__filename) + '/client.html', function (error, content) {
        response.writeHead(200, {
          'Content-Type': 'text/html'
        });
        response.end(content, 'utf-8');
      });
      break;
    case '/snub-ws-client.js':
      fs.readFile(path.dirname(__filename) + '/../snub-ws-client.js', function (error, content) {
        response.writeHead(200, {
          'Content-Type': 'text/javascript'
        });
        response.end(content, 'utf-8');
      });
      break;

    default:
      response.writeHead(404);
      response.end();
      break;
  }
}).listen(8686);
