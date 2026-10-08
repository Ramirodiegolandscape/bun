// A server whose 'upgrade' and 'connect' listeners never read the socket. The socket buffers what
// the peer sends up to its high-water mark and then stops its reads. The process prints what every
// tunnel saw when it exits, and it must exit by itself: it has no timer and calls no process.exit().
//
//   PROTO=http|https (with CERT and KEY)
//   CLOSE=before|after   server.close() before or after the reads of the tunnels stop
//   READ_LATER=1         then a 'data' listener reads every tunnel to its end
"use strict";
const { PROTO, CLOSE, READ_LATER, CERT, KEY } = process.env;
const secure = PROTO === "https";
const server = secure
  ? require("node:https").createServer({ cert: CERT, key: KEY })
  : require("node:http").createServer();
const connect = secure ? require("node:tls").connect : require("node:net").connect;

const requests = {
  "upgrade": ["GET / HTTP/1.1\r\nHost: a\r\nConnection: Upgrade\r\nUpgrade: raw\r\n", ""],
  "connect": ["CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n", ""],
  // The tunnel starts behind the body.
  "upgrade with a body": [
    "POST / HTTP/1.1\r\nHost: a\r\nConnection: Upgrade\r\nUpgrade: raw\r\nContent-Length: 5\r\n",
    "hello",
  ],
};
const reply = "HTTP/1.1 200 OK\r\n\r\n";
// Twice what the socket buffers before it stops its reads.
const unread = Buffer.alloc(2 * 65536, "x");
// More than a loopback socket takes in one write.
const large = Buffer.alloc(16 * 1024 * 1024, "y");

// `start` answers the request. `stopped` runs when the reads of the tunnel have stopped.
const kinds = {
  // The peer closes its socket behind its bytes.
  "the peer leaves": {
    stopped(tunnel, next) {
      tunnel.client.once("close", next);
      tunnel.client.destroySoon();
    },
  },
  // The peer is still connected when the process exits.
  "the peer stays": {},
  "the listener ended it at once": {
    start: tunnel => tunnel.socket.end(reply),
  },
  "the listener ends it later": {
    stopped(tunnel, next) {
      tunnel.socket.end();
      next();
    },
  },
  // The bytes that are left to send keep the process alive, and then nothing does.
  "the listener writes to it later": {
    stopped(tunnel, next) {
      tunnel.socket.write(large, () => tunnel.result.events.push("write callback"));
      next();
    },
  },
};

const tunnels = {};
for (const request in requests) {
  for (const kind of READ_LATER ? ["the peer stays"] : Object.keys(kinds)) {
    tunnels[`${request}, ${kind}`] = { request, kind: kinds[kind], result: { events: [], received: 0 } };
  }
}
const results = {};
const count = Object.keys(tunnels).length;
let handedOff = 0;
let atRest = 0;

function close() {
  server.close(() => (results.server = "close"));
}

function start(tunnel) {
  const { socket, kind, result } = tunnel;
  (kind.start ?? (() => socket.write(reply)))(tunnel);
  (function untilReadsStop() {
    if (socket.readableLength < socket.readableHighWaterMark) return setImmediate(untilReadsStop);
    (kind.stopped ?? ((_, next) => next()))(tunnel, () => {
      if (++atRest === count && CLOSE === "after") close();
      if (!READ_LATER) return;
      setImmediate(() => {
        socket.on("data", chunk => {
          result.received += chunk.length;
          // From here on this tunnel has nothing buffered and nothing to send. It reads, and only that keeps the process alive.
          if (result.received === unread.length) socket.write("more");
        });
      });
    });
  })();
}

function onTunnel(req, socket) {
  const name = req.headers.tunnel;
  const tunnel = tunnels[name];
  tunnel.socket = socket;
  results[name] = tunnel.result;
  socket.on("error", error => tunnel.result.events.push(error.code));
  socket.on("end", () => {
    tunnel.result.events.push("end");
    socket.end();
  });
  socket.on("close", () => tunnel.result.events.push("close"));
  const handOff = () => {
    if (CLOSE === "after") return start(tunnel);
    if (++handedOff < count) return;
    close();
    for (const name in tunnels) start(tunnels[name]);
  };
  // The tunnel starts behind the body of the request.
  if (requests[tunnel.request][1]) req.on("end", handOff).resume();
  else handOff();
}
server.on("upgrade", onTunnel).on("connect", onTunnel);

server.listen(0, "127.0.0.1", () => {
  for (const name in tunnels) {
    const tunnel = tunnels[name];
    const [head, body] = requests[tunnel.request];
    const client = (tunnel.client = connect({
      port: server.address().port,
      host: "127.0.0.1",
      rejectUnauthorized: false,
    }));
    // Only the server's side of a tunnel may keep this process alive.
    client.unref();
    client.on("error", () => {});
    client.once(secure ? "secureConnect" : "connect", () => client.write(`${head}Tunnel: ${name}\r\n\r\n${body}`));
    // The reply: the listener has run, so the socket gets these bytes and not the `head` argument.
    client.once("data", () => {
      client.write(unread);
      if (READ_LATER) client.once("data", () => client.end(unread));
      else client.resume();
    });
  }
});

process.on("exit", () => console.log(JSON.stringify(results)));
