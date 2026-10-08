// 'upgrade' and 'connect' listeners that call socket.unref() and socket.ref(). A tunnel keeps the
// process alive while it reads and is ref'd, or while it has bytes left to send. The process prints
// what every tunnel saw when it exits, and it must exit by itself: it has no timer and calls no
// process.exit().
//
//   PROTO=http|https (with CERT and KEY)
"use strict";
const { PROTO, CERT, KEY } = process.env;
const secure = PROTO === "https";
const server = secure
  ? require("node:https").createServer({ cert: CERT, key: KEY })
  : require("node:http").createServer();
const connect = secure ? require("node:tls").connect : require("node:net").connect;

const requests = {
  upgrade: "GET / HTTP/1.1\r\nHost: a\r\nConnection: Upgrade\r\nUpgrade: raw\r\n",
  connect: "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n",
};
const reply = "HTTP/1.1 200 OK\r\n\r\n";
// What the socket buffers before it stops its reads.
const full = Buffer.alloc(65536, "x");
// More than a loopback socket takes in one write.
const large = Buffer.alloc(16 * 1024 * 1024, "y");

function untilReadsStop(socket, then) {
  if (socket.readableLength < socket.readableHighWaterMark) return setImmediate(untilReadsStop, socket, then);
  then();
}

// `start` runs when the server has closed, before the peer has the reply. The tunnel calls `rest()`
// when it has done what it does. `later` runs when every tunnel is at rest: from there on, only
// that tunnel can keep the process alive.
const kinds = {
  "unref() while it reads": {
    start({ socket, rest }) {
      socket.unref();
      rest();
    },
  },
  // The socket of the first connection, in the 'connection' listener.
  "unref() before the request": {
    start: ({ rest }) => rest(),
  },
  "unref(), then a write": {
    start({ socket, result, rest }) {
      socket.unref();
      socket.write(large, () => (result.events.push("write callback"), rest()));
    },
  },
  "a write, then unref()": {
    start({ socket, result, rest }) {
      socket.write(large, () => (result.events.push("write callback"), rest()));
      socket.unref();
    },
  },
  "ref() after its reads stopped": {
    peerSends: full,
    start({ socket, rest }) {
      untilReadsStop(socket, () => {
        socket.ref();
        rest();
      });
    },
  },
  "unref(), then its reads stop, then a reader": {
    peerSends: full,
    start({ socket, result, rest }) {
      socket.unref();
      untilReadsStop(socket, () => {
        socket.on("data", chunk => (result.received += chunk.length));
        // Behind the 'data' events for what the socket has buffered.
        setImmediate(rest);
      });
    },
  },
  "unref(), then ref(), then a reader": {
    peerSends: full,
    start({ socket, rest }) {
      socket.unref();
      untilReadsStop(socket, rest);
    },
    later({ socket, result }) {
      socket.ref();
      socket.on("data", chunk => {
        result.received += chunk.length;
        // This tunnel has nothing buffered and nothing to send. It reads and it is ref'd.
        if (result.received === full.length) socket.write("more");
      });
    },
  },
};

// Over TLS the end of the stream closes the socket in Node.js.
if (!secure) {
  kinds["ref() at the end of the stream"] = {
    peerEnds: true,
    start: ({ socket }) => socket.resume(),
    end({ socket, rest }) {
      socket.ref();
      rest();
    },
  };
}

const tunnels = {};
const results = {};
let handedOff = 0;
let atRest = 0;
function rest() {
  if (++atRest < count) return;
  for (const name in tunnels) tunnels[name].kind.later?.(tunnels[name]);
}
for (const kind in kinds) {
  for (const request of kind === "unref() while it reads" ? ["upgrade", "connect"] : ["upgrade"]) {
    tunnels[`${request}, ${kind}`] = { request, kind: kinds[kind], result: { events: [], received: 0 }, rest };
  }
}
const count = Object.keys(tunnels).length;

function onTunnel(req, socket) {
  const name = req.headers.tunnel;
  const tunnel = tunnels[name];
  tunnel.socket = socket;
  results[name] = tunnel.result;
  socket.on("error", error => tunnel.result.events.push(error.code));
  socket.on("end", () => {
    tunnel.result.events.push("end");
    if (tunnel.kind.end) tunnel.kind.end(tunnel);
    else socket.end();
  });
  socket.on("close", () => tunnel.result.events.push("close"));
  if (++handedOff < count) return;
  server.close(() => (results.server = "close"));
  for (const name in tunnels) {
    tunnels[name].socket.write(reply);
    tunnels[name].kind.start(tunnels[name]);
  }
}
server.on("upgrade", onTunnel).on("connect", onTunnel);

function open(name) {
  const { request, kind } = tunnels[name];
  const client = connect({ port: server.address().port, host: "127.0.0.1", rejectUnauthorized: false });
  // Only the server's side of a tunnel may keep this process alive.
  client.unref();
  client.on("error", () => {});
  client.once(secure ? "secureConnect" : "connect", () => client.write(`${requests[request]}Tunnel: ${name}\r\n\r\n`));
  // The reply: the listener has run, so the socket gets these bytes and not the `head` argument.
  client.once("data", () => {
    if (kind.peerEnds) return client.end();
    if (kind.peerSends) client.write(kind.peerSends);
    if (kind.later) client.once("data", () => client.end(full));
    else client.resume();
  });
}

server.once("connection", socket => {
  socket.unref();
  for (const name in tunnels) if (name !== "upgrade, unref() before the request") open(name);
});
server.listen(0, "127.0.0.1", () => open("upgrade, unref() before the request"));

process.on("exit", () => console.log(JSON.stringify(results)));
