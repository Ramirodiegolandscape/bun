/**
 * The lifecycle getters of Http2Session: `connecting`, `closed`, `destroyed` and `state`.
 *
 * A session has no native handle while it connects and after it is destroyed. node then reports an
 * empty `state` object. node keeps a session `connecting` until the session took its socket: through
 * the TLS handshake, and until the session's own socket listener ran.
 * The expected values are node's (v24.21.0, v26.3.0 and v26.11.1 agree). Where Bun reports something
 * else, the test says so and why.
 *
 * Works with both:
 *   bun bd test test/js/node/http2/node-http2-session-lifecycle.test.ts
 *   node --test test/js/node/http2/node-http2-session-lifecycle.test.ts
 */
import assert from "node:assert";
import { once, type EventEmitter } from "node:events";
import fs from "node:fs";
import http2 from "node:http2";
import net from "node:net";
import path from "node:path";
import { duplexPair } from "node:stream";
import { describe, test } from "node:test";
import tls from "node:tls";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const isBun = typeof Bun !== "undefined";

const keys = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "test", "fixtures", "keys");
const TLS = {
  key: fs.readFileSync(path.join(keys, "agent1-key.pem")),
  cert: fs.readFileSync(path.join(keys, "agent1-cert.pem")),
  ALPNProtocols: ["h2"],
};

type Session = http2.ClientHttp2Session | http2.ServerHttp2Session;
type Connect = (authority: string, options: http2.ClientSessionOptions) => Promise<http2.ClientHttp2Session>;

const sessionStateKeys = [
  "deflateDynamicTableSize",
  "effectiveLocalWindowSize",
  "effectiveRecvDataLength",
  "inflateDynamicTableSize",
  "lastProcStreamID",
  "localWindowSize",
  "nextStreamID",
  "outboundQueueSize",
  "remoteWindowSize",
];
// "empty" for {}, "populated" for the nine numeric fields, the value itself for anything else.
function stateKind(state: unknown) {
  if (state === null || typeof state !== "object") return state;
  const fields = state as Record<string, unknown>;
  const names = Object.keys(fields).sort();
  if (names.length === 0) return "empty";
  if (names.join() === sessionStateKeys.join() && names.every(name => typeof fields[name] === "number")) {
    return "populated";
  }
  return state;
}
function lifecycle(session: Session) {
  return {
    connecting: session.connecting,
    closed: session.closed,
    destroyed: session.destroyed,
    state: stateKind(session.state),
  };
}
const connecting = { connecting: true, closed: false, destroyed: false, state: "empty" };
const ready = { connecting: false, closed: false, destroyed: false, state: "populated" };
const destroyed = { connecting: false, closed: false, destroyed: true, state: "empty" };
const destroyedBeforeReady = { ...destroyed, connecting: true };

// The code of every 'error' event that the emitter emits.
function errorCodes(emitter: EventEmitter) {
  const codes: unknown[] = [];
  emitter.on("error", (error: NodeJS.ErrnoException) => codes.push(error.code));
  return codes;
}
// Resolves on 'close'. Unlike once(), it does not reject when an 'error' event comes first.
function closeOf(emitter: EventEmitter) {
  return new Promise<void>(resolve => emitter.once("close", () => resolve()));
}
function immediate() {
  return new Promise<void>(resolve => setImmediate(resolve));
}
async function listen(server: net.Server) {
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return (server.address() as net.AddressInfo).port;
}
// An h2c server that answers every request at once. Every test ends the connection from the
// client side. What the server sessions and streams report about that is not under test.
async function h2cServer() {
  const server = http2.createServer();
  server.on("sessionError", () => {});
  server.on("stream", (stream: http2.ServerHttp2Stream) => {
    stream.on("error", () => {});
    stream.respond({ ":status": 200 }, { endStream: true });
  });
  return { server, port: await listen(server) };
}

describe("Http2Session lifecycle getters", () => {
  test("client: {} while it connects, populated from 'connect' until it is destroyed", async () => {
    const { server, port } = await h2cServer();
    const client = http2.connect(`http://127.0.0.1:${port}`);
    const errors = errorCodes(client);
    try {
      const seen: Record<string, unknown> = { afterConnectCall: lifecycle(client) };
      await once(client, "connect");
      seen.connectEvent = lifecycle(client);
      seen.sameObjectOnTwoReads = client.state === client.state;
      const closed = closeOf(client);
      client.close();
      seen.afterCloseCall = lifecycle(client);
      await closed;
      seen.closeEvent = lifecycle(client);
      assert.deepStrictEqual(
        { ...seen, errors },
        {
          afterConnectCall: connecting,
          connectEvent: ready,
          sameObjectOnTwoReads: false,
          afterCloseCall: { ...ready, closed: true },
          closeEvent: { ...destroyed, closed: true },
          errors: [],
        },
      );
    } finally {
      client.destroy();
      server.close();
    }
  });

  test("client and server: {} after destroy()", async () => {
    const { server, port } = await h2cServer();
    const serverSessionCreated = once(server, "session");
    const client = http2.connect(`http://127.0.0.1:${port}`);
    const errors = errorCodes(client);
    const connected = once(client, "connect");
    try {
      const [serverSession] = (await serverSessionCreated) as [http2.ServerHttp2Session];
      const seen: Record<string, unknown> = { serverSessionEvent: lifecycle(serverSession) };
      await connected;
      const serverClosed = closeOf(serverSession);
      const clientClosed = closeOf(client);
      serverSession.destroy();
      seen.serverAfterDestroyCall = lifecycle(serverSession);
      client.destroy();
      seen.clientAfterDestroyCall = lifecycle(client);
      await serverClosed;
      seen.serverCloseEvent = lifecycle(serverSession);
      await clientClosed;
      seen.clientCloseEvent = lifecycle(client);
      assert.deepStrictEqual(
        { ...seen, errors },
        {
          serverSessionEvent: ready,
          serverAfterDestroyCall: destroyed,
          clientAfterDestroyCall: destroyed,
          serverCloseEvent: destroyed,
          clientCloseEvent: destroyed,
          errors: [],
        },
      );
    } finally {
      client.destroy();
      server.close();
    }
  });

  test("server: closed after close(), destroyed at 'close'", async () => {
    const { server, port } = await h2cServer();
    const serverSessionCreated = once(server, "session");
    const client = http2.connect(`http://127.0.0.1:${port}`);
    const errors = errorCodes(client);
    const connected = once(client, "connect");
    try {
      const [serverSession] = (await serverSessionCreated) as [http2.ServerHttp2Session];
      await connected;
      const closed = closeOf(serverSession);
      serverSession.close();
      const seen: Record<string, unknown> = { afterCloseCall: lifecycle(serverSession) };
      await closed;
      seen.closeEvent = lifecycle(serverSession);
      assert.deepStrictEqual(
        { ...seen, errors },
        { afterCloseCall: { ...ready, closed: true }, closeEvent: { ...destroyed, closed: true }, errors: [] },
      );
    } finally {
      client.destroy();
      server.close();
    }
  });

  test("client: destroyed before its socket connects, it stays connecting", async () => {
    const { server, port } = await h2cServer();
    const socket = net.connect(port, "127.0.0.1");
    const client = http2.connect(`http://127.0.0.1:${port}`, { createConnection: () => socket });
    const errors = errorCodes(client);
    try {
      // Added after the session's own listener, so the session handled the connect when it runs.
      const socketConnected = Promise.race([
        once(socket, "connect").then(() => lifecycle(client)),
        closeOf(socket).then(() => "the socket closed before it connected"),
      ]);
      const closed = closeOf(client);
      client.destroy();
      const seen: Record<string, unknown> = { afterDestroyCall: lifecycle(client) };
      await closed;
      seen.closeEvent = lifecycle(client);
      seen.afterSocketConnect = await socketConnected;
      assert.deepStrictEqual(
        { ...seen, errors },
        {
          afterDestroyCall: destroyedBeforeReady,
          closeEvent: destroyedBeforeReady,
          afterSocketConnect: destroyedBeforeReady,
          errors: [],
        },
      );
    } finally {
      client.destroy();
      socket.destroy();
      server.close();
    }
  });

  test("client: a refused connection leaves it connecting and destroyed", async () => {
    // A port that was just released: nothing listens on it.
    const released = net.createServer();
    const port = await listen(released);
    released.close();
    await once(released, "close");
    const client = http2.connect(`http://127.0.0.1:${port}`);
    const seen: Record<string, unknown> = {};
    client.on("error", (error: NodeJS.ErrnoException) => {
      seen.errorEvent = { code: error.code, ...lifecycle(client) };
    });
    try {
      await closeOf(client);
      seen.closeEvent = lifecycle(client);
      assert.deepStrictEqual(seen, {
        errorEvent: { code: "ECONNREFUSED", ...destroyedBeforeReady },
        closeEvent: destroyedBeforeReady,
      });
    } finally {
      client.destroy();
    }
  });

  test("client: still connecting through the TLS handshake", async () => {
    const server = http2.createSecureServer(TLS);
    server.on("sessionError", () => {});
    const port = await listen(server);
    const socket = tls.connect({ host: "127.0.0.1", port, ALPNProtocols: ["h2"], rejectUnauthorized: false });
    const seen: Record<string, unknown> = {};
    // TCP is up and the handshake has not finished.
    socket.once("connect", () => {
      seen.tcpConnected = {
        ...lifecycle(client),
        localSettings: client.localSettings,
        remoteSettings: client.remoteSettings,
      };
    });
    const client = http2.connect(`https://127.0.0.1:${port}`, { createConnection: () => socket });
    const errors = errorCodes(client);
    // Added after the session's own listener, so the session is set up when it runs.
    socket.once("secureConnect", () => {
      seen.handshakeDone = lifecycle(client);
    });
    try {
      await once(client, "connect");
      assert.deepStrictEqual(
        { ...seen, errors },
        {
          tcpConnected: { ...connecting, localSettings: {}, remoteSettings: {} },
          handshakeDone: ready,
          errors: [],
        },
      );
    } finally {
      client.destroy();
      server.close();
    }
  });

  test("client: connecting until the session's own socket 'connect' listener ran", async () => {
    const { server, port } = await h2cServer();
    const socket = net.connect(port, "127.0.0.1");
    const seen: Record<string, unknown> = {};
    socket.once("connect", () => {
      seen.listenerAddedBeforeSession = lifecycle(client);
    });
    const client = http2.connect(`http://127.0.0.1:${port}`, { createConnection: () => socket });
    const errors = errorCodes(client);
    socket.once("connect", () => {
      seen.listenerAddedAfterSession = lifecycle(client);
    });
    try {
      await once(client, "connect");
      assert.deepStrictEqual(
        { ...seen, errors },
        { listenerAddedBeforeSession: connecting, listenerAddedAfterSession: ready, errors: [] },
      );
    } finally {
      client.destroy();
      server.close();
    }
  });

  test("a session over a connected socket or a Duplex is ready at once", async () => {
    const { server, port } = await h2cServer();
    const socket = net.connect(port, "127.0.0.1");
    const [clientSide, serverSide] = duplexPair();
    const sessions: Session[] = [];
    try {
      await once(socket, "connect");
      const overSocket = http2.connect(`http://127.0.0.1:${port}`, { createConnection: () => socket });
      const overDuplexServer = http2.performServerHandshake(serverSide);
      const overDuplex = http2.connect("http://localhost", { createConnection: () => clientSide });
      sessions.push(overSocket, overDuplexServer, overDuplex);
      const errors = sessions.map(errorCodes);
      const seen = {
        overSocket: lifecycle(overSocket),
        overDuplexServer: lifecycle(overDuplexServer),
        overDuplex: lifecycle(overDuplex),
      };
      await Promise.all(sessions.map(session => once(session, "connect")));
      assert.deepStrictEqual(
        { ...seen, errors },
        { overSocket: ready, overDuplexServer: ready, overDuplex: ready, errors: [[], [], []] },
      );
    } finally {
      for (const session of sessions) session.destroy();
      socket.destroy();
      server.close();
    }
  });

  test("the getters are accessors of the prototype that client and server sessions share", async () => {
    const { server, port } = await h2cServer();
    const serverSessionCreated = once(server, "session");
    const client = http2.connect(`http://127.0.0.1:${port}`);
    const errors = errorCodes(client);
    try {
      const [serverSession] = (await serverSessionCreated) as [http2.ServerHttp2Session];
      const clientPrototype = Object.getPrototypeOf(client);
      const serverPrototype = Object.getPrototypeOf(serverSession);
      const shared = Object.getPrototypeOf(clientPrototype);
      // node defines `destroyed` there too. Bun defines it on each of the two classes.
      const names = ["connecting", "closed", "state"];
      assert.deepStrictEqual(
        {
          sharedPrototype: Object.getPrototypeOf(serverPrototype) === shared,
          onShared: names.map(name => typeof Object.getOwnPropertyDescriptor(shared, name)?.get),
          onClient: names.filter(name => Object.hasOwn(clientPrototype, name)),
          onServer: names.filter(name => Object.hasOwn(serverPrototype, name)),
          errors,
        },
        {
          sharedPrototype: true,
          onShared: ["function", "function", "function"],
          onClient: [],
          onServer: [],
          errors: [],
        },
      );
    } finally {
      client.destroy();
      server.close();
    }
  });

  test("client: a socket that closes before the session is ready gives ERR_SOCKET_CLOSED", async () => {
    const { server, port } = await h2cServer();
    const socket = net.connect(port, "127.0.0.1");
    const client = http2.connect(`http://127.0.0.1:${port}`, { createConnection: () => socket });
    const errors = errorCodes(client);
    try {
      const closed = closeOf(client);
      socket.destroy();
      await closed;
      // node emits a first 'close', and the error one tick later. Bun emits the error first.
      await immediate();
      assert.deepStrictEqual(errors, ["ERR_SOCKET_CLOSED"]);
    } finally {
      client.destroy();
      server.close();
    }
  });

  test("client: a promisified connect() rejects when its socket closes before the session is ready", async () => {
    const { server, port } = await h2cServer();
    const socket = net.connect(port, "127.0.0.1");
    try {
      // The types do not declare the signature of connect[util.promisify.custom].
      const connect = promisify(http2.connect) as unknown as Connect;
      const outcome = connect(`http://127.0.0.1:${port}`, { createConnection: () => socket }).then(
        session => {
          session.destroy();
          return "connected";
        },
        (error: NodeJS.ErrnoException) => error.code,
      );
      const socketClosed = once(socket, "close");
      socket.destroy();
      await socketClosed;
      // The rejection comes from a tick. An immediate runs after every tick.
      assert.strictEqual(await Promise.race([outcome, immediate().then(() => "still pending")]), "ERR_SOCKET_CLOSED");
    } finally {
      socket.destroy();
      server.close();
    }
  });

  test("client: no error for destroy() while it connects, or for a socket that closes once it is ready", async () => {
    const { server, port } = await h2cServer();
    const socket = net.connect(port, "127.0.0.1");
    const destroyedEarly = http2.connect(`http://127.0.0.1:${port}`);
    const lostSocket = http2.connect(`http://127.0.0.1:${port}`, { createConnection: () => socket });
    const errors = { destroyedEarly: errorCodes(destroyedEarly), lostSocket: errorCodes(lostSocket) };
    try {
      const destroyedEarlyClosed = closeOf(destroyedEarly);
      destroyedEarly.destroy();
      await once(lostSocket, "connect");
      const lostSocketClosed = closeOf(lostSocket);
      socket.destroy();
      await Promise.all([destroyedEarlyClosed, lostSocketClosed]);
      await immediate();
      assert.deepStrictEqual(errors, { destroyedEarly: [], lostSocket: [] });
    } finally {
      destroyedEarly.destroy();
      lostSocket.destroy();
      server.close();
    }
  });

  // What the session reports to the 'aborted' listener of a request that waits for a stream slot,
  // when the socket of the session goes away.
  async function waitingRequestAborted(socketError: Error | undefined) {
    const server = http2.createServer({ settings: { maxConcurrentStreams: 1 } });
    server.on("sessionError", () => {});
    // The response stays open, so the one stream slot stays taken.
    server.on("stream", (stream: http2.ServerHttp2Stream) => {
      stream.on("error", () => {});
      stream.respond({ ":status": 200 });
    });
    const port = await listen(server);
    const socket = net.connect(port, "127.0.0.1");
    const client = http2.connect(`http://127.0.0.1:${port}`, { createConnection: () => socket });
    const errors = errorCodes(client);
    try {
      await once(client, "remoteSettings");
      // What the two requests report is not under test.
      const active = client.request({ ":method": "POST", ":path": "/" });
      active.on("error", () => {});
      await once(active, "response");
      const waiting = client.request({ ":method": "POST", ":path": "/" });
      waiting.on("error", () => {});
      const seen: Record<string, unknown> = {};
      waiting.on("aborted", () => {
        seen.waitingAborted = lifecycle(client);
      });
      const closed = closeOf(client);
      socket.destroy(socketError);
      await closed;
      return { ...seen, errors };
    } finally {
      client.destroy();
      server.close();
    }
  }

  test("client: a request that waits for a stream slot reads {} when the socket fails", async () => {
    const error = Object.assign(new Error("the socket failed"), { code: "ESOCKETFAILED" });
    assert.deepStrictEqual(await waitingRequestAborted(error), {
      waitingAborted: destroyed,
      errors: ["ESOCKETFAILED"],
    });
  });

  test("client: a request that waits for a stream slot reads the state when the socket closes", async () => {
    assert.deepStrictEqual(await waitingRequestAborted(undefined), {
      // node's nghttp2 holds the request, and node cancels it while the session has its handle.
      // Bun holds the request itself, and cancels it after it dropped the parser.
      waitingAborted: isBun ? { ...ready, state: "empty" } : ready,
      errors: [],
    });
  });

  // The socket of a server session is connected in each of these, so node sets the session up in its constructor.
  const acceptPaths: Record<string, () => { h2: http2.Http2Server | http2.Http2SecureServer; front: net.Server }> = {
    "http2.createServer()": () => {
      const h2 = http2.createServer();
      return { h2, front: h2 };
    },
    "http2.createSecureServer()": () => {
      const h2 = http2.createSecureServer(TLS);
      return { h2, front: h2 };
    },
    "a net.Server that forwards with emit('connection')": () => {
      const h2 = http2.createSecureServer(TLS);
      return { h2, front: net.createServer(socket => h2.emit("connection", socket)) };
    },
    "a tls.Server that forwards with emit('secureConnection')": () => {
      const h2 = http2.createSecureServer(TLS);
      return { h2, front: tls.createServer(TLS, socket => h2.emit("secureConnection", socket)) };
    },
  };
  for (const [name, accept] of Object.entries(acceptPaths)) {
    test(`server: ready at the 'session' event, ${name}`, async () => {
      const { h2, front } = accept();
      const seen: Record<string, unknown> = {};
      h2.on("sessionError", () => {});
      h2.on("session", (session: http2.ServerHttp2Session) => {
        seen.sessionEvent = lifecycle(session);
        session.once("connect", () => {
          seen.connectEvent = lifecycle(session);
        });
      });
      h2.on("stream", (stream: http2.ServerHttp2Stream) => {
        stream.on("error", () => {});
        seen.streamEvent = {
          ...lifecycle(stream.session as http2.ServerHttp2Session),
          remoteSettingsCount: Object.keys(stream.session!.remoteSettings).length > 0 ? "some" : "none",
          pushAllowed: stream.pushAllowed,
        };
        stream.respond({ ":status": 200 }, { endStream: true });
      });
      const port = await listen(front);
      const scheme = name === "http2.createServer()" ? "http" : "https";
      const client = http2.connect(`${scheme}://127.0.0.1:${port}`, { rejectUnauthorized: false });
      const errors = errorCodes(client);
      try {
        const [headers] = await once(client.request({ ":path": "/" }), "response");
        assert.deepStrictEqual(
          { ...seen, status: headers[":status"], errors },
          {
            sessionEvent: ready,
            connectEvent: ready,
            streamEvent: { ...ready, remoteSettingsCount: "some", pushAllowed: true },
            status: 200,
            errors: [],
          },
        );
      } finally {
        client.destroy();
        front.close();
      }
    });
  }

  test("server: connecting until its socket connects", async () => {
    // Accepts the connection and never speaks HTTP/2.
    const sink = net.createServer(socket => socket.resume());
    const port = await listen(sink);
    const socket = net.connect(port, "127.0.0.1");
    const session = http2.performServerHandshake(socket);
    const errors = errorCodes(session);
    try {
      const seen: Record<string, unknown> = { afterHandshakeCall: lifecycle(session) };
      socket.once("connect", () => {
        seen.socketConnected = lifecycle(session);
      });
      await once(session, "connect");
      seen.connectEvent = lifecycle(session);
      assert.deepStrictEqual(
        { ...seen, errors },
        { afterHandshakeCall: connecting, socketConnected: ready, connectEvent: ready, errors: [] },
      );
    } finally {
      session.destroy();
      sink.close();
    }
  });

  test("server: connecting through the handshake of a tls.connect() socket", async () => {
    // Completes the handshake and never speaks HTTP/2.
    const sink = tls.createServer(TLS, socket => {
      socket.on("error", () => {});
      socket.resume();
    });
    const port = await listen(sink);
    const socket = tls.connect({ host: "127.0.0.1", port, ALPNProtocols: ["h2"], rejectUnauthorized: false });
    const session = http2.performServerHandshake(socket);
    const errors = errorCodes(session);
    try {
      const seen: Record<string, unknown> = { afterHandshakeCall: lifecycle(session) };
      socket.once("connect", () => {
        seen.tcpConnected = lifecycle(session);
      });
      socket.once("secureConnect", () => {
        seen.handshakeDone = lifecycle(session);
      });
      await once(session, "connect");
      seen.connectEvent = lifecycle(session);
      assert.deepStrictEqual(
        { ...seen, errors },
        {
          afterHandshakeCall: connecting,
          tcpConnected: connecting,
          handshakeDone: ready,
          connectEvent: ready,
          errors: [],
        },
      );
    } finally {
      session.destroy();
      sink.close();
    }
  });

  // node waits for 'secureConnect', and a server-side TLSSocket emits none: node never sets this
  // session up and never answers the request. Bun serves it.
  test("server: ready when a server-side TLSSocket finishes its handshake", { skip: !isBun }, async () => {
    const seen: Record<string, unknown> = {};
    const front = net.createServer(rawSocket => {
      const socket = new tls.TLSSocket(rawSocket, { isServer: true, ...TLS });
      const session = http2.performServerHandshake(socket);
      seen.serverErrors = errorCodes(session);
      seen.afterHandshakeCall = lifecycle(session);
      session.once("connect", () => {
        seen.connectEvent = lifecycle(session);
      });
      session.on("stream", stream => {
        stream.on("error", () => {});
        seen.streamEvent = { ...lifecycle(session), pushAllowed: stream.pushAllowed };
        stream.respond({ ":status": 200 }, { endStream: true });
      });
    });
    const port = await listen(front);
    const client = http2.connect(`https://127.0.0.1:${port}`, { rejectUnauthorized: false });
    const errors = errorCodes(client);
    try {
      const [headers] = await once(client.request({ ":path": "/" }), "response");
      assert.deepStrictEqual(
        { ...seen, status: headers[":status"], errors },
        {
          serverErrors: [],
          afterHandshakeCall: connecting,
          connectEvent: ready,
          streamEvent: { ...ready, pushAllowed: true },
          status: 200,
          errors: [],
        },
      );
    } finally {
      client.destroy();
      front.close();
    }
  });

  test("server: destroyed before its socket connects", async () => {
    const sink = net.createServer(socket => socket.resume());
    const port = await listen(sink);
    const socket = net.connect(port, "127.0.0.1");
    const session = http2.performServerHandshake(socket);
    const errors = errorCodes(session);
    const events: string[] = [];
    session.on("connect", () => events.push("connect"));
    session.on("close", () => events.push("close"));
    try {
      const socketClosed = once(socket, "close");
      session.destroy();
      const seen: Record<string, unknown> = { afterDestroyCall: lifecycle(session) };
      await socketClosed;
      await immediate();
      seen.afterSocketClose = lifecycle(session);
      assert.deepStrictEqual(
        { ...seen, events, errors },
        {
          afterDestroyCall: destroyedBeforeReady,
          afterSocketClose: destroyedBeforeReady,
          // node holds the session's 'close' until the socket closed, so its 'connect' comes first.
          // Bun emits 'close' one tick after destroy(), and drops the 'connect' that would follow it.
          events: isBun ? ["close"] : ["connect", "close"],
          errors: [],
        },
      );
    } finally {
      session.destroy();
      socket.destroy();
      sink.close();
    }
  });
});

if (typeof Bun !== "undefined") {
  const node = Bun.which("node");
  // Alpine's node segfaults at a random point of an http2 test file (see node-http2-client-close.test.ts),
  // and the CI runner fails a file for any new core dump.
  const isMusl =
    process.platform === "linux" &&
    !(process.report.getReport() as { header: { glibcVersionRuntime?: string } }).header.glibcVersionRuntime;
  describe("Node.js compatibility", () => {
    test("tests should run on node.js", { skip: !node || isMusl }, async () => {
      await using proc = Bun.spawn({
        cmd: [node as string, "--test", import.meta.filename],
        stdout: "inherit",
        stderr: "inherit",
        stdin: "ignore",
      });
      assert.strictEqual(await proc.exited, 0);
    });
  });
}
