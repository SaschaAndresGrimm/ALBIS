import { describe, expect, it } from "vitest";
import { presenceUrl, startPresence } from "../modules/presence.js";

function fakeSockets() {
  const sockets = [];
  class FakeSocket {
    constructor(url) {
      this.url = url;
      this.listeners = {};
      this.closed = false;
      sockets.push(this);
    }
    addEventListener(name, fn) {
      this.listeners[name] = fn;
    }
    emit(name) {
      this.listeners[name]?.();
    }
    close() {
      this.closed = true;
    }
  }
  return { sockets, FakeSocket };
}

function fakeTimers() {
  const pending = [];
  return {
    pending,
    setTimer: (fn, ms) => pending.push({ fn, ms }),
    runNext: () => pending.shift().fn(),
  };
}

describe("presence", () => {
  it("connects to /api/presence over ws, or wss on https", () => {
    expect(presenceUrl("/api", new URL("http://127.0.0.1:5000/"))).toBe("ws://127.0.0.1:5000/api/presence");
    expect(presenceUrl("/api", new URL("https://albis.lab/view"))).toBe("wss://albis.lab/api/presence");
  });

  it("opens one connection and reconnects later each time it drops", () => {
    const { sockets, FakeSocket } = fakeSockets();
    const timers = fakeTimers();
    startPresence({ apiBase: "/api", WebSocketImpl: FakeSocket, setTimer: timers.setTimer, location: new URL("http://127.0.0.1/") });
    expect(sockets).toHaveLength(1);

    sockets[0].emit("close");
    expect(timers.pending[0].ms).toBe(1000);
    timers.runNext();
    sockets[1].emit("close");
    expect(timers.pending[0].ms).toBe(2000);
    timers.runNext();
    // Once a connection opens, the next drop retries quickly again.
    sockets[2].emit("open");
    sockets[2].emit("close");
    expect(timers.pending[0].ms).toBe(1000);
  });

  it("waits no longer than 30 s between attempts", () => {
    const { sockets, FakeSocket } = fakeSockets();
    const timers = fakeTimers();
    startPresence({ apiBase: "/api", WebSocketImpl: FakeSocket, setTimer: timers.setTimer, location: new URL("http://127.0.0.1/") });
    for (let i = 0; i < 10; i += 1) {
      sockets.at(-1).emit("close");
      timers.runNext();
    }
    sockets.at(-1).emit("close");
    expect(timers.pending[0].ms).toBe(30000);
  });

  it("closes and stops reconnecting when stopped", () => {
    const { sockets, FakeSocket } = fakeSockets();
    const timers = fakeTimers();
    const presence = startPresence({ apiBase: "/api", WebSocketImpl: FakeSocket, setTimer: timers.setTimer, location: new URL("http://127.0.0.1/") });
    presence.stop();
    expect(sockets[0].closed).toBe(true);
    sockets[0].emit("close");
    expect(timers.pending).toHaveLength(0);
  });
});
