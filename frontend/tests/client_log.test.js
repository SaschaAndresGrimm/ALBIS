import { afterEach, describe, expect, it, vi } from "vitest";

import { createClientLog, formatClientArg } from "../modules/client_log.js";

describe("client_log", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    delete globalThis.fetch;
  });

  it("writes an Error with its stack, an object as JSON, anything else as text", () => {
    const err = new Error("boom");
    expect(formatClientArg(err)).toContain("boom\n");
    expect(formatClientArg({ a: 1 })).toBe('{"a":1}');
    expect(formatClientArg(7)).toBe("7");
    const cyclic = {};
    cyclic.self = cyclic;
    expect(formatClientArg(cyclic)).toBe("[object Object]");
  });

  it("posts queued entries one at a time, each message capped at 2000 characters", async () => {
    vi.useFakeTimers();
    globalThis.fetch = vi.fn(async () => ({ ok: true, status: 204 }));
    const { logClient } = createClientLog({ apiBase: "/api" });
    logClient("error", "x".repeat(3000));
    logClient("warning", "second");
    logClient("info", "");
    await vi.advanceTimersByTimeAsync(250);
    expect(globalThis.fetch).toHaveBeenCalledTimes(1);
    const first = JSON.parse(globalThis.fetch.mock.calls[0][1].body);
    expect(first).toMatchObject({ level: "error" });
    expect(first.message).toHaveLength(2000);
    await vi.advanceTimersByTimeAsync(250);
    expect(globalThis.fetch).toHaveBeenCalledTimes(2);
    expect(JSON.parse(globalThis.fetch.mock.calls[1][1].body).message).toBe("second");
  });

  it("drops an entry it cannot post without logging, which would post again", async () => {
    vi.useFakeTimers();
    const error = vi.spyOn(console, "error");
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    globalThis.fetch = vi.fn(async () => {
      throw new TypeError("Failed to fetch");
    });
    const { logClient } = createClientLog({ apiBase: "/api" });
    logClient("error", "lost");
    await vi.advanceTimersByTimeAsync(250);
    expect(globalThis.fetch).toHaveBeenCalledTimes(1);
    expect(error).not.toHaveBeenCalled();
    // http.js may warn about a missing translation; nothing else is logged.
    expect(warn.mock.calls.every(([message]) => String(message).includes("i18n"))).toBe(true);
  });
});
