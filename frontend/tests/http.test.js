import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { fetchJSON, fetchJSONWithInit } from "../modules/http.js";

// Reject like a real fetch() does when its AbortSignal fires.
function abortableFetch() {
  return vi.fn(
    (_url, init) =>
      new Promise((_resolve, reject) => {
        init.signal.addEventListener("abort", () => {
          const err = new Error("aborted");
          err.name = "AbortError";
          reject(err);
        });
      }),
  );
}

describe("http", () => {
  beforeEach(() => {
    // friendlyHttpMessage() calls t(); no dictionary is loaded in this suite,
    // so it falls back to the key name and warns once. Silence that noise.
    vi.spyOn(console, "warn").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
    delete globalThis.fetch;
  });

  it("returns parsed JSON on success", async () => {
    globalThis.fetch = vi.fn(async () => ({ ok: true, json: async () => ({ value: 1 }) }));
    await expect(fetchJSON("/x")).resolves.toEqual({ value: 1 });
  });

  it("maps an HTTP error status and server detail onto the thrown error", async () => {
    globalThis.fetch = vi.fn(async () => ({
      ok: false,
      status: 404,
      json: async () => ({ detail: "missing file" }),
    }));
    await expect(fetchJSON("/x")).rejects.toMatchObject({ status: 404, detail: "missing file" });
  });

  it("maps a network-level failure to status 0 and preserves the cause", async () => {
    const cause = new TypeError("Failed to fetch");
    globalThis.fetch = vi.fn(async () => {
      throw cause;
    });
    await expect(fetchJSON("/x")).rejects.toMatchObject({ status: 0, cause });
  });

  it("aborts and reports a timeout (408) once timeoutMs elapses", async () => {
    vi.useFakeTimers();
    globalThis.fetch = abortableFetch();
    const result = fetchJSON("/x", { timeoutMs: 1000 }).catch((err) => err);
    await vi.advanceTimersByTimeAsync(1000);
    const err = await result;
    expect(err.status).toBe(408);
  });

  it("does not abort before timeoutMs elapses", async () => {
    vi.useFakeTimers();
    let resolveFetch;
    globalThis.fetch = vi.fn(
      () =>
        new Promise((resolve) => {
          resolveFetch = resolve;
        }),
    );
    const result = fetchJSONWithInit("/x", { timeoutMs: 5000 }).catch((err) => err);
    await vi.advanceTimersByTimeAsync(4000);
    resolveFetch({ ok: true, json: async () => ({ ok: true }) });
    await expect(result).resolves.toEqual({ ok: true });
  });

  it("re-throws a caller-initiated cancel as an AbortError, not a timeout", async () => {
    const controller = new AbortController();
    globalThis.fetch = abortableFetch();
    const result = fetchJSONWithInit("/x", {
      timeoutMs: 60000,
      signal: controller.signal,
    }).catch((err) => err);
    controller.abort();
    const err = await result;
    expect(err.name).toBe("AbortError");
    expect(err.status).toBeUndefined();
  });

  it("does not attach a signal when no timeout or caller signal is given", async () => {
    const fetchMock = vi.fn(async () => ({ ok: true, json: async () => ({}) }));
    globalThis.fetch = fetchMock;
    await fetchJSON("/x");
    const init = fetchMock.mock.calls[0][1] || {};
    expect(init.signal).toBeUndefined();
  });
});
