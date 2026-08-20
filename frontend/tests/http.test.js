import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { fetchJSON, fetchJSONWithInit, readHeaderText } from "../modules/http.js";

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

describe("readHeaderText", () => {
  // Mirrors sanitize_header_value() in backend/routes/binary_response_utils.py:
  // percent-encode everything outside printable ASCII, plus `%` itself.
  function encodeLikeBackend(value) {
    return [...value]
      .map((ch) => {
        const code = ch.codePointAt(0);
        return code >= 0x20 && code <= 0x7e && ch !== "%" ? ch : encodeURIComponent(ch);
      })
      .join("");
  }

  const headers = (value) => ({ get: (name) => (name === "X-Remote-Display" ? value : null) });

  it("returns plain ASCII labels unchanged", () => {
    expect(readHeaderText(headers("Remote stream (default) S1 Img2"), "X-Remote-Display")).toBe(
      "Remote stream (default) S1 Img2",
    );
  });

  it("round-trips text the backend had to encode to send at all", () => {
    for (const original of ["結晶 α-helix ~2.1 Å", "a\r\nX-Evil: 1", "100% sure", "a\tb"]) {
      expect(readHeaderText(headers(encodeLikeBackend(original)), "X-Remote-Display")).toBe(
        original,
      );
    }
  });

  it("returns an empty string for a missing or empty header", () => {
    expect(readHeaderText(headers(""), "X-Remote-Display")).toBe("");
    expect(readHeaderText(headers("x"), "X-Absent")).toBe("");
    expect(readHeaderText(null, "X-Remote-Display")).toBe("");
  });

  it("falls back to the raw value rather than throwing on a malformed escape", () => {
    // decodeURIComponent throws a URIError here; losing the frame over a
    // cosmetic label would be the wrong trade.
    expect(readHeaderText(headers("broken%E0%A4"), "X-Remote-Display")).toBe("broken%E0%A4");
  });
});
