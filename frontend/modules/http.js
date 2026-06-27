import { t } from "./i18n.js";

export const API = "/api";

// Map an HTTP status (or 0 for a network-level failure) to a friendly,
// localized message. The server-provided `detail`, when present, is appended
// so support and power users still see the underlying cause.
function friendlyHttpMessage(status, detail) {
  let base;
  if (status === 0) base = t("http.error.network");
  else if (status === 401 || status === 403) base = t("http.error.forbidden");
  else if (status === 404) base = t("http.error.not_found");
  else if (status === 408 || status === 504) base = t("http.error.timeout");
  else if (status >= 500) base = t("http.error.server");
  else if (status >= 400) base = t("http.error.client");
  else base = t("http.error.generic");
  return detail ? `${base} (${detail})` : base;
}

function httpError(status, detail) {
  const err = new Error(friendlyHttpMessage(status, detail));
  err.status = status;
  if (detail) err.detail = detail;
  return err;
}

async function readDetail(res) {
  try {
    const body = await res.json();
    return body?.detail ? String(body.detail) : "";
  } catch {
    // Body is missing or not JSON; fall back to the status-based message.
    return "";
  }
}

// `init.timeoutMs` (opt-in, non-standard) aborts the request after the given
// duration and surfaces it as a localized timeout. It is applied per call site
// rather than globally so legitimately slow operations (large-file scans, job
// starts) are never false-aborted. A caller-supplied `init.signal` is honored
// and forwarded; a caller cancel rethrows as AbortError, a timeout does not.
async function request(url, init = {}) {
  const { timeoutMs, signal: callerSignal, ...fetchInit } = init || {};

  let timer = null;
  let timedOut = false;
  let onCallerAbort = null;
  let cleanupCallerAbort = null;

  if (timeoutMs && timeoutMs > 0) {
    const controller = new AbortController();
    timer = setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, timeoutMs);
    if (callerSignal) {
      if (callerSignal.aborted) {
        controller.abort();
      } else {
        onCallerAbort = () => controller.abort();
        callerSignal.addEventListener("abort", onCallerAbort, { once: true });
        cleanupCallerAbort = () => callerSignal.removeEventListener("abort", onCallerAbort);
      }
    }
    fetchInit.signal = controller.signal;
  } else if (callerSignal) {
    fetchInit.signal = callerSignal;
  }

  let res;
  try {
    res = await fetch(url, fetchInit);
  } catch (cause) {
    if (timedOut) {
      throw httpError(408, "");
    }
    // A caller-initiated cancel should stay an AbortError so callers can detect it.
    if (cause?.name === "AbortError") {
      throw cause;
    }
    // fetch() rejects on network-level failures (server down, DNS, offline).
    const err = httpError(0, "");
    err.cause = cause;
    throw err;
  } finally {
    if (timer) clearTimeout(timer);
    if (cleanupCallerAbort) cleanupCallerAbort();
  }
  if (!res.ok) {
    throw httpError(res.status, await readDetail(res));
  }
  return res.json();
}

// Plain pass-throughs (not async) so they add no extra microtask hop over the
// single await inside request() — callers and tests see the original timing.
// Both accept an optional init/options object that may carry `timeoutMs`.
export function fetchJSON(url, opts) {
  return request(url, opts);
}

export function fetchJSONWithInit(url, init) {
  return request(url, init);
}
