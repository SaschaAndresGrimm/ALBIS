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

function httpError(status, detail, data) {
  const err = new Error(friendlyHttpMessage(status, detail));
  err.status = status;
  if (detail) err.detail = detail;
  // The structured half, when the server sent one, so a caller can render the
  // failure in the user's language from `code` instead of showing `message`.
  if (data) err.detailData = data;
  return err;
}

async function readDetail(res) {
  try {
    const body = await res.json();
    const detail = body?.detail;
    // Some endpoints answer with an object rather than a sentence -- the
    // SIMPLON probe and the dataset scan both do. `String()` on one of those
    // yields "[object Object]", which is what the user used to be shown.
    if (detail && typeof detail === "object") {
      return {
        text: typeof detail.message === "string" ? detail.message : "",
        data: detail,
      };
    }
    return { text: detail ? String(detail) : "", data: null };
  } catch {
    // Body is missing or not JSON; fall back to the status-based message.
    return { text: "", data: null };
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
    const { text, data } = await readDetail(res);
    throw httpError(res.status, text, data);
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

// Read a text-valued response header written by the backend.
//
// Free-text header values (a remote-stream display name, a SIMPLON timestamp,
// an HDF5 mask path) are percent-encoded server-side, because a raw one can
// carry characters that are not legal in a header at all — see
// sanitize_header_value() in backend/routes/binary_response_utils.py. This
// reverses that so a non-ASCII sample name displays as itself.
//
// The encoding is designed to always be decodable, but a malformed value must
// never cost us the whole frame: on failure the raw text is returned, which is
// at worst cosmetic.
export function readHeaderText(headers, name) {
  const raw = headers?.get?.(name);
  if (!raw) return "";
  try {
    return decodeURIComponent(raw);
  } catch {
    return raw;
  }
}
