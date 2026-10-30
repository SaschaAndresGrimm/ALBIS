/**
 * The page's half of ALBIS's log: entries queued and posted to /api/client-log
 * one at a time, so warnings and errors from the browser land in the same log
 * file as the backend's (client_logging_bindings.js routes console.warn,
 * console.error and uncaught errors here).
 *
 * A failed post is dropped, never reported: reporting it would log, and the
 * log would post again.
 */

import { fetchResponse } from "./http.js";

/** One console argument as log text: an Error with its stack, an object as JSON. */
export function formatClientArg(arg) {
  if (arg instanceof Error) {
    return `${arg.message}${arg.stack ? `\n${arg.stack}` : ""}`;
  }
  if (typeof arg === "object") {
    try {
      return JSON.stringify(arg);
    } catch {
      return String(arg);
    }
  }
  return String(arg);
}

export function createClientLog({ apiBase, delayMs = 250 }) {
  const buffer = [];
  let timer = null;
  let sending = false;

  function logClient(level, message, context, extra) {
    if (!message) return;
    buffer.push({
      level,
      message: String(message).slice(0, 2000),
      context,
      extra,
      url: window.location.href,
      userAgent: navigator.userAgent,
    });
    if (!timer) {
      timer = window.setTimeout(flush, delayMs);
    }
  }

  async function flush() {
    timer = null;
    if (sending || buffer.length === 0) return;
    sending = true;
    const entry = buffer.shift();
    try {
      await fetchResponse(`${apiBase}/client-log`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(entry),
      });
    } catch {
      // drop on network errors
    } finally {
      sending = false;
      if (buffer.length) {
        timer = window.setTimeout(flush, delayMs);
      }
    }
  }

  return { logClient };
}
