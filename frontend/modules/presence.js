/**
 * Tells the server this tab is open, for ALBIS's automatic stop
 * (backend/lifecycle.py, IdleWatch).
 *
 * One WebSocket per tab, held open and otherwise silent: the server counts
 * open connections, and only starts counting down when there are none. Polling
 * alone could not say this reliably -- macOS App Nap and a browser's own
 * throttling can silence a background tab's timers for a quarter of an hour,
 * and ALBIS stopped under open tabs. Throttling slows a page; it does not close
 * its connections. Closing the tab or the browser does.
 *
 * If the connection drops -- ALBIS restarted, say -- it is opened again, a
 * little later each time, up to every 30 s.
 */

const FIRST_RETRY_MS = 1000;
const MAX_RETRY_MS = 30000;

export function presenceUrl(apiBase, location = window.location) {
  const url = new URL(`${apiBase}/presence`, location.href);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}

export function startPresence({
  apiBase,
  WebSocketImpl = window.WebSocket,
  setTimer = (fn, ms) => window.setTimeout(fn, ms),
  location = window.location,
} = {}) {
  let socket = null;
  let stopped = false;
  let retryMs = FIRST_RETRY_MS;

  function retry() {
    if (stopped) return;
    setTimer(open, retryMs);
    retryMs = Math.min(retryMs * 2, MAX_RETRY_MS);
  }

  function open() {
    if (stopped || typeof WebSocketImpl !== "function") return;
    try {
      socket = new WebSocketImpl(presenceUrl(apiBase, location));
    } catch {
      retry();
      return;
    }
    socket.addEventListener("open", () => {
      retryMs = FIRST_RETRY_MS;
    });
    socket.addEventListener("close", () => {
      socket = null;
      retry();
    });
  }

  open();
  return {
    stop() {
      stopped = true;
      socket?.close();
    },
  };
}
