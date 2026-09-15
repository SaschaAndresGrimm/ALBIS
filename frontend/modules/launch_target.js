/**
 * The file ALBIS was launched to open, read from the URL fragment.
 *
 * When the desktop environment opens a document with ALBIS, the launcher puts
 * the path in the fragment -- `#albis-open=<percent-encoded path>` -- and this
 * reads it back out. A fragment rather than a query string because the browser
 * never sends it to the server, so a local file path stays out of the access
 * log, and because `#albis-clone=` already established the convention.
 */

export const LAUNCH_OPEN_HASH_PREFIX = "albis-open=";

/**
 * The path in `#albis-open=<path>`, or "" if the fragment carries none.
 *
 * The value stops at `&`, so the fragment can hold other keys beside this one.
 * The launcher encodes the whole path with nothing left safe, so a path
 * containing `&`, `#` or a space arrives intact.
 */
export function readLaunchTargetFromHash(hash) {
  const text = String(hash || "").replace(/^#/, "");
  if (!text) return "";
  const match = /(?:^|&)albis-open=([^&]*)/.exec(text);
  if (!match) return "";
  const raw = match[1];
  if (!raw) return "";
  try {
    return decodeURIComponent(raw).trim();
  } catch {
    // A malformed escape is not worth failing startup over; the splash is a
    // better outcome than a broken app, and the launcher logged the path.
    return "";
  }
}
