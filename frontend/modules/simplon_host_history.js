/**
 * Remembered SIMPLON detector addresses.
 *
 * Beamlines switch between a handful of detectors, and retyping an address is
 * both tedious and the moment a typo creeps in. Addresses that have proven
 * themselves — a successful connection test, or a started monitor — are offered
 * back as autocomplete suggestions on the address field.
 */

export const MAX_RECENT_SIMPLON_HOSTS = 8;

/**
 * Normalize a stored list: strings only, trimmed, de-duplicated, capped.
 *
 * @param {unknown} value Raw value from persisted settings.
 * @returns {string[]} Clean list, most recent first.
 */
export function sanitizeRecentSimplonHosts(value) {
  if (!Array.isArray(value)) return [];
  const seen = new Set();
  const out = [];
  for (const entry of value) {
    // Strings only: coercing would turn corrupted storage into entries like
    // "[object Object]" and offer them as addresses.
    const url = typeof entry === "string" ? entry.trim() : "";
    if (!url || seen.has(url)) continue;
    seen.add(url);
    out.push(url);
    if (out.length >= MAX_RECENT_SIMPLON_HOSTS) break;
  }
  return out;
}

/**
 * Move `url` to the front of the history, dropping any duplicate.
 *
 * @param {string[]} hosts Current history.
 * @param {string} url Address that just worked.
 * @returns {string[]} New list, most recent first (input is not mutated).
 */
export function addRecentSimplonHost(hosts, url) {
  const value = typeof url === "string" ? url.trim() : "";
  const current = sanitizeRecentSimplonHosts(hosts);
  if (!value) return current;
  return [value, ...current.filter((entry) => entry !== value)].slice(
    0,
    MAX_RECENT_SIMPLON_HOSTS,
  );
}

/**
 * Record a working address on `state`, returning whether anything changed.
 *
 * Callers persist and re-render only when this reports a change, so a repeated
 * connection test against the same detector costs nothing.
 *
 * @param {object} state Application state (uses `state.autoload`).
 * @param {string} url Address that just worked.
 * @returns {boolean} True when the history changed.
 */
export function recordSimplonHost(state, url) {
  const autoload = state?.autoload;
  if (!autoload) return false;
  const before = sanitizeRecentSimplonHosts(autoload.simplonRecentHosts);
  const after = addRecentSimplonHost(before, url);
  if (after.length === before.length && after.every((entry, i) => entry === before[i])) {
    return false;
  }
  autoload.simplonRecentHosts = after;
  return true;
}

/**
 * Fill a `<datalist>` with the remembered addresses.
 *
 * @param {HTMLDataListElement|null|undefined} element Target datalist.
 * @param {string[]} hosts Addresses to offer.
 */
export function renderSimplonHostOptions(element, hosts) {
  if (!element) return;
  const list = sanitizeRecentSimplonHosts(hosts);
  element.innerHTML = "";
  const doc = element.ownerDocument;
  if (!doc) return;
  for (const url of list) {
    const option = doc.createElement("option");
    option.value = url;
    element.appendChild(option);
  }
}
