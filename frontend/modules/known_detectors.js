/**
 * The detectors this ALBIS server has talked to, offered back on every
 * SIMPLON address field (the Detector tab and the Live source).
 *
 * A lab has a handful of detectors at fixed addresses, and an IP says nothing
 * about which detector it is. So a working address is remembered with what
 * the detector says it is -- model and serial -- without a save step, and a
 * detector can be kept (starred) and given a name of its own. Kept ones stay;
 * the others are the last few used.
 *
 * The list belongs to the server (/api/known-detectors, a file beside its
 * other settings), so it is the same for everyone using it and does not depend
 * on the browser. A change shows at once and is sent; the server's answer, the
 * whole list, replaces it -- or, if it could not be saved, the list goes back.
 * Changes by others arrive when the list is read again: at start, when it is
 * opened, when the window gets the focus.
 *
 * Entries are unique by address. The serial follows a detector: found at a
 * new address (DHCP, moved to another room), its entry moves with it rather
 * than appearing twice; a different detector answering at a known address
 * takes that entry over, without the old one's name. The rules here mirror
 * the server's (backend/services/known_detectors.py), for the instant view.
 */

import { normalizeSimplonBaseUrl } from "./simplon_url_utils.js";
import { fetchJSONWithInit } from "./http.js";

export const MAX_RECENT_DETECTORS = 8;
export const MAX_KEPT_DETECTORS = 50;
const NAME_LIMIT = 80;

function text(value, limit = 200) {
  return typeof value === "string" ? value.trim().slice(0, limit) : "";
}

/** "http://192.168.20.191" -> "192.168.20.191": the address as people type it. */
export function shortAddress(address) {
  return String(address || "")
    .replace(/^http:\/\//i, "")
    .replace(/\/+$/, "");
}

/** What a detector is called in a list: its own name, its model, or its address. */
export function detectorLabel(entry) {
  return entry?.name || entry?.model || shortAddress(entry?.address);
}

/**
 * Normalize a stored list: well-formed entries only, one per address, kept
 * ones first, then the most recently used.
 *
 * @param {unknown} value Raw value from storage.
 * @returns {object[]} Clean entries.
 */
export function sanitizeDetectors(value) {
  if (!Array.isArray(value)) return [];
  const seen = new Set();
  const out = [];
  for (const item of value) {
    // Objects only: coercing would turn corrupted storage into entries like
    // "[object Object]" and offer them as addresses.
    if (!item || typeof item !== "object") continue;
    const address = normalizeSimplonBaseUrl(text(item.address));
    if (!address || seen.has(address)) continue;
    seen.add(address);
    out.push({
      address,
      name: text(item.name, NAME_LIMIT),
      model: text(item.model),
      serial: text(item.serial),
      kept: item.kept === true,
      lastUsed: Number.isFinite(item.lastUsed) ? item.lastUsed : 0,
    });
  }
  return trim(out);
}

// Every kept entry (up to a sane cap), and the most recent of the others.
function trim(entries) {
  const byRecent = [...entries].sort((a, b) => b.lastUsed - a.lastUsed);
  const kept = byRecent.filter((entry) => entry.kept).slice(0, MAX_KEPT_DETECTORS);
  const recent = byRecent.filter((entry) => !entry.kept).slice(0, MAX_RECENT_DETECTORS);
  return [...kept, ...recent];
}

/**
 * Remember that a detector answered at `address`.
 *
 * @param {object[]} entries Current list.
 * @param {{address: string, model?: string, serial?: string}} seen What answered.
 * @param {number} now Timestamp (ms).
 * @returns {object[]} New list (the input is not changed).
 */
export function recordDetector(entries, { address, model, serial } = {}, now = Date.now()) {
  const current = sanitizeDetectors(entries);
  const at = normalizeSimplonBaseUrl(text(address));
  if (!at) return current;
  const id = text(serial);
  const here = current.find((entry) => entry.address === at);
  // The same detector known at another address: it moved, its entry follows.
  const moved = id ? current.find((entry) => entry.serial === id && entry.address !== at) : null;
  let base;
  if (moved) {
    base = { ...moved, kept: moved.kept || Boolean(here?.kept) };
  } else if (here && id && here.serial && here.serial !== id) {
    // Another detector now answers here: the old name was for the old one.
    base = { address: at, name: "", model: "", serial: "", kept: here.kept };
  } else {
    base = here || { address: at, name: "", model: "", serial: "", kept: false };
  }
  const next = {
    ...base,
    address: at,
    model: text(model) || base.model,
    serial: id || base.serial,
    lastUsed: now,
  };
  const others = current.filter((entry) => entry.address !== at && entry !== moved);
  return trim([next, ...others]);
}

/** Change one entry's name or kept flag; an unknown address changes nothing. */
export function updateDetector(entries, address, changes = {}) {
  const at = normalizeSimplonBaseUrl(text(address));
  return sanitizeDetectors(
    sanitizeDetectors(entries).map((entry) => {
      if (entry.address !== at) return entry;
      const next = { ...entry };
      if ("name" in changes) next.name = text(changes.name, NAME_LIMIT);
      if ("kept" in changes) next.kept = Boolean(changes.kept);
      return next;
    }),
  );
}

export function forgetDetector(entries, address) {
  const at = normalizeSimplonBaseUrl(text(address));
  return sanitizeDetectors(entries).filter((entry) => entry.address !== at);
}

/**
 * The list as shown: kept ones by name, then the others, most recent first,
 * narrowed to those whose name, address, model or serial contain `query`.
 */
export function arrangeDetectors(entries, query = "") {
  const words = String(query || "")
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean);
  const matches = (entry) => {
    const haystack = [entry.name, entry.model, entry.serial, shortAddress(entry.address)].join(" ").toLowerCase();
    return words.every((word) => haystack.includes(word));
  };
  const shown = sanitizeDetectors(entries).filter(matches);
  const kept = shown
    .filter((entry) => entry.kept)
    .sort((a, b) => detectorLabel(a).localeCompare(detectorLabel(b), undefined, { numeric: true }));
  const recent = shown.filter((entry) => !entry.kept).sort((a, b) => b.lastUsed - a.lastUsed);
  return { kept, recent };
}

// Where earlier versions kept it, in each browser: handed to the server once.
// 0.27.0 kept the list itself; before, the Live source settings kept the
// addresses alone.
const BROWSER_KEY = "albis.knownDetectors";
const AUTOLOAD_KEY = "albis.autoload";

function readBrowserList() {
  try {
    const raw = window.localStorage.getItem(BROWSER_KEY);
    return raw === null ? null : JSON.parse(raw);
  } catch {
    return null;
  }
}

function readBrowserAddresses() {
  try {
    const hosts = JSON.parse(window.localStorage.getItem(AUTOLOAD_KEY) || "{}")?.simplonRecentHosts;
    return Array.isArray(hosts) ? hosts : [];
  } catch {
    return [];
  }
}

function dropBrowserLists() {
  try {
    window.localStorage.removeItem(BROWSER_KEY);
    const autoload = JSON.parse(window.localStorage.getItem(AUTOLOAD_KEY) || "null");
    if (autoload && typeof autoload === "object" && "simplonRecentHosts" in autoload) {
      delete autoload.simplonRecentHosts;
      window.localStorage.setItem(AUTOLOAD_KEY, JSON.stringify(autoload));
    }
  } catch {
    // Not dropped: offered to the server again next time, which only
    // refreshes what it already has.
  }
}

/**
 * The shared list, as this page knows it. `list()` answers at once from what
 * was last read; every change and every read tells the `subscribe`rs, so each
 * address field and star shows the same list.
 */
export function createKnownDetectors({ apiBase = "/api" } = {}) {
  let cache = [];
  let lastError = "";
  const listeners = new Set();

  function announce() {
    for (const listener of listeners) listener(cache);
  }

  async function call(method, path = "", body = undefined) {
    const payload = await fetchJSONWithInit(`${apiBase}/known-detectors${path}`, {
      method,
      headers: body === undefined ? undefined : { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    return sanitizeDetectors(payload?.detectors);
  }

  // Shown at once, then whatever the server says; if it could not save,
  // back to what was there, and the reason for the list to show.
  async function change(local, method, path, body) {
    const before = cache;
    cache = local(cache);
    announce();
    try {
      cache = await call(method, path, body);
      lastError = "";
    } catch (err) {
      cache = before;
      lastError = String(err?.message || err || "");
    }
    announce();
    return cache;
  }

  return {
    list: () => cache,
    get lastError() {
      return lastError;
    },
    find(address) {
      const at = normalizeSimplonBaseUrl(text(address));
      return cache.find((entry) => entry.address === at) || null;
    },
    /** Read the list again, for changes made by others. */
    async refresh() {
      try {
        const next = await call("GET");
        lastError = "";
        if (JSON.stringify(next) !== JSON.stringify(cache)) {
          cache = next;
          announce();
        }
      } catch {
        // Not reachable now: keep showing what was read last.
      }
      return cache;
    },
    record(seen) {
      const { address, model = "", serial = "" } = seen || {};
      if (!normalizeSimplonBaseUrl(text(address))) return Promise.resolve(cache);
      return change((list) => recordDetector(list, seen), "POST", "/seen", {
        address: String(address),
        model: text(model),
        serial: text(serial),
      });
    },
    rename(address, name) {
      return change((list) => updateDetector(list, address, { name }), "PATCH", "", { address, name });
    },
    setKept(address, kept) {
      return change((list) => updateDetector(list, address, { kept }), "PATCH", "", { address, kept });
    },
    forget(address) {
      return change(
        (list) => forgetDetector(list, address),
        "DELETE",
        `?address=${encodeURIComponent(address)}`,
      );
    },
    /**
     * Hand the lists earlier versions kept in this browser to the server,
     * merged with what it has, then forget them here. Resolves true when
     * there was one and the server took it.
     */
    async adoptBrowserLists() {
      const entries = sanitizeDetectors(readBrowserList());
      const now = Date.now();
      // Most recent first, older ones a little earlier.
      readBrowserAddresses().forEach((address, i) => {
        if (typeof address === "string" && address.trim()) {
          entries.push({ address, name: "", model: "", serial: "", kept: false, lastUsed: now - 1000 * (i + 1) });
        }
      });
      if (!entries.length) {
        dropBrowserLists();
        return false;
      }
      try {
        cache = await call("POST", "/import", { detectors: entries });
        lastError = "";
        announce();
        dropBrowserLists();
        return true;
      } catch {
        // Kept in the browser, offered again next time.
        return false;
      }
    },
    /** At start: take over the browser's old lists, read the list, and read
     *  it again whenever the window comes back into focus. */
    async start() {
      window.addEventListener?.("focus", () => void this.refresh());
      await this.adoptBrowserLists();
      return this.refresh();
    },
    subscribe(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  };
}

export const knownDetectors = createKnownDetectors();
