/**
 * The detectors this viewer has talked to, offered back on every SIMPLON
 * address field (the Detector tab and the Live source).
 *
 * A lab has a handful of detectors at fixed addresses, and an IP says nothing
 * about which detector it is. So a working address is remembered with what
 * the detector says it is -- model and serial -- without a save step, and a
 * detector can be kept (starred) and given a name of its own. Kept ones stay;
 * the others are the last few used.
 *
 * Entries are unique by address. The serial follows a detector: found at a
 * new address (DHCP, moved to another room), its entry moves with it rather
 * than appearing twice; a different detector answering at a known address
 * takes that entry over, without the old one's name.
 */

import { normalizeSimplonBaseUrl } from "./simplon_url_utils.js";

export const MAX_RECENT_DETECTORS = 8;
export const MAX_KEPT_DETECTORS = 50;
const DEFAULT_STORAGE_KEY = "albis.knownDetectors";
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

function readStored(key) {
  try {
    return window.localStorage.getItem(key);
  } catch {
    // Private windows and locked-down browsers throw rather than return null.
    return null;
  }
}

function writeStored(key, value) {
  try {
    window.localStorage.setItem(key, value);
  } catch {
    // Not remembered, but still offered for this session.
  }
}

/**
 * The shared list, kept in browser storage. Every change is announced to
 * `subscribe`rs, so each address field and star shows the same list.
 */
export function createKnownDetectors({ storageKey = DEFAULT_STORAGE_KEY } = {}) {
  let cache = null;
  const listeners = new Set();

  function list() {
    if (!cache) {
      let parsed = [];
      try {
        parsed = JSON.parse(readStored(storageKey) || "[]");
      } catch {
        // Written by a different version, or truncated: an empty list, never
        // a broken field.
        parsed = [];
      }
      cache = sanitizeDetectors(parsed);
    }
    return cache;
  }

  function commit(next) {
    cache = next;
    writeStored(storageKey, JSON.stringify(next));
    for (const listener of listeners) listener(next);
    return next;
  }

  return {
    list,
    find(address) {
      const at = normalizeSimplonBaseUrl(text(address));
      return list().find((entry) => entry.address === at) || null;
    },
    record(seen) {
      return commit(recordDetector(list(), seen));
    },
    rename(address, name) {
      return commit(updateDetector(list(), address, { name }));
    },
    setKept(address, kept) {
      return commit(updateDetector(list(), address, { kept }));
    },
    forget(address) {
      return commit(forgetDetector(list(), address));
    },
    /**
     * Take over the address-only history older versions kept with the Live
     * source settings, once: only while this list has never been written.
     */
    adoptLegacy(addresses) {
      if (readStored(storageKey) !== null || !Array.isArray(addresses)) return;
      const now = Date.now();
      let next = [];
      // Oldest first, so the most recent ends up most recent.
      [...addresses].reverse().forEach((address, i) => {
        if (typeof address === "string") next = recordDetector(next, { address }, now - addresses.length + i);
      });
      if (next.length) commit(next);
    },
    subscribe(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    _reset() {
      cache = null;
      listeners.clear();
    },
  };
}

export const knownDetectors = createKnownDetectors();
