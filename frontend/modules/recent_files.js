/**
 * The files this viewer opened last, most recent first.
 *
 * The interface remembered panel widths, section states, autoload settings and
 * SIMPLON hosts, but not which data anyone had looked at -- so reopening
 * yesterday's dataset meant navigating the file browser to it again, every
 * morning. For a viewer somebody sits in front of all day that is the smallest
 * change with the most visible payoff.
 *
 * Only explicit opens are recorded. A watched folder that loads a new file every
 * second would otherwise fill the list with frames nobody chose, and a live
 * stream has no file to record at all.
 */

const DEFAULT_STORAGE_KEY = "albis.recentFiles";
const DEFAULT_LIMIT = 10;

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
    return true;
  } catch {
    return false;
  }
}

function parseEntries(raw, limit) {
  if (!raw) return [];
  let parsed = null;
  try {
    parsed = JSON.parse(raw);
  } catch {
    // Written by a different version, or truncated: an unreadable list is an
    // empty list, never a broken menu.
    return [];
  }
  if (!Array.isArray(parsed)) return [];
  const seen = new Set();
  const entries = [];
  for (const item of parsed) {
    const path = typeof item === "string" ? item : String(item?.path || "");
    const trimmed = path.trim();
    if (!trimmed || seen.has(trimmed)) continue;
    seen.add(trimmed);
    entries.push(trimmed);
    if (entries.length >= limit) break;
  }
  return entries;
}

export function createRecentFiles({
  storageKey = DEFAULT_STORAGE_KEY,
  limit = DEFAULT_LIMIT,
} = {}) {
  const cap = Math.max(1, Number(limit) || DEFAULT_LIMIT);

  function list() {
    return parseEntries(readStored(storageKey), cap);
  }

  function persist(entries) {
    return writeStored(storageKey, JSON.stringify(entries.slice(0, cap)));
  }

  /** Move `path` to the front, keeping the list unique and capped. */
  function record(path) {
    const trimmed = String(path || "").trim();
    if (!trimmed) return list();
    const next = [trimmed, ...list().filter((entry) => entry !== trimmed)].slice(0, cap);
    persist(next);
    return next;
  }

  function remove(path) {
    const trimmed = String(path || "").trim();
    const next = list().filter((entry) => entry !== trimmed);
    persist(next);
    return next;
  }

  function clear() {
    persist([]);
    return [];
  }

  return { list, record, remove, clear, limit: cap, storageKey };
}
