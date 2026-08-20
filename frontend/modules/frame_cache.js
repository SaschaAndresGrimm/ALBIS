/**
 * Bounded LRU cache for decoded frames.
 *
 * Re-fetching a frame the user has already seen is the most wasteful thing the
 * viewer does over a remote link: stepping back one frame costs a full transfer
 * of bytes that were in memory moments ago. Keeping recent frames around makes
 * revisiting them free.
 *
 * The budget is in bytes rather than in frames, deliberately. A frame is 4.4 MB
 * on an EIGER 1M and around 18 MB on a 4M detector, so a fixed frame count would
 * mean wildly different memory use per instrument — and a tab running out of
 * memory mid-experiment is a far worse outcome than a cache miss.
 *
 * Only raw payloads are stored. The Float32 copy the WebGL path derives in
 * applyFrame() is intentionally not cached: it would double the cost of every
 * entry to save work that is cheap to redo.
 */

export function createFrameCache({ getMaxBytes }) {
  // Map iteration order is insertion order, which is what makes this an LRU: a
  // re-inserted key moves to the end, so the first key is always the least
  // recently used one.
  const entries = new Map();
  let bytes = 0;
  let hits = 0;
  let misses = 0;
  let evictions = 0;

  function budget() {
    const value = Number(getMaxBytes?.() ?? 0);
    return Number.isFinite(value) && value > 0 ? value : 0;
  }

  function clear() {
    entries.clear();
    bytes = 0;
  }

  function evictDownTo(target) {
    for (const key of entries.keys()) {
      if (bytes <= target) return;
      bytes -= entries.get(key).bytes;
      entries.delete(key);
      evictions += 1;
    }
  }

  function get(key) {
    const limit = budget();
    if (!limit) {
      // Disabled, possibly just now: release anything still held.
      if (entries.size) clear();
      return null;
    }
    if (!key) return null;
    const entry = entries.get(key);
    if (!entry) {
      misses += 1;
      return null;
    }
    entries.delete(key);
    entries.set(key, entry);
    hits += 1;
    return entry;
  }

  function set(key, { data, shape, dtype } = {}) {
    const limit = budget();
    if (!limit) {
      if (entries.size) clear();
      return false;
    }
    if (!key || !data) return false;
    const size = Number(data.byteLength) || 0;
    // A frame too large to fit on its own would evict everything and still
    // overflow, so it is never stored.
    if (!size || size > limit) return false;

    const existing = entries.get(key);
    if (existing) {
      bytes -= existing.bytes;
      entries.delete(key);
    }
    evictDownTo(limit - size);
    entries.set(key, { data, shape, dtype, bytes: size });
    bytes += size;
    return true;
  }

  function stats() {
    return { count: entries.size, bytes, maxBytes: budget(), hits, misses, evictions };
  }

  return { get, set, clear, stats };
}
