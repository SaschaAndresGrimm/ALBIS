import { describe, expect, it } from "vitest";

import { createFrameCache } from "../modules/frame_cache.js";

const KB = 1024;

function payload(bytes) {
  return { data: new Uint8Array(bytes), shape: [1, bytes], dtype: "<u1" };
}

function cacheWith(maxBytes) {
  let budget = maxBytes;
  const cache = createFrameCache({ getMaxBytes: () => budget });
  return { cache, setBudget: (value) => { budget = value; } };
}

describe("frame cache", () => {
  it("returns a stored frame with its shape and dtype intact", () => {
    const { cache } = cacheWith(10 * KB);
    const frame = { data: new Uint16Array(64), shape: [8, 8], dtype: "<u2" };

    cache.set("a", frame);
    const hit = cache.get("a");

    expect(hit.data).toBe(frame.data);
    expect(hit.shape).toEqual([8, 8]);
    expect(hit.dtype).toBe("<u2");
  });

  it("misses on an unknown key", () => {
    const { cache } = cacheWith(10 * KB);
    expect(cache.get("nope")).toBeNull();
  });

  it("evicts the least recently used frame, not the oldest stored", () => {
    const { cache } = cacheWith(3 * KB);
    cache.set("a", payload(KB));
    cache.set("b", payload(KB));
    cache.set("c", payload(KB));

    cache.get("a"); // promotes a, leaving b as least recently used
    cache.set("d", payload(KB));

    expect(cache.get("b")).toBeNull();
    expect(cache.get("a")).not.toBeNull();
    expect(cache.get("c")).not.toBeNull();
    expect(cache.get("d")).not.toBeNull();
  });

  it("keeps total memory within the budget", () => {
    const { cache } = cacheWith(4 * KB);

    for (let index = 0; index < 50; index += 1) {
      cache.set(`frame-${index}`, payload(KB));
      expect(cache.stats().bytes).toBeLessThanOrEqual(4 * KB);
    }

    expect(cache.stats().count).toBe(4);
  });

  it("budgets by memory rather than frame count", () => {
    // The reason the budget is in bytes: the same setting has to behave sanely
    // for a 4 MB EIGER 1M frame and an 18 MB 4M frame.
    const small = cacheWith(8 * KB);
    const large = cacheWith(8 * KB);

    for (let index = 0; index < 20; index += 1) {
      small.cache.set(`s${index}`, payload(KB));
      large.cache.set(`l${index}`, payload(4 * KB));
    }

    expect(small.cache.stats().count).toBe(8);
    expect(large.cache.stats().count).toBe(2);
    expect(small.cache.stats().bytes).toBe(large.cache.stats().bytes);
  });

  it("refuses a frame larger than the whole budget instead of thrashing", () => {
    const { cache } = cacheWith(2 * KB);
    cache.set("keep", payload(KB));

    expect(cache.set("huge", payload(8 * KB))).toBe(false);
    expect(cache.get("huge")).toBeNull();
    // The oversized frame must not have flushed what was already there.
    expect(cache.get("keep")).not.toBeNull();
  });

  it("replaces an existing key without double-counting its memory", () => {
    const { cache } = cacheWith(10 * KB);
    cache.set("a", payload(KB));
    cache.set("a", payload(2 * KB));

    expect(cache.stats().count).toBe(1);
    expect(cache.stats().bytes).toBe(2 * KB);
  });

  it("stores nothing and releases everything when disabled", () => {
    const { cache, setBudget } = cacheWith(10 * KB);
    cache.set("a", payload(KB));
    expect(cache.stats().count).toBe(1);

    setBudget(0);

    expect(cache.get("a")).toBeNull();
    expect(cache.set("b", payload(KB))).toBe(false);
    expect(cache.stats().count).toBe(0);
    expect(cache.stats().bytes).toBe(0);
  });

  it("shrinks to fit when the budget is lowered", () => {
    const { cache, setBudget } = cacheWith(8 * KB);
    for (let index = 0; index < 8; index += 1) cache.set(`f${index}`, payload(KB));
    expect(cache.stats().count).toBe(8);

    setBudget(2 * KB);
    cache.set("new", payload(KB));

    expect(cache.stats().bytes).toBeLessThanOrEqual(2 * KB);
  });

  it("releases everything on clear", () => {
    const { cache } = cacheWith(10 * KB);
    cache.set("a", payload(KB));
    cache.set("b", payload(KB));

    cache.clear();

    expect(cache.stats().count).toBe(0);
    expect(cache.stats().bytes).toBe(0);
    expect(cache.get("a")).toBeNull();
  });

  it("ignores empty keys and empty payloads", () => {
    const { cache } = cacheWith(10 * KB);

    expect(cache.set("", payload(KB))).toBe(false);
    expect(cache.set("a", {})).toBe(false);
    expect(cache.set("a", payload(0))).toBe(false);
    expect(cache.get("")).toBeNull();
    expect(cache.stats().count).toBe(0);
  });

  it("tracks hits and misses for diagnosis", () => {
    const { cache } = cacheWith(10 * KB);
    cache.set("a", payload(KB));

    cache.get("a");
    cache.get("a");
    cache.get("b");

    const stats = cache.stats();
    expect(stats.hits).toBe(2);
    expect(stats.misses).toBe(1);
  });
});
