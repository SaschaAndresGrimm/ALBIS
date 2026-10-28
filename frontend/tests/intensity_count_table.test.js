import { describe, expect, it } from "vitest";

import { computeAutoLevels, computeHistogram, computeStats } from "../modules/intensity_scale_utils.js";

// Integer frames take the count-table path; the same values as Float64Array
// take the per-pixel one. They must agree exactly.
function rand(seed) {
  let s = seed;
  return () => ((s = (Math.imul(s, 1664525) + 1013904223) >>> 0) / 2 ** 32);
}

const CASES = [
  ["<u2", Uint16Array, 65535],
  ["<u4", Uint32Array, 4294967295],
  ["<i4", Int32Array, 2147483647],
  ["<u1", Uint8Array, 255],
  ["<i2", Int16Array, 32767],
];

function frames() {
  const out = [];
  let seed = 3;
  for (const [dtype, Arr, top] of CASES) {
    for (const kind of ["counts", "saturated", "only-saturated", "negatives", "sparse", "empty", "wide"]) {
      const r = rand((seed += 7));
      const n = kind === "empty" ? 0 : 1 + Math.floor(r() * 5000);
      const data = new Arr(n);
      for (let i = 0; i < n; i += 1) {
        if (kind === "only-saturated") data[i] = top;
        else if (kind === "sparse") data[i] = r() < 0.98 ? 0 : Math.floor(r() * 50);
        else if (kind === "wide") data[i] = Math.floor(r() * Math.min(top, 3_000_000));
        else data[i] = 5 + Math.floor(r() * Math.min(top - 5, 400));
      }
      if (kind === "saturated") for (let i = 0; i < n; i += 37) data[i] = top;
      if (kind === "negatives" && Arr.name.startsWith("Int")) for (let i = 0; i < n; i += 11) data[i] = -1 - (i % 2);
      out.push({ label: `${dtype} ${kind}`, dtype, data, reference: Float64Array.from(data) });
    }
  }
  return out;
}

describe("statistics from a count per value", () => {
  it.each(frames())("match the per-pixel path exactly: $label", ({ dtype, data, reference }) => {
    for (const logX of [false, true]) {
      const fast = computeStats(data, dtype, logX);
      const slow = computeStats(reference, dtype, logX);
      expect({ ...fast, hist: Array.from(fast.hist) }).toEqual({ ...slow, hist: Array.from(slow.hist) });
      if (slow.bins) {
        const a = computeHistogram(data, slow.min, slow.max, slow.satMax, 333, logX);
        const b = computeHistogram(reference, slow.min, slow.max, slow.satMax, 333, logX);
        expect(Array.from(a)).toEqual(Array.from(b));
      }
    }
    const stats = computeStats(reference, dtype, false);
    for (const satMax of [stats.satMax, null]) {
      expect(computeAutoLevels(data, satMax, stats, dtype)).toEqual(computeAutoLevels(reference, satMax, stats, dtype));
    }
  });
});
