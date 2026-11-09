import { describe, expect, it } from "vitest";

import { clampMinMaxCount, findExtremePixels, normalizeMinMaxMode, parseMinMaxCutoff } from "../modules/minmax_finder_utils.js";

// A 4 x 3 frame, row by row.
const FRAME = Float32Array.from([
  0, 5, 0, 9,
  2, 0, 7, -1,
  9, 1, 0, 3,
]);

const highest = (data, w, h, opts) => findExtremePixels(data, w, h, opts);
const lowest = (data, w, h, opts) => findExtremePixels(data, w, h, { ...opts, lowest: true });

describe("findExtremePixels, highest", () => {
  it("lists the highest values first, with their positions", () => {
    expect(highest(FRAME, 4, 3, { count: 3 }).pixels).toEqual([
      { x: 3, y: 0, index: 3, value: 9 },
      { x: 0, y: 2, index: 8, value: 9 },
      { x: 2, y: 1, index: 6, value: 7 },
    ]);
  });

  it("counts the pixels sharing the highest value", () => {
    expect(highest(FRAME, 4, 3, { count: 1 }).extremeCount).toBe(2);
  });

  it("keeps neighbouring and lone pixels, which the Peak Finder would drop", () => {
    const spike = new Float32Array(25);
    spike[12] = 100; // a lone spike in the middle
    spike[13] = 90; // and its neighbour
    expect(highest(spike, 5, 5, { count: 2 }).pixels.map((p) => p.index)).toEqual([12, 13]);
  });

  it("puts equal values in reading order however many it has to drop", () => {
    const flat = new Uint16Array(100).fill(4);
    const { pixels, extremeCount } = highest(flat, 10, 10, { count: 5 });
    expect(pixels.map((p) => p.index)).toEqual([0, 1, 2, 3, 4]);
    expect(extremeCount).toBe(100);
  });

  it("leaves out what `ignore` rejects, and pixels that are no reading or not above zero", () => {
    const data = Float32Array.from([NaN, Infinity, -3, 0, 6, 8]);
    const found = highest(data, 6, 1, { count: 10, ignore: (index) => index === 5 });
    expect(found.pixels.map((p) => p.index)).toEqual([4]);
  });

  it("returns nothing for a dark frame or without a frame", () => {
    expect(highest(new Uint32Array(12), 4, 3)).toEqual({ pixels: [], extremeCount: 0 });
    expect(highest(null, 4, 3)).toEqual({ pixels: [], extremeCount: 0 });
    expect(highest(FRAME, 0, 0)).toEqual({ pixels: [], extremeCount: 0 });
  });
});

describe("findExtremePixels, lowest", () => {
  it("lists the lowest values first, zero and negative ones included", () => {
    expect(lowest(FRAME, 4, 3, { count: 3 }).pixels.map((p) => [p.index, p.value])).toEqual([
      [7, -1],
      [0, 0],
      [2, 0],
    ]);
  });

  it("says how many pixels share the floor, beyond the ones listed", () => {
    const frame = new Uint32Array(10000);
    frame[5000] = 3;
    const { pixels, extremeCount } = lowest(frame, 100, 100, { count: 20 });
    expect(pixels).toHaveLength(20);
    expect(pixels.every((p) => p.value === 0)).toBe(true);
    expect(extremeCount).toBe(9999);
  });

  it("leaves out non-finite and ignored pixels", () => {
    const data = Float32Array.from([NaN, -Infinity, -2, 4]);
    const { pixels } = lowest(data, 4, 1, { count: 5, ignore: (index) => index === 2 });
    expect(pixels.map((p) => p.index)).toEqual([3]);
  });
});

describe("findExtremePixels against a full sort", () => {
  let seed = 7;
  const random = () => {
    seed = (seed * 1103515245 + 12345) % 2147483648;
    return seed / 2147483648;
  };
  const data = Float32Array.from({ length: 4096 }, () => Math.floor(random() * 500) - 50);
  const all = Array.from(data, (value, index) => ({ value, index }));

  it("finds the same highest pixels", () => {
    const expected = all
      .filter((p) => p.value > 0)
      .sort((a, b) => b.value - a.value || a.index - b.index)
      .slice(0, 50)
      .map((p) => p.index);
    expect(highest(data, 64, 64, { count: 50 }).pixels.map((p) => p.index)).toEqual(expected);
  });

  it("finds the same lowest pixels and counts the floor", () => {
    const sorted = all.slice().sort((a, b) => a.value - b.value || a.index - b.index);
    const { pixels, extremeCount } = lowest(data, 64, 64, { count: 50 });
    expect(pixels.map((p) => p.index)).toEqual(sorted.slice(0, 50).map((p) => p.index));
    expect(extremeCount).toBe(all.filter((p) => p.value === sorted[0].value).length);
  });
});

describe("cutoffs", () => {
  it("skips pixels below the low cutoff at the low end, keeping a pixel equal to it", () => {
    expect(lowest(FRAME, 4, 3, { count: 3, lowCutoff: 1 }).pixels.map((p) => p.value)).toEqual([1, 2, 3]);
  });

  it("counts the pixels sharing the lowest value that survives the cutoff", () => {
    expect(lowest(FRAME, 4, 3, { count: 1, lowCutoff: 1 }).extremeCount).toBe(1);
  });

  it("skips pixels above the high cutoff at the high end", () => {
    expect(highest(FRAME, 4, 3, { count: 2, highCutoff: 7 }).pixels.map((p) => p.value)).toEqual([7, 5]);
  });

  it("applies both cutoffs together, and finds nothing outside them", () => {
    expect(highest(FRAME, 4, 3, { lowCutoff: 2, highCutoff: 3 }).pixels.map((p) => p.value)).toEqual([3, 2]);
    expect(lowest(FRAME, 4, 3, { lowCutoff: 20 })).toEqual({ pixels: [], extremeCount: 0 });
  });

  it("reads a typed cutoff as a number, or none", () => {
    expect(parseMinMaxCutoff("1.5")).toBe(1.5);
    expect(parseMinMaxCutoff("0")).toBe(0);
    expect(parseMinMaxCutoff("")).toBeNull();
    expect(parseMinMaxCutoff("abc")).toBeNull();
    expect(parseMinMaxCutoff(null)).toBeNull();
  });
});

describe("clampMinMaxCount and normalizeMinMaxMode", () => {
  it("rounds and keeps the count between 1 and 1000", () => {
    expect(clampMinMaxCount("12.4")).toBe(12);
    expect(clampMinMaxCount(0)).toBe(1);
    expect(clampMinMaxCount(5000)).toBe(1000);
    expect(clampMinMaxCount("abc", 20)).toBe(20);
  });

  it("knows max, min and both, and falls back to max", () => {
    expect(normalizeMinMaxMode("both")).toBe("both");
    expect(normalizeMinMaxMode("min")).toBe("min");
    expect(normalizeMinMaxMode("sideways")).toBe("max");
  });
});
