import { describe, expect, it } from "vitest";

import { clampHottestPixelCount, findHottestPixels } from "../modules/hottest_pixels_utils.js";

// A 4 x 3 frame, row by row.
const FRAME = Uint32Array.from([
  0, 5, 0, 9,
  2, 0, 7, 0,
  9, 1, 0, 3,
]);

describe("findHottestPixels", () => {
  it("lists the highest values first, with their positions", () => {
    expect(findHottestPixels(FRAME, 4, 3, { count: 3 })).toEqual([
      { x: 3, y: 0, index: 3, value: 9 },
      { x: 0, y: 2, index: 8, value: 9 },
      { x: 2, y: 1, index: 6, value: 7 },
    ]);
  });

  it("keeps neighbouring and lone pixels, which the Peak Finder would drop", () => {
    const spike = new Float32Array(25);
    spike[12] = 100; // a lone spike in the middle
    spike[13] = 90; // and its neighbour
    expect(findHottestPixels(spike, 5, 5, { count: 2 }).map((p) => p.index)).toEqual([12, 13]);
  });

  it("puts equal values in reading order however many it has to drop", () => {
    const flat = new Uint16Array(100).fill(4);
    expect(findHottestPixels(flat, 10, 10, { count: 5 }).map((p) => p.index)).toEqual([0, 1, 2, 3, 4]);
  });

  it("finds the same pixels as sorting the whole frame", () => {
    let seed = 7;
    const random = () => {
      seed = (seed * 1103515245 + 12345) % 2147483648;
      return seed / 2147483648;
    };
    const data = Float32Array.from({ length: 4096 }, () => Math.floor(random() * 500));
    const expected = Array.from(data, (value, index) => ({ value, index }))
      .filter((p) => p.value > 0)
      .sort((a, b) => b.value - a.value || a.index - b.index)
      .slice(0, 50)
      .map((p) => p.index);
    expect(findHottestPixels(data, 64, 64, { count: 50 }).map((p) => p.index)).toEqual(expected);
  });

  it("leaves out what `ignore` rejects, and pixels that are no reading", () => {
    const data = Float32Array.from([NaN, Infinity, -3, 0, 6, 8]);
    const found = findHottestPixels(data, 6, 1, { count: 10, ignore: (index) => index === 5 });
    expect(found.map((p) => p.index)).toEqual([4]);
  });

  it("returns fewer than asked when the frame has fewer bright pixels", () => {
    expect(findHottestPixels(FRAME, 4, 3, { count: 100 })).toHaveLength(7);
    expect(findHottestPixels(new Uint32Array(12), 4, 3)).toEqual([]);
  });

  it("returns nothing without a frame", () => {
    expect(findHottestPixels(null, 4, 3)).toEqual([]);
    expect(findHottestPixels(FRAME, 0, 0)).toEqual([]);
  });
});

describe("clampHottestPixelCount", () => {
  it("rounds and keeps the count between 1 and 1000", () => {
    expect(clampHottestPixelCount("12.4")).toBe(12);
    expect(clampHottestPixelCount(0)).toBe(1);
    expect(clampHottestPixelCount(5000)).toBe(1000);
    expect(clampHottestPixelCount("abc", 20)).toBe(20);
  });
});
