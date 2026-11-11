import { describe, expect, it } from "vitest";

import {
  COLD_BIT,
  DEAD_BIT,
  HOT_BIT,
  VALUE_BIT,
  addFound,
  applyValueRule,
  findBadPixels,
  findByValue,
} from "../modules/pixel_mask_find.js";

// A flat image: Poisson-like noise around `level`, the same every run.
function flat(width, height, level) {
  const data = new Uint32Array(width * height);
  let seed = 12345;
  for (let i = 0; i < data.length; i += 1) {
    seed = (seed * 1103515245 + 12345) & 0x7fffffff;
    const u = seed / 0x7fffffff;
    data[i] = Math.max(0, Math.round(level + (u - 0.5) * 2 * Math.sqrt(level)));
  }
  return data;
}

describe("finding bad pixels", () => {
  it("finds hot, dead and cold pixels in a flat image, and nothing else", async () => {
    const width = 96;
    const height = 64;
    const data = flat(width, height, 1000);
    data[10 * width + 10] = 5000; // hot
    data[40 * width + 70] = 0; // dead
    data[20 * width + 50] = 90; // cold
    const found = await findBadPixels({ data, width, height });
    expect(found.hot).toEqual([10 * width + 10]);
    expect(found.dead).toEqual([40 * width + 70]);
    expect(found.cold).toEqual([20 * width + 50]);
  });

  it("does not call single counts in a dark image hot", async () => {
    const width = 64;
    const height = 64;
    const data = new Uint32Array(width * height);
    for (let i = 0; i < data.length; i += 37) data[i] = 1 + (i % 3);
    data[5 * width + 5] = 400;
    const found = await findBadPixels({ data, width, height });
    expect(found.hot).toEqual([5 * width + 5]);
    // Nothing is lit, so nothing can be dead or cold.
    expect(found.dead).toEqual([]);
    expect(found.cold).toEqual([]);
  });

  it("judges each module by its own level", async () => {
    const width = 64;
    const height = 32;
    const left = flat(32, 32, 200);
    const right = flat(32, 32, 2000);
    const data = new Uint32Array(width * height);
    for (let y = 0; y < height; y += 1) {
      for (let x = 0; x < 32; x += 1) {
        data[y * width + x] = left[y * 32 + x];
        data[y * width + 32 + x] = right[y * 32 + x];
      }
    }
    // Ordinary in the bright module, far above the dim one.
    data[3 * width + 3] = 1900;
    const found = await findBadPixels({ data, width, height });
    expect(found.hot).toEqual([3 * width + 3]);
  });

  it("leaves out masked pixels and in-data flags, and follows the settings", async () => {
    const width = 64;
    const height = 64;
    const data = flat(width, height, 500);
    const mask = new Uint32Array(width * height);
    data[1 * width + 1] = 9000;
    mask[1 * width + 1] = 2; // already masked
    const flagged = new Int32Array(data);
    flagged[2 * width + 2] = -2;
    data[30 * width + 30] = 9000;
    flagged[30 * width + 30] = 9000;
    flagged[31 * width + 31] = 0;

    const found = await findBadPixels({ data: flagged, width, height, mask });
    expect(found.hot).toEqual([30 * width + 30]);
    expect(found.dead).toEqual([31 * width + 31]);

    const hotOnly = await findBadPixels({ data: flagged, width, height, mask, options: { cold: false } });
    expect(hotOnly.dead).toEqual([]);
    const strict = await findBadPixels({ data: flagged, width, height, mask, options: { hotSigma: 1000 } });
    expect(strict.hot).toEqual([]);
  });

  it("reports progress and waits for it", async () => {
    const data = flat(64, 96, 100);
    const seen = [];
    await findBadPixels({
      data,
      width: 64,
      height: 96,
      onProgress: (fraction) => {
        seen.push(fraction);
        return Promise.resolve();
      },
    });
    expect(seen).toEqual([1 / 3, 2 / 3, 1]);
  });

  it("marks what was found with its own bit, leaving masked pixels alone", () => {
    const mask = new Uint32Array([0, 0, 0, 1, 0]);
    expect(addFound(mask, { hot: [0, 3], dead: [1], cold: [2], value: [4] })).toBe(4);
    expect(Array.from(mask)).toEqual([HOT_BIT, DEAD_BIT, COLD_BIT, 1, VALUE_BIT]);
  });

  it("finds pixels by value: exactly, at or above, at or below", () => {
    const data = new Uint16Array([0, 5, 65535, 7, 0, 65535]);
    const args = { data, width: 3, height: 2 };
    expect(findByValue({ ...args, mode: "eq", limit: 0 })).toEqual([0, 4]);
    expect(findByValue({ ...args, mode: "gte", limit: 65535 })).toEqual([2, 5]);
    expect(findByValue({ ...args, mode: "lte", limit: 5 })).toEqual([0, 1, 4]);
    expect(findByValue({ ...args, mode: "eq", limit: NaN })).toEqual([]);
    // Masked pixels are left out.
    expect(findByValue({ ...args, mask: new Uint32Array([2, 0, 0, 0, 0, 0]), mode: "eq", limit: 0 })).toEqual([4]);
  });

  it("drops what the value rule took from hot, dead and cold, and only that", () => {
    const data = new Uint32Array([0, 9, 70000, 3, 0, 65535]);
    const found = { hot: [2, 5], dead: [0, 4], cold: [3] };
    applyValueRule(found, { on: true, text: " 0 ", mode: "eq", data, width: 3, height: 2 });
    expect(found).toEqual({ hot: [2, 5], dead: [], cold: [3], value: [0, 4] });

    const off = { hot: [2], dead: [0], cold: [] };
    applyValueRule(off, { on: false, text: "0", mode: "eq", data, width: 3, height: 2 });
    expect(off).toEqual({ hot: [2], dead: [0], cold: [], value: [] });
  });

  it("takes every pixel of a frame larger than a Set can hold", () => {
    // 4097 x 4097 is past 2^24 pixels, the most a Set holds; an EIGER2 16M
    // frame (4148 x 4362) is too. The rule used to collect its pixels in one.
    const width = 4097;
    const data = new Uint8Array(width * width).fill(1);
    const found = { hot: [7], dead: [], cold: [] };
    applyValueRule(found, { on: true, text: "0", mode: "gte", data, width, height: width });
    expect(found.value).toHaveLength(width * width);
    expect(found.hot).toEqual([]);
  });

  it("leaves negative flags alone unless the value asked for is negative", () => {
    const data = new Int32Array([-1, -2, 0, 3]);
    const args = { data, width: 4, height: 1 };
    expect(findByValue({ ...args, mode: "lte", limit: 2 })).toEqual([2]);
    expect(findByValue({ ...args, mode: "eq", limit: -1 })).toEqual([0]);
  });
});
