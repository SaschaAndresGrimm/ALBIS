import { describe, expect, it } from "vitest";

import { COLD_BIT, DEAD_BIT, HOT_BIT, addFound, findBadPixels } from "../modules/pixel_mask_find.js";

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
    const mask = new Uint32Array([0, 0, 0, 1]);
    expect(addFound(mask, { hot: [0, 3], dead: [1], cold: [2] })).toBe(3);
    expect(Array.from(mask)).toEqual([HOT_BIT, DEAD_BIT, COLD_BIT, 1]);
  });
});
