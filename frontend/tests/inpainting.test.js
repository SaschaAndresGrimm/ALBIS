import fs from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { createInpaintCache, createInpaintView, inpaint, inpaintHoles } from "../modules/inpainting.js";
import { computeGlobalStats, createRoiAccumulator } from "../modules/roi_stats_engine.js";

const { cases } = JSON.parse(fs.readFileSync(path.join(process.cwd(), "tests", "fixtures", "inpainting_cases.json"), "utf8"));

describe("inpainting", () => {
  it.each(cases.map((c) => [c.name, c]))("fills %s like the exports do", (_name, c) => {
    const out = inpaint(Float64Array.from(c.data), c.width, c.height, Uint8Array.from(c.holes));
    expect(out).not.toBeNull();
    c.expected.forEach((value, i) => expect(out[i]).toBeCloseTo(value, 3));
  });

  it("carries a gradient straight through a gap and leaves the input alone", () => {
    const width = 12;
    const data = new Uint32Array(width * 3).map((_, i) => (i % width) * 5);
    const holes = new Uint8Array(data.length).map((_, i) => (i % width >= 4 && i % width < 8 ? 1 : 0));
    const out = inpaint(data, width, 3, holes);
    expect([...out.slice(0, width)]).toEqual([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]);
    expect(data[5]).toBe(25);
  });

  it("finds the gaps and defective pixels by the mask, or by -1 and -2 without one", () => {
    const data = new Int32Array([5, -1, 7, -2, 8, 9]);
    const mask = new Uint32Array([0, 0, 1, 0, 0, 0x10]);
    expect([...inpaintHoles({ data, mask }).holes]).toEqual([0, 0, 1, 0, 0, 1]);
    expect([...inpaintHoles({ data, inDataFlags: true }).holes]).toEqual([0, 1, 0, 1, 0, 0]);
  });

  it("fills saturated pixels only when asked", () => {
    const data = new Uint16Array([10, 65535, 12]);
    const isSaturatedValue = (value, max) => value === max;
    const base = { data, satMax: 65535, isSaturatedValue };
    expect(inpaintHoles(base).count).toBe(0);
    expect([...inpaintHoles({ ...base, fillSaturated: true }).holes]).toEqual([0, 1, 0]);
  });

  it("works a frame out once per frame and setting", () => {
    const cache = createInpaintCache();
    const data = new Uint32Array([1, 2, 3, 4]);
    const mask = new Uint32Array([0, 1, 0, 0]);
    const first = cache.get({ data, width: 2, height: 2, mask });
    expect(first.filled).toBe(1);
    expect(cache.get({ data, width: 2, height: 2, mask })).toBe(first);
    expect(cache.get({ data, width: 2, height: 2, mask: null }).data).toBeNull();
  });

  it("fills an EIGER2 4M-sized frame quickly", () => {
    const width = 2068;
    const height = 2162;
    const data = new Uint32Array(width * height).map((_, i) => 40 + (i % 23));
    const holes = new Uint8Array(data.length);
    for (let y = 514; y < height; y += 551) holes.fill(1, y * width, Math.min(height, y + 37) * width);
    for (let y = 0; y < height; y += 1) holes.fill(1, y * width + 1030, y * width + 1040);
    const start = performance.now();
    const out = inpaint(data, width, height, holes);
    const elapsed = performance.now() - start;
    expect(out.every(Number.isFinite)).toBe(true);
    expect(elapsed).toBeLessThan(3000);
  });

  it("is off until switched on, and uses the mask only when it fits the frame", () => {
    const state = { inpaintEnabled: false, dataRaw: new Uint32Array([4, 9, 6, 8]), width: 2, height: 2, maskAvailable: true, maskRaw: new Uint32Array([0, 1, 0, 0]), maskShape: [2, 2] };
    const view = createInpaintView({ state, getActiveSaturationMax: () => null, isSaturatedValue: () => false });
    expect(view.frame()).toBeNull();
    state.inpaintEnabled = true;
    expect(view.frame().filled).toBe(1);
    expect(view.frame().data[1]).toBeCloseTo(6, 5);
    state.maskShape = [1, 4];
    expect(view.frame().filled).toBe(0);
  });

  it("lets the statistics follow the image: filled pixels counted as what they are, measured as what they show", () => {
    const data = new Uint32Array([10, 4294967295, 30, 40]);
    const mask = new Uint32Array([0, 1, 0, 0]);
    const fill = { data: new Float32Array([10, 20, 30, 40]), holes: new Uint8Array([0, 1, 0, 0]) };
    const acc = createRoiAccumulator({ data, mask, useMasking: true, capacity: 4, fill });
    [0, 1, 2, 3].forEach((i) => acc.add(i));
    expect(acc).toMatchObject({ count: 4, gap: 1, sum: 100, max: 40 });
    const measured = createRoiAccumulator({ data, mask, useMasking: true, capacity: 4 });
    [0, 1, 2, 3].forEach((i) => measured.add(i));
    expect(measured).toMatchObject({ count: 3, gap: 1, sum: 80 });
    const whole = computeGlobalStats({ dataRaw: data, width: 2, height: 2, maskAvailable: true, maskRaw: mask, maskShape: [2, 2], maskEnabled: true, maskSaturatedEnabled: true, satMax: null, isSaturatedValue: () => false, computeMedian: (v) => [...v].sort((a, b) => a - b)[Math.floor(v.length / 2)], fill });
    expect(whole).toMatchObject({ count: 4, gapPixels: 1, mean: 25 });
  });
});
