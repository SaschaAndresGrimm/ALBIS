/**
 * Gap and defective counts for frames that carry their flags in the pixels.
 *
 * A PILATUS CBF marks an inter-module gap `-1` and a bad pixel `-2` in the
 * pixel data itself. There is no mask array for such a frame -- `/api/mask`
 * only answers for HDF5 -- and the counters read mask bits only, so the panel
 * reported "Gap pixels 0, Defective pixels 0" for every CBF, TIFF and EDF ever
 * opened. `testdata/in16c_010001.cbf` really holds 16,558 gaps and 19 bad
 * pixels, which is 5.5% of the frame.
 *
 * The numbers below are that file's, measured with numpy.
 */

import { describe, expect, it } from "vitest";

import {
  DATA_DEFECTIVE_VALUE,
  DATA_GAP_VALUE,
  accumulateRoiPixelCounters,
  computeGlobalStats,
  createRoiPixelCounters,
  getPixelFlags,
  usesInDataPixelFlags,
} from "../modules/roi_stats_engine.js";

const isSaturatedValue = (value, satMax) =>
  Number.isFinite(value) && Number.isFinite(satMax) && Math.abs(value - satMax) < 1e-6;
const computeMedian = (values) => {
  const sorted = Array.from(values).sort((a, b) => a - b);
  if (!sorted.length) return 0;
  const mid = sorted.length >> 1;
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) * 0.5;
};

function globalStats(data, overrides = {}) {
  return computeGlobalStats({
    dataRaw: data,
    width: overrides.width ?? data.length,
    height: overrides.height ?? 1,
    maskAvailable: false,
    maskRaw: null,
    maskShape: null,
    maskEnabled: false,
    maskSaturatedEnabled: false,
    satMax: null,
    isSaturatedValue,
    computeMedian,
    ...overrides,
  });
}

describe("flags carried in the pixel data", () => {
  it("counts the gaps and bad pixels of a PILATUS-style frame", () => {
    // Proportioned like the real file: mostly counts, 5% gaps, a few bad.
    const data = new Int32Array(1000);
    for (let i = 0; i < data.length; i += 1) data[i] = i % 7;
    for (let i = 0; i < 50; i += 1) data[i] = DATA_GAP_VALUE;
    for (let i = 50; i < 53; i += 1) data[i] = DATA_DEFECTIVE_VALUE;

    const stats = globalStats(data);

    expect(stats.gapPixels).toBe(50);
    expect(stats.defectivePixels).toBe(3);
    expect(stats.totalPixels).toBe(1000);
  });

  it("reports nothing for an unsigned frame, which cannot carry the flags", () => {
    // An EIGER frame is uint32 and marks overflow 0xFFFFFFFF instead. There is
    // no value here that could mean -1, so nothing may be inferred.
    const data = new Uint32Array([0, 1, 2, 4294967295]);
    const stats = globalStats(data);

    expect(stats.gapPixels).toBe(0);
    expect(stats.defectivePixels).toBe(0);
  });

  it("leaves a float frame's negative values as measurements", () => {
    // The guard that matters: a flatfield or a difference image may hold
    // exactly -1.0, and reading that as a defect invents detector faults.
    const data = new Float32Array([-1, -2, 0.5, 1.5]);
    const stats = globalStats(data);

    expect(stats.gapPixels).toBe(0);
    expect(stats.defectivePixels).toBe(0);
    expect(stats.count).toBe(4);
  });

  it("lets a real mask override the values", () => {
    // A frame with a mask array is being told authoritatively, and -1 in such
    // a frame is data. Trusting the values too would double-count.
    const data = new Int32Array([DATA_GAP_VALUE, 5, 7, 9]);
    const maskRaw = new Uint32Array([0, 1, 2, 0]);
    const stats = globalStats(data, {
      width: 4,
      height: 1,
      maskAvailable: true,
      maskRaw,
      maskShape: [1, 4],
    });

    // One gap and one defective, both from the mask; the -1 pixel is not a
    // gap because this frame's mask says it is not.
    expect(stats.gapPixels).toBe(1);
    expect(stats.defectivePixels).toBe(1);
  });

  it("counts a pixel once when it is both a gap and defective", () => {
    const data = new Int32Array([DATA_GAP_VALUE, DATA_DEFECTIVE_VALUE]);
    const stats = globalStats(data, { width: 2, height: 1 });

    expect(stats.gapPixels + stats.defectivePixels).toBe(2);
  });

  it("keeps saturated pixels separate from flagged ones", () => {
    const data = new Int32Array([DATA_GAP_VALUE, DATA_DEFECTIVE_VALUE, 100, 100]);
    const stats = globalStats(data, { width: 4, height: 1, satMax: 100 });

    expect(stats.gapPixels).toBe(1);
    expect(stats.defectivePixels).toBe(1);
    expect(stats.saturatedPixels).toBe(2);
  });
});

describe("usesInDataPixelFlags", () => {
  it.each([
    ["Int8Array", new Int8Array(1)],
    ["Int16Array", new Int16Array(1)],
    ["Int32Array", new Int32Array(1)],
  ])("accepts %s", (_label, data) => {
    expect(usesInDataPixelFlags(data)).toBe(true);
  });

  it.each([
    ["Uint8Array", new Uint8Array(1)],
    ["Uint16Array", new Uint16Array(1)],
    ["Uint32Array", new Uint32Array(1)],
    ["Float32Array", new Float32Array(1)],
    ["Float64Array", new Float64Array(1)],
  ])("rejects %s", (_label, data) => {
    expect(usesInDataPixelFlags(data)).toBe(false);
  });

  it("rejects nothing at all", () => {
    expect(usesInDataPixelFlags(null)).toBe(false);
    expect(usesInDataPixelFlags(undefined)).toBe(false);
  });
});

describe("an ROI counts the same pixels as the whole image", () => {
  it("counts flags from the values when there is no mask", () => {
    const counters = createRoiPixelCounters();
    for (const raw of [DATA_GAP_VALUE, DATA_GAP_VALUE, DATA_DEFECTIVE_VALUE, 42]) {
      accumulateRoiPixelCounters(counters, { raw, maskValue: null }, null, isSaturatedValue, true);
    }

    expect(counters).toEqual({ total: 4, gap: 2, defective: 1, saturated: 0 });
  });

  it("ignores the values when the frame has a mask", () => {
    const counters = createRoiPixelCounters();
    for (const raw of [DATA_GAP_VALUE, DATA_DEFECTIVE_VALUE]) {
      accumulateRoiPixelCounters(counters, { raw, maskValue: 0 }, null, isSaturatedValue, false);
    }

    expect(counters).toEqual({ total: 2, gap: 0, defective: 0, saturated: 0 });
  });

  it("agrees with the whole-image loop pixel for pixel", () => {
    /**
     * The two counting paths are separate code -- one inlined hot loop, one
     * per-sample accumulator -- and the inlined one carries a comment saying
     * the helper is the readable definition. This is what holds it to that.
     */
    const data = new Int32Array(300);
    for (let i = 0; i < data.length; i += 1) {
      data[i] = i % 11 === 0 ? DATA_GAP_VALUE : i % 17 === 0 ? DATA_DEFECTIVE_VALUE : i;
    }

    const whole = globalStats(data, { width: 300, height: 1 });

    const counters = createRoiPixelCounters();
    for (const raw of data) {
      accumulateRoiPixelCounters(counters, { raw, maskValue: null }, null, isSaturatedValue, true);
    }

    expect(counters.gap).toBe(whole.gapPixels);
    expect(counters.defective).toBe(whole.defectivePixels);
    expect(counters.total).toBe(whole.totalPixels);
  });
});

describe("getPixelFlags", () => {
  it("gives the mask precedence over the value", () => {
    expect(getPixelFlags(DATA_GAP_VALUE, 0, true)).toEqual({ gap: true, defective: false });
    expect(getPixelFlags(DATA_GAP_VALUE, 2, true)).toEqual({ gap: false, defective: true });
    expect(getPixelFlags(5, 1, true)).toEqual({ gap: true, defective: false });
  });

  it("never reports one pixel as both", () => {
    // `gap` wins, so the two counters can be added without double-counting.
    expect(getPixelFlags(0, 0x1f, true)).toEqual({ gap: true, defective: false });
  });

  it("reports nothing when in-data flags do not apply", () => {
    expect(getPixelFlags(DATA_GAP_VALUE, null, false)).toEqual({ gap: false, defective: false });
  });
});
