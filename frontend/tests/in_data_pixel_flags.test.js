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


describe("flagged pixels are excluded from the statistics", () => {
  /**
   * `-1` is a missing pixel and `-2` a broken one. Neither is a measurement,
   * and including them put `Min: -2` on the panel and pulled the mean down --
   * on `testdata/in16c_010001.cbf`, 16,577 of 301,453 pixels, moving the mean
   * from 6.623 to 6.204 and the median from 5 to 4.
   *
   * Excluding them is not a new policy: a frame with a mask array enables the
   * mask when one is found, so masked pixels were already out of the
   * statistics by default. A PILATUS frame carries the same information in its
   * values and was the only kind still counting gaps as data.
   */

  const FLAGGED = [DATA_GAP_VALUE, DATA_DEFECTIVE_VALUE];

  it("leaves the minimum at the smallest real count", () => {
    const stats = globalStats(new Int32Array([...FLAGGED, 3, 7, 11]), { width: 5, height: 1 });

    expect(stats.min).toBe(3);
    expect(stats.max).toBe(11);
  });

  it("sums and averages only the measurements", () => {
    const stats = globalStats(new Int32Array([...FLAGGED, 4, 6, 20]), { width: 5, height: 1 });

    expect(stats.count).toBe(3);
    expect(stats.sum).toBe(30);
    expect(stats.mean).toBeCloseTo(10, 10);
  });

  it("still reports every pixel as a total", () => {
    // `totalPixels` is the frame; `count` is what the statistics used. Both
    // are shown, and the difference is the gap and defective counts.
    const stats = globalStats(new Int32Array([...FLAGGED, 1, 2]), { width: 4, height: 1 });

    expect(stats.totalPixels).toBe(4);
    expect(stats.count).toBe(2);
    expect(stats.gapPixels + stats.defectivePixels).toBe(stats.totalPixels - stats.count);
  });

  it("takes the median over the same pixels as the mean", () => {
    // The counting-median path bins `value - min`, and `min` is now the
    // smallest accepted value -- so a skipped pixel would index before the
    // bins and be dropped silently by the typed array, leaving the bin total
    // short of `count` and the walk on the wrong order statistic.
    const data = new Int32Array([DATA_GAP_VALUE, DATA_GAP_VALUE, 10, 20, 30]);
    const stats = globalStats(data, { width: 5, height: 1 });

    expect(stats.count).toBe(3);
    expect(stats.median).toBe(20);
  });

  it("agrees between the counting median and the selection fallback", () => {
    /**
     * Two implementations, chosen by how wide the value range is. Both have to
     * exclude the same pixels or the median jumps when a frame happens to
     * cross the threshold.
     */
    const narrow = new Int32Array([DATA_GAP_VALUE, 1, 2, 3, 4, 5, 6, 7]);
    // Past COUNTING_MEDIAN_MAX_SPAN (1 << 21), which forces selection.
    const wide = new Int32Array([DATA_GAP_VALUE, 1, 2, 3, 4, 5, 6, 1 << 22]);

    const narrowStats = globalStats(narrow, { width: 8, height: 1 });
    const wideStats = globalStats(wide, { width: 8, height: 1 });

    expect(narrowStats.count).toBe(7);
    expect(wideStats.count).toBe(7);
    expect(narrowStats.median).toBe(4);
    // Same seven accepted values but for the last, so the median is the same.
    expect(wideStats.median).toBe(4);
  });

  it("excludes them from the standard deviation", () => {
    const stats = globalStats(new Int32Array([...FLAGGED, 5, 5, 5, 5]), { width: 6, height: 1 });

    expect(stats.count).toBe(4);
    expect(stats.std).toBe(0);
  });

  it("keeps a float frame's negative values in the statistics", () => {
    // The guard again, on the statistics rather than the counters: a
    // difference image legitimately has a negative minimum.
    const stats = globalStats(new Float32Array([-2, -1, 1, 2]), { width: 4, height: 1 });

    expect(stats.count).toBe(4);
    expect(stats.min).toBe(-2);
    expect(stats.mean).toBeCloseTo(0, 10);
  });

  it("keeps an unsigned frame whole", () => {
    const stats = globalStats(new Uint16Array([0, 1, 2, 3]), { width: 4, height: 1 });

    expect(stats.count).toBe(4);
    expect(stats.min).toBe(0);
  });

  it("leaves a masked frame to its mask", () => {
    /**
     * With a mask array present the mask decides, and it can be switched off.
     * A `-1` in such a frame is a measurement, so turning the mask off must
     * bring it back into the statistics rather than being overruled by its
     * value.
     */
    const data = new Int32Array([DATA_GAP_VALUE, 5, 7, 9]);
    const maskRaw = new Uint32Array([0, 0, 0, 0]);
    const withMaskOff = globalStats(data, {
      width: 4,
      height: 1,
      maskAvailable: true,
      maskRaw,
      maskShape: [1, 4],
      maskEnabled: false,
    });

    expect(withMaskOff.count).toBe(4);
    expect(withMaskOff.min).toBe(DATA_GAP_VALUE);
  });

  it("excludes a masked frame's flagged pixels when the mask is on", () => {
    const data = new Int32Array([100, 5, 7, 9]);
    const maskRaw = new Uint32Array([1, 0, 0, 0]);
    const stats = globalStats(data, {
      width: 4,
      height: 1,
      maskAvailable: true,
      maskRaw,
      maskShape: [1, 4],
      maskEnabled: true,
    });

    expect(stats.count).toBe(3);
    expect(stats.max).toBe(9);
  });

  it("reports zeroes rather than infinities for a frame of nothing but flags", () => {
    // Every pixel skipped leaves `count` at 0, where `min`/`max` still hold
    // their sentinels. The early return exists for exactly this.
    const stats = globalStats(new Int32Array(FLAGGED), { width: 2, height: 1 });

    expect(stats.count).toBe(0);
    expect(stats.min).toBe(0);
    expect(stats.max).toBe(0);
    expect(stats.mean).toBe(0);
    expect(stats.gapPixels).toBe(1);
    expect(stats.defectivePixels).toBe(1);
  });
});
