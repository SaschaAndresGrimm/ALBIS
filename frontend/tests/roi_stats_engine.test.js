import {
  applyMaskToValue,
  buildRoiHistogram,
  computeGlobalStats,
  getMaskFlags,
  normalizeRoiHistogramBinCount,
} from "../modules/roi_stats_engine.js";

describe("roi_stats_engine", () => {
  it("extracts mask flags", () => {
    expect(getMaskFlags(0)).toEqual({ gap: false, defective: false });
    expect(getMaskFlags(1)).toEqual({ gap: true, defective: false });
    expect(getMaskFlags(2)).toEqual({ gap: false, defective: true });
  });

  it("masks invalid values and saturated values", () => {
    expect(
      applyMaskToValue(100, 1, {
        satMax: 65535,
        maskSaturatedEnabled: true,
        isSaturatedValue: (value, max) => value >= max,
      }),
    ).toEqual({ value: 0, skip: true });

    expect(
      applyMaskToValue(70000, 0, {
        satMax: 65535,
        maskSaturatedEnabled: true,
        isSaturatedValue: (value, max) => value >= max,
      }),
    ).toEqual({ value: 0, skip: true });
  });

  it("computes global stats with mask handling", () => {
    const stats = computeGlobalStats({
      dataRaw: new Float64Array([1, 2, 3, 4]),
      width: 2,
      height: 2,
      maskAvailable: true,
      maskRaw: new Uint32Array([0, 1, 0, 0]),
      maskShape: [2, 2],
      maskEnabled: true,
      maskSaturatedEnabled: false,
      satMax: 1000,
      isSaturatedValue: (value, max) => value >= max,
      computeMedian: (values) => {
        const sorted = [...values].sort((a, b) => a - b);
        const mid = Math.floor(sorted.length / 2);
        return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
      },
    });
    expect(stats.count).toBe(3);
    expect(stats.sum).toBe(8);
    expect(stats.gapPixels).toBe(1);
    expect(stats.defectivePixels).toBe(0);
  });

  it("builds integer histogram bins", () => {
    const hist = buildRoiHistogram([1, 2, 2, 3, 3, 3]);
    expect(hist.xTickMode).toBe("integer");
    expect(hist.xStart).toBe(1);
    expect(hist.xStep).toBe(1);
    expect(hist.data).toEqual([1, 2, 3]);
  });

  it("builds fixed-count histogram bins when requested", () => {
    const hist = buildRoiHistogram([0, 1, 2, 3], { mode: "fixed", count: 2 });
    expect(hist.xTickMode).toBe("");
    expect(hist.xStart).toBe(0.75);
    expect(hist.xStep).toBe(1.5);
    expect(hist.data).toEqual([2, 2]);
  });

  it("clamps fixed histogram bin counts", () => {
    expect(normalizeRoiHistogramBinCount(1)).toBe(2);
    expect(normalizeRoiHistogramBinCount(9999)).toBe(512);
    expect(normalizeRoiHistogramBinCount("not-a-number")).toBe(128);

    const hist = buildRoiHistogram([0, 1, 2, 3], { mode: "fixed", count: 1 });
    expect(hist.data).toHaveLength(2);
  });
});

describe("computeGlobalStats median strategies", () => {
  // Reference median: sort a plain copy. Deliberately not the implementation's
  // approach, so a bug in either selection or counting shows up as a mismatch.
  function referenceMedian(values) {
    const sorted = Array.from(values).sort((a, b) => a - b);
    const mid = sorted.length >> 1;
    return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) * 0.5;
  }

  function quickSelect(values, k) {
    let left = 0;
    let right = values.length - 1;
    while (left < right) {
      const pivot = values[(left + right) >> 1];
      let i = left;
      let j = right;
      while (i <= j) {
        while (values[i] < pivot) i += 1;
        while (values[j] > pivot) j -= 1;
        if (i <= j) {
          const tmp = values[i];
          values[i] = values[j];
          values[j] = tmp;
          i += 1;
          j -= 1;
        }
      }
      if (k <= j) right = j;
      else if (k >= i) left = i;
      else break;
    }
    return values[k];
  }

  // Mirrors app.js computeMedian, which is injected as a callback.
  const computeMedian = (values) => {
    if (!values || values.length === 0) return Number.NaN;
    const work = values.slice();
    const mid = values.length >> 1;
    const high = quickSelect(work, mid);
    if (values.length % 2 === 1) return high;
    return (quickSelect(work, mid - 1) + high) * 0.5;
  };

  const run = (data, overrides = {}) =>
    computeGlobalStats({
      dataRaw: data,
      width: data.length,
      height: 1,
      maskAvailable: false,
      maskRaw: null,
      maskShape: null,
      maskEnabled: false,
      maskSaturatedEnabled: false,
      satMax: null,
      isSaturatedValue: () => false,
      computeMedian,
      ...overrides,
    });

  it("matches a sorted reference for integer data", () => {
    for (const values of [[5], [3, 8], [3, 1, 2], [4, 1, 3, 2], [7, 7, 7, 7, 8], [0, 0, 1]]) {
      expect(run(Uint16Array.from(values)).median).toBe(referenceMedian(values));
    }
  });

  it("matches a sorted reference for signed data spanning zero", () => {
    const values = Array.from({ length: 501 }, (_, i) => ((i * 37) % 400) - 200);
    expect(run(Int32Array.from(values)).median).toBe(referenceMedian(values));
  });

  it("keeps fractional float data exact even when its extremes are integers", () => {
    // The trap this guards: float32 rounding puts min and max on exact integers
    // here, so an "are the endpoints integral?" test would wrongly route this
    // through counting and truncate every fractional value into the wrong bin.
    // Integral endpoints around a fractional interior. This is not contrived:
    // float32 rounding lands real data on exact extremes readily -- sin(i)*1000
    // over a million samples has min and max of exactly -1000 and 1000.
    const data = new Float32Array(1024);
    for (let i = 0; i < data.length; i += 1) data[i] = (i - 512) * 0.37;
    data[0] = -1000;
    data[data.length - 1] = 1000;
    expect(Number.isInteger(Math.min(...data))).toBe(true);
    expect(Number.isInteger(Math.max(...data))).toBe(true);
    expect(Number.isInteger(data[500])).toBe(false);

    const median = run(data).median;
    expect(median).toBe(referenceMedian(data));
    expect(Number.isInteger(median)).toBe(false);
  });

  it("falls back to selection when the value span is too wide to count", () => {
    // A real EIGER frame marks overflow pixels 0xFFFFFFFF; with no mask to
    // remove them the span is billions of values wide.
    const data = Uint32Array.from([1, 2, 3, 4, 5, 4294967295]);
    expect(run(data).median).toBe(referenceMedian(data));
  });

  it("counts only unmasked pixels, matching the reference over survivors", () => {
    const data = Uint16Array.from([10, 20, 30, 40, 50, 60, 70, 80]);
    const mask = Uint32Array.from([0, 1, 0, 0x1e, 0, 0, 1, 0]);
    const survivors = [10, 30, 50, 60, 80];

    const stats = computeGlobalStats({
      dataRaw: data,
      width: 8,
      height: 1,
      maskAvailable: true,
      maskRaw: mask,
      maskShape: [1, 8],
      maskEnabled: true,
      maskSaturatedEnabled: false,
      satMax: null,
      isSaturatedValue: () => false,
      computeMedian,
    });

    expect(stats.median).toBe(referenceMedian(survivors));
    expect(stats.count).toBe(survivors.length);
    expect(stats.gapPixels).toBe(2);
    expect(stats.defectivePixels).toBe(1);
  });

  it("excludes saturated pixels from the median when asked to", () => {
    const data = Uint16Array.from([1, 2, 3, 999, 999]);
    const stats = computeGlobalStats({
      dataRaw: data,
      width: 5,
      height: 1,
      maskAvailable: false,
      maskRaw: null,
      maskShape: null,
      maskEnabled: false,
      maskSaturatedEnabled: true,
      satMax: 999,
      isSaturatedValue: (value, sat) => value === sat,
      computeMedian,
    });

    expect(stats.median).toBe(referenceMedian([1, 2, 3]));
    expect(stats.saturatedPixels).toBe(2);
  });

  it("reuses its scratch buffers without leaking state between calls", () => {
    // The bins and the selection buffer are module-level and reused, so a
    // larger frame followed by a smaller one must not read stale entries.
    const big = Uint16Array.from(Array.from({ length: 4000 }, (_, i) => i % 900));
    const small = Uint16Array.from([1, 2, 3, 4]);
    run(big);
    expect(run(small).median).toBe(referenceMedian([1, 2, 3, 4]));
    expect(run(big).median).toBe(referenceMedian(Array.from(big)));
  });
});
