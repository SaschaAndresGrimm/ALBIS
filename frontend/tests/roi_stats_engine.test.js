import {
  applyMaskToValue,
  buildRoiHistogram,
  computeGlobalStats,
  getMaskFlags,
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
});
