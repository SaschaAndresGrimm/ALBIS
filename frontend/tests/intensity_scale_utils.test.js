import { describe, expect, it } from "vitest";

import { computeAutoLevels, formatPixelLabelValue } from "../modules/intensity_scale_utils.js";

describe("intensity_scale_utils", () => {
  it("keeps integer auto labels unchanged", () => {
    expect(formatPixelLabelValue(12.7, 40, "auto", "uint16")).toBe("13");
    expect(formatPixelLabelValue(1234567, 40, "auto", "uint16")).toBe("1.2M");
  });

  it("shows decimal labels for float auto mode when space allows", () => {
    expect(formatPixelLabelValue(1.2345, 40, "auto", "float32")).toBe("1.234");
    expect(formatPixelLabelValue(0.999791, 40, "auto", "float32")).toBe("1.000");
    expect(formatPixelLabelValue(1.2345, 28, "auto", "float32")).toBe("1.23");
    expect(formatPixelLabelValue(0.1234, 28, "auto", "float32")).toBe("0.12");
  });

  it("falls back to compact float representations as space gets tighter", () => {
    expect(formatPixelLabelValue(123456.789, 40, "auto", "float32")).toBe("1.23e5");
    expect(formatPixelLabelValue(1.2345, 18, "auto", "float32")).toBe("1");
  });

  it("preserves explicit integer and scientific modes for float data", () => {
    expect(formatPixelLabelValue(1.2345, 40, "integer", "float32")).toBe("1");
    expect(formatPixelLabelValue(0.0001234, 60, "scientific", "float32")).toBe("1.234e-4");
  });
});

describe("computeAutoLevels", () => {
  const N = 100000;
  function makeBulk() {
    // Smooth continuum of counts 1..1000.
    const data = new Float64Array(N);
    for (let i = 0; i < N; i += 1) data[i] = 1 + (i % 1000);
    return data;
  }

  it("tracks the upper percentile of a clean continuum", () => {
    const { max } = computeAutoLevels(makeBulk(), undefined, { min: 0, max: 1 }, "<u4");
    expect(max).toBeGreaterThan(800);
    expect(max).toBeLessThanOrEqual(1000);
  });

  it("ignores a detached cluster of summed sentinel pixels", () => {
    // ~8% of pixels stuck at an extreme value far above the real signal,
    // mimicking summed gap/dead-pixel sentinels (e.g. 65535 x frames).
    const bulk = makeBulk();
    const data = new Float64Array(N + 9000);
    data.set(bulk, 0);
    data.fill(10_000_000, N);
    const { max } = computeAutoLevels(data, undefined, { min: 0, max: 1 }, "<u4");
    // Without rejection the 99.9th percentile would land at 10,000,000.
    expect(max).toBeLessThan(2000);
    expect(max).toBeGreaterThan(800);
  });

  it("does not treat a few isolated bright pixels as a sentinel cluster", () => {
    // Below the cluster fraction threshold: must not perturb the bulk levels.
    const bulk = makeBulk();
    const data = new Float64Array(N + 5);
    data.set(bulk, 0);
    data.fill(10_000_000, N);
    const { max } = computeAutoLevels(data, undefined, { min: 0, max: 1 }, "<u4");
    expect(max).toBeLessThan(2000);
  });
});
