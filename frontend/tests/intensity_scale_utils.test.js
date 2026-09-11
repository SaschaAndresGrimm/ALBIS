import { describe, expect, it } from "vitest";

import {
  computeAutoLevels,
  formatPixelLabelValue,
  pixelLabelFontPx,
} from "../modules/intensity_scale_utils.js";

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


/**
 * A pixel label must fit inside its pixel.
 *
 * Reported from a 50x view of a detector whose counts are around 3.67e6: the
 * seven-digit labels ran across their cell borders into each other, with the
 * format on "auto". The compaction to "3.7M" existed already and was never
 * reached, because the character budget divided the cell by a flat 5.6px while
 * the font caps at 13px -- so past a ~25px cell the budget kept growing and
 * the glyphs did not.
 *
 * WIDTHS BELOW ARE MEASURED, not assumed: Inter's "8" advances 0.6187em at
 * every size tested (7 / 9.36 / 11.5 / 13 px), so seven digits at 13px is
 * 51.9px -- wider than the 50px cell they were drawn in.
 */
describe("a pixel label fits inside its pixel", () => {
  const WIDEST_DIGIT_EM = 0.6187;

  const renderedWidth = (text, cellPx) =>
    text.length * pixelLabelFontPx(cellPx) * WIDEST_DIGIT_EM;

  it("compacts the seven-digit counts that overran a 50px cell", () => {
    // The exact case from the report.
    expect(formatPixelLabelValue(3673170, 50, "auto", "uint32")).toBe("3.7M");
  });

  it.each([25, 30, 40, 50, 64, 80, 120])(
    "keeps an auto label inside a %ipx cell",
    (cellPx) => {
      for (const value of [7, 88, 999, 8888, 88888, 888888, 8888888, 88888888, 3673170]) {
        const text = formatPixelLabelValue(value, cellPx, "auto", "uint32");
        expect(renderedWidth(text, cellPx)).toBeLessThanOrEqual(cellPx);
      }
    }
  );

  it("still shows the plain number when the cell is roomy enough", () => {
    // 7 digits at 13px is 51.9px; a 96px cell has room to spare.
    expect(formatPixelLabelValue(3673170, 96, "auto", "uint32")).toBe("3673170");
  });

  /**
   * The contract is "fits, or nothing" -- never "overflows". Below about a
   * 20px cell a seven-digit count cannot be shown at any abbreviation, so an
   * empty label is the correct answer there and was also the old behaviour;
   * the controller's own minCellPx gate (18 by default) means the formatter
   * is rarely asked at those sizes anyway.
   */
  it.each([8, 12, 18, 20, 25, 32, 50, 64, 96, 160])(
    "either fits or draws nothing at a %ipx cell, never overflows",
    (cellPx) => {
      for (const value of [0, 7, 888, 65535, 3673170, 4294967295]) {
        const text = formatPixelLabelValue(value, cellPx, "auto", "uint32");
        if (text === "") continue;
        expect(renderedWidth(text, cellPx)).toBeLessThanOrEqual(cellPx);
      }
    }
  );

  it("does show something once the cell can hold an abbreviation", () => {
    // 25px is where the font caps; "3.7M" at 13px is 32px, so it needs more.
    for (const cellPx of [40, 50, 64]) {
      expect(formatPixelLabelValue(3673170, cellPx, "auto", "uint32")).not.toBe("");
    }
  });

  it("reports the font size the labels are actually drawn at", () => {
    // Integer labels: zoom * 0.52, floored at 7 and capped at 13 -- the cap is
    // the whole reason a cell-derived budget drifted.
    expect(pixelLabelFontPx(10)).toBe(7);
    expect(pixelLabelFontPx(20)).toBeCloseTo(10.4, 5);
    expect(pixelLabelFontPx(25)).toBe(13);
    expect(pixelLabelFontPx(200)).toBe(13);
    // Float labels run smaller, to make room for the separators.
    expect(pixelLabelFontPx(50, { float: true })).toBe(11.5);
    expect(pixelLabelFontPx(10, { float: true })).toBe(6.5);
  });
});
