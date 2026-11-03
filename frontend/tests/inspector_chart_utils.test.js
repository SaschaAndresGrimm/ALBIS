/**
 * The chart of a 1D array: a line when every value is a point, a band when
 * points stand for stretches, gaps where values are NaN.
 */

import { describe, expect, it } from "vitest";

import { outlineGeometry, outlinePointAt } from "../modules/inspector_chart_utils.js";

const BOX = { width: 102, height: 52, pad: 1 };

describe("outlineGeometry", () => {
  it("draws every value as a line point when step is 1", () => {
    const g = outlineGeometry({ step: 1, low: [0, 10, 5], high: [0, 10, 5] }, BOX);
    expect(g.band).toBe("");
    expect(g.line).toBe("M1 51 L51 1 L101 26");
    expect([g.yMin, g.yMax]).toEqual([0, 10]);
  });

  it("draws a band from high forward and low back when points cover stretches", () => {
    const g = outlineGeometry({ step: 100, low: [0, 0], high: [10, 5] }, BOX);
    expect(g.line).toBe("");
    expect(g.band).toBe("M1 1 L101 26 L101 51 L1 51 Z");
  });

  it("breaks the path at NaN instead of drawing through it", () => {
    const g = outlineGeometry({ step: 1, low: [1, null, 2, 3], high: [1, null, 2, 3] }, BOX);
    expect(g.line.match(/M/g)).toHaveLength(2);
  });

  it("puts a constant array on a level line in the middle", () => {
    const g = outlineGeometry({ step: 1, low: [4, 4], high: [4, 4] }, BOX);
    expect(g.line).toBe("M1 26 L101 26");
    expect([g.yMin, g.yMax]).toEqual([4, 4]);
  });

  it("has nothing to draw without a finite value", () => {
    expect(outlineGeometry({ step: 1, low: [null], high: [null] }, BOX)).toBeNull();
    expect(outlineGeometry(null, BOX)).toBeNull();
  });
});

describe("outlinePointAt", () => {
  const outline = { step: 100, low: [0, 1, 2, 3], high: [9, 8, 7, 6] };

  it("names the indices a point covers, the last one cut at the array's end", () => {
    expect(outlinePointAt(outline, 0, 350)).toEqual({ index: 0, from: 0, to: 99, low: 0, high: 9 });
    expect(outlinePointAt(outline, 1, 350)).toEqual({ index: 3, from: 300, to: 349, low: 3, high: 6 });
    expect(outlinePointAt(outline, 0.5, 350).index).toBe(2);
  });

  it("covers one index per point when step is 1", () => {
    expect(outlinePointAt({ step: 1, low: [5, 6], high: [5, 6] }, 1, 2)).toEqual({ index: 1, from: 1, to: 1, low: 6, high: 6 });
    expect(outlinePointAt({ step: 1, low: [], high: [] }, 0, 0)).toBeNull();
  });
});
