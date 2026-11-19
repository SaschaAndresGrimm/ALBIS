import { describe, expect, it } from "vitest";

import { roiPlotSampleIndexAt, roiPlotXDomain, roiPlotXView } from "../modules/roi_plot_view_utils.js";

describe("roiPlotXDomain", () => {
  it("runs from first to last sample, or bin edge to bin edge for a histogram", () => {
    expect(roiPlotXDomain({ length: 5, xStart: 10, xStep: 2 })).toEqual({ min: 10, max: 18 });
    expect(roiPlotXDomain({ length: 4, xStart: 0.5, xStep: 1, histogram: true })).toEqual({ min: 0, max: 4 });
  });
});

describe("roiPlotXView", () => {
  const base = { length: 10, xStart: 100, xStep: 1, autoscale: false };

  it("keeps a pinned range exactly and draws one sample past each end", () => {
    const view = roiPlotXView({ ...base, limits: { xMin: 102.3, xMax: 105.6 } });
    expect(view).toMatchObject({ min: 102.3, max: 105.6, firstIdx: 2, lastIdx: 6, yFirstIdx: 3, yLastIdx: 5 });
  });

  it("clamps the range to the data and shows all of it when the pin misses", () => {
    expect(roiPlotXView({ ...base, limits: { xMin: 95, xMax: 103 } })).toMatchObject({ min: 100, max: 103 });
    expect(roiPlotXView({ ...base, limits: { xMin: 10, xMax: 20 } })).toMatchObject({ min: 100, max: 109 });
    expect(roiPlotXView({ ...base, autoscale: true, limits: { xMin: 102, xMax: 103 } })).toMatchObject({
      min: 100,
      max: 109,
    });
  });

  it("uses the two samples around a view that falls between them", () => {
    const view = roiPlotXView({ ...base, limits: { xMin: 103.2, xMax: 103.7 } });
    expect(view).toMatchObject({ firstIdx: 3, lastIdx: 4, yFirstIdx: 3, yLastIdx: 4 });
  });

  it("counts a histogram bar as in view when any of it is", () => {
    const view = roiPlotXView({ length: 4, xStart: 0.5, xStep: 1, histogram: true, autoscale: false, limits: { xMin: 0.8, xMax: 2.2 } });
    expect(view).toMatchObject({ yFirstIdx: 0, yLastIdx: 2 });
  });
});

describe("roiPlotSampleIndexAt", () => {
  it("finds the sample nearest the pointer within the view", () => {
    // View 102..106 over 100 px starting at px 50; data slice starts at x 102.
    const plot = { data: [0, 0, 0, 0, 0], xStart: 102, xStep: 1, xMin: 102, xMax: 106, padL: 50, padR: 10, width: 160 };
    expect(roiPlotSampleIndexAt(plot, 50)).toBe(0);
    expect(roiPlotSampleIndexAt(plot, 76)).toBe(1);
    expect(roiPlotSampleIndexAt(plot, 150)).toBe(4);
    expect(roiPlotSampleIndexAt({ data: [] }, 10)).toBe(-1);
  });
});
