import { describe, expect, it } from "vitest";

import { panAxisRange, roiPlotHitZone, roiPlotPanMode } from "../modules/roi_plot_pan_utils.js";

const plot = { padL: 40, padR: 8, padT: 8, padB: 30, width: 240, height: 120 };

describe("roiPlotHitZone", () => {
  it("tells the plot area from the two axis strips", () => {
    expect(roiPlotHitZone(plot, 100, 50)).toBe("plot");
    expect(roiPlotHitZone(plot, 20, 50)).toBe("y-axis");
    expect(roiPlotHitZone(plot, 100, 100)).toBe("x-axis");
  });

  it("leaves the corner between the axes and the margins to nobody", () => {
    expect(roiPlotHitZone(plot, 20, 100)).toBeNull();
    expect(roiPlotHitZone(plot, 236, 100)).toBeNull();
    expect(roiPlotHitZone(plot, 100, 4)).toBeNull();
    expect(roiPlotHitZone(null, 100, 50)).toBeNull();
  });

  it("maps zones to the axes a drag moves", () => {
    expect(roiPlotPanMode("plot")).toBe("both");
    expect(roiPlotPanMode("x-axis")).toBe("x");
    expect(roiPlotPanMode("y-axis")).toBe("y");
    expect(roiPlotPanMode(null)).toBeNull();
  });
});

describe("panAxisRange", () => {
  it("shifts by a fraction of the span", () => {
    expect(panAxisRange({ min: 10, max: 20 }, 0.5)).toEqual({ min: 5, max: 15 });
    expect(panAxisRange({ min: 10, max: 20 }, -0.2)).toEqual({ min: 12, max: 22 });
  });

  it("stops at the ends of the data", () => {
    expect(panAxisRange({ min: 10, max: 20, domainMin: 0, domainMax: 100 }, 2)).toEqual({ min: 0, max: 10 });
    expect(panAxisRange({ min: 80, max: 95, domainMin: 0, domainMax: 100 }, -1)).toEqual({ min: 85, max: 100 });
  });

  it("shows the whole domain when the view is wider than the data", () => {
    expect(panAxisRange({ min: -5, max: 120, domainMin: 0, domainMax: 100 }, 0.1)).toEqual({ min: 0, max: 100 });
  });

  it("moves a log axis by the same distance on screen at any height", () => {
    // 0..99 spans 2 decades of log10(1 + v); half a span is one decade.
    const up = panAxisRange({ min: 0, max: 99, log: true }, -0.5);
    expect(up.min).toBeCloseTo(9);
    expect(up.max).toBeCloseTo(999);
  });

  it("refuses a range without a span", () => {
    expect(panAxisRange({ min: 5, max: 5 }, 0.5)).toBeNull();
    expect(panAxisRange({ min: Number.NaN, max: 5 }, 0.5)).toBeNull();
  });
});
