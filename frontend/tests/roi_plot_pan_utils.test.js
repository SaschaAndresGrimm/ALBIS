import { describe, expect, it } from "vitest";

import { panAxisRange, roiPlotHitZone, roiPlotPanMode, zoomAxisRange } from "../modules/roi_plot_pan_utils.js";

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

describe("zoomAxisRange", () => {
  it("keeps the value under the pointer where it is", () => {
    const next = zoomAxisRange({ min: 0, max: 100 }, 0.25, 2);
    expect(next.min).toBeCloseTo(12.5);
    expect(next.max).toBeCloseTo(62.5);
  });

  it("zooms by the same proportion on a short range as on a long one", () => {
    const long = zoomAxisRange({ min: 0, max: 1000 }, 0.5, 1.25);
    const short = zoomAxisRange({ min: 100, max: 106 }, 0.5, 1.25);
    expect(long.max - long.min).toBeCloseTo(800);
    expect(short.max - short.min).toBeCloseTo(4.8);
    const back = zoomAxisRange(short, 0.5, 1 / 1.25);
    expect(back.min).toBeCloseTo(100);
    expect(back.max).toBeCloseTo(106);
  });

  it("stops zooming in at the smallest span and still zooms out from it", () => {
    const stopped = zoomAxisRange({ min: 10, max: 12.2, minSpan: 2 }, 0.5, 4);
    expect(stopped.max - stopped.min).toBeCloseTo(2);
    expect(zoomAxisRange({ min: 10, max: 11, minSpan: 2 }, 0.5, 2)).toEqual({ min: 10, max: 11 });
    const out = zoomAxisRange({ min: 10, max: 11, minSpan: 2 }, 0.5, 0.5);
    expect(out.max - out.min).toBeCloseTo(2);
  });

  it("stops at about a millionth of the values shown", () => {
    const next = zoomAxisRange({ min: 1000, max: 1000.01 }, 0.5, 1e6);
    expect(next.max - next.min).toBeCloseTo(1000.01e-6, 9);
  });

  it("zooms out no further than the data and keeps the range inside it", () => {
    expect(zoomAxisRange({ min: 10, max: 20, domainMin: 0, domainMax: 30 }, 0.5, 0.1)).toEqual({ min: 0, max: 30 });
    const edge = zoomAxisRange({ min: 0, max: 10, domainMin: 0, domainMax: 100 }, 0, 0.5);
    expect(edge).toEqual({ min: 0, max: 20 });
  });

  it("zooms a log axis about the drawn position", () => {
    // 0..99 is 0..2 drawn; zooming in 2x about the middle (drawn 1, value 9).
    const next = zoomAxisRange({ min: 0, max: 99, log: true }, 0.5, 2);
    expect(next.min).toBeCloseTo(10 ** 0.5 - 1);
    expect(next.max).toBeCloseTo(10 ** 1.5 - 1);
  });
});
