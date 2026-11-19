import { renderRoiPlot } from "../modules/roi_plot_renderer.js";

function createMockContext() {
  return {
    fillStyle: "",
    strokeStyle: "",
    lineWidth: 1,
    textAlign: "",
    textBaseline: "",
    font: "",
    shadowColor: "",
    shadowBlur: 0,
    setTransform: vi.fn(),
    clearRect: vi.fn(),
    fillRect: vi.fn(),
    strokeRect: vi.fn(),
    beginPath: vi.fn(),
    moveTo: vi.fn(),
    lineTo: vi.fn(),
    stroke: vi.fn(),
    fillText: vi.fn(),
    save: vi.fn(),
    restore: vi.fn(),
    rect: vi.fn(),
    clip: vi.fn(),
    arc: vi.fn(),
    fill: vi.fn(),
    translate: vi.fn(),
    rotate: vi.fn(),
    measureText: vi.fn((text) => ({ width: String(text).length * 6 })),
  };
}

const PLOT_THEME = {
  bg: "#0b1020",
  frame: "#2b3a57",
  axis: "#6f88ad",
  grid: "rgba(159,190,236,0.22)",
  line: "#8fc4ff",
  lineGlow: "rgba(143,196,255,0.28)",
  text: "#d7e6ff",
  bar: "#bar-colour",
  peak: "#peak-colour",
  markerOutline: "#outline",
  axisHover: "#hover",
};

describe("roi_plot_renderer", () => {
  it("stores plot metadata for line series", () => {
    const canvasEl = { clientWidth: 300, clientHeight: 120, _roiPlotMeta: { xStart: 0, xStep: 1 } };
    const ctx = createMockContext();
    const limits = { xMin: null, xMax: null, yMin: null, yMax: null };
    renderRoiPlot({
      canvasEl,
      ctx,
      data: [1, 2, 3, 4],
      logScale: false,
      plotTheme: PLOT_THEME,
      getRoiPlotKey: () => "line",
      getRoiPlotLimits: () => limits,
      autoscale: true,
      formatRoiTick: (value) => value.toFixed(1),
    });
    expect(canvasEl._roiPlot).not.toBeNull();
    expect(canvasEl._roiPlot.xMin).toBe(0);
    expect(canvasEl._roiPlot.xMax).toBe(3);
  });

  // Box profiles are plotted against the image's own columns. With the X axis
  // pinned on one ROI and the box then moved to columns outside that window,
  // nothing was left to draw, and the plot went blank -- no line, no axes, no
  // hint -- for every box on that side of the image.
  it("draws the whole profile when the pinned x-range misses the data entirely", () => {
    const canvasEl = { clientWidth: 300, clientHeight: 120, _roiPlotMeta: { xStart: 453, xStep: 1 } };
    const ctx = createMockContext();
    const limits = { xMin: 100, xMax: 300, yMin: 0.9, yMax: 1.1 };
    renderRoiPlot({
      canvasEl,
      ctx,
      data: [0.87, 0.88, 0.86],
      logScale: false,
      plotTheme: PLOT_THEME,
      getRoiPlotKey: () => "x",
      getRoiPlotLimits: () => limits,
      autoscale: false,
      formatRoiTick: (value) => value.toFixed(2),
    });
    expect(canvasEl._roiPlot).not.toBeNull();
    expect(canvasEl._roiPlot.data).toEqual([0.87, 0.88, 0.86]);
    expect(canvasEl._roiPlot.xMin).toBe(453);
    expect(canvasEl._roiPlot.xMax).toBe(455);
    expect(ctx.lineTo).toHaveBeenCalled();
  });

  it("applies manual x-range limits when autoscale is disabled", () => {
    const canvasEl = { clientWidth: 300, clientHeight: 120, _roiPlotMeta: { xStart: 0, xStep: 1 } };
    const ctx = createMockContext();
    const limits = { xMin: 1, xMax: 2, yMin: null, yMax: null };
    renderRoiPlot({
      canvasEl,
      ctx,
      data: [10, 20, 30, 40],
      logScale: false,
      plotTheme: PLOT_THEME,
      getRoiPlotKey: () => "line",
      getRoiPlotLimits: () => limits,
      autoscale: false,
      formatRoiTick: (value) => value.toFixed(1),
    });
    expect(canvasEl._roiPlot.data).toEqual([20, 30]);
    expect(canvasEl._roiPlot.xMin).toBe(1);
    expect(canvasEl._roiPlot.xMax).toBe(2);
  });

  // The range used to be rounded inwards to whole samples, and the wheel took
  // its next step from that: zooming on a few samples lost two per notch
  // whatever the factor, zooming out rounded back to where it was, and the
  // point under the pointer drifted.
  it("draws exactly the x range asked for, between samples too", () => {
    const canvasEl = { clientWidth: 300, clientHeight: 120, _roiPlotMeta: { xStart: 0, xStep: 1 } };
    const ctx = createMockContext();
    const limits = { xMin: 1.4, xMax: 2.6, yMin: null, yMax: null };
    renderRoiPlot({
      canvasEl,
      ctx,
      data: [10, 20, 30, 40, 50],
      logScale: false,
      plotTheme: PLOT_THEME,
      getRoiPlotKey: () => "line",
      getRoiPlotLimits: () => limits,
      autoscale: false,
    });
    const plot = canvasEl._roiPlot;
    expect(plot.xMin).toBe(1.4);
    expect(plot.xMax).toBe(2.6);
    // One sample past each end, so the line runs to the frame (and is clipped).
    expect(plot.data).toEqual([20, 30, 40]);
    expect(ctx.clip).toHaveBeenCalled();
    const right = plot.width - plot.padR;
    const xs = ctx.lineTo.mock.calls.map(([x]) => x).filter((x) => x > right);
    expect(xs.length).toBeGreaterThan(0);
    // The y range comes from the one sample inside the view, not the extras.
    expect(plot.yMin).toBe(30);
  });

  it("gives y the autoscale headroom while only x is pinned", () => {
    const canvasEl = { clientWidth: 300, clientHeight: 120, _roiPlotMeta: { xStart: 0, xStep: 1 } };
    renderRoiPlot({
      canvasEl,
      ctx: createMockContext(),
      data: [10, 20, 30, 40, 50],
      logScale: false,
      plotTheme: PLOT_THEME,
      getRoiPlotKey: () => "line",
      getRoiPlotLimits: () => ({ xMin: 1.5, xMax: 3.5, yMin: null, yMax: null }),
      autoscale: false,
    });
    // Samples 30 and 40 in view, 3% of their range either side.
    expect(canvasEl._roiPlot.yMin).toBeCloseTo(29.7);
    expect(canvasEl._roiPlot.yMax).toBeCloseTo(40.3);
  });

  it("marks each sample once they are far enough apart, and not before", () => {
    const draw = (limits) => {
      const canvasEl = { clientWidth: 300, clientHeight: 120, _roiPlotMeta: { xStart: 0, xStep: 1 } };
      const ctx = createMockContext();
      renderRoiPlot({
        canvasEl,
        ctx,
        data: Array.from({ length: 200 }, (_, i) => i % 7),
        logScale: false,
        plotTheme: PLOT_THEME,
        getRoiPlotKey: () => "line",
        getRoiPlotLimits: () => limits,
        autoscale: false,
      });
      return ctx.arc.mock.calls.length;
    };
    expect(draw({ xMin: null, xMax: null })).toBe(0);
    // 10 samples across about 250 px, 25 px apart: 51..60 in view, plus 50
    // and 61 just past the ends (clipped at the frame).
    expect(draw({ xMin: 50.5, xMax: 60.5 })).toBe(12);
  });

  it("marks the sample under the pointer and draws the zoom box", () => {
    const canvasEl = {
      clientWidth: 300,
      clientHeight: 120,
      _roiPlotMeta: { xStart: 0, xStep: 1 },
      _roiHoverX: 2,
      _roiZoomBox: { x0: 60, y0: 20, x1: 120, y1: 70 },
    };
    const ctx = createMockContext();
    const fills = [];
    Object.defineProperty(ctx, "fillStyle", { set: (v) => fills.push(v), get: () => fills.at(-1) });
    renderRoiPlot({
      canvasEl,
      ctx,
      data: [1, 5, 3, 4],
      logScale: false,
      plotTheme: PLOT_THEME,
      getRoiPlotKey: () => "line",
      getRoiPlotLimits: () => ({ xMin: null, xMax: null, yMin: null, yMax: null }),
      autoscale: true,
    });
    const plot = canvasEl._roiPlot;
    const hoverPx = plot.padL + (2 / 3) * (plot.width - plot.padL - plot.padR);
    expect(ctx.arc.mock.calls.some(([x]) => Math.abs(x - hoverPx) < 0.01)).toBe(true);
    expect(ctx.fillRect).toHaveBeenCalledWith(60, 20, 60, 50);
    expect(fills).toContain("#hover");
  });

  it("draws histogram bars in the theme's bar colour", () => {
    const canvasEl = { clientWidth: 300, clientHeight: 120, _roiPlotMeta: { xStart: 0.5, xStep: 1, seriesType: "histogram" } };
    const ctx = createMockContext();
    const fills = [];
    Object.defineProperty(ctx, "fillStyle", { set: (v) => fills.push(v), get: () => fills.at(-1) });
    renderRoiPlot({
      canvasEl,
      ctx,
      data: [1, 2, 3],
      logScale: false,
      plotTheme: PLOT_THEME,
      getRoiPlotKey: () => "hist",
      getRoiPlotLimits: () => ({ xMin: null, xMax: null, yMin: null, yMax: null }),
      autoscale: true,
    });
    expect(fills).toContain("#bar-colour");
  });

  // The title took the baseline of whatever was drawn before it ("middle"
  // after the y ticks) and was centred 4 px above the bottom edge, so the
  // lower half of "X pixel" was cut off.
  it("draws the x axis title whole, above the bottom edge", () => {
    const canvasEl = { clientWidth: 300, clientHeight: 120, _roiPlotMeta: { xStart: 0, xStep: 1, xLabel: "X pixel" } };
    const ctx = createMockContext();
    let titleCall = null;
    ctx.fillText = vi.fn((text, x, y) => {
      if (text === "X pixel") titleCall = { y, baseline: ctx.textBaseline };
    });
    renderRoiPlot({
      canvasEl,
      ctx,
      data: [1, 2, 3, 4],
      logScale: false,
      plotTheme: PLOT_THEME,
      getRoiPlotKey: () => "x",
      getRoiPlotLimits: () => ({ xMin: null, xMax: null, yMin: null, yMax: null }),
      autoscale: true,
    });
    expect(titleCall.baseline).toBe("bottom");
    expect(titleCall.y).toBeLessThan(120);
    // Clear of the tick labels, which start 6 px below the axis.
    const ticksTop = canvasEl._roiPlot.height - canvasEl._roiPlot.padB + 6;
    expect(titleCall.y - 12).toBeGreaterThanOrEqual(ticksTop + 12);
  });

  it("spans each histogram bar across its bin", () => {
    const canvasEl = {
      clientWidth: 300,
      clientHeight: 120,
      _roiPlotMeta: { xStart: 0.5, xStep: 1, seriesType: "histogram" },
    };
    const ctx = createMockContext();
    renderRoiPlot({
      canvasEl,
      ctx,
      data: [1, 2, 3],
      logScale: false,
      plotTheme: PLOT_THEME,
      getRoiPlotKey: () => "hist",
      getRoiPlotLimits: () => ({ xMin: null, xMax: null, yMin: null, yMax: null }),
      autoscale: true,
    });
    const plot = canvasEl._roiPlot;
    // Bin centres 0.5, 1.5, 2.5: the axis runs from edge to edge, 0 to 3.
    expect(plot.xMin).toBe(0);
    expect(plot.xMax).toBe(3);
    const bars = ctx.fillRect.mock.calls.filter(([x, , w]) => x >= plot.padL && w < 200);
    expect(bars).toHaveLength(3);
    const binPx = (plot.width - plot.padL - plot.padR) / 3;
    expect(bars[0][0]).toBeCloseTo(plot.padL + 0.5);
    expect(bars[2][0]).toBeCloseTo(plot.padL + 2 * binPx + 0.5);
  });

  it("keeps histogram autoscale y-min at zero", () => {
    const canvasEl = {
      clientWidth: 300,
      clientHeight: 120,
      _roiPlotMeta: { xStart: 0, xStep: 1, seriesType: "histogram" },
    };
    const ctx = createMockContext();
    const limits = { xMin: null, xMax: null, yMin: null, yMax: null };
    renderRoiPlot({
      canvasEl,
      ctx,
      data: [0, 2, 4],
      logScale: false,
      plotTheme: PLOT_THEME,
      getRoiPlotKey: () => "hist",
      getRoiPlotLimits: () => limits,
      autoscale: true,
      formatRoiTick: (value) => value.toFixed(1),
    });
    expect(canvasEl._roiPlot.yMin).toBe(0);
    expect(canvasEl._roiPlot.totalYMin).toBe(0);
    expect(canvasEl._roiPlot.yMax).toBeGreaterThan(4);
  });

  /**
   * A line profile of photon counts had its axis padded below zero: a series
   * whose minimum was 0.18 was labelled down to -1.9, which reads as the
   * baseline dipping negative. The headroom is still applied, just never past
   * zero for a quantity that never goes there -- and a profile that really is
   * negative (module gaps are -1, bad pixels -2) must keep its negative axis.
   */
  function renderLine(data, autoscale = true) {
    const canvasEl = {
      clientWidth: 300,
      clientHeight: 120,
      _roiPlotMeta: { xStart: 0, xStep: 1 },
    };
    const limits = { xMin: null, xMax: null, yMin: null, yMax: null };
    renderRoiPlot({
      canvasEl,
      ctx: createMockContext(),
      data,
      logScale: false,
      plotTheme: PLOT_THEME,
      getRoiPlotKey: () => "line",
      getRoiPlotLimits: () => limits,
      autoscale,
      formatRoiTick: (value) => value.toFixed(1),
    });
    return canvasEl._roiPlot;
  }

  it("never pads a non-negative line profile below zero", () => {
    const plot = renderLine([0.18, 12, 70.5, 4]);
    expect(plot.yMin).toBe(0);
    // The headroom above is untouched.
    expect(plot.yMax).toBeGreaterThan(70.5);
  });

  it("leaves zero alone when the profile starts exactly at zero", () => {
    expect(renderLine([0, 5, 10]).yMin).toBe(0);
  });

  it("keeps the negative axis a profile over module gaps actually needs", () => {
    // -1 is a PILATUS module gap, -2 a bad pixel; both are real values here.
    const plot = renderLine([-2, -1, 40, 90]);
    expect(plot.yMin).toBeLessThan(-2);
  });

  /**
   * The axes were labelled at even fractions of the data range (0.043, 0.087;
   * columns 562, 941, 1320, 1699), and the last x label ran off the canvas.
   */
  it("labels both axes at round numbers and keeps x labels on the canvas", () => {
    const data = Array.from({ length: 1138 }, (_, i) => (i % 7) * 0.02);
    const canvasEl = { clientWidth: 480, clientHeight: 240, _roiPlotMeta: { xStart: 562, xStep: 1, xTickMode: "integer" } };
    const ctx = createMockContext();
    const limits = { xMin: null, xMax: null, yMin: null, yMax: null };
    renderRoiPlot({
      canvasEl,
      ctx,
      data,
      logScale: false,
      plotTheme: PLOT_THEME,
      getRoiPlotKey: () => "x",
      getRoiPlotLimits: () => limits,
      autoscale: true,
    });
    const drawn = ctx.fillText.mock.calls.map(([text, x]) => ({ text: String(text), x }));
    const labels = drawn.map((call) => call.text);
    expect(labels).toEqual(expect.arrayContaining(["0", "0.02", "0.04", "0.06", "0.08", "0.10", "0.12"]));
    expect(labels).toEqual(expect.arrayContaining(["600", "800", "1000", "1600"]));
    expect(labels).not.toContain("562");
    for (const { text, x } of drawn.filter((call) => /^\d{3,4}$/.test(call.text))) {
      expect(x + (text.length * 6) / 2).toBeLessThanOrEqual(480);
    }
  });

  it("puts log-axis ticks on zero and powers of ten", () => {
    const canvasEl = { clientWidth: 300, clientHeight: 240, _roiPlotMeta: { xStart: 0, xStep: 1 } };
    const ctx = createMockContext();
    const limits = { xMin: null, xMax: null, yMin: null, yMax: null };
    renderRoiPlot({
      canvasEl,
      ctx,
      data: [0, 5, 40000, 120],
      logScale: true,
      plotTheme: PLOT_THEME,
      getRoiPlotKey: () => "x",
      getRoiPlotLimits: () => limits,
      autoscale: true,
    });
    const labels = ctx.fillText.mock.calls.map(([text]) => String(text));
    expect(labels).toEqual(expect.arrayContaining(["0", "10", "100", "1000", "10000"]));
  });

  it("puts the resolution axis on round d values", () => {
    const canvasEl = {
      clientWidth: 480,
      clientHeight: 240,
      _roiPlotMeta: { xStart: 10, xStep: 1, dSpacingForX: (r) => 600 / r },
    };
    const ctx = createMockContext();
    const limits = { xMin: null, xMax: null, yMin: null, yMax: null };
    renderRoiPlot({
      canvasEl,
      ctx,
      data: Array.from({ length: 491 }, (_, i) => i),
      logScale: false,
      plotTheme: PLOT_THEME,
      getRoiPlotKey: () => "radial",
      getRoiPlotLimits: () => limits,
      autoscale: true,
      resolutionAxisUnit: "d",
    });
    const labels = ctx.fillText.mock.calls.map(([text]) => String(text));
    expect(labels).toEqual(expect.arrayContaining(["10", "5", "4", "3", "2"]));
  });

  it("clamps manual histogram y-min limits to zero", () => {
    const canvasEl = {
      clientWidth: 300,
      clientHeight: 120,
      _roiPlotMeta: { xStart: 0, xStep: 1, seriesType: "histogram" },
    };
    const ctx = createMockContext();
    const limits = { xMin: null, xMax: null, yMin: -5, yMax: 2 };
    renderRoiPlot({
      canvasEl,
      ctx,
      data: [0, 2, 4],
      logScale: false,
      plotTheme: PLOT_THEME,
      getRoiPlotKey: () => "hist",
      getRoiPlotLimits: () => limits,
      autoscale: false,
      formatRoiTick: (value) => value.toFixed(1),
    });
    expect(canvasEl._roiPlot.yMin).toBe(0);
    expect(canvasEl._roiPlot.yMax).toBe(2);
  });
});
