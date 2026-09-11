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
