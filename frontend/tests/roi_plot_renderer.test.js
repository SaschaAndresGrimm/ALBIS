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
});
