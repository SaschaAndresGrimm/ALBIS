import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { bindRoiPlotInteractions } from "../modules/roi_plot_bindings.js";

function createCanvas(id) {
  const canvas = document.createElement("canvas");
  canvas.id = id;
  canvas.getBoundingClientRect = () => ({
    left: 0,
    top: 0,
    width: 240,
    height: 120,
  });
  return canvas;
}

function createPlotContainer(id, canvas) {
  const container = document.createElement("div");
  container.id = id;
  const resizeHandle = document.createElement("div");
  resizeHandle.className = "roi-resize-handle";
  container.append(canvas, resizeHandle);
  document.body.append(container);
  return container;
}

function createBindings() {
  const roiLineCanvas = createCanvas("roi-line-canvas");
  const roiXCanvas = createCanvas("roi-x-canvas");
  const roiYCanvas = createCanvas("roi-y-canvas");
  const roiHistCanvas = createCanvas("roi-hist-canvas");
  const roiLinePlot = createPlotContainer("roi-line-plot", roiLineCanvas);
  const roiBoxPlotX = createPlotContainer("roi-box-plot-x", roiXCanvas);
  const roiBoxPlotY = createPlotContainer("roi-box-plot-y", roiYCanvas);
  const roiHistogramPlot = createPlotContainer("roi-hist-plot", roiHistCanvas);

  const callbacks = {
    updateRoiTooltip: vi.fn(),
    hideRoiTooltip: vi.fn(),
    getRoiPlotKey: vi.fn((canvasEl) => {
      if (canvasEl === roiLineCanvas) return "line";
      if (canvasEl === roiXCanvas) return "x";
      if (canvasEl === roiYCanvas) return "y";
      if (canvasEl === roiHistCanvas) return "hist";
      return "line";
    }),
    getRoiPlotLimits: vi.fn(() => ({ xMin: null, xMax: null, yMin: null, yMax: null })),
    setRoiPlotAxisLimits: vi.fn(),
    syncRoiPlotLimitControls: vi.fn(),
    redrawRoiPlots: vi.fn(),
    clearRoiPlotLimitsForKey: vi.fn(),
    hasAnyManualRoiPlotLimits: vi.fn(() => false),
    normalizeWheelDelta: vi.fn(() => 0),
  };

  bindRoiPlotInteractions({
    roiState: {
      plotLimits: {
        autoscale: true,
      },
    },
    elements: {
      roiLineCanvas,
      roiXCanvas,
      roiYCanvas,
      roiHistCanvas,
      roiLinePlot,
      roiBoxPlotX,
      roiBoxPlotY,
      roiHistogramPlot,
      roiLimitsEnable: document.createElement("input"),
    },
    callbacks,
  });

  return {
    roiLineCanvas,
    roiXCanvas,
    roiYCanvas,
    roiHistCanvas,
    callbacks,
  };
}

describe("roi_plot_bindings", () => {
  let resizeObserverInstances = [];
  let requestAnimationFrameSpy;

  beforeEach(() => {
    resizeObserverInstances = [];
    window.ResizeObserver = class ResizeObserver {
      constructor(callback) {
        this.callback = callback;
        this.observe = vi.fn();
        this.disconnect = vi.fn();
        resizeObserverInstances.push(this);
      }
    };
    requestAnimationFrameSpy = vi
      .spyOn(window, "requestAnimationFrame")
      .mockImplementation((callback) => {
        callback(0);
        return 1;
      });
  });

  afterEach(() => {
    requestAnimationFrameSpy.mockRestore();
    document.body.innerHTML = "";
    delete window.ResizeObserver;
  });

  it("redraws ROI plots when the observed canvas size changes", () => {
    const { roiLineCanvas, roiXCanvas, roiYCanvas, roiHistCanvas, callbacks } = createBindings();

    expect(resizeObserverInstances).toHaveLength(1);
    const observer = resizeObserverInstances[0];
    expect(observer.observe).toHaveBeenCalledTimes(4);
    expect(observer.observe).toHaveBeenNthCalledWith(1, roiLineCanvas);
    expect(observer.observe).toHaveBeenNthCalledWith(2, roiXCanvas);
    expect(observer.observe).toHaveBeenNthCalledWith(3, roiYCanvas);
    expect(observer.observe).toHaveBeenNthCalledWith(4, roiHistCanvas);

    observer.callback([{ target: roiLineCanvas }, { target: roiXCanvas }]);

    expect(callbacks.redrawRoiPlots).toHaveBeenCalledTimes(1);
  });

  it("uses redraw-only refresh for plot double-click resets", () => {
    const { roiLineCanvas, callbacks } = createBindings();

    roiLineCanvas.dispatchEvent(new window.MouseEvent("dblclick", { bubbles: true }));

    expect(callbacks.clearRoiPlotLimitsForKey).toHaveBeenCalledWith("line");
    expect(callbacks.syncRoiPlotLimitControls).toHaveBeenCalledTimes(1);
    expect(callbacks.redrawRoiPlots).toHaveBeenCalledTimes(1);
  });
});
