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
    getRoiPlotLog: vi.fn(() => false),
    setRoiPlotLog: vi.fn(),
    setRoiPlotAxisLimits: vi.fn(),
    syncRoiPlotLimitControls: vi.fn(),
    redrawRoiPlots: vi.fn(),
    clearRoiPlotLimitsForKey: vi.fn(),
    hasManualRoiPlotLimits: vi.fn(() => false),
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

function setAxisLimits(limits, axis, minValue, maxValue) {
  const minKey = axis === "x" ? "xMin" : "yMin";
  const maxKey = axis === "x" ? "xMax" : "yMax";
  limits[minKey] = Number.isFinite(minValue) ? minValue : null;
  limits[maxKey] = Number.isFinite(maxValue) ? maxValue : null;
  if (limits[minKey] !== null && limits[maxKey] !== null && limits[minKey] > limits[maxKey]) {
    [limits[minKey], limits[maxKey]] = [limits[maxKey], limits[minKey]];
  }
}

function clearAxisLimits(limits) {
  setAxisLimits(limits, "x", null, null);
  setAxisLimits(limits, "y", null, null);
}

function hasManualLimits(limits) {
  return (
    Number.isFinite(limits.xMin) ||
    Number.isFinite(limits.xMax) ||
    Number.isFinite(limits.yMin) ||
    Number.isFinite(limits.yMax)
  );
}

describe("roi_plot_bindings", () => {
  let resizeObserverInstances = [];
  let requestAnimationFrameSpy;
  let warnSpy;

  beforeEach(() => {
    warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
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
    warnSpy.mockRestore();
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

  it("applies and resets per-plot manual axis limits", () => {
    document.body.innerHTML = `
      <div class="roi-plot" id="roi-line-plot">
        <button class="roi-axis-limits-toggle" aria-expanded="false" aria-controls="roi-line-axis-popover"></button>
        <span data-roi-axis-chip class="roi-axis-limit-chip is-hidden"></span>
        <div class="roi-axis-limits-popover" id="roi-line-axis-popover" aria-hidden="true">
          <input type="checkbox" data-roi-plot-log />
          <input type="checkbox" data-roi-axis-auto checked />
          <div class="roi-axis-limits-grid">
            <input data-axis="x" data-bound="min" />
            <input data-axis="x" data-bound="max" />
            <input data-axis="y" data-bound="min" />
            <input data-axis="y" data-bound="max" />
          </div>
          <button data-roi-axis-reset></button>
        </div>
      </div>
      <canvas id="roi-line-canvas"></canvas>
    `;
    const canvas = document.getElementById("roi-line-canvas");
    canvas._roiPlot = {
      xMin: 0,
      xMax: 9,
      yMin: 1,
      yMax: 5,
      padL: 10,
      padR: 10,
      padT: 5,
      padB: 20,
      width: 200,
      height: 100,
    };
    canvas.getBoundingClientRect = () => ({ left: 0, top: 0, width: 200, height: 100 });

    const roiState = {
      plotLimits: {
        autoscale: true,
        line: { xMin: null, xMax: null, yMin: null, yMax: null },
      },
      plotLog: {
        line: false,
      },
    };
    const limits = roiState.plotLimits.line;
    const redrawRoiPlots = vi.fn();

    bindRoiPlotInteractions({
      roiState,
      elements: {
        roiLineCanvas: canvas,
        roiLinePlot: document.getElementById("roi-line-plot"),
      },
      callbacks: {
        updateRoiTooltip: vi.fn(),
        hideRoiTooltip: vi.fn(),
        getRoiPlotKey: () => "line",
        getRoiPlotLimits: () => limits,
        getRoiPlotLog: () => Boolean(roiState.plotLog.line),
        setRoiPlotLog: (_plotKey, enabled) => {
          roiState.plotLog.line = Boolean(enabled);
        },
        setRoiPlotAxisLimits: (_plotKey, axis, minValue, maxValue) =>
          setAxisLimits(limits, axis, minValue, maxValue),
        syncRoiPlotLimitControls: vi.fn(),
        redrawRoiPlots,
        clearRoiPlotLimitsForKey: () => clearAxisLimits(limits),
        hasManualRoiPlotLimits: () => hasManualLimits(limits),
        hasAnyManualRoiPlotLimits: () => hasManualLimits(limits),
        normalizeWheelDelta: () => 0,
      },
    });

    const toggle = document.querySelector(".roi-axis-limits-toggle");
    const popover = document.getElementById("roi-line-axis-popover");
    const logToggle = document.querySelector("[data-roi-plot-log]");
    const autoToggle = document.querySelector("[data-roi-axis-auto]");
    const chip = document.querySelector("[data-roi-axis-chip]");
    const resetBtn = document.querySelector("[data-roi-axis-reset]");
    const [xMinInput] = document.querySelectorAll(".roi-axis-limits-grid input");

    toggle.click();
    expect(toggle.getAttribute("aria-expanded")).toBe("true");
    expect(popover.classList.contains("is-open")).toBe(true);
    expect(popover.parentElement).toBe(document.body);
    expect(popover.style.left).not.toBe("");
    expect(popover.style.top).not.toBe("");
    expect(popover.style.maxHeight).not.toBe("");

    logToggle.checked = true;
    logToggle.dispatchEvent(new Event("change", { bubbles: true }));
    expect(roiState.plotLog.line).toBe(true);

    xMinInput.value = "2.5";
    xMinInput.dispatchEvent(new Event("input", { bubbles: true }));
    expect(limits.xMin).toBe(2.5);
    expect(limits.xMax).toBeNull();
    expect(roiState.plotLimits.autoscale).toBe(false);
    expect(autoToggle.checked).toBe(false);
    expect(chip.classList.contains("is-hidden")).toBe(false);

    autoToggle.checked = false;
    autoToggle.dispatchEvent(new Event("change", { bubbles: true }));
    expect(limits).toEqual({ xMin: 0, xMax: 9, yMin: 1, yMax: 5 });

    resetBtn.click();
    expect(limits).toEqual({ xMin: null, xMax: null, yMin: null, yMax: null });
    expect(roiState.plotLimits.autoscale).toBe(true);
    expect(autoToggle.checked).toBe(true);
    expect(chip.classList.contains("is-hidden")).toBe(true);
    expect(redrawRoiPlots).toHaveBeenCalled();
  });
});
