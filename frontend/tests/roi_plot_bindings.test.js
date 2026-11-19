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
  const backBtn = document.createElement("button");
  backBtn.dataset.roiViewBack = "";
  backBtn.className = "is-hidden";
  container.append(backBtn, canvas, resizeHandle);
  document.body.append(container);
  return container;
}

function createBindings(overrides = {}) {
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
    redrawRoiPlots: vi.fn(),
    clearRoiPlotLimitsForKey: vi.fn(),
    hasManualRoiPlotLimits: vi.fn(() => false),
    hasAnyManualRoiPlotLimits: vi.fn(() => false),
    normalizeWheelDelta: vi.fn(() => 0),
    ...overrides,
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
    expect(callbacks.redrawRoiPlots).toHaveBeenCalledTimes(1);
  });

  it("applies and resets per-plot manual axis limits", () => {
    document.body.innerHTML = `
      <div class="roi-plot" id="roi-line-plot">
        <button class="roi-axis-limits-toggle" aria-expanded="false" aria-controls="roi-line-axis-popover"></button>
        <button type="button" data-roi-axis-chip class="roi-axis-limit-chip is-hidden"></button>
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

    chip.click();
    expect(limits).toEqual({ xMin: null, xMax: null, yMin: null, yMax: null });
    expect(chip.classList.contains("is-hidden")).toBe(false);
    // The tag carries the key of its state, so a language switch
    // (applyI18nToDom) retranslates it in place, and says what a click does.
    expect(chip.dataset.i18n).toBe("roi.axis.auto_chip");
    expect(chip.dataset.i18nTitle).toBe("roi.axis.chip_hint.auto");
    chip.click();
    expect(limits).toEqual({ xMin: 0, xMax: 9, yMin: 1, yMax: 5 });
    expect(chip.dataset.i18n).toBe("roi.axis.manual_chip");
    expect(chip.dataset.i18nTitle).toBe("roi.axis.chip_hint.manual");

    resetBtn.click();
    expect(limits).toEqual({ xMin: null, xMax: null, yMin: null, yMax: null });
    expect(roiState.plotLimits.autoscale).toBe(true);
    expect(autoToggle.checked).toBe(true);
    expect(chip.classList.contains("is-hidden")).toBe(true);
    expect(redrawRoiPlots).toHaveBeenCalled();
  });

  describe("dragging on an axis", () => {
    // 240 x 120 canvas: data area x 40..232, y 8..90 (192 x 82 px).
    const shownPlot = () => ({
      xMin: 0,
      xMax: 100,
      yMin: 0,
      yMax: 50,
      totalXMin: 0,
      totalXMax: 1000,
      totalYMin: 0,
      totalYMax: 500,
      padL: 40,
      padR: 8,
      padT: 8,
      padB: 30,
      width: 240,
      height: 120,
    });

    function pointer(type, clientX, clientY) {
      const event = new window.MouseEvent(type, { bubbles: true, cancelable: true, clientX, clientY, button: 0 });
      Object.defineProperty(event, "pointerId", { value: 1 });
      return event;
    }

    function setup() {
      const limits = { xMin: null, xMax: null, yMin: null, yMax: null };
      const bindings = createBindings({
        getRoiPlotLimits: vi.fn(() => limits),
        setRoiPlotAxisLimits: vi.fn((key, axis, min, max) => setAxisLimits(limits, axis, min, max)),
        clearRoiPlotLimitsForKey: vi.fn(() => clearAxisLimits(limits)),
        hasManualRoiPlotLimits: vi.fn(() => hasManualLimits(limits)),
        hasAnyManualRoiPlotLimits: vi.fn(() => hasManualLimits(limits)),
      });
      bindings.roiLineCanvas._roiPlot = shownPlot();
      return { ...bindings, limits };
    }

    function drag(canvas, from, to) {
      canvas.dispatchEvent(pointer("pointerdown", ...from));
      canvas.dispatchEvent(pointer("pointermove", ...to));
      canvas.dispatchEvent(pointer("pointerup", ...to));
    }

    it("moves only x when dragged on the x axis", () => {
      const { roiLineCanvas, limits } = setup();
      // 96 px right is half the 192 px wide data area; the 40 px down is ignored.
      drag(roiLineCanvas, [100, 100], [196, 140]);
      expect(limits.xMin).toBeCloseTo(0);
      expect(limits.xMax).toBeCloseTo(100);
      drag(roiLineCanvas, [196, 100], [100, 60]);
      expect(limits.xMin).toBeCloseTo(50);
      expect(limits.xMax).toBeCloseTo(150);
      expect(limits.yMin).toBe(0);
      expect(limits.yMax).toBe(50);
    });

    it("moves only y when dragged on the y axis", () => {
      const { roiLineCanvas, limits } = setup();
      // 41 px up is half the 82 px tall data area: the curve moves up, so the
      // view moves down by half, past the bottom of the data.
      drag(roiLineCanvas, [20, 60], [80, 19]);
      expect(limits.yMin).toBeCloseTo(-25);
      expect(limits.yMax).toBeCloseTo(25);
      drag(roiLineCanvas, [20, 19], [80, 60]);
      drag(roiLineCanvas, [20, 19], [80, 60]);
      expect(limits.yMin).toBeCloseTo(25);
      expect(limits.yMax).toBeCloseTo(75);
      expect(limits.xMin).toBe(0);
      expect(limits.xMax).toBe(100);
    });

    // An autoscaled y range is a little wider than the data. y was kept
    // inside the data, so a drag could only snap it onto the data: the
    // curve rescaled slightly ("clicking resets the scale") and never moved.
    it("moves y from the autoscaled view instead of snapping it to the data", () => {
      const { roiLineCanvas, limits } = setup();
      Object.assign(roiLineCanvas._roiPlot, { yMin: -1.5, yMax: 51.5, totalYMin: 0, totalYMax: 50 });
      // 41 px down is half the 82 px tall data area, on the axis and in the plot.
      drag(roiLineCanvas, [20, 19], [20, 60]);
      expect(limits.yMin).toBeCloseTo(25);
      expect(limits.yMax).toBeCloseTo(78);
      roiLineCanvas._roiPlot.yMin = limits.yMin;
      roiLineCanvas._roiPlot.yMax = limits.yMax;
      drag(roiLineCanvas, [100, 60], [100, 19]);
      expect(limits.yMin).toBeCloseTo(-1.5);
      expect(limits.yMax).toBeCloseTo(51.5);
    });

    it("shows which axis a drag would move", () => {
      const { roiLineCanvas, callbacks } = setup();
      roiLineCanvas.dispatchEvent(pointer("pointermove", 100, 100));
      expect(roiLineCanvas.dataset.panZone).toBe("x");
      expect(callbacks.redrawRoiPlots).toHaveBeenCalledTimes(1);
      roiLineCanvas.dispatchEvent(pointer("pointermove", 20, 50));
      expect(roiLineCanvas.dataset.panZone).toBe("y");
      roiLineCanvas.dispatchEvent(pointer("pointermove", 100, 50));
      expect(roiLineCanvas.dataset.panZone).toBe("plot");
      roiLineCanvas.dispatchEvent(new window.MouseEvent("mouseleave"));
      expect(roiLineCanvas.dataset.panZone).toBeUndefined();
    });

    // Each notch used to start from the drawn range, which the renderer had
    // rounded to whole samples, so on a short range zooming lost samples per
    // notch, zooming out stuck, and the point under the pointer drifted.
    it("zooms a short x range in and back out to where it was, about the pointer", () => {
      const limits = { xMin: 20, xMax: 26, yMin: null, yMax: null };
      const { roiLineCanvas } = createBindings({
        getRoiPlotLimits: vi.fn(() => limits),
        setRoiPlotAxisLimits: vi.fn((key, axis, min, max) => setAxisLimits(limits, axis, min, max)),
        normalizeWheelDelta: vi.fn((event) => event.deltaY),
      });
      // The drawn range is stale on purpose: the wheel must not read it.
      roiLineCanvas._roiPlot = { ...shownPlot(), xMin: 21, xMax: 25, xStep: 1 };
      // Pointer a quarter of the way along the 192 px data area: x = 21.5.
      const wheel = (deltaY) =>
        roiLineCanvas.dispatchEvent(new window.WheelEvent("wheel", { clientX: 88, clientY: 100, deltaY, cancelable: true }));
      const at = () => limits.xMin + 0.25 * (limits.xMax - limits.xMin);
      for (let i = 0; i < 3; i += 1) wheel(-100);
      expect(limits.xMax - limits.xMin).toBeCloseTo(6 * Math.exp(-0.6));
      expect(at()).toBeCloseTo(21.5);
      for (let i = 0; i < 3; i += 1) wheel(100);
      expect(limits.xMin).toBeCloseTo(20);
      expect(limits.xMax).toBeCloseTo(26);
    });

    it("zooms into a box dragged with Shift", () => {
      const { roiLineCanvas, limits } = setup();
      const down = pointer("pointerdown", 40, 49);
      Object.defineProperty(down, "shiftKey", { value: true });
      roiLineCanvas.dispatchEvent(down);
      roiLineCanvas.dispatchEvent(pointer("pointermove", 136, 90));
      expect(roiLineCanvas._roiZoomBox).toEqual({ x0: 40, y0: 49, x1: 136, y1: 90 });
      roiLineCanvas.dispatchEvent(pointer("pointerup", 136, 90));
      expect(roiLineCanvas._roiZoomBox).toBeNull();
      expect(limits.xMin).toBeCloseTo(0);
      expect(limits.xMax).toBeCloseTo(50);
      expect(limits.yMin).toBeCloseTo(0);
      expect(limits.yMax).toBeCloseTo(25);
    });

    it("pans x on a sideways swipe and keeps y where it is", () => {
      const { roiLineCanvas, limits } = setup();
      roiLineCanvas.dispatchEvent(
        new window.WheelEvent("wheel", { clientX: 100, clientY: 50, deltaX: 96, deltaY: 2, cancelable: true })
      );
      // 96 px is half the data area: half of the 0..100 view to the right.
      expect(limits.xMin).toBeCloseTo(50);
      expect(limits.xMax).toBeCloseTo(150);
      expect(limits.yMin).toBe(0);
      expect(limits.yMax).toBe(50);
    });

    it("marks the sample under the pointer and forgets it on leaving", () => {
      const { roiLineCanvas, callbacks } = setup();
      Object.assign(roiLineCanvas._roiPlot, { data: new Array(101).fill(1), xStart: 0, xStep: 1 });
      // x 0..100 over 192 px from px 40: px 136 is x 50.
      roiLineCanvas.dispatchEvent(new window.MouseEvent("mousemove", { clientX: 136, clientY: 50 }));
      expect(roiLineCanvas._roiHoverX).toBe(50);
      roiLineCanvas.dispatchEvent(new window.MouseEvent("mouseleave"));
      expect(roiLineCanvas._roiHoverX).toBeNull();
      expect(callbacks.redrawRoiPlots).toHaveBeenCalled();
    });

    it("goes back to the previous view after a reset or a zoom", () => {
      const { roiLineCanvas, limits } = setup();
      const back = roiLineCanvas.parentElement.querySelector("[data-roi-view-back]");
      expect(back.classList.contains("is-hidden")).toBe(true);
      drag(roiLineCanvas, [196, 100], [100, 60]);
      const zoomed = { ...limits };
      roiLineCanvas.dispatchEvent(new window.MouseEvent("dblclick", { bubbles: true, clientX: 100, clientY: 50 }));
      expect(limits.xMin).toBeNull();
      expect(back.classList.contains("is-hidden")).toBe(false);
      back.click();
      expect(limits).toEqual(zoomed);
      back.click();
      expect(limits).toEqual({ xMin: null, xMax: null, yMin: null, yMax: null });
      expect(back.classList.contains("is-hidden")).toBe(true);
    });

    it("resets only the axis that is double-clicked", () => {
      const { roiLineCanvas, limits, callbacks } = setup();
      setAxisLimits(limits, "x", 10, 20);
      setAxisLimits(limits, "y", 1, 2);
      roiLineCanvas.dispatchEvent(new window.MouseEvent("dblclick", { bubbles: true, clientX: 20, clientY: 50 }));
      expect(limits).toEqual({ xMin: 10, xMax: 20, yMin: null, yMax: null });
      expect(callbacks.clearRoiPlotLimitsForKey).not.toHaveBeenCalled();
      roiLineCanvas.dispatchEvent(new window.MouseEvent("dblclick", { bubbles: true, clientX: 100, clientY: 50 }));
      expect(limits).toEqual({ xMin: null, xMax: null, yMin: null, yMax: null });
    });
  });
});
