import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createRoiInteractionController } from "../modules/roi_interaction_controller.js";

describe("roi_interaction_controller", () => {
  beforeEach(() => {
    vi.spyOn(window, "requestAnimationFrame").mockImplementation((callback) => {
      callback(0);
      return 1;
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("allows moving circular ROIs so the center can move off-image", () => {
    const state = { width: 200, height: 200, zoom: 1, renderOffsetX: 0, renderOffsetY: 0 };
    const roiState = {
      enabled: true,
      active: true,
      mode: "circle",
      start: { x: 80, y: 80 },
      end: { x: 120, y: 80 },
      innerRadius: 0,
      outerRadius: 40,
    };
    const canvasWrap = document.createElement("div");
    const controller = createRoiInteractionController({
      state,
      roiState,
      elements: {
        canvasWrap,
        roiOverlay: null,
        roiCtx: null,
        roiRadiusInput: document.createElement("input"),
        roiInnerInput: document.createElement("input"),
        roiOuterInput: document.createElement("input"),
      },
      callbacks: {
        getEffectiveScrollLeft: () => 0,
        getEffectiveScrollTop: () => 0,
        syncOverlayCanvas: () => null,
        updateRoiCenterInputs: vi.fn(),
        updateRoiStats: vi.fn(),
      },
    });

    controller.startRoiEdit("move", { x: 80, y: 80 });
    controller.applyRoiEdit({ x: 80, y: -20 });

    expect(roiState.start).toEqual({ x: 80, y: -20 });
    expect(roiState.end).toEqual({ x: 120, y: -20 });
    expect(roiState.outerRadius).toBe(40);
  });

  it("exposes the outer handle on the visible arc when the stored endpoint is off-image", () => {
    const state = { width: 100, height: 100, zoom: 1, renderOffsetX: 0, renderOffsetY: 0 };
    const roiState = {
      enabled: true,
      active: true,
      mode: "circle",
      start: { x: 50, y: -10 },
      end: { x: 80, y: -10 },
      innerRadius: 0,
      outerRadius: 30,
    };
    const canvasWrap = document.createElement("div");
    canvasWrap.getBoundingClientRect = () => ({ left: 0, top: 0, width: 100, height: 100 });
    const controller = createRoiInteractionController({
      state,
      roiState,
      elements: {
        canvasWrap,
        roiOverlay: null,
        roiCtx: null,
        roiRadiusInput: document.createElement("input"),
        roiInnerInput: document.createElement("input"),
        roiOuterInput: document.createElement("input"),
      },
      callbacks: {
        getEffectiveScrollLeft: () => 0,
        getEffectiveScrollTop: () => 0,
        syncOverlayCanvas: () => null,
        updateRoiCenterInputs: vi.fn(),
        updateRoiStats: vi.fn(),
      },
    });

    const handle = controller.getRoiHandleAt({ clientX: 78, clientY: 0 });

    expect(handle).toBe("outer");
  });

  it("gives a box a centre handle that moves it", () => {
    const state = { width: 200, height: 200, zoom: 1, renderOffsetX: 0, renderOffsetY: 0 };
    const roiState = { enabled: true, active: true, mode: "box", start: { x: 20, y: 40 }, end: { x: 120, y: 100 } };
    const canvasWrap = document.createElement("div");
    canvasWrap.getBoundingClientRect = () => ({ left: 0, top: 0, width: 200, height: 200 });
    const controller = createRoiInteractionController({
      state,
      roiState,
      elements: { canvasWrap, roiOverlay: null, roiCtx: null },
      callbacks: {
        getEffectiveScrollLeft: () => 0,
        getEffectiveScrollTop: () => 0,
        syncOverlayCanvas: () => null,
        updateRoiCenterInputs: vi.fn(),
        updateRoiStats: vi.fn(),
      },
    });
    const at = (x, y) => ({ clientX: x, clientY: y });
    // The box spans 20..120 x 40..100 on screen: its centre is 70, 70.
    expect(controller.getRoiHandleAt(at(70, 70))).toBe("center");
    expect(controller.getRoiHandleAt(at(60, 60))).toBeNull();
    controller.startRoiEdit("center", { x: 70, y: 70 });
    controller.applyRoiEdit({ x: 80, y: 75 });
    expect(roiState.start).toEqual({ x: 30, y: 45 });
    expect(roiState.end).toEqual({ x: 130, y: 105 });
  });

  it("updates slow statistics less often during a drag, and always for the last change", () => {
    vi.useFakeTimers();
    let clock = 1000;
    vi.spyOn(performance, "now").mockImplementation(() => clock);
    const seen = [];
    const roiState = { enabled: true, active: true, mode: "circle", start: { x: 50, y: 50 }, end: { x: 90, y: 50 } };
    const controller = createRoiInteractionController({
      state: { width: 200, height: 200, zoom: 1 },
      roiState,
      elements: { canvasWrap: document.createElement("div"), roiOverlay: null, roiCtx: null },
      callbacks: {
        getEffectiveScrollLeft: () => 0,
        getEffectiveScrollTop: () => 0,
        syncOverlayCanvas: () => null,
        updateRoiCenterInputs: vi.fn(),
        // A large ROI: 30 ms of statistics.
        updateRoiStats: () => {
          seen.push(roiState.start.x);
          clock += 30;
        },
      },
    });
    controller.scheduleRoiUpdate();
    expect(seen).toEqual([50]);
    // A drag: a move every 16 ms. The statistics wait 3 x 30 ms.
    for (let x = 51; x <= 55; x += 1) {
      clock += 16;
      roiState.start = { x, y: 50 };
      controller.scheduleRoiUpdate();
    }
    expect(seen).toEqual([50]);
    vi.advanceTimersByTime(200);
    // Once, with the ROI as it was last.
    expect(seen).toEqual([50, 55]);
    vi.useRealTimers();
  });
});
