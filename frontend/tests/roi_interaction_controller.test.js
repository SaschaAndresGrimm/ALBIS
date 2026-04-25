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
});
