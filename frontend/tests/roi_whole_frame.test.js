import { describe, expect, it, vi } from "vitest";

import { wholeFrameBox } from "../modules/roi_geometry_utils.js";
import { createRoiInteractionController } from "../modules/roi_interaction_controller.js";

describe("wholeFrameBox", () => {
  it("runs from the first pixel to the last, ends inclusive", () => {
    expect(wholeFrameBox(1030, 514)).toEqual({ start: { x: 0, y: 0 }, end: { x: 1029, y: 513 } });
  });

  it("is null without a frame", () => {
    expect(wholeFrameBox(0, 514)).toBeNull();
    expect(wholeFrameBox(undefined, undefined)).toBeNull();
  });
});

describe("selectWholeFrame", () => {
  function setup({ state = { width: 400, height: 300 }, roiState = {} } = {}) {
    const fullRoiState = { enabled: true, active: false, mode: "circle", start: null, end: null, innerRadius: 5, outerRadius: 20, ...roiState };
    const roiModeSelect = document.createElement("select");
    ["line", "box", "circle"].forEach((value) => roiModeSelect.append(new Option(value, value)));
    roiModeSelect.value = fullRoiState.mode;
    const updateRoiModeUI = vi.fn();
    const handleRoiChanged = vi.fn();
    const controller = createRoiInteractionController({
      state,
      roiState: fullRoiState,
      elements: { canvasWrap: document.createElement("div"), roiOverlay: null, roiCtx: null, roiModeSelect },
      callbacks: {
        getEffectiveScrollLeft: () => 0,
        getEffectiveScrollTop: () => 0,
        syncOverlayCanvas: () => null,
        handleRoiChanged,
      },
      core: { roiStats: { updateRoiModeUI, updateRoiStats: vi.fn(), updateRoiCenterInputs: vi.fn() } },
    });
    return { controller, roiState: fullRoiState, roiModeSelect, updateRoiModeUI, handleRoiChanged };
  }

  it("makes the ROI a box over the whole frame, switching the mode to Box", () => {
    const { controller, roiState, roiModeSelect, updateRoiModeUI, handleRoiChanged } = setup();
    expect(controller.selectWholeFrame()).toBe(true);
    expect(roiState).toMatchObject({
      mode: "box",
      active: true,
      start: { x: 0, y: 0 },
      end: { x: 399, y: 299 },
      innerRadius: 0,
      outerRadius: 0,
    });
    expect(roiModeSelect.value).toBe("box");
    expect(updateRoiModeUI).toHaveBeenCalled();
    expect(handleRoiChanged).toHaveBeenCalledWith("roi");
  });

  it("does nothing without a frame or with the ROI tools off", () => {
    const noFrame = setup({ state: { width: 0, height: 0 } });
    expect(noFrame.controller.selectWholeFrame()).toBe(false);
    expect(noFrame.roiState.active).toBe(false);

    const off = setup({ roiState: { enabled: false } });
    expect(off.controller.selectWholeFrame()).toBe(false);
    expect(off.roiState.mode).toBe("circle");
  });
});
