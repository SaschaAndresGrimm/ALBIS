import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createResolutionRingInteractionController } from "../modules/resolution_ring_interaction_controller.js";

function setup({ params, ringValues = [], getResolutionAtPixel = () => null } = {}) {
  const state = { hasFrame: true, zoom: 1, renderOffsetX: 0, renderOffsetY: 0 };
  const analysisState = { ringsEnabled: true };
  const canvasWrap = document.createElement("div");
  canvasWrap.getBoundingClientRect = () => ({ left: 0, top: 0, width: 400, height: 400 });
  const scheduleResolutionOverlay = vi.fn();
  const setBeamCenter = vi.fn();
  const setRingValue = vi.fn();
  const commitRings = vi.fn();

  const defaultParams = {
    mode: "planar",
    energyEv: 12222,
    distanceMm: 50,
    pixelSizeUm: 75,
    centerX: 200,
    centerY: 200,
    centerKnown: true,
    rings: ringValues.map(Number),
  };

  const controller = createResolutionRingInteractionController({
    state,
    analysisState,
    elements: { canvasWrap },
    callbacks: {
      setBeamCenter,
      getEffectiveScrollLeft: () => 0,
      getEffectiveScrollTop: () => 0,
      getRingParams: () => ({ ...defaultParams, ...params }),
      getResolutionAtPixel,
      setRingValue,
      commitRings,
    },
    core: {
      overlayRender: { scheduleResolutionOverlay },
    },
  });

  return { controller, state, setBeamCenter, setRingValue, commitRings, canvasWrap };
}

describe("resolution_ring_interaction_controller", () => {
  beforeEach(() => {
    vi.spyOn(window, "requestAnimationFrame").mockImplementation((callback) => {
      callback(0);
      return 1;
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("detects the beam-center handle near the center", () => {
    const { controller } = setup();
    const handle = controller.getRingHandleAt({ clientX: 203, clientY: 198 });
    expect(handle).toEqual({ type: "center" });
  });

  it("ignores the center handle when the center is unknown", () => {
    const { controller } = setup({ params: { centerKnown: false } });
    expect(controller.getRingHandleAt({ clientX: 200, clientY: 200 })).toBeNull();
  });

  it("detects a ring handle on the ring radius", () => {
    // For E=12222 eV, d=3.67 A, distance 50 mm, pixel 75 um the ring sits at
    // ~190 px from the center.
    const { controller } = setup({ ringValues: ["3.67"] });
    const handle = controller.getRingHandleAt({ clientX: 200 + 190, clientY: 200 });
    expect(handle?.type).toBe("ring");
    expect(handle?.index).toBe(0);
    expect(handle?.d).toBeCloseTo(3.67, 2);
  });

  it("finds a ring in geometry mode by the scattering angle under the pointer", () => {
    // Rings on panels or a tilted detector are not circles; the handle is found
    // from the d-spacing at the pointer instead. Here the same flat geometry
    // as above, given as a function, so the ring is again ~190 px out.
    const flatD = (x, y) => {
      const twoTheta = Math.atan((Math.hypot(x - 200, y - 200) * 0.075) / 50);
      return 12398.4193 / 12222 / (2 * Math.sin(twoTheta / 2));
    };
    const { controller } = setup({
      params: { mode: "geometry", geometry: { panels: [{}] } },
      ringValues: ["3.67"],
      getResolutionAtPixel: flatD,
    });

    const handle = controller.getRingHandleAt({ clientX: 200 + 190, clientY: 200 });
    expect(handle?.type).toBe("ring");
    expect(handle?.index).toBe(0);
    expect(controller.getRingHandleAt({ clientX: 200 + 150, clientY: 200 })).toBeNull();
  });

  it("finds no ring in a gap between panels, where there is no angle", () => {
    const { controller } = setup({
      params: { mode: "geometry", geometry: { panels: [{}] } },
      ringValues: ["3.67"],
      getResolutionAtPixel: () => null,
    });
    expect(controller.getRingHandleAt({ clientX: 200 + 190, clientY: 200 })).toBeNull();
  });

  it("sets the beam centre as a manual override, in whole pixels, on drag", () => {
    // Dragging the centre is a manual geometry edit like typing it, so it goes
    // to the override and stays across files instead of being replaced by the
    // next frame's header.
    const { controller, setBeamCenter } = setup();

    controller.startRingEdit({ type: "center" });
    controller.applyRingEdit({ x: 123.4, y: 56.6 });

    expect(setBeamCenter).toHaveBeenCalledTimes(1);
    expect(setBeamCenter).toHaveBeenCalledWith(123, 57);
  });

  it("grabs the marker where the beam meets a tilted detector, not at the 2θ = 0 field", () => {
    // I19-2's 2θ arm: the field holds (985, 1419), the beam meets the detector
    // 413 px away. The marker is drawn there, so that is where it is grabbed.
    const params = { mode: "geometry", centerX: 300, centerY: 200, markerX: 100, markerY: 200 };
    const { controller } = setup({ params });

    expect(controller.getRingHandleAt({ clientX: 101, clientY: 199 })).toEqual({ type: "center" });
    expect(controller.getRingHandleAt({ clientX: 300, clientY: 200 })).toBeNull();
  });

  it("drags a tilted detector's marker by keeping the field's offset from it", () => {
    const params = { mode: "geometry", centerX: 300, centerY: 200, markerX: 100, markerY: 200 };
    const { controller, setBeamCenter } = setup({ params });

    controller.startRingEdit({ type: "center" });
    controller.applyRingEdit({ x: 110, y: 205 });

    // The marker went 10 right and 5 down; the 2θ = 0 field goes the same way.
    expect(setBeamCenter).toHaveBeenCalledWith(310, 205);
  });

  it("sets the dragged ring's d-spacing by its index, and re-sorts when the drag ends", () => {
    const { controller, setRingValue, commitRings } = setup({
      ringValues: ["3.67", "11.01"],
      getResolutionAtPixel: () => 2.5,
    });

    controller.startRingEdit({ type: "ring", index: 1, d: 11.01 });
    controller.applyRingEdit({ x: 250, y: 200 });

    expect(setRingValue).toHaveBeenCalledWith(1, 2.5);
    expect(commitRings).not.toHaveBeenCalled();
    controller.stopRingEdit();
    expect(commitRings).toHaveBeenCalledTimes(1);
  });

  it("reports the active handle while dragging and the hovered handle otherwise", () => {
    const { controller } = setup();
    expect(controller.getRingInteractionState().handle).toBeNull();

    controller.updateRingHover({ clientX: 200, clientY: 200 });
    expect(controller.getRingInteractionState().handle).toEqual({ type: "center" });

    controller.startRingEdit({ type: "ring", index: 0, d: 3.67 });
    expect(controller.getRingInteractionState().handle).toEqual({ type: "ring", index: 0, d: 3.67 });

    controller.stopRingEdit();
    expect(controller.isRingEditing()).toBe(false);
  });
});
