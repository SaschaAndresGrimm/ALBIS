import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createResolutionRingInteractionController } from "../modules/resolution_ring_interaction_controller.js";

function makeRingInput(value) {
  const input = document.createElement("input");
  input.value = value;
  return input;
}

function setup({ params, ringValues = [], getResolutionAtPixel = () => null } = {}) {
  const state = { hasFrame: true, zoom: 1, renderOffsetX: 0, renderOffsetY: 0 };
  const analysisState = { ringsEnabled: true };
  const canvasWrap = document.createElement("div");
  canvasWrap.getBoundingClientRect = () => ({ left: 0, top: 0, width: 400, height: 400 });
  const ringsCenterX = document.createElement("input");
  const ringsCenterY = document.createElement("input");
  const ringInputs = ringValues.map(makeRingInput);
  const scheduleResolutionOverlay = vi.fn();

  const defaultParams = {
    mode: "planar",
    energyEv: 12222,
    distanceMm: 50,
    pixelSizeUm: 75,
    centerX: 200,
    centerY: 200,
    centerKnown: true,
    rings: [],
  };

  const controller = createResolutionRingInteractionController({
    state,
    analysisState,
    elements: { canvasWrap, ringsCenterX, ringsCenterY, ringInputs },
    callbacks: {
      getEffectiveScrollLeft: () => 0,
      getEffectiveScrollTop: () => 0,
      getRingParams: () => ({ ...defaultParams, ...params }),
      getResolutionAtPixel,
      scheduleResolutionOverlay,
    },
  });

  return { controller, state, ringsCenterX, ringsCenterY, ringInputs, canvasWrap };
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

  it("does not expose ring handles in geometry mode", () => {
    const { controller } = setup({ params: { mode: "geometry" }, ringValues: ["3.67"] });
    const handle = controller.getRingHandleAt({ clientX: 200 + 190, clientY: 200 });
    expect(handle).toBeNull();
  });

  it("writes both center inputs and fires a single input event on drag", () => {
    const { controller, ringsCenterX, ringsCenterY } = setup();
    const xEvents = vi.fn();
    const yEvents = vi.fn();
    ringsCenterX.addEventListener("input", xEvents);
    ringsCenterY.addEventListener("input", yEvents);

    controller.startRingEdit({ type: "center" });
    controller.applyRingEdit({ x: 123.4, y: 56.6 });

    expect(ringsCenterX.value).toBe("123");
    expect(ringsCenterY.value).toBe("57");
    expect(xEvents).toHaveBeenCalledTimes(1);
    expect(yEvents).toHaveBeenCalledTimes(0);
  });

  it("writes the resolved d-spacing into the dragged ring input", () => {
    const { controller, ringInputs } = setup({
      ringValues: ["3.67", "11.01"],
      getResolutionAtPixel: () => 2.5,
    });
    const events = vi.fn();
    ringInputs[1].addEventListener("input", events);

    controller.startRingEdit({ type: "ring", index: 1, d: 11.01 });
    controller.applyRingEdit({ x: 250, y: 200 });

    expect(ringInputs[1].value).toBe("2.5");
    expect(events).toHaveBeenCalledTimes(1);
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
