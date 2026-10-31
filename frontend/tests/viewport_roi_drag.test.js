import { afterEach, describe, expect, it, vi } from "vitest";

import { bindViewportInteractions } from "../modules/viewport_bindings.js";

// A left-drag inside an ROI pans the image like anywhere else, so a large ROI
// never hides the image from the hand; the ROI moves by its centre handle
// (or, for a line, by dragging the line, which covers nothing).

function pointer(type, init = {}) {
  const event = new MouseEvent(type, { bubbles: true, cancelable: true, button: 0, clientX: 50, clientY: 50, ...init });
  Object.defineProperty(event, "pointerType", { value: "mouse" });
  Object.defineProperty(event, "pointerId", { value: 1 });
  return event;
}

function bind({ mode = "box", handle = null, inside = true } = {}) {
  const canvasWrap = document.createElement("div");
  canvasWrap.setPointerCapture = vi.fn();
  canvasWrap.hasPointerCapture = () => false;
  canvasWrap.releasePointerCapture = vi.fn();
  document.body.append(canvasWrap);
  const roiState = { enabled: true, active: true, mode, start: { x: 0, y: 0 }, end: { x: 100, y: 100 } };
  const startRoiEdit = vi.fn();
  const callbacks = new Proxy(
    {
      getImagePointFromEvent: () => ({ x: 50, y: 50 }),
      getRoiHandleAt: () => handle,
      isPointInRoi: () => inside,
      startRoiEdit,
      getRingHandleAt: () => null,
      isRoiEditing: () => false,
      isRingEditing: () => false,
      getRoiDragging: () => false,
      isTouchGestureActive: () => false,
      getEffectiveScrollLeft: () => 0,
      getEffectiveScrollTop: () => 0,
      getMinZoom: () => 0.1,
    },
    { get: (target, key) => (key in target ? target[key] : vi.fn()) },
  );
  bindViewportInteractions({
    state: { zoom: 1, width: 200, height: 200 },
    roiState,
    constants: { MAX_ZOOM: 50 },
    elements: { canvasWrap },
    callbacks,
    // Any controller reached through core (decision 0006): every method a stub.
    core: new Proxy({}, { get: () => new Proxy({}, { get: () => vi.fn() }) }),
  });
  return { canvasWrap, startRoiEdit };
}

describe("dragging on an ROI", () => {
  afterEach(() => {
    document.body.innerHTML = "";
  });

  it.each(["box", "circle", "annulus"])("pans the image when a %s is dragged from its inside", (mode) => {
    const { canvasWrap, startRoiEdit } = bind({ mode, inside: true });
    canvasWrap.dispatchEvent(pointer("pointerdown"));
    expect(startRoiEdit).not.toHaveBeenCalled();
    expect(canvasWrap.classList.contains("is-panning")).toBe(true);
  });

  it("moves the ROI by its centre handle", () => {
    const { canvasWrap, startRoiEdit } = bind({ mode: "box", handle: "center" });
    canvasWrap.dispatchEvent(pointer("pointerdown"));
    expect(startRoiEdit).toHaveBeenCalledWith("center", { x: 50, y: 50 });
    expect(canvasWrap.classList.contains("is-panning")).toBe(false);
  });

  it("moves a line by dragging the line itself", () => {
    const { canvasWrap, startRoiEdit } = bind({ mode: "line", inside: true });
    canvasWrap.dispatchEvent(pointer("pointerdown"));
    expect(startRoiEdit).toHaveBeenCalledWith("move", { x: 50, y: 50 });
  });

  it("shows what a drag does: move over the centre, resize over a corner, nothing elsewhere", () => {
    const centre = bind({ mode: "box", handle: "center" });
    centre.canvasWrap.dispatchEvent(pointer("pointermove"));
    expect(centre.canvasWrap.dataset.roiHover).toBe("move");
    const corner = bind({ mode: "box", handle: "box-ne" });
    corner.canvasWrap.dispatchEvent(pointer("pointermove"));
    expect(corner.canvasWrap.dataset.roiHover).toBe("nesw");
    const inside = bind({ mode: "box", handle: null, inside: true });
    inside.canvasWrap.dispatchEvent(pointer("pointermove"));
    expect(inside.canvasWrap.dataset.roiHover).toBeUndefined();
  });
});
