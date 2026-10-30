import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../modules/i18n.js", () => ({ t: (key) => key }));

import { LENS_SIZES, MAGNIFICATIONS, createMagnifierController, isTypingTarget, lensSourcePixel, stepAlong } from "../modules/magnifier_controller.js";

function setup(extraCallbacks = {}) {
  document.body.innerHTML = `<div id="shell"><div id="wrap"></div></div><button id="btn"></button><input id="field" />`;
  const canvasWrap = document.getElementById("wrap");
  canvasWrap.getBoundingClientRect = () => ({ left: 0, top: 0, width: 400, height: 400 });
  const canvasShell = document.getElementById("shell");
  canvasShell.getBoundingClientRect = () => ({ left: 0, top: 0, width: 400, height: 400 });
  const state = { hasFrame: true, dataRaw: new Uint32Array(100).fill(5), width: 10, height: 10, zoom: 40, colormap: "gray" };
  const controller = createMagnifierController({
    state,
    elements: { canvasWrap, canvasShell, toggleBtn: document.getElementById("btn") },
    callbacks: {
      buildPalette: () => new Uint8Array(256 * 4),
      getPaletteColorCount: () => 256,
      mapValueToNorm: () => 0.5,
      getActiveSaturationMax: () => null,
      isSaturatedValue: () => false,
      getEffectiveScrollLeft: () => 0,
      getEffectiveScrollTop: () => 0,
      ...extraCallbacks,
    },
  });
  return { controller, canvasWrap, state };
}

const key = (type, k, target = window) =>
  target.dispatchEvent(new KeyboardEvent(type, { key: k, bubbles: true, cancelable: true }));

describe("magnifier", () => {
  beforeEach(() => {
    localStorage.clear();
    vi.stubGlobal("requestAnimationFrame", () => 1);
  });
  afterEach(() => vi.unstubAllGlobals());

  it("steps magnification and size along their scales, clamped", () => {
    expect(stepAlong(MAGNIFICATIONS, 4, 1)).toBe(6);
    expect(stepAlong(MAGNIFICATIONS, 4, -1)).toBe(3);
    expect(stepAlong(MAGNIFICATIONS, 32, 1)).toBe(32);
    expect(stepAlong(LENS_SIZES, 220, -1)).toBe(180);
  });

  it("shows the pixels around the mouse, each scale screen pixels wide", () => {
    // A 200 px lens at 8 px per image pixel, centred on image point (50.5, 20.5).
    expect(lensSourcePixel(100, 100, 200, 50.5, 20.5, 8, 8)).toEqual([50, 20]);
    expect(lensSourcePixel(0, 0, 200, 50.5, 20.5, 8, 8)).toEqual([38, 8]);
    expect(lensSourcePixel(199, 199, 200, 50.5, 20.5, 8, 8)).toEqual([62, 32]);
  });

  it("changes magnification with the wheel only while the lens is held, and remembers it", () => {
    const { controller, canvasWrap } = setup();
    const wheel = (deltaY, shiftKey = false) => {
      const event = new WheelEvent("wheel", { deltaY, shiftKey, bubbles: true, cancelable: true });
      canvasWrap.dispatchEvent(event);
      return event.defaultPrevented;
    };
    canvasWrap.dispatchEvent(new PointerEvent("pointermove", { clientX: 100, clientY: 100 }));
    expect(wheel(-100)).toBe(false); // not held: the view zooms as usual
    key("keydown", "z");
    expect(wheel(-100)).toBe(true);
    expect(controller.settings.magnification).toBe(6);
    wheel(-100, true);
    expect(controller.settings.size).toBe(280);
    expect(JSON.parse(localStorage.getItem("albis.magnifier"))).toEqual({ magnification: 6, size: 280 });
    key("keyup", "z");
    expect(wheel(-100)).toBe(false);
  });

  it("ignores Z while typing and with a modifier", () => {
    const { controller, canvasWrap } = setup();
    canvasWrap.dispatchEvent(new PointerEvent("pointermove", { clientX: 100, clientY: 100 }));
    key("keydown", "z", document.getElementById("field"));
    const event = new WheelEvent("wheel", { deltaY: -100, bubbles: true, cancelable: true });
    canvasWrap.dispatchEvent(event);
    expect(event.defaultPrevented).toBe(false);
    expect(controller.settings.magnification).toBe(4);
  });

  it("stays on from the toolbar button until clicked again", () => {
    const { controller } = setup();
    const button = document.getElementById("btn");
    button.click();
    expect(button.getAttribute("aria-pressed")).toBe("true");
    expect(controller.toggleLocked()).toBe(false);
    expect(button.classList.contains("is-active")).toBe(false);
  });

  it("takes Z after a panel control was used: a click on the image returns focus, and a checkbox does not type", () => {
    const { canvasWrap } = setup();
    const field = document.getElementById("field");
    field.focus();
    expect(document.activeElement).toBe(field);
    canvasWrap.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true }));
    expect(document.activeElement).toBe(document.body);
    const box = Object.assign(document.createElement("input"), { type: "checkbox" });
    expect(isTypingTarget(box)).toBe(false);
    expect(isTypingTarget(document.createElement("select"))).toBe(true);
    expect(isTypingTarget(Object.assign(document.createElement("input"), { type: "number" }))).toBe(true);
  });

  it("says when the readout must move: the lens appeared or went under a still mouse", () => {
    // jsdom has no canvas; a context whose every method does nothing is enough
    // to let the lens draw, and so become visible.
    const context = new Proxy(
      {
        measureText: () => ({ width: 10 }),
        createImageData: (w, h) => ({ data: new Uint8ClampedArray(w * h * 4), width: w, height: h }),
      },
      { get: (target, name) => (name in target ? target[name] : () => {}), set: () => true },
    );
    vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockReturnValue(context);
    vi.stubGlobal("requestAnimationFrame", (draw) => {
      draw();
      return 0;
    });
    const onClearanceChange = vi.fn();
    const { controller, canvasWrap } = setup({ onClearanceChange });
    canvasWrap.dispatchEvent(new MouseEvent("pointermove", { clientX: 200, clientY: 200, bubbles: true }));
    expect(controller.clearance()).toBe(0);

    key("keydown", "z");
    expect(controller.isShowing).toBe(true);
    expect(controller.clearance()).toBeGreaterThan(0);
    expect(onClearanceChange).toHaveBeenCalledTimes(1);

    key("keyup", "z");
    expect(controller.clearance()).toBe(0);
    expect(onClearanceChange).toHaveBeenCalledTimes(2);
  });
});
