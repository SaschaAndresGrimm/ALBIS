import { describe, expect, it, vi } from "vitest";

import { bindViewerControls } from "../modules/viewer_controls_bindings.js";
import { sliderToZoom, zoomToSlider } from "../modules/zoom_slider.js";

// The zoom slider is logarithmic: each doubling of the zoom takes the same
// length of track, and 1x sits where zooming out and zooming in meet.
describe("zoom slider scale", () => {
  it("puts 1x at 0 and a doubling at each step of 1", () => {
    expect(zoomToSlider(1)).toBe(0);
    expect(zoomToSlider(2)).toBe(1);
    expect(zoomToSlider(0.25)).toBe(-2);
  });

  it("reads back the zoom it was given", () => {
    for (const zoom of [0.02, 0.37, 1, 3.5, 50]) {
      expect(sliderToZoom(zoomToSlider(zoom))).toBeCloseTo(zoom, 10);
    }
    expect(sliderToZoom("1.5")).toBeCloseTo(2 ** 1.5);
  });

  it("gives 0.5x to 2x as much track as 2x to 8x", () => {
    const span = (a, b) => zoomToSlider(b) - zoomToSlider(a);
    expect(span(0.5, 2)).toBeCloseTo(span(2, 8));
  });
});

function bindSlider() {
  document.body.innerHTML = `
    <input id="auto" type="checkbox" /><input id="min" /><input id="max" />
    <input id="invert" type="checkbox" /><input id="zoom" type="range" min="-5" max="5" step="any" value="0" /><button id="reset"></button>`;
  const $ = (id) => document.getElementById(id);
  const setZoom = vi.fn();
  bindViewerControls({
    state: { stats: null },
    elements: {
      autoScaleToggle: $("auto"), minInput: $("min"), maxInput: $("max"),
      invertToggle: $("invert"), zoomRange: $("zoom"), resetView: $("reset"),
    },
    callbacks: {
      setZoom, scheduleOverview: vi.fn(), deferViewportInteraction: vi.fn(),
      registerChromeActivity: () => {}, updateFullscreenUi: () => {},
    },
  });
  const press = (key, init = {}) => {
    const event = new KeyboardEvent("keydown", { key, bubbles: true, cancelable: true, ...init });
    $("zoom").dispatchEvent(event);
    return event;
  };
  return { slider: $("zoom"), setZoom, press };
}

describe("dragging the zoom slider", () => {
  it("zooms to 2 to the power of its value", () => {
    const { slider, setZoom } = bindSlider();
    slider.value = "1";
    slider.dispatchEvent(new Event("input"));
    expect(setZoom).toHaveBeenCalledWith(2);
    slider.value = "-2";
    slider.dispatchEvent(new Event("input"));
    expect(setZoom).toHaveBeenLastCalledWith(0.25);
  });
});

describe("the zoom slider's keys", () => {
  it("zoom by a fourth of a doubling per arrow press", () => {
    const { setZoom, press } = bindSlider();
    expect(press("ArrowRight").defaultPrevented).toBe(true);
    expect(setZoom).toHaveBeenLastCalledWith(2 ** 0.25);
    press("ArrowUp");
    press("ArrowUp");
    press("ArrowUp");
    expect(setZoom.mock.lastCall[0]).toBeCloseTo(2);
    press("ArrowDown");
    press("ArrowLeft");
    expect(setZoom.mock.lastCall[0]).toBeCloseTo(2 ** 0.5);
  });

  it("double and halve with Page Up and Page Down", () => {
    const { setZoom, press } = bindSlider();
    press("PageUp");
    expect(setZoom).toHaveBeenLastCalledWith(2);
    press("PageDown");
    press("PageDown");
    expect(setZoom).toHaveBeenLastCalledWith(0.5);
  });

  it("stop at the slider's ends", () => {
    const { slider, setZoom, press } = bindSlider();
    slider.value = "4.9";
    press("PageUp");
    expect(slider.value).toBe("5");
    expect(setZoom).toHaveBeenLastCalledWith(32);
  });

  it("leave other keys and modified presses to the browser", () => {
    const { setZoom, press } = bindSlider();
    expect(press("Home").defaultPrevented).toBe(false);
    expect(press("ArrowUp", { metaKey: true }).defaultPrevented).toBe(false);
    expect(setZoom).not.toHaveBeenCalled();
  });
});
