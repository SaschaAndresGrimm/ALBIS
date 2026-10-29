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

describe("dragging the zoom slider", () => {
  it("zooms to 2 to the power of its value", () => {
    document.body.innerHTML = `
      <input id="auto" type="checkbox" /><input id="min" /><input id="max" /><button id="autoc"></button>
      <input id="invert" type="checkbox" /><input id="zoom" type="range" min="-5" max="5" step="0.01" /><button id="reset"></button>`;
    const $ = (id) => document.getElementById(id);
    const setZoom = vi.fn();
    bindViewerControls({
      state: { stats: null },
      elements: {
        autoScaleToggle: $("auto"), minInput: $("min"), maxInput: $("max"), autoContrastBtn: $("autoc"),
        invertToggle: $("invert"), zoomRange: $("zoom"), resetView: $("reset"),
      },
      callbacks: {
        setZoom, scheduleOverview: vi.fn(), deferViewportInteraction: vi.fn(),
        registerChromeActivity: () => {}, updateFullscreenUi: () => {},
      },
    });
    $("zoom").value = "1";
    $("zoom").dispatchEvent(new Event("input"));
    expect(setZoom).toHaveBeenCalledWith(2);
    $("zoom").value = "-2";
    $("zoom").dispatchEvent(new Event("input"));
    expect(setZoom).toHaveBeenLastCalledWith(0.25);
  });
});
