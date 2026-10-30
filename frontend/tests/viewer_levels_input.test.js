import { describe, expect, it, vi } from "vitest";

import { bindViewerControls } from "../modules/viewer_controls_bindings.js";

// Background and foreground typed into their fields: taken as typed, not
// clamped to the frame's own range; only the order is kept.
function setup() {
  document.body.innerHTML = `
    <input id="auto" type="checkbox" checked /><input id="min" /><input id="max" />
    <input id="invert" type="checkbox" />
    <input id="zoom" type="range" /><button id="reset"></button>`;
  const $ = (id) => document.getElementById(id);
  const state = { dataRaw: new Uint16Array(4), stats: { min: 10, max: 200 }, min: 20, max: 150, autoScale: true };
  const redraw = vi.fn();
  bindViewerControls({
    state,
    elements: {
      autoScaleToggle: $("auto"), minInput: $("min"), maxInput: $("max"),
      invertToggle: $("invert"), zoomRange: $("zoom"), resetView: $("reset"),
    },
    callbacks: {
      redraw, scheduleHistogram: vi.fn(), handleContrastChanged: vi.fn(), formatValue: (v) => String(v),
      snapHistogramValue: (v) => Math.round(v), registerChromeActivity: () => {}, updateFullscreenUi: () => {},
    },
  });
  const type = (field, value) => {
    $(field).value = value;
    $(field).dispatchEvent(new Event("change"));
  };
  return { state, type, $, redraw };
}

describe("typed background and foreground", () => {
  it("are taken as typed, beyond the frame's own range", () => {
    const { state, type, $ } = setup();
    type("max", "5000");
    expect(state.max).toBe(5000);
    expect($("max").value).toBe("5000");
    type("min", "-30");
    expect(state.min).toBe(-30);
    expect(state.autoScale).toBe(false);
    expect($("auto").checked).toBe(false);
  });

  it("keep the background below the foreground, and ignore what is not a number", () => {
    const { state, type } = setup();
    type("min", "400");
    expect([state.min, state.max]).toEqual([150, 150]);
    type("max", "abc");
    expect([state.min, state.max]).toEqual([150, 150]);
    type("max", "1000.6");
    expect(state.max).toBe(1001);
  });
});
