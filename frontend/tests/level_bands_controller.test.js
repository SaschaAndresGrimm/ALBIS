import { afterEach, describe, expect, it, vi } from "vitest";

import { LARGE_PIXELS, SETTLE_MS, createLevelBandsController } from "../modules/level_bands_controller.js";
import { buildLevelSample } from "../modules/level_bands_utils.js";

function setup({ frameValues = Uint16Array.from([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), pixels } = {}) {
  vi.useFakeTimers();
  document.body.innerHTML = `<strong id="below"></strong><strong id="between"></strong><strong id="above"></strong>`;
  const state = { hasFrame: true, min: 3, max: 8, dataRaw: { length: pixels ?? frameValues.length } };
  const roiState = {};
  let frameId = 1;
  const getWholeFrameSample = vi.fn(() => buildLevelSample(frameValues));
  const controller = createLevelBandsController({
    state,
    roiState,
    elements: {
      below: document.getElementById("below"),
      between: document.getElementById("between"),
      above: document.getElementById("above"),
    },
    callbacks: { getWholeFrameSample, wholeFrameKey: () => [frameId] },
  });
  const rows = () => ["below", "between", "above"].map((id) => document.getElementById(id).textContent);
  return { state, roiState, controller, getWholeFrameSample, rows, nextFrame: () => (frameId += 1) };
}

afterEach(() => vi.useRealTimers());

describe("level bands", () => {
  it("counts the whole frame against BG and FG", () => {
    const { controller, rows, roiState } = setup();
    controller.showWholeFrame();
    expect(rows()).toEqual(["2 (20.0 %)", "6 (60.0 %)", "2 (20.0 %)"]);
    expect(roiState.levelBands).toMatchObject({ below: 2, between: 6, above: 2, total: 10, bg: 3, fg: 8 });
  });

  it("follows BG and FG on refresh, and builds the frame's sample once", () => {
    const { controller, rows, state, getWholeFrameSample } = setup();
    controller.showWholeFrame();
    state.min = 0;
    state.max = 5;
    controller.refresh();
    expect(rows()).toEqual(["0 (0.0 %)", "5 (50.0 %)", "5 (50.0 %)"]);
    controller.refresh();
    expect(getWholeFrameSample).toHaveBeenCalledTimes(1);
  });

  it("builds a new sample for a new frame", () => {
    const { controller, getWholeFrameSample, nextFrame } = setup();
    controller.showWholeFrame();
    nextFrame();
    controller.refresh();
    expect(getWholeFrameSample).toHaveBeenCalledTimes(2);
  });

  it("counts an ROI's pixels instead while one is drawn", () => {
    const { controller, rows } = setup();
    controller.showRoi(buildLevelSample([1, 5, 9, 9], { integral: true }));
    expect(rows()).toEqual(["1 (25.0 %)", "1 (25.0 %)", "2 (50.0 %)"]);
  });

  it("waits for a large float sample's levels to settle before counting again", () => {
    const { controller, rows, state } = setup();
    const big = buildLevelSample(new Float32Array(LARGE_PIXELS + 1).fill(5));
    controller.showRoi(big);
    expect(rows()[1]).toBe(`${LARGE_PIXELS + 1} (100.0 %)`); // a new sample counts at once
    state.max = 4;
    controller.refresh();
    expect(rows()[1]).toBe(`${LARGE_PIXELS + 1} (100.0 %)`);
    vi.advanceTimersByTime(SETTLE_MS);
    expect(rows()[2]).toBe(`${LARGE_PIXELS + 1} (100.0 %)`);
  });

  it("shows … rather than the previous frame's numbers while a large frame's sample waits", () => {
    const { controller, rows, getWholeFrameSample } = setup({ pixels: LARGE_PIXELS + 1 });
    controller.showWholeFrame();
    expect(rows()).toEqual(["…", "…", "…"]);
    expect(getWholeFrameSample).not.toHaveBeenCalled();
    vi.advanceTimersByTime(SETTLE_MS);
    expect(getWholeFrameSample).toHaveBeenCalledTimes(1);
    expect(rows()).toEqual(["2 (20.0 %)", "6 (60.0 %)", "2 (20.0 %)"]);
  });

  it("clears to dashes", () => {
    const { controller, rows, roiState } = setup();
    controller.showWholeFrame();
    controller.clear();
    expect(rows()).toEqual(["-", "-", "-"]);
    expect(roiState.levelBands).toBeNull();
  });
});
