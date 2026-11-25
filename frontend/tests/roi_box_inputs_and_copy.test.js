import { describe, expect, it, vi } from "vitest";

vi.mock("../modules/i18n.js", () => ({
  t: (key, vars) => (vars ? `${key}(${Object.values(vars).join(",")})` : key),
}));

import { roiEndpointsFromInputs } from "../modules/roi_geometry_utils.js";
import { buildRoiStatsTsv } from "../modules/roi_csv_export.js";
import { createRoiInteractionController } from "../modules/roi_interaction_controller.js";
import { bindRoiControlInteractions } from "../modules/roi_controls_bindings.js";

describe("roiEndpointsFromInputs", () => {
  it("rounds to whole pixels and keeps the ends on the frame", () => {
    expect(roiEndpointsFromInputs({ x0: "10.4", y0: "-5", x1: "2000", y1: "99.6" }, 1030, 514)).toEqual({
      start: { x: 10, y: 0 },
      end: { x: 1029, y: 100 },
    });
  });

  it("waits until all four fields hold a number", () => {
    expect(roiEndpointsFromInputs({ x0: "10", y0: "", x1: "20", y1: "30" }, 100, 100)).toBeNull();
    expect(roiEndpointsFromInputs({ x0: "10", y0: "abc", x1: "20", y1: "30" }, 100, 100)).toBeNull();
    expect(roiEndpointsFromInputs({ x0: "0", y0: "0", x1: "0", y1: "0" }, 0, 0)).toBeNull();
  });
});

describe("the box's Start/End fields", () => {
  function setup(roiState) {
    const inputs = ["x0", "y0", "x1", "y1"].map(() => document.createElement("input"));
    inputs.forEach((input) => document.body.append(input));
    const handleRoiChanged = vi.fn();
    const controller = createRoiInteractionController({
      state: { width: 200, height: 100 },
      roiState,
      elements: {
        canvasWrap: document.createElement("div"),
        roiOverlay: null,
        roiCtx: null,
        roiBoxX0Input: inputs[0],
        roiBoxY0Input: inputs[1],
        roiBoxX1Input: inputs[2],
        roiBoxY1Input: inputs[3],
      },
      callbacks: { getEffectiveScrollLeft: () => 0, getEffectiveScrollTop: () => 0, syncOverlayCanvas: () => null, handleRoiChanged },
      core: { roiStats: { updateRoiStats: vi.fn() } },
    });
    return { controller, inputs, handleRoiChanged };
  }

  it("move the box to the typed ends, as applied", () => {
    const roiState = { enabled: true, mode: "box", start: null, end: null, active: false };
    const { controller, inputs, handleRoiChanged } = setup(roiState);
    ["10", "20", "300", "40.6"].forEach((value, i) => (inputs[i].value = value));
    expect(controller.applyRoiEndpointInputs()).toBe(true);
    expect(roiState).toMatchObject({ active: true, start: { x: 10, y: 20 }, end: { x: 199, y: 41 } });
    expect(inputs.map((input) => input.value)).toEqual(["10", "20", "199", "41"]);
    expect(handleRoiChanged).toHaveBeenCalledWith("roi");
  });

  it("do nothing for a circle, or before all four are filled", () => {
    const circle = { enabled: true, mode: "circle", start: null, end: null };
    const a = setup(circle);
    ["1", "2", "3", "4"].forEach((value, i) => (a.inputs[i].value = value));
    expect(a.controller.applyRoiEndpointInputs()).toBe(false);
    expect(circle.start).toBeNull();

    const box = { enabled: true, mode: "box", start: null, end: null };
    const b = setup(box);
    b.inputs[0].value = "5";
    expect(b.controller.applyRoiEndpointInputs()).toBe(false);
  });

  it("follow the ROI, but leave the field being typed in alone", () => {
    const roiState = { enabled: true, mode: "line", start: { x: 1.2, y: 2 }, end: { x: 30, y: 40.7 } };
    const { controller, inputs } = setup(roiState);
    inputs[1].value = "9";
    inputs[1].focus();
    controller.syncRoiEndpointInputs();
    expect(inputs.map((input) => input.value)).toEqual(["1", "9", "30", "41"]);

    controller.syncRoiEndpointInputs([0, 2, 199, 99]);
    expect(inputs.map((input) => input.value)).toEqual(["0", "9", "199", "99"]);

    roiState.start = null;
    inputs[1].blur();
    controller.syncRoiEndpointInputs();
    expect(inputs.map((input) => input.value)).toEqual(["", "", "", ""]);
  });
});

describe("buildRoiStatsTsv", () => {
  const state = { file: "data/scan_01.h5", dataset: "/entry/data/data", frameIndex: 4, frameCount: 10, thresholdCount: 1 };
  const statistics = { min: 0, max: 3363, sum: 1887123, median: 5, mean: 6.623123456, std: 19.39612 };

  it("lists every statistic at full precision under the source and the ROI", () => {
    const text = buildRoiStatsTsv(state, {
      enabled: true,
      active: true,
      mode: "box",
      start: { x: 0, y: 0 },
      end: { x: 486, y: 618 },
      statistics,
      pixelCounts: { total: 301453, gap: 16558, defective: 19, saturated: 0 },
      levelBands: { bg: 0, fg: 36.95234009337005, below: 0, between: 283406, above: 1470, total: 284876 },
    });
    expect(text.split("\n")).toEqual([
      "# Produced by ALBIS",
      "# Source: scan_01.h5 /entry/data/data frame 5/10",
      "# ROI: box from 0 0 to 486 618",
      "analysis.roi.stats.total_pixels\t301453",
      "analysis.roi.stats.gap_pixels\t16558",
      "analysis.roi.stats.defective_pixels\t19",
      "analysis.roi.stats.saturated_pixels\t0",
      "analysis.roi.stats.min\t0",
      "analysis.roi.stats.max\t3363",
      "analysis.roi.stats.sum\t1887123",
      "analysis.roi.stats.median\t5",
      "analysis.roi.stats.mean\t6.623123456",
      "analysis.roi.stats.std\t19.39612",
      "analysis.roi.stats.below_bg_at(0,37)\t0",
      "analysis.roi.stats.bg_to_fg_at(0,37)\t283406",
      "analysis.roi.stats.above_fg_at(0,37)\t1470",
      "",
    ]);
  });

  it("says when the numbers are the whole frame's, and skips what is unknown", () => {
    const text = buildRoiStatsTsv(state, { enabled: true, active: false, statistics: { ...statistics, median: Number.NaN } });
    expect(text).toContain("# ROI: none, whole frame");
    expect(text).not.toContain("median");
    expect(text).not.toContain("total_pixels");
  });

  it("is null with nothing to copy", () => {
    expect(buildRoiStatsTsv(state, { enabled: true, statistics: null })).toBeNull();
  });
});

describe("bindings", () => {
  it("apply a typed end and copy the statistics", () => {
    const roiBoxFields = document.createElement("div");
    const input = document.createElement("input");
    roiBoxFields.append(input);
    const roiCopyStatsBtn = document.createElement("button");
    const core = {
      roiInteraction: { applyRoiEndpointInputs: vi.fn() },
      roiStats: { copyRoiStats: vi.fn() },
    };
    bindRoiControlInteractions({ roiState: {}, elements: { roiBoxFields, roiCopyStatsBtn }, callbacks: {}, core });
    input.dispatchEvent(new Event("change"));
    roiCopyStatsBtn.click();
    expect(core.roiInteraction.applyRoiEndpointInputs).toHaveBeenCalledTimes(1);
    expect(core.roiStats.copyRoiStats).toHaveBeenCalledTimes(1);
  });
});
