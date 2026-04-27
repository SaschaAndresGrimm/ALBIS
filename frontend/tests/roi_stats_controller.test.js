import { describe, expect, it, vi } from "vitest";

import { createRoiStatsController } from "../modules/roi_stats_controller.js";

function createController() {
  return createRoiStatsController({
    state: {},
    roiState: {
      plotLimits: {
        autoscale: true,
        line: { xMin: null, xMax: null, yMin: null, yMax: null },
        x: { xMin: null, xMax: null, yMin: null, yMax: null },
        y: { xMin: null, xMax: null, yMin: null, yMax: null },
        hist: { xMin: null, xMax: null, yMin: null, yMax: null },
      },
    },
    scheduleRoiUpdate: vi.fn(),
    updateRoiSectionState: vi.fn(),
    drawRoiOverlay: vi.fn(),
    getActiveSaturationMax: vi.fn(() => 0),
    isSaturatedValue: vi.fn(() => false),
    computeMedian: vi.fn(() => 0),
    formatStat: (value) => value.toFixed(1),
    formatRoiTick: (value) => value.toFixed(1),
    PLOT_THEME: {},
    setStatus: vi.fn(),
  });
}

describe("roi_stats_controller", () => {
  it("shows ROI plot tooltip on hover without throwing", () => {
    const controller = createController();
    const container = document.createElement("div");
    const canvas = document.createElement("canvas");
    const tooltip = document.createElement("div");

    tooltip.className = "roi-tooltip";
    container.append(canvas, tooltip);
    document.body.append(container);

    canvas.getBoundingClientRect = () => ({
      left: 0,
      top: 0,
      width: 200,
      height: 100,
      right: 200,
      bottom: 100,
    });
    container.getBoundingClientRect = () => ({
      left: 0,
      top: 0,
      width: 200,
      height: 100,
      right: 200,
      bottom: 100,
    });

    canvas._roiPlot = {
      data: [10, 20, 30],
      padL: 10,
      padR: 10,
      padT: 10,
      padB: 10,
      width: 200,
      height: 100,
      xStart: 296,
      xStep: 1,
      xTickMode: "integer",
      xLabel: "X pixel",
    };

    expect(() => controller.updateRoiTooltip({ clientX: 100, clientY: 50 }, canvas)).not.toThrow();
    expect(tooltip.classList.contains("is-visible")).toBe(true);
    expect(tooltip.textContent).toContain("X pixel 297");
    expect(tooltip.textContent).toContain("20.0");
  });
});
