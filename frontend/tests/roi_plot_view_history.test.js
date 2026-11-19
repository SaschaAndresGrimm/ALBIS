import { describe, expect, it } from "vitest";

import { createRoiPlotViewHistory } from "../modules/roi_plot_view_history.js";

describe("roi_plot_view_history", () => {
  it("gives views back last first, per plot", () => {
    const history = createRoiPlotViewHistory();
    history.push("line", { xMin: 0, xMax: 10, yMin: null, yMax: null });
    history.push("line", { xMin: 2, xMax: 5, yMin: 1, yMax: 3 });
    history.push("x", { xMin: 7, xMax: 8, yMin: null, yMax: null });
    expect(history.size("line")).toBe(2);
    expect(history.pop("line")).toEqual({ xMin: 2, xMax: 5, yMin: 1, yMax: 3 });
    expect(history.pop("line")).toEqual({ xMin: 0, xMax: 10, yMin: null, yMax: null });
    expect(history.pop("line")).toBeNull();
    expect(history.size("x")).toBe(1);
  });

  it("copies the view, skips a repeat and keeps the newest", () => {
    const history = createRoiPlotViewHistory({ limit: 2 });
    const live = { xMin: 1, xMax: 2, yMin: null, yMax: null };
    history.push("line", live);
    history.push("line", live);
    live.xMin = 0;
    expect(history.size("line")).toBe(1);
    history.push("line", live);
    history.push("line", { xMin: 5, xMax: 6 });
    expect(history.size("line")).toBe(2);
    expect(history.pop("line")).toEqual({ xMin: 5, xMax: 6, yMin: null, yMax: null });
    expect(history.pop("line").xMin).toBe(0);
  });
});
