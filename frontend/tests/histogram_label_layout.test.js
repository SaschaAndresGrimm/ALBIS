import { describe, expect, it } from "vitest";
import { layoutLevelLabels } from "../modules/histogram_label_layout.js";

const overlaps = (a, aw, b, bw) => a.row === b.row && a.x < b.x + bw && b.x < a.x + aw;

describe("layoutLevelLabels", () => {
  it("puts the captions inside the markers, on one row, when the levels are far apart", () => {
    const { bg, fg } = layoutLevelLabels({ minX: 40, maxX: 400, bgWidth: 30, fgWidth: 34, width: 500 });
    expect(bg).toEqual({ x: 46, row: 0 });
    expect(fg).toEqual({ x: 360, row: 0 });
  });

  it("turns the captions outwards when the levels are close", () => {
    const { bg, fg } = layoutLevelLabels({ minX: 200, maxX: 230, bgWidth: 30, fgWidth: 34, width: 500 });
    expect(bg.row).toBe(0);
    expect(fg.row).toBe(0);
    expect(bg.x + 30).toBeLessThanOrEqual(200);
    expect(fg.x).toBeGreaterThanOrEqual(230);
  });

  it("uses a second row only when both are pinned to one edge", () => {
    // BG 0 / FG 1 on a log axis: both markers at the left edge.
    const result = layoutLevelLabels({ minX: 2, maxX: 6, bgWidth: 30, fgWidth: 30, width: 500 });
    expect(result.fg.row).toBe(1);
    expect(overlaps(result.bg, 30, result.fg, 30)).toBe(false);
  });

  it("keeps every caption inside the plot", () => {
    for (const [minX, maxX] of [[0, 0], [498, 500], [0, 500], [250, 251]]) {
      const { bg, fg } = layoutLevelLabels({ minX, maxX, bgWidth: 40, fgWidth: 40, width: 500 });
      for (const x of [bg.x, fg.x]) {
        expect(x).toBeGreaterThanOrEqual(8);
        expect(x + 40).toBeLessThanOrEqual(492);
      }
    }
  });
});
