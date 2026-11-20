import { describe, expect, it } from "vitest";
import { fromScreenZoom, snapToScreenPixel, toScreenZoom } from "../modules/screen_zoom.js";

describe("screen zoom", () => {
  it("counts screen pixels per image pixel", () => {
    expect(toScreenZoom(1, 2)).toBe(2);
    expect(toScreenZoom(0.5, 2)).toBe(1);
    expect(toScreenZoom(1, 1.5)).toBe(1.5);
  });

  it("gives the CSS zoom for one image pixel on one screen pixel", () => {
    expect(fromScreenZoom(1, 2)).toBe(0.5);
    expect(fromScreenZoom(1, 1.5)).toBeCloseTo(2 / 3);
    expect(fromScreenZoom(4, 1)).toBe(4);
  });

  it("round-trips", () => {
    for (const dpr of [1, 1.25, 1.5, 2, 3]) {
      expect(toScreenZoom(fromScreenZoom(3.7, dpr), dpr)).toBeCloseTo(3.7);
    }
  });

  it("treats a missing or broken ratio as 1", () => {
    for (const dpr of [undefined, 0, -2, Number.NaN, "x"]) {
      expect(toScreenZoom(2, dpr)).toBe(2);
      expect(fromScreenZoom(2, dpr)).toBe(2);
    }
  });

  it("snaps a CSS length to whole screen pixels", () => {
    expect(snapToScreenPixel(10.3, 1)).toBe(10);
    expect(snapToScreenPixel(10.3, 2)).toBe(10.5);
    expect(snapToScreenPixel(10.2, 1.5) * 1.5).toBe(15);
    expect(snapToScreenPixel(-3.3, 2)).toBe(-3.5);
  });
});
