import { describe, expect, it } from "vitest";

import { SCREEN_OVERLAY_COLORS } from "../modules/overlay_painters.js";
import { MINMAX_MARKER_COLORS, minmaxColorsFor } from "../modules/minmax_finder_painter.js";
import {
  SCREEN_OVERLAY_COLORS_ON_LIGHT,
  imageToneFor,
  overlayColorsFor,
} from "../modules/overlay_tone_utils.js";

describe("imageToneFor", () => {
  it("reads the background end of the colour map", () => {
    expect(imageToneFor("albulaHdr")).toBe("light"); // white where nothing was counted
    expect(imageToneFor("gray")).toBe("dark");
    expect(imageToneFor("viridis")).toBe("dark");
    expect(imageToneFor("heat")).toBe("dark");
  });

  it("flips with Invert", () => {
    expect(imageToneFor("gray", true)).toBe("light");
    expect(imageToneFor("albulaHdr", true)).toBe("light"); // its top end is pale yellow
  });
});

describe("overlay colours per tone", () => {
  it("keeps the hues and changes only halos and ink weight", () => {
    expect(overlayColorsFor("dark")).toBe(SCREEN_OVERLAY_COLORS);
    expect(overlayColorsFor("light")).toBe(SCREEN_OVERLAY_COLORS_ON_LIGHT);
    expect(Object.keys(SCREEN_OVERLAY_COLORS_ON_LIGHT)).toEqual(Object.keys(SCREEN_OVERLAY_COLORS));
    expect(SCREEN_OVERLAY_COLORS_ON_LIGHT.selectedRing).toBe(SCREEN_OVERLAY_COLORS.selectedRing);
  });

  it("puts a dark halo behind rings and peaks on a light image", () => {
    const alpha = (c) => Number(c.match(/[\d.]+(?=\))/)[0]);
    expect(SCREEN_OVERLAY_COLORS_ON_LIGHT.ringHalo).toMatch(/rgba\(1?\d,/);
    expect(alpha(SCREEN_OVERLAY_COLORS_ON_LIGHT.peakHalo)).toBeGreaterThan(alpha(SCREEN_OVERLAY_COLORS.peakHalo));
    expect(alpha(minmaxColorsFor("light").halo)).toBeGreaterThan(alpha(MINMAX_MARKER_COLORS.halo));
  });

  it("keeps the highest-pixel square apart from the magenta reference rings", () => {
    const [r, g, b] = MINMAX_MARKER_COLORS.max.match(/\d+/g).map(Number);
    const [rr, , rb] = SCREEN_OVERLAY_COLORS.referenceRing.match(/\d+/g).map(Number);
    expect(b).toBeLessThan(rb / 2);
    expect(r).toBeGreaterThanOrEqual(rr);
    expect(g).toBeGreaterThan(0);
  });
});
