import { afterEach, describe, expect, it } from "vitest";
import { buildPalette, getPaletteColorCount } from "../modules/intensity_scale_utils.js";
import {
  DEFAULT_MASK_COLORS,
  autoMaskColors,
  colorDistance,
  blendsWithMap,
  mapEndColors,
  setMaskColorOverride,
  currentMaskColors,
  readMaskColorOverrides,
} from "../modules/mask_colors.js";

const MAPS = ["heat", "albulaHdr", "albisHdr", "gray", "viridis", "magma", "inferno", "cividis", "turbo"];

function mapColors(name) {
  const p = buildPalette(name);
  return Array.from({ length: getPaletteColorCount(p) }, (_, i) => [p[i * 4], p[i * 4 + 1], p[i * 4 + 2]]);
}

describe("mask colours", () => {
  afterEach(() => localStorage.clear());

  it.each(MAPS)("on %s, picks three colours clear of the map and of each other", (name) => {
    const colors = currentMaskColors({ colormap: name });
    const map = mapColors(name);
    // ALBULA HDR keeps black gaps on purpose, though the map itself has black.
    const chosen = name === "albulaHdr" ? [colors.defective, colors.saturated] : Object.values(colors);
    for (const color of chosen) {
      const nearest = Math.min(...map.map((c) => colorDistance(color, c)));
      expect(nearest).toBeGreaterThanOrEqual(30);
    }
    expect(colorDistance(colors.gap, colors.defective)).toBeGreaterThanOrEqual(40);
    expect(colorDistance(colors.gap, colors.saturated)).toBeGreaterThanOrEqual(40);
    expect(colorDistance(colors.defective, colors.saturated)).toBeGreaterThanOrEqual(40);
  });

  it("replaces the teal and dark blue that sit on Viridis, and keeps its black gaps", () => {
    const colors = autoMaskColors(buildPalette("viridis"));
    expect(colors.gap).toEqual(DEFAULT_MASK_COLORS.gap);
    expect(colors.defective).not.toEqual(DEFAULT_MASK_COLORS.defective);
    expect(colors.saturated).not.toEqual(DEFAULT_MASK_COLORS.saturated);
  });

  it("keeps black gaps on ALBULA HDR, as ALBULA draws them", () => {
    expect(currentMaskColors({ colormap: "albulaHdr" }).gap).toEqual([0, 0, 0]);
  });

  it("does not draw gaps black on Grey Scale, where black is the lowest value", () => {
    expect(autoMaskColors(buildPalette("gray")).gap).not.toEqual([0, 0, 0]);
  });

  it("lets a hand-set colour win and keeps Auto for the rest", () => {
    const state = { colormap: "viridis", maskColorOverrides: { gap: "#123456", defective: null, saturated: null } };
    const colors = currentMaskColors(state);
    expect(colors.gap).toEqual([0x12, 0x34, 0x56]);
    expect(colors.saturated).toEqual(autoMaskColors(buildPalette("viridis")).saturated);
  });

  it("remembers a hand-set colour, and null goes back to Auto", () => {
    const state = { colormap: "gray" };
    setMaskColorOverride(state, "gap", "#00ff00");
    expect(readMaskColorOverrides().gap).toBe("#00ff00");
    expect(currentMaskColors(state).gap).toEqual([0, 255, 0]);
    setMaskColorOverride(state, "gap", null);
    expect(readMaskColorOverrides().gap).toBeNull();
    expect(currentMaskColors(state).gap).toEqual(autoMaskColors(buildPalette("gray")).gap);
  });

  it("offers the colour map's own ends, as drawn with Invert Color", () => {
    expect(mapEndColors({ colormap: "gray" })).toEqual({ low: [0, 0, 0], middle: [128, 128, 128], high: [255, 255, 255] });
    expect(mapEndColors({ colormap: "gray", invert: true }).low).toEqual([255, 255, 255]);
  });

  it("offers black instead of a second white on ALBULA HDR, whose ends are both white", () => {
    expect(mapEndColors({ colormap: "albulaHdr" })).toEqual({ low: [255, 255, 255], middle: [255, 0, 0], other: [0, 0, 0] });
  });

  it("says which colours blend into the map, without refusing them", () => {
    expect(blendsWithMap({ colormap: "gray" }, [128, 128, 128])).toBe(true);
    expect(blendsWithMap({ colormap: "gray" }, [255, 0, 255])).toBe(false);
  });
});
