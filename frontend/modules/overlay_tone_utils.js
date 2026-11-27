/**
 * Overlay colours that follow the image's background.
 *
 * Rings and markers sit on the image, which stays dark in both themes but is
 * not always dark: the default Albula HDR map is white where nothing was
 * counted, and Invert flips any map. A halo that helps on black (white) is
 * invisible on white, and the other way round, so the overlay picks its
 * halos and a slightly stronger ink for the tone the image shows at its
 * low end, where most pixels are. Hues stay the same in both tones, so a
 * ring or a peak is recognised at a glance whichever colour map is on.
 */

import { buildPalette } from "./intensity_scale_utils.js";
import { SCREEN_OVERLAY_COLORS } from "./overlay_painters.js";

const LIGHT_LUMINANCE = 0.55;

const lowEndCache = new Map();

/** Rec. 709 luminance, 0..1, of the colour map's background end. */
function lowEndLuminance(colormap, invert) {
  const key = `${colormap}|${invert ? 1 : 0}`;
  if (lowEndCache.has(key)) return lowEndCache.get(key);
  const palette = buildPalette(colormap);
  const count = palette.length / 4;
  // A few entries at the low end, not just the first: the lowest counts are
  // what the background is made of, and a ramp starts steep.
  const span = Math.max(1, Math.round(count * 0.04));
  let sum = 0;
  for (let i = 0; i < span; i += 1) {
    const at = (invert ? count - 1 - i : i) * 4;
    sum += (0.2126 * palette[at] + 0.7152 * palette[at + 1] + 0.0722 * palette[at + 2]) / 255;
  }
  const value = sum / span;
  lowEndCache.set(key, value);
  return value;
}

/** "light" when the image's background end is light, else "dark". */
export function imageToneFor(colormap, invert = false) {
  return lowEndLuminance(String(colormap || ""), Boolean(invert)) >= LIGHT_LUMINANCE ? "light" : "dark";
}

// Same hues as on dark. Halos turn dark and heavier so every ring keeps an
// edge on white; the user ring brightens, since the dark blue it uses on
// dark frames would sit on a dark halo; amber warms a step for weight.
export const SCREEN_OVERLAY_COLORS_ON_LIGHT = Object.freeze({
  ...SCREEN_OVERLAY_COLORS,
  ringHalo: "rgba(12, 14, 22, 0.62)",
  ring: "rgba(70, 140, 255, 0.98)",
  ringActive: "rgba(130, 190, 255, 1)",
  referenceRing: "rgba(235, 70, 235, 0.98)",
  peakHalo: "rgba(8, 10, 14, 0.9)",
  peakRing: "rgba(255, 184, 64, 1)",
  externalHalo: "rgba(10, 10, 10, 0.85)",
});

export function overlayColorsFor(tone) {
  return tone === "light" ? SCREEN_OVERLAY_COLORS_ON_LIGHT : SCREEN_OVERLAY_COLORS;
}
