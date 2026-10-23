/**
 * The colours of masked pixels -- gaps, defective pixels, saturated pixels --
 * chosen so they cannot be mistaken for a value on the current colour map.
 *
 * They used to be fixed: gaps black, defective pixels dark blue, saturated
 * pixels teal. On Viridis the dark blue sits on the scale's own low end and
 * the teal in its middle, and on Grey Scale black gaps look like the lowest
 * values, so a marker read as data. "Auto" keeps each of those colours where
 * it stands well clear of every colour the map produces, and otherwise picks
 * the candidate farthest from the map that is also clear of the other two.
 * The choice depends on the map, not on Invert Color, which only reverses it.
 *
 * Each colour can also be set by hand (the colour chips under the histogram), kept
 * in this browser. Every renderer -- the image, the magnifier, PNG and GIF
 * exports -- reads them from here, so they cannot drift apart.
 */

import { buildPalette, getPaletteColorCount } from "./intensity_scale_utils.js";

export const MASK_CLASSES = ["gap", "defective", "saturated"];

export const DEFAULT_MASK_COLORS = Object.freeze({
  gap: [0, 0, 0],
  defective: [25, 50, 120],
  saturated: [88, 183, 198],
});

// Kept whatever the measure says. ALBULA HDR passes through black itself, but
// black gaps on it are what users of ALBULA know and expect; the other two
// classes are still chosen clear of the map and of black.
const PINNED = Object.freeze({
  albulaHdr: Object.freeze({ gap: [0, 0, 0] }),
});

// Tried in this order after a class's own default, and offered as the
// "stands out" choices when a colour is picked by hand.
export const CANDIDATES = [
  [0, 0, 0],
  [255, 255, 255],
  [255, 0, 255],
  [230, 25, 75],
  [255, 140, 0],
  [0, 220, 255],
  [0, 200, 80],
  [40, 80, 230],
  [255, 105, 180],
  [180, 255, 0],
];

// Perceived colour difference, CIE76 Delta E in Lab: about 2.3 is just
// noticeable, about 30 two clearly different colours.
const CLEAR_OF_MAP = 30;
const CLEAR_OF_EACH_OTHER = 40;
const STORAGE_KEY = "albis.maskColors";

function srgbToLinear(v) {
  const c = v / 255;
  return c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
}

function rgbToLab(rgb) {
  const [r, g, b] = rgb.map(srgbToLinear);
  const x = (0.4124 * r + 0.3576 * g + 0.1805 * b) / 0.95047;
  const y = 0.2126 * r + 0.7152 * g + 0.0722 * b;
  const z = (0.0193 * r + 0.1192 * g + 0.9505 * b) / 1.08883;
  const f = (t) => (t > 0.008856 ? Math.cbrt(t) : 7.787 * t + 16 / 116);
  return [116 * f(y) - 16, 500 * (f(x) - f(y)), 200 * (f(y) - f(z))];
}

export function colorDistance(a, b) {
  const la = rgbToLab(a);
  const lb = rgbToLab(b);
  return Math.hypot(la[0] - lb[0], la[1] - lb[1], la[2] - lb[2]);
}

function paletteColors(palette) {
  const count = getPaletteColorCount(palette);
  const step = Math.max(1, Math.floor(count / 256));
  const colors = [];
  for (let i = 0; i < count; i += step) colors.push([palette[i * 4], palette[i * 4 + 1], palette[i * 4 + 2]]);
  return colors;
}

function clearance(color, mapColors) {
  let nearest = Infinity;
  for (const c of mapColors) nearest = Math.min(nearest, colorDistance(color, c));
  return nearest;
}

/**
 * The automatic colours for a palette: { gap, defective, saturated } as [r, g, b].
 * `pinned` fixes some of them up front; the rest are chosen clear of those too.
 */
export function autoMaskColors(palette, pinned = {}) {
  const mapColors = paletteColors(palette);
  const chosen = {};
  for (const [cls, color] of Object.entries(pinned)) chosen[cls] = color.slice();
  for (const cls of MASK_CLASSES) {
    if (chosen[cls]) continue;
    const taken = Object.values(chosen);
    const distinct = (color) => taken.every((other) => colorDistance(color, other) >= CLEAR_OF_EACH_OTHER);
    const own = DEFAULT_MASK_COLORS[cls];
    if (clearance(own, mapColors) >= CLEAR_OF_MAP && distinct(own)) {
      chosen[cls] = own.slice();
      continue;
    }
    let best = null;
    let bestScore = -1;
    for (const candidate of CANDIDATES) {
      if (!distinct(candidate)) continue;
      const score = clearance(candidate, mapColors);
      if (score > bestScore) {
        best = candidate;
        bestScore = score;
      }
    }
    chosen[cls] = (best || own).slice();
  }
  return chosen;
}

export function hexToRgb(hex) {
  const match = /^#?([0-9a-f]{6})$/i.exec(String(hex || "").trim());
  if (!match) return null;
  const n = parseInt(match[1], 16);
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
}

export function rgbToHex(rgb) {
  return `#${rgb.map((v) => Math.max(0, Math.min(255, Math.round(v))).toString(16).padStart(2, "0")).join("")}`;
}

/** Hand-set colours as { gap: "#rrggbb" | null, ... }; null means Auto. */
export function readMaskColorOverrides() {
  try {
    const stored = JSON.parse(window.localStorage?.getItem(STORAGE_KEY) || "null");
    const out = {};
    for (const cls of MASK_CLASSES) out[cls] = hexToRgb(stored?.[cls]) ? stored[cls] : null;
    return out;
  } catch {
    return { gap: null, defective: null, saturated: null };
  }
}

export function storeMaskColorOverrides(overrides) {
  try {
    window.localStorage?.setItem(STORAGE_KEY, JSON.stringify(overrides));
  } catch {
    // Kept for this session only.
  }
}

const autoCache = new Map();

/**
 * The colours to draw with now: the hand-set ones where set, Auto for the
 * rest, from `state.colormap` and `state.maskColorOverrides`.
 */
export function currentMaskColors(state) {
  const name = String(state?.colormap || "");
  if (!autoCache.has(name)) autoCache.set(name, autoMaskColors(buildPalette(name), PINNED[name]));
  const auto = autoCache.get(name);
  const overrides = state?.maskColorOverrides || {};
  const out = {};
  for (const cls of MASK_CLASSES) out[cls] = hexToRgb(overrides[cls]) || auto[cls];
  return out;
}

/**
 * Set one class's colour by hand (a "#rrggbb"), or null for Auto, and keep it.
 */
export function setMaskColorOverride(state, cls, hex) {
  const next = { ...(state.maskColorOverrides || readMaskColorOverrides()), [cls]: hexToRgb(hex) ? hex : null };
  state.maskColorOverrides = next;
  storeMaskColorOverrides(next);
  return next;
}

/**
 * The colour map's own colours at its low end, middle and high end, as drawn
 * (Invert Color included). A map whose ends are the same colour -- ALBULA HDR
 * is white at both -- offers, instead of its high end, the map's colour least
 * like the other two (black on ALBULA HDR), returned as `other`.
 */
export function mapEndColors(state) {
  const palette = buildPalette(String(state?.colormap || ""));
  const count = getPaletteColorCount(palette);
  const at = (norm) => {
    const n = state?.invert ? 1 - norm : norm;
    const i = Math.min(count - 1, Math.max(0, Math.round(n * (count - 1)))) * 4;
    return [palette[i], palette[i + 1], palette[i + 2]];
  };
  const low = at(0);
  const middle = at(0.5);
  const high = at(1);
  if (colorDistance(low, high) >= CLEAR_OF_EACH_OTHER) return { low, middle, high };
  let other = high;
  let best = -1;
  for (const color of paletteColors(palette)) {
    const score = Math.min(colorDistance(color, low), colorDistance(color, middle));
    if (score > best) {
      other = color;
      best = score;
    }
  }
  return { low, middle, other };
}

/** Whether a colour is close enough to one of the map's to blend into the image. */
export function blendsWithMap(state, rgb) {
  const palette = buildPalette(String(state?.colormap || ""));
  return clearance(rgb, paletteColors(palette)) < CLEAR_OF_MAP;
}
