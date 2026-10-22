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
 * Each colour can also be set by hand (View -> Advanced view controls), kept
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

// Tried in this order after a class's own default.
const CANDIDATES = [
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
 * View -> Advanced view controls: a colour picker per class showing the
 * colour in use, and Auto, which drops every hand-set colour.
 */
export function createMaskColorControls({ state, elements, callbacks = {} }) {
  const { inputs = {}, autoBtn } = elements;
  const { onChange } = callbacks;
  if (!state.maskColorOverrides) state.maskColorOverrides = readMaskColorOverrides();

  function refresh() {
    const colors = currentMaskColors(state);
    for (const cls of MASK_CLASSES) {
      const input = inputs[cls];
      if (!input) continue;
      input.value = rgbToHex(colors[cls]);
      input.closest("label")?.classList.toggle("is-custom", Boolean(state.maskColorOverrides[cls]));
    }
    if (autoBtn) autoBtn.disabled = MASK_CLASSES.every((cls) => !state.maskColorOverrides[cls]);
  }

  function set(overrides) {
    state.maskColorOverrides = overrides;
    storeMaskColorOverrides(overrides);
    refresh();
    onChange?.();
  }

  for (const cls of MASK_CLASSES) {
    inputs[cls]?.addEventListener("input", (event) => {
      set({ ...state.maskColorOverrides, [cls]: event.target.value });
    });
  }
  autoBtn?.addEventListener("click", () => set({ gap: null, defective: null, saturated: null }));
  refresh();
  return { refresh };
}
