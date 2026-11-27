/**
 * Min/Max Finder markers: a square around each pixel, so they read apart from
 * the Peak Finder's round markers when both are on; magenta for the highest,
 * cyan for the lowest, green for the picked ones. The highest is coral, not
 * pink: the reference resolution rings are magenta, and a pink square on a ring
 * read as part of it.
 *
 * The square frames the pixel itself, a few screen pixels out, and never
 * shrinks below MIN_HALF_PX: zoomed in, it outlines exactly the pixel the list
 * names; zoomed out, it stays big enough to find.
 */

import { markerInkScale, viewX, viewY } from "./overlay_painters.js";

export const MINMAX_MARKER_COLORS = Object.freeze({
  halo: "rgba(8, 10, 14, 0.72)",
  max: "rgba(255, 99, 71, 0.98)",
  min: "rgba(64, 220, 255, 0.98)",
  selected: "rgba(72, 255, 105, 0.98)",
});

/** On a light image the halo is heavier, as the peak markers' is. */
export const MINMAX_MARKER_COLORS_ON_LIGHT = Object.freeze({
  ...MINMAX_MARKER_COLORS,
  halo: "rgba(8, 10, 14, 0.9)",
});

export function minmaxColorsFor(tone) {
  return tone === "light" ? MINMAX_MARKER_COLORS_ON_LIGHT : MINMAX_MARKER_COLORS;
}

const MIN_HALF_PX = 5;
const PAD_PX = 2;
const SELECTED_EXTRA_PX = 3;

export function paintMinMaxMarkers(
  ctx,
  { pixels = [], selected = [], view, width, height, uiScale = 1, colors = MINMAX_MARKER_COLORS } = {},
) {
  if (!ctx || !Array.isArray(pixels) || !pixels.length) return;
  const ink = markerInkScale(pixels.length);
  ctx.setLineDash([]);
  // The selected ones last, so no other square crosses them.
  const picked = new Set(Array.isArray(selected) ? selected : []);
  const order = pixels.map((_, idx) => idx).filter((idx) => !picked.has(idx));
  picked.forEach((idx) => pixels[idx] && order.push(idx));

  order.forEach((idx) => {
    const pixel = pixels[idx];
    const isSelected = picked.has(idx);
    const extra = (PAD_PX + (isSelected ? SELECTED_EXTRA_PX : 0)) * uiScale;
    const halfX = Math.max(MIN_HALF_PX * uiScale, view.scaleX / 2) + extra;
    const halfY = Math.max(MIN_HALF_PX * uiScale, view.scaleY / 2) + extra;
    const sx = viewX(view, pixel.x + 0.5);
    const sy = viewY(view, pixel.y + 0.5);
    if (sx + halfX < 0 || sy + halfY < 0 || sx - halfX > width || sy - halfY > height) return;

    ctx.lineWidth = (isSelected ? 4.4 : 1.7 * ink + 1.7) * uiScale;
    ctx.strokeStyle = colors.halo;
    ctx.strokeRect(sx - halfX, sy - halfY, halfX * 2, halfY * 2);
    ctx.lineWidth = (isSelected ? 2.6 : 1.6 * ink) * uiScale;
    ctx.strokeStyle = isSelected ? colors.selected : pixel.kind === "min" ? colors.min : colors.max;
    ctx.strokeRect(sx - halfX, sy - halfY, halfX * 2, halfY * 2);
  });
}
