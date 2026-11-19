/**
 * Where a pointer is on an ROI plot, and how a drag or the wheel changes an
 * axis range.
 *
 * Pure helpers for roi_plot_bindings.js: the plot area pans and zooms both
 * axes, the tick-label strip left of it only y, the strip below it only x.
 */

/**
 * The part of the plot under a point in canvas pixels: "plot" for the data
 * area, "y-axis" for the strip left of it, "x-axis" for the strip below it,
 * null elsewhere. The corner where the two strips meet belongs to neither, so
 * a drag or wheel there never picks an axis by accident.
 */
export function roiPlotHitZone(plot, x, y) {
  if (!plot) return null;
  const left = plot.padL;
  const right = plot.width - plot.padR;
  const top = plot.padT;
  const bottom = plot.height - plot.padB;
  const inColumn = x >= left && x <= right;
  const inRow = y >= top && y <= bottom;
  if (inColumn && inRow) return "plot";
  if (inRow && x >= 0 && x < left) return "y-axis";
  if (inColumn && y > bottom && y <= plot.height) return "x-axis";
  return null;
}

/** Which axes a drag starting in a zone moves: "both", "x", "y" or null. */
export function roiPlotPanMode(zone) {
  if (zone === "plot") return "both";
  if (zone === "x-axis") return "x";
  if (zone === "y-axis") return "y";
  return null;
}

// The log scale plots log10(1 + v), as roi_plot_renderer.js does.
function toDisplay(value, log) {
  return log ? Math.log10(1 + Math.max(0, value)) : value;
}

function fromDisplay(value, log) {
  return log ? Math.max(0, 10 ** value - 1) : value;
}

function clampToDomain(lo, hi, domainMin, domainMax, log) {
  if (!Number.isFinite(domainMin) || !Number.isFinite(domainMax)) return [lo, hi];
  const dLo = toDisplay(Math.min(domainMin, domainMax), log);
  const dHi = toDisplay(Math.max(domainMin, domainMax), log);
  if (hi - lo >= dHi - dLo) return [dLo, dHi];
  if (lo < dLo) return [dLo, hi + (dLo - lo)];
  if (hi > dHi) return [lo - (hi - dHi), dHi];
  return [lo, hi];
}

// Below about a millionth of the values shown, the tick labels run out of
// digits and the plot only shows floating-point noise.
const RELATIVE_MIN_SPAN = 1e-6;

/**
 * Zoom an axis range by `factor` (above 1 zooms in) about the point at
 * `anchor`, a fraction of the axis from its low end (where the pointer is),
 * so the value under the pointer stays under it.
 *
 * Zooming in stops at `minSpan` (in data units; on a log axis, in the drawn
 * log units) and at a millionth of the values shown. With a domain, zooming
 * out stops at the whole data and the range is kept inside it: x past the
 * data shows nothing, and a range grown past it would need notches of
 * zooming in before anything changed on screen.
 *
 * Returns { min, max } in data units, or null when the range has no span.
 */
export function zoomAxisRange(
  { min, max, domainMin = null, domainMax = null, log = false, minSpan = 0 },
  anchor,
  factor
) {
  if (![min, max, anchor, factor].every(Number.isFinite) || !(factor > 0)) return null;
  const lo = toDisplay(Math.min(min, max), log);
  const hi = toDisplay(Math.max(min, max), log);
  const span = hi - lo;
  if (!(span > 0)) return null;
  const at = Math.max(0, Math.min(1, anchor));
  const anchorValue = lo + at * span;
  const floor = Math.max(minSpan, Math.max(Math.abs(lo), Math.abs(hi)) * RELATIVE_MIN_SPAN);
  let next = span / factor;
  // Never zoom in past the floor; a range already below it may only grow.
  if (next < span) next = Math.max(next, Math.min(span, floor));
  const [nextLo, nextHi] = clampToDomain(anchorValue - at * next, anchorValue + (1 - at) * next, domainMin, domainMax, log);
  return { min: fromDisplay(nextLo, log), max: fromDisplay(nextHi, log) };
}

/**
 * Shift an axis range by `fraction` of its span towards smaller values (a
 * negative fraction shifts towards larger ones), keeping it inside the data
 * domain when one is given. On a log axis the shift is taken on the drawn
 * scale, so the curve moves with the pointer at any height.
 *
 * Returns { min, max } in data units, or null when the range has no span.
 */
export function panAxisRange({ min, max, domainMin = null, domainMax = null, log = false }, fraction) {
  if (!Number.isFinite(min) || !Number.isFinite(max) || !Number.isFinite(fraction)) return null;
  let lo = toDisplay(Math.min(min, max), log);
  let hi = toDisplay(Math.max(min, max), log);
  const span = hi - lo;
  if (!(span > 0)) return null;
  [lo, hi] = clampToDomain(lo - fraction * span, hi - fraction * span, domainMin, domainMax, log);
  return { min: fromDisplay(lo, log), max: fromDisplay(hi, log) };
}
