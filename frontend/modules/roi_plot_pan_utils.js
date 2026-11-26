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

// Zooming out an axis without a domain (y) stops once the data fills this
// fraction of it: without a stop, a few wheel turns took a log count axis to
// 1e50, with the data a flat line along the bottom.
const MAX_ZOOM_OUT = 4;

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
 * zooming in before anything changed on screen. Without a domain, `dataMin`
 * and `dataMax` stop zooming out once the data fills a quarter of the axis,
 * and keep the axis at zero or above for data that is never negative.
 *
 * Returns { min, max } in data units, or null when the range has no span.
 */
export function zoomAxisRange(
  { min, max, domainMin = null, domainMax = null, log = false, minSpan = 0, dataMin = null, dataMax = null },
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
  const hasData = Number.isFinite(dataMin) && Number.isFinite(dataMax);
  if (hasData && next > span) {
    const dataSpan = toDisplay(Math.max(dataMin, dataMax), log) - toDisplay(Math.min(dataMin, dataMax), log);
    if (dataSpan > 0) next = Math.min(next, Math.max(span, MAX_ZOOM_OUT * dataSpan));
  }
  let [nextLo, nextHi] = clampToDomain(anchorValue - at * next, anchorValue + (1 - at) * next, domainMin, domainMax, log);
  // Data that is never negative (counts, a log axis) gets no axis below zero.
  const zero = toDisplay(0, log);
  if (hasData && Math.min(dataMin, dataMax) >= 0 && nextLo < zero && lo >= zero) {
    nextHi += zero - nextLo;
    nextLo = zero;
  }
  return { min: fromDisplay(nextLo, log), max: fromDisplay(nextHi, log) };
}

// How much of the view the data must keep when an axis may leave it.
const MIN_DATA_IN_VIEW = 0.1;

// Keep at least part of the data in view, without pulling the view onto it:
// the range may run past the data, but not so far that the curve is lost.
function keepDataInView(lo, hi, domainMin, domainMax, log) {
  if (!Number.isFinite(domainMin) || !Number.isFinite(domainMax)) return [lo, hi];
  const dLo = toDisplay(Math.min(domainMin, domainMax), log);
  const dHi = toDisplay(Math.max(domainMin, domainMax), log);
  const keep = Math.min(MIN_DATA_IN_VIEW * (hi - lo), (dHi - dLo) / 2);
  if (lo > dHi - keep) return [dHi - keep, hi - (lo - (dHi - keep))];
  if (hi < dLo + keep) return [lo + (dLo + keep - hi), dLo + keep];
  return [lo, hi];
}

/**
 * Shift an axis range by `fraction` of its span towards smaller values (a
 * negative fraction shifts towards larger ones). On a log axis the shift is
 * taken on the drawn scale, so the curve moves with the pointer at any
 * height.
 *
 * With a domain, `bound` says how the range stays near the data:
 * "inside" (x) keeps it within the data, as the plot draws nothing past the
 * ends of a profile; "overlap" (y) lets it run past the data as long as a
 * tenth of the view still shows some. y used to be kept inside too, but an
 * autoscaled y range is a little wider than the data, so a drag could only
 * snap it onto the data and then not move it at all: the curve rescaled
 * slightly instead of moving.
 *
 * Returns { min, max } in data units, or null when the range has no span.
 */
export function panAxisRange(
  { min, max, domainMin = null, domainMax = null, log = false, bound = "inside" },
  fraction
) {
  if (!Number.isFinite(min) || !Number.isFinite(max) || !Number.isFinite(fraction)) return null;
  let lo = toDisplay(Math.min(min, max), log);
  let hi = toDisplay(Math.max(min, max), log);
  const span = hi - lo;
  if (!(span > 0)) return null;
  const keep = bound === "overlap" ? keepDataInView : clampToDomain;
  [lo, hi] = keep(lo - fraction * span, hi - fraction * span, domainMin, domainMax, log);
  return { min: fromDisplay(lo, log), max: fromDisplay(hi, log) };
}

// A mouse wheel notch is about 100 px of delta; a trackpad pinch arrives as
// a wheel event with ctrlKey and a delta of a few px per event, so it needs
// a larger rate to zoom at the speed the fingers move.
const WHEEL_ZOOM_RATE = 0.002;
const PINCH_ZOOM_RATE = 0.01;
const MAX_WHEEL_DELTA = 200;

/**
 * What a wheel event over a plot asks for, from the browser's wheel fields:
 * { kind: "zoom", factor } (above 1 zooms in) for the mouse wheel, a vertical
 * two-finger scroll or a pinch, { kind: "pan-x", px } for a sideways
 * two-finger swipe (or Shift with the wheel, which the browser turns
 * sideways), or null.
 */
export function roiPlotWheelAction({ deltaX = 0, deltaY = 0, deltaMode = 0, ctrlKey = false } = {}) {
  const scale = deltaMode === 1 ? 16 : deltaMode === 2 ? 120 : 1;
  const clamp = (value) => Math.max(-MAX_WHEEL_DELTA, Math.min(MAX_WHEEL_DELTA, value));
  const dx = Number.isFinite(deltaX) ? clamp(deltaX * scale) : 0;
  const dy = Number.isFinite(deltaY) ? clamp(deltaY * scale) : 0;
  if (ctrlKey) {
    return dy ? { kind: "zoom", factor: Math.exp(-dy * PINCH_ZOOM_RATE) } : null;
  }
  if (Math.abs(dx) > Math.abs(dy)) return { kind: "pan-x", px: dx };
  return dy ? { kind: "zoom", factor: Math.exp(-dy * WHEEL_ZOOM_RATE) } : null;
}

// A box narrower than this in one direction zooms the other direction only:
// a flat box is a zoom along x, a thin tall one along y.
const BOX_MIN_PX = 4;

/**
 * The axis ranges a box dragged over the plot covers, from its corners in
 * canvas pixels (clamped to the data area): { x: { min, max } | null,
 * y: { min, max } | null }, null for a direction the box is too thin in. On
 * a log y axis the box is read on the drawn scale. The x range is widened to
 * `minXSpan` about its middle when narrower.
 */
export function roiPlotBoxLimits(plot, { x0, y0, x1, y1 }, { minXSpan = 0 } = {}) {
  const left = plot.padL;
  const right = plot.width - plot.padR;
  const top = plot.padT;
  const bottom = plot.height - plot.padB;
  const clampX = (v) => Math.max(left, Math.min(right, v));
  const clampY = (v) => Math.max(top, Math.min(bottom, v));
  const ax = clampX(Math.min(x0, x1));
  const bx = clampX(Math.max(x0, x1));
  const ay = clampY(Math.min(y0, y1));
  const by = clampY(Math.max(y0, y1));
  const result = { x: null, y: null };
  if (bx - ax >= BOX_MIN_PX && right > left) {
    const span = plot.xMax - plot.xMin;
    let min = plot.xMin + ((ax - left) / (right - left)) * span;
    let max = plot.xMin + ((bx - left) / (right - left)) * span;
    if (max - min < minXSpan) {
      const mid = (min + max) / 2;
      min = mid - minXSpan / 2;
      max = mid + minXSpan / 2;
    }
    result.x = { min, max };
  }
  if (by - ay >= BOX_MIN_PX && bottom > top) {
    const log = Boolean(plot.log);
    const lo = toDisplay(plot.yMin, log);
    const hi = toDisplay(plot.yMax, log);
    const at = (py) => fromDisplay(lo + ((bottom - py) / (bottom - top)) * (hi - lo), log);
    result.y = { min: at(by), max: at(ay) };
  }
  return result;
}
