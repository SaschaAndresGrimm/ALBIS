/**
 * Where a pointer is on an ROI plot, and how a drag moves an axis.
 *
 * Pure helpers for roi_plot_bindings.js: the plot area pans both axes, the
 * tick-label strip left of it pans only y, the strip below it only x.
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
  lo -= fraction * span;
  hi -= fraction * span;

  if (Number.isFinite(domainMin) && Number.isFinite(domainMax)) {
    const dLo = toDisplay(Math.min(domainMin, domainMax), log);
    const dHi = toDisplay(Math.max(domainMin, domainMax), log);
    if (span >= dHi - dLo) {
      lo = dLo;
      hi = dHi;
    } else if (lo < dLo) {
      hi += dLo - lo;
      lo = dLo;
    } else if (hi > dHi) {
      lo -= hi - dHi;
      hi = dHi;
    }
  }
  return { min: fromDisplay(lo, log), max: fromDisplay(hi, log) };
}
