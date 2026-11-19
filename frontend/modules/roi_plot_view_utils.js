/**
 * Which part of an ROI plot's data is in view, for roi_plot_renderer.js and
 * the tooltip.
 *
 * The view is the x range that was asked for, not rounded to whole samples.
 * Rounding it was what made wheel zoom on a short range go wrong: the wheel
 * took its next step from the drawn range, which had lost up to one sample at
 * each end, so every notch zoomed in by about two samples whatever the zoom
 * factor, zooming out from a few samples rounded back to where it was, and
 * the uneven rounding slid the point under the pointer sideways.
 */

/**
 * The x extent of the data: first to last sample, or for a histogram (whose
 * x values are bin centres) the outer edges of the first and last bins.
 */
export function roiPlotXDomain({ length, xStart, xStep, histogram = false }) {
  const last = xStart + (Math.max(1, length) - 1) * xStep;
  let min = Math.min(xStart, last);
  let max = Math.max(xStart, last);
  if (histogram) {
    min -= Math.abs(xStep) / 2;
    max += Math.abs(xStep) / 2;
  }
  return { min, max };
}

function clampIndex(value, length) {
  return Math.max(0, Math.min(length - 1, value));
}

/**
 * The x range to draw and the samples it needs.
 *
 * min / max: the view, the pinned limits clamped to the data (the whole data
 * when nothing is pinned, or when the pin misses this data entirely, as a box
 * profile's pin does once the box has moved).
 * firstIdx / lastIdx: the samples to draw, one past the view on each side so
 * the line runs to the frame instead of stopping at the last whole sample.
 * yFirstIdx / yLastIdx: the samples inside the view, which set the y range.
 */
export function roiPlotXView({ length, xStart, xStep, histogram = false, limits = null, autoscale = true }) {
  const domain = roiPlotXDomain({ length, xStart, xStep, histogram });
  let min = domain.min;
  let max = domain.max;
  if (!autoscale && limits) {
    const lo = Number.isFinite(limits.xMin) ? Math.max(domain.min, limits.xMin) : domain.min;
    const hi = Number.isFinite(limits.xMax) ? Math.min(domain.max, limits.xMax) : domain.max;
    if (hi > lo) {
      min = lo;
      max = hi;
    }
  }
  if (!(length > 0)) {
    return { min, max, domainMin: domain.min, domainMax: domain.max, firstIdx: 0, lastIdx: -1, yFirstIdx: 0, yLastIdx: -1 };
  }
  const a = (min - xStart) / xStep;
  const b = (max - xStart) / xStep;
  const lowIdx = Math.min(a, b);
  const highIdx = Math.max(a, b);
  // A histogram bar is in view when any of it is: its centre within half a bin.
  const slack = histogram ? 0.5 : 0;
  let yFirstIdx = clampIndex(Math.ceil(lowIdx - slack), length);
  let yLastIdx = clampIndex(Math.floor(highIdx + slack), length);
  const firstIdx = clampIndex(Math.floor(lowIdx), length);
  const lastIdx = clampIndex(Math.ceil(highIdx), length);
  if (yLastIdx < yFirstIdx) {
    // Zoomed in between two samples: the two around the view set the range.
    yFirstIdx = firstIdx;
    yLastIdx = lastIdx;
  }
  return { min, max, domainMin: domain.min, domainMax: domain.max, firstIdx, lastIdx, yFirstIdx, yLastIdx };
}

/**
 * The index into plot.data of the sample nearest to a pointer `px` pixels
 * from the canvas's left edge, or -1 when there is none.
 */
export function roiPlotSampleIndexAt(plot, px) {
  if (!plot?.data?.length) return -1;
  const plotW = plot.width - plot.padL - plot.padR;
  const step = plot.xStep || 1;
  // Without a view, the data spans the plot.
  const viewMin = Number.isFinite(plot.xMin) ? plot.xMin : plot.xStart;
  const viewMax = Number.isFinite(plot.xMax) ? plot.xMax : plot.xStart + (plot.data.length - 1) * step;
  const span = viewMax - viewMin;
  const xValue = viewMin + (plotW > 0 && span ? (px - plot.padL) / plotW : 0) * span;
  const idx = Math.round((xValue - plot.xStart) / step);
  return clampIndex(idx, plot.data.length);
}
