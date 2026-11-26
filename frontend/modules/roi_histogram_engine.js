/**
 * The ROI histogram: which bins, over which range, and what the plot shows
 * first.
 *
 * Detector counts are a pile near zero and a long, thin tail: in a typical
 * diffraction ROI 99 % of the pixels are below 30 and one is at 4086. Bins
 * spread evenly from the minimum to the maximum put almost every pixel in the
 * first bar and leave the rest of the plot empty. So:
 *
 * - The plot opens on the range that holds all but the outer 0.1 % at each
 *   end, when those tails would stretch the axis to more than twice that
 *   width. The bins still run over the whole data (up to a budget of bins),
 *   so zooming or panning out reaches the tail, and the plot says how many
 *   pixels lie outside what it shows.
 * - Whole-number data gets whole-number bin widths, with the edges between
 *   two values. A bin 21.3 counts wide holds 21 values or 22 in turn, and the
 *   histogram showed that as a regular comb the data does not have.
 * - Whole-number data is binned from the table of counts per value the level
 *   bands build anyway (level_bands_utils.js), which costs the value range
 *   rather than the pixel count: a whole EIGER2 16M frame took 0.8 s here,
 *   on every frame step.
 * - Auto bins in view are about 1.5 sqrt(N), never narrower than two screen
 *   pixels (`maxViewBins`, from the plot width).
 */

import { buildLevelSample } from "./level_bands_utils.js";

export const ROI_HISTOGRAM_MIN_FIXED_BINS = 2;
export const ROI_HISTOGRAM_MAX_FIXED_BINS = 512;
export const ROI_HISTOGRAM_DEFAULT_FIXED_BINS = 128;

// The share of pixels at each end the opening view may leave out.
export const ROI_HISTOGRAM_TAIL_FRACTION = 0.001;
// Leave the tails out only when they would make the axis this much wider.
const TAIL_STRETCH = 2;
// Most bins over the whole data. Past it, the far tail is not binned at all
// and is only counted (outsideBelow / outsideAbove).
export const ROI_HISTOGRAM_MAX_BINS = 4096;
const AUTO_MIN_BINS = 16;
const AUTO_DEFAULT_MAX_VIEW_BINS = 192;
// Narrowest bar, in CSS pixels, auto bins are chosen for.
const MIN_BAR_PX = 2;
// Float quantiles are taken from at most this many evenly spaced values.
const QUANTILE_SAMPLE_MAX = 1 << 20;

export function normalizeRoiHistogramBinCount(value) {
  const count = Math.round(Number(value));
  if (!Number.isFinite(count)) return ROI_HISTOGRAM_DEFAULT_FIXED_BINS;
  return Math.max(ROI_HISTOGRAM_MIN_FIXED_BINS, Math.min(ROI_HISTOGRAM_MAX_FIXED_BINS, count));
}

/** Most auto bins in view for a plot this many CSS pixels wide (0: not laid out). */
export function roiHistogramMaxViewBins(plotWidthPx) {
  const width = Number(plotWidthPx);
  if (!(width > 0)) return AUTO_DEFAULT_MAX_VIEW_BINS;
  return Math.max(AUTO_MIN_BINS, Math.min(ROI_HISTOGRAM_MAX_FIXED_BINS, Math.floor(width / MIN_BAR_PX)));
}

function looksIntegral(values) {
  for (let i = 0; i < values.length; i += 1) {
    const v = values[i];
    if (Number.isFinite(v) && v !== Math.floor(v)) return false;
  }
  return true;
}

// The k-th smallest (0-based) of `a`, which it reorders. Hoare selection.
function selectInPlace(a, k) {
  let left = 0;
  let right = a.length - 1;
  while (right > left) {
    const pivot = a[(left + right) >> 1];
    let i = left;
    let j = right;
    while (i <= j) {
      while (a[i] < pivot) i += 1;
      while (a[j] > pivot) j -= 1;
      if (i <= j) {
        const tmp = a[i];
        a[i] = a[j];
        a[j] = tmp;
        i += 1;
        j -= 1;
      }
    }
    if (k <= j) right = j;
    else if (k >= i) left = i;
    else return a[k];
  }
  return a[k];
}

/** Min, max, count and the two tail quantiles of a table of counts per value. */
function summariseTable(sample, tail) {
  const { cumulative, min, total } = sample;
  const counts = new Float64Array(cumulative.length);
  let first = -1;
  let last = -1;
  for (let k = 0; k < cumulative.length; k += 1) {
    counts[k] = cumulative[k] - (k ? cumulative[k - 1] : 0);
    if (counts[k] > 0) {
      if (first < 0) first = k;
      last = k;
    }
  }
  // The value at order statistic `rank`: the first with more than `rank` at or below it.
  const orderStat = (rank) => {
    let lo = 0;
    let hi = cumulative.length - 1;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (cumulative[mid] > rank) hi = mid;
      else lo = mid + 1;
    }
    return min + lo;
  };
  const cut = Math.floor(tail * total);
  return {
    counts,
    countsMin: min,
    min: min + first,
    max: min + last,
    total,
    qLo: orderStat(cut),
    qHi: orderStat(total - 1 - cut),
  };
}

/** The same, for a list of values (float data, or whole numbers too far apart for a table). */
function summariseValues(values, tail) {
  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;
  let total = 0;
  for (let i = 0; i < values.length; i += 1) {
    const v = values[i];
    if (!Number.isFinite(v)) continue;
    total += 1;
    if (v < min) min = v;
    if (v > max) max = v;
  }
  if (!total) return null;
  const stride = Math.max(1, Math.ceil(total / QUANTILE_SAMPLE_MAX));
  const sample = new Float64Array(Math.ceil(total / stride));
  let n = 0;
  let seen = 0;
  for (let i = 0; i < values.length; i += 1) {
    const v = values[i];
    if (!Number.isFinite(v)) continue;
    if (seen % stride === 0 && n < sample.length) sample[n++] = v;
    seen += 1;
  }
  const picked = sample.subarray(0, n);
  const cut = Math.floor(tail * n);
  const qLo = selectInPlace(picked, cut);
  const qHi = selectInPlace(picked, n - 1 - cut);
  return { min, max, total, qLo, qHi };
}

/** Bins below and above the view that fit the budget, the nearer tail first. */
function splitBudget(needBelow, needAbove, budget) {
  if (needBelow + needAbove <= budget) return [needBelow, needAbove];
  const half = Math.floor(budget / 2);
  if (needBelow <= half) return [needBelow, budget - needBelow];
  if (needAbove <= half) return [budget - needAbove, needAbove];
  return [half, budget - half];
}

/**
 * The histogram of `values`.
 *
 * options:
 * - mode: "auto" (bins from the pixel count and `maxViewBins`) or "fixed"
 *   (`count` bins across the view).
 * - integral: the values are whole numbers (detected when not given).
 * - levelSample: the level bands' sample of the same values; a table of
 *   counts is binned instead of the values.
 * - maxViewBins: most auto bins in view (roiHistogramMaxViewBins).
 *
 * Returns null without a finite value, else
 * { data, xStart, xStep, xTickMode, binWidth, view, outsideBelow, outsideAbove }:
 * `data` the counts per bin over the whole binned range, `xStart` the centre
 * of the first bin, `binWidth` the number of whole values per bin (0 for
 * float data), `view` the { min, max } the plot opens on (null: everything),
 * outsideBelow / outsideAbove the pixels beyond the binned range.
 */
export function buildRoiHistogram(values, options = {}) {
  const list = values || [];
  const integral =
    typeof options.integral === "boolean" ? options.integral : looksIntegral(list);
  let sample = options.levelSample;
  if (integral && sample?.kind !== "table" && list.length) {
    sample = buildLevelSample(list, { integral: true });
  }
  const table = integral && sample?.kind === "table" && sample.total > 0 ? sample : null;
  const summary = table
    ? summariseTable(table, ROI_HISTOGRAM_TAIL_FRACTION)
    : summariseValues(list, ROI_HISTOGRAM_TAIL_FRACTION);
  if (!summary || !summary.total) return null;
  const { min, max, total } = summary;
  const xTickMode = integral ? "integer" : "";

  if (!(max > min)) {
    return {
      data: [total],
      xStart: min,
      xStep: integral ? 1 : Math.max(1e-6, Math.abs(min) * 1e-6),
      xTickMode,
      binWidth: integral ? 1 : 0,
      view: null,
      outsideBelow: 0,
      outsideAbove: 0,
    };
  }

  let lo = min;
  let hi = max;
  const { qLo, qHi } = summary;
  if (qHi > qLo && (qHi - qLo) * TAIL_STRETCH < max - min) {
    lo = qLo;
    hi = qHi;
  }
  const clipped = lo > min || hi < max;

  const fixed = options.mode === "fixed";
  const maxViewBins = Math.max(
    AUTO_MIN_BINS,
    Math.round(Number(options.maxViewBins) || AUTO_DEFAULT_MAX_VIEW_BINS),
  );
  const autoBins = Math.max(AUTO_MIN_BINS, Math.min(maxViewBins, Math.round(Math.sqrt(total) * 1.5)));

  // Bin grid: bin i covers [start + i * w, start + (i + 1) * w), for whole
  // numbers the values start + i * w ... start + (i + 1) * w - 1.
  let w;
  let viewBins;
  if (integral) {
    const span = hi - lo + 1;
    if (fixed) w = Math.max(1, Math.ceil(span / normalizeRoiHistogramBinCount(options.count)));
    else w = span <= maxViewBins ? 1 : Math.ceil(span / autoBins);
    viewBins = Math.ceil(span / w);
  } else {
    viewBins = fixed ? normalizeRoiHistogramBinCount(options.count) : autoBins;
    w = (hi - lo) / viewBins;
  }
  const viewEnd = lo + viewBins * w; // first value (or edge) past the view
  // The float grid's last edge lands on the maximum give or take rounding,
  // which must not ask for a bin of its own.
  const binsFor = (extent) => Math.max(0, Math.ceil(extent / w - 1e-9));
  const needBelow = binsFor(lo - min);
  const needAbove = binsFor(integral ? max + 1 - viewEnd : max - viewEnd);
  const [below, above] = splitBudget(needBelow, needAbove, Math.max(0, ROI_HISTOGRAM_MAX_BINS - viewBins));
  const start = lo - below * w;
  const binCount = below + viewBins + above;
  const coversBelow = below === needBelow;
  const coversAbove = above === needAbove;
  const counts = new Float64Array(binCount);
  let outsideBelow = 0;
  let outsideAbove = 0;
  const add = (value, weight) => {
    let idx = Math.floor((value - start) / w);
    // Float rounding at the outer edges, and the maximum itself on a float
    // grid whose last edge it is.
    if (idx < 0 && coversBelow) idx = 0;
    if (idx >= binCount && coversAbove) idx = binCount - 1;
    if (idx < 0) outsideBelow += weight;
    else if (idx >= binCount) outsideAbove += weight;
    else counts[idx] += weight;
  };
  if (table) {
    const { counts: perValue, countsMin } = summary;
    for (let k = 0; k < perValue.length; k += 1) {
      if (perValue[k]) add(countsMin + k, perValue[k]);
    }
  } else {
    for (let i = 0; i < list.length; i += 1) {
      const v = list[i];
      if (Number.isFinite(v)) add(v, 1);
    }
  }

  // Bars span centre +- w/2; for whole numbers a bin's values sit inside it.
  const edgeOffset = integral ? -0.5 : 0;
  return {
    data: Array.from(counts),
    xStart: start + edgeOffset + w / 2,
    xStep: w,
    xTickMode,
    binWidth: integral ? w : 0,
    view: clipped ? { min: lo + edgeOffset, max: viewEnd + edgeOffset } : null,
    outsideBelow,
    outsideAbove,
  };
}

/**
 * Pixels the plot's x range leaves out, for the note on the plot: whole bins
 * outside [viewMin, viewMax], plus the pixels beyond the binned range.
 * `belowEdge` / `aboveEdge` are the edges of the hidden bins nearest the view.
 */
export function roiHistogramHiddenCounts({ data, xStart, xStep, viewMin, viewMax, outsideBelow = 0, outsideAbove = 0 }) {
  const half = Math.abs(xStep) / 2;
  const eps = Math.abs(xStep) * 1e-9;
  let below = outsideBelow;
  let above = outsideAbove;
  let belowEdge = Number.NaN;
  let aboveEdge = Number.NaN;
  for (let i = 0; i < (data?.length || 0); i += 1) {
    const centre = xStart + i * xStep;
    if (centre + half <= viewMin + eps) {
      below += data[i] || 0;
      belowEdge = centre + half;
    } else if (centre - half >= viewMax - eps) {
      above += data[i] || 0;
      if (!Number.isFinite(aboveEdge)) aboveEdge = centre - half;
    }
  }
  if (!Number.isFinite(belowEdge) && below > 0) belowEdge = xStart - half;
  if (!Number.isFinite(aboveEdge) && above > 0) aboveEdge = xStart + ((data?.length || 1) - 1) * xStep + half;
  return { below, above, belowEdge, aboveEdge };
}
