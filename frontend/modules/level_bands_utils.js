/**
 * How many of the counted pixels lie below BG, between BG and FG, and above
 * FG: the three rows under the statistics.
 *
 * The counting has to keep up with BG and FG being dragged, so the pixels are
 * prepared once (per frame, or per ROI) into a "level sample", and each new
 * BG/FG pair is counted against that:
 *
 * - Whole numbers within COUNT_TABLE_MAX_SPAN of each other -- every integer
 *   detector frame -- become a table of running totals, so a count is two
 *   lookups whatever the frame size.
 * - Anything else (float data, a flatfield-corrected frame) keeps its values
 *   and is counted in one pass with two comparisons per pixel.
 *
 * The edges: below is value < BG, between is BG <= value <= FG, above is
 * value > FG. With BG above FG nothing is between.
 */

// The same bound the statistics' counting median uses: a table of 2M entries
// is 8 MB, built once.
export const COUNT_TABLE_MAX_SPAN = 1 << 21;

function isWholeNumberArray(values) {
  return ArrayBuffer.isView(values) && !(values instanceof Float32Array) && !(values instanceof Float64Array);
}

/**
 * `cumulative[k]` is the number of values <= `min + k`.
 * `total` is the number of values counted.
 */
export function levelSampleFromCounts(counts, min) {
  const cumulative = new Float64Array(counts.length);
  let running = 0;
  for (let k = 0; k < counts.length; k += 1) {
    running += counts[k];
    cumulative[k] = running;
  }
  return { kind: "table", min, cumulative, total: running };
}

/**
 * A level sample from the values to count (the pixels the statistics kept).
 * `integral` says the values are whole numbers when the array type cannot
 * (an ROI's values arrive as a plain array or a Float64Array).
 */
export function buildLevelSample(values, { integral = isWholeNumberArray(values) } = {}) {
  const length = values?.length || 0;
  if (!length) return { kind: "values", values: [], total: 0 };
  if (integral) {
    let min = Number.POSITIVE_INFINITY;
    let max = Number.NEGATIVE_INFINITY;
    for (let i = 0; i < length; i += 1) {
      const v = values[i];
      if (v < min) min = v;
      if (v > max) max = v;
    }
    const span = max - min + 1;
    if (Number.isFinite(span) && span <= COUNT_TABLE_MAX_SPAN) {
      const counts = new Uint32Array(span);
      for (let i = 0; i < length; i += 1) counts[values[i] - min] += 1;
      return levelSampleFromCounts(counts, min);
    }
  }
  return { kind: "values", values, total: length };
}

// Values <= x, from a table sample.
function countAtMost(sample, x) {
  const k = Math.floor(x) - sample.min;
  if (k < 0) return 0;
  const { cumulative } = sample;
  return k >= cumulative.length ? sample.total : cumulative[k];
}

/** `{ below, between, above, total }` for this BG and FG. */
export function countLevelBands(sample, bg, fg) {
  const total = sample?.total || 0;
  if (!total || !Number.isFinite(bg) || !Number.isFinite(fg)) return { below: 0, between: 0, above: 0, total };
  if (sample.kind === "table") {
    // Whole numbers: v < bg is v <= ceil(bg) - 1.
    const below = countAtMost(sample, Math.ceil(bg) - 1);
    const atMostFg = countAtMost(sample, fg);
    const between = Math.max(0, atMostFg - below);
    return { below, between, above: total - below - between, total };
  }
  let below = 0;
  let above = 0;
  const { values } = sample;
  for (let i = 0; i < values.length; i += 1) {
    const v = values[i];
    if (v < bg) below += 1;
    else if (v > fg) above += 1;
  }
  return { below, between: total - below - above, above, total };
}

/** "12345 (1.1 %)"; a share too small for one decimal shows as "< 0.1 %". */
export function formatLevelBand(count, total) {
  if (!total) return "-";
  const percent = (count / total) * 100;
  let shown;
  if (count > 0 && percent < 0.05) shown = "< 0.1";
  else if (count < total && percent > 99.95) shown = "> 99.9";
  else shown = percent.toFixed(1);
  return `${count} (${shown} %)`;
}
