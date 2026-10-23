/**
 * Finding bad pixels in one image (beta): hot pixels far above their
 * surroundings, dead and cold ones far below.
 *
 * "Surroundings" is the block of BLOCK x BLOCK pixels a pixel sits in: its
 * median, and its spread as 1.4826 x the median absolute deviation -- robust,
 * so the bad pixels being looked for do not raise the bar for themselves, and
 * local, so a detector whose modules differ in gain is judged module by module.
 * For counts the spread is never taken below Poisson noise (sqrt of the median,
 * and 1), so a dark image of mostly zeros does not call every single count hot.
 *
 * Pixels already masked, and the negative values some files use to flag gaps
 * and bad pixels, are left out of the statistics and never reported. Each
 * class found gets its own bit of the defective range: dead 2, cold 4, hot 8.
 *
 * This looks at a single image, so it cannot tell a hot pixel from a sharp
 * reflection one pixel wide; it is meant for flat and dark images.
 */

export const DEAD_BIT = 0x2;
export const COLD_BIT = 0x4;
export const HOT_BIT = 0x8;
export const BLOCK = 32;

export const FIND_DEFAULTS = Object.freeze({
  hot: true,
  hotSigma: 8,
  cold: true,
  coldPercent: 20,
});

// Fewer valid pixels than this in a block: judge it by the whole image.
const MIN_BLOCK_PIXELS = 16;
// Dead and cold need light to fall short of: a block whose median is below
// this (counts) has none to speak of.
const MIN_COLD_MEDIAN = 5;
const MAD_TO_SIGMA = 1.4826;
// Pixels sampled for the whole image's statistics.
const GLOBAL_SAMPLE = 1 << 20;

function median(values, count) {
  const sorted = values.subarray(0, count).sort();
  const mid = count >> 1;
  return count % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

/** Median and robust spread of the first `count` entries; reorders them. */
function robustStats(values, count, scratch) {
  const m = median(values, count);
  for (let i = 0; i < count; i += 1) scratch[i] = Math.abs(values[i] - m);
  return { median: m, mad: median(scratch, count) };
}

function spreadOf(stats, counts) {
  const sigma = MAD_TO_SIGMA * stats.mad;
  if (counts) return Math.max(sigma, Math.sqrt(Math.max(stats.median, 0)), 1);
  return Math.max(sigma, Math.abs(stats.median) * 1e-6, Number.MIN_VALUE);
}

const usable = (value, maskValue) => maskValue === 0 && Number.isFinite(value) && value >= 0;

/**
 * Search `data` (width x height, row-major) for bad pixels.
 *
 * `mask` (same size, or null) is the mask in use; `counts` says the values are
 * photon counts (an integer type). Calls `onProgress(fraction)` between rows
 * of blocks and awaits it when it returns a promise, so a large image can let
 * the page breathe. Resolves to `{ hot, dead, cold }`, arrays of pixel indices.
 */
export async function findBadPixels({ data, width, height, mask = null, counts = true, options = {}, onProgress }) {
  const opts = { ...FIND_DEFAULTS, ...options };
  const result = { hot: [], dead: [], cold: [] };
  if (!data || !width || !height || data.length < width * height) return result;
  if (!opts.hot && !opts.cold) return result;
  const maskAt = mask && mask.length === data.length ? (i) => mask[i] : () => 0;

  const values = new Float64Array(Math.max(BLOCK * BLOCK, GLOBAL_SAMPLE));
  const scratch = new Float64Array(values.length);

  // The whole image, for blocks with too few pixels of their own.
  const step = Math.max(1, Math.floor(data.length / GLOBAL_SAMPLE));
  let n = 0;
  for (let i = 0; i < data.length && n < values.length; i += step) {
    if (usable(data[i], maskAt(i))) values[n++] = data[i];
  }
  if (!n) return result;
  const globalStats = robustStats(values, n, scratch);

  const hotSigma = Math.max(0, Number(opts.hotSigma) || 0);
  const coldFraction = Math.max(0, Math.min(1, (Number(opts.coldPercent) || 0) / 100));
  const blockRows = Math.ceil(height / BLOCK);
  const blockCols = Math.ceil(width / BLOCK);

  for (let by = 0; by < blockRows; by += 1) {
    const y0 = by * BLOCK;
    const y1 = Math.min(height, y0 + BLOCK);
    for (let bx = 0; bx < blockCols; bx += 1) {
      const x0 = bx * BLOCK;
      const x1 = Math.min(width, x0 + BLOCK);
      let count = 0;
      for (let y = y0; y < y1; y += 1) {
        const row = y * width;
        for (let x = x0; x < x1; x += 1) {
          const i = row + x;
          if (usable(data[i], maskAt(i))) values[count++] = data[i];
        }
      }
      if (!count) continue;
      const stats = count >= MIN_BLOCK_PIXELS ? robustStats(values, count, scratch) : globalStats;
      const sigma = spreadOf(stats, counts);
      const hotAbove = stats.median + hotSigma * sigma;
      const lit = counts ? stats.median >= MIN_COLD_MEDIAN : stats.median > 0;
      const coldBelow = stats.median * coldFraction;
      for (let y = y0; y < y1; y += 1) {
        const row = y * width;
        for (let x = x0; x < x1; x += 1) {
          const i = row + x;
          const v = data[i];
          if (!usable(v, maskAt(i))) continue;
          if (opts.hot && v > hotAbove) result.hot.push(i);
          else if (opts.cold && lit && v < coldBelow) (v <= 0 ? result.dead : result.cold).push(i);
        }
      }
    }
    const pending = onProgress?.((by + 1) / blockRows);
    if (pending && typeof pending.then === "function") await pending;
  }
  return result;
}

/** Mark what was found in `data` (a mask); returns how many pixels changed. */
export function addFound(data, found) {
  let changed = 0;
  const mark = (indices, bit) => {
    for (const i of indices || []) {
      if (data[i]) continue;
      data[i] = bit;
      changed += 1;
    }
  };
  mark(found?.hot, HOT_BIT);
  mark(found?.dead, DEAD_BIT);
  mark(found?.cold, COLD_BIT);
  return changed;
}
