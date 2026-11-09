/**
 * The extreme pixels of a frame: the N highest values, highest first, or the
 * N lowest, lowest first.
 *
 * Unlike the Peak Finder, nothing here asks whether a pixel looks like a
 * diffraction spot. No local maximum, no footprint, no background, no
 * minimum distance: a hot pixel is often a lone spike, and two hot pixels
 * side by side are two results, not one.
 *
 * Which pixels count differs by end. The highest are taken from pixels above
 * zero, so a dark frame lists nothing rather than the first N zeros. The
 * lowest include zero and negative values, since a dead pixel reads 0. That
 * end has a floor every low-flux frame reaches, often in hundreds of
 * thousands of pixels, so a list of N of them would look exact and say
 * nothing; `extremeCount` reports how many pixels share the extreme value, so
 * the interface can say so.
 *
 * An optional low and high cutoff narrow the pixels that count: a pixel below
 * the low cutoff or above the high one is left out. Setting the low cutoff
 * just above the floor (1 on a frame full of zeros) lets the Min end find the
 * lowest pixel that read something. A cutoff applies to the value, whatever
 * the pixel, so it is not a mask.
 *
 * One pass over the frame keeps the best N in a heap; a pixel is compared
 * with the weakest kept one and only a better one touches the heap, so the
 * cost stays linear in the number of pixels.
 */

export const MINMAX_MIN_COUNT = 1;
export const MINMAX_MAX_COUNT = 1000;
export const MINMAX_DEFAULT_COUNT = 20;
export const MINMAX_MODES = ["max", "min", "both"];

/** A count typed by the user, as a whole number within the limits. */
export function clampMinMaxCount(value, fallback = MINMAX_DEFAULT_COUNT) {
  const number = Math.round(Number(value));
  if (!Number.isFinite(number)) return fallback;
  return Math.max(MINMAX_MIN_COUNT, Math.min(MINMAX_MAX_COUNT, number));
}

/** A cutoff typed by the user: a finite number, or null for "no cutoff". */
export function parseMinMaxCutoff(value) {
  if (value === null || value === undefined || String(value).trim() === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

export function normalizeMinMaxMode(value) {
  return MINMAX_MODES.includes(value) ? value : "max";
}

/**
 * `data` is the frame, row by row, `width` pixels wide. `ignore(index)` leaves
 * a pixel out (masked, saturated). `lowest` picks the low end. `lowCutoff` and
 * `highCutoff` (a number, or null for none) leave out pixels below or above
 * that value; a pixel equal to a cutoff stays.
 *
 * Returns `{ pixels, extremeCount }`: `pixels` as `{ x, y, index, value }`,
 * best first, equal values in reading order; `extremeCount` the number of
 * counted pixels holding the best value (0 when there are none).
 */
export function findExtremePixels(
  data,
  width,
  height,
  { count = MINMAX_DEFAULT_COUNT, ignore = null, lowest = false, lowCutoff = null, highCutoff = null } = {},
) {
  const total = Math.min(data?.length || 0, width * height);
  const limit = clampMinMaxCount(count);
  if (!total || !width || !height) return { pixels: [], extremeCount: 0 };

  // Work on a key that is larger for a better pixel -- the value, or its
  // negative for the low end -- so one heap serves both. keys[0] is the
  // weakest kept. Ties: the later pixel counts as weaker, so a pixel equal to
  // the weakest one never displaces it and equal values come out in reading
  // order.
  const sign = lowest ? -1 : 1;
  const keys = new Float64Array(limit);
  const indices = new Int32Array(limit);
  let size = 0;
  let bestKey = Number.NEGATIVE_INFINITY;
  let bestCount = 0;

  const weaker = (a, b) => keys[a] < keys[b] || (keys[a] === keys[b] && indices[a] > indices[b]);

  function swap(a, b) {
    const key = keys[a];
    keys[a] = keys[b];
    keys[b] = key;
    const index = indices[a];
    indices[a] = indices[b];
    indices[b] = index;
  }

  function siftUp(slot) {
    while (slot > 0) {
      const parent = (slot - 1) >> 1;
      if (!weaker(slot, parent)) return;
      swap(slot, parent);
      slot = parent;
    }
  }

  function siftDown(slot) {
    for (;;) {
      const left = slot * 2 + 1;
      const right = left + 1;
      let weakest = slot;
      if (left < size && weaker(left, weakest)) weakest = left;
      if (right < size && weaker(right, weakest)) weakest = right;
      if (weakest === slot) return;
      swap(slot, weakest);
      slot = weakest;
    }
  }

  for (let index = 0; index < total; index += 1) {
    const value = data[index];
    // NaN fails both tests; an infinite value is no reading either.
    if (lowest ? !Number.isFinite(value) : !(value > 0) || value === Number.POSITIVE_INFINITY) continue;
    if ((lowCutoff !== null && value < lowCutoff) || (highCutoff !== null && value > highCutoff)) continue;
    const key = sign * value;
    // Below both the heap's weakest and the best value: nothing to update.
    if (size === limit && key <= keys[0] && key < bestKey) continue;
    if (ignore && ignore(index)) continue;
    if (key > bestKey) {
      bestKey = key;
      bestCount = 1;
    } else if (key === bestKey) {
      bestCount += 1;
    }
    if (size < limit) {
      keys[size] = key;
      indices[size] = index;
      size += 1;
      siftUp(size - 1);
    } else if (key > keys[0]) {
      keys[0] = key;
      indices[0] = index;
      siftDown(0);
    }
  }

  const pixels = [];
  for (let slot = 0; slot < size; slot += 1) {
    const index = indices[slot];
    pixels.push({ x: index % width, y: Math.floor(index / width), index, value: sign * keys[slot] });
  }
  pixels.sort((a, b) => sign * (b.value - a.value) || a.index - b.index);
  return { pixels, extremeCount: bestCount };
}
