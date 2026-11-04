/**
 * The hottest pixels of a frame: the N highest values, brightest first.
 *
 * Unlike the Peak Finder, nothing here asks whether a pixel looks like a
 * diffraction spot. No local maximum, no footprint, no background, no
 * minimum distance: a hot pixel is often a lone spike, and two hot pixels
 * side by side are two results, not one. Only pixels above zero count, so a
 * dark frame lists nothing rather than the first N zeros.
 *
 * One pass over the frame keeps the best N in a min-heap; a pixel is compared
 * with the weakest kept one and only a brighter one touches the heap, so the
 * cost stays linear in the number of pixels.
 */

export const HOTTEST_PIXELS_MIN = 1;
export const HOTTEST_PIXELS_MAX = 1000;
export const HOTTEST_PIXELS_DEFAULT = 20;

/** A count typed by the user, as a whole number within the limits. */
export function clampHottestPixelCount(value, fallback = HOTTEST_PIXELS_DEFAULT) {
  const number = Math.round(Number(value));
  if (!Number.isFinite(number)) return fallback;
  return Math.max(HOTTEST_PIXELS_MIN, Math.min(HOTTEST_PIXELS_MAX, number));
}

/**
 * `data` is the frame, row by row, `width` pixels wide. `ignore(index)` leaves
 * a pixel out (masked, saturated). Returns `{ x, y, index, value }` sorted by
 * value, highest first; equal values in reading order.
 */
export function findHottestPixels(data, width, height, { count = HOTTEST_PIXELS_DEFAULT, ignore = null } = {}) {
  const total = Math.min(data?.length || 0, width * height);
  const limit = clampHottestPixelCount(count);
  if (!total || !width || !height) return [];

  // heapValues[0] is the weakest pixel kept. Ties: the later pixel counts as
  // weaker, so a pixel equal to the weakest one never displaces it and equal
  // values come out in reading order.
  const heapValues = new Float64Array(limit);
  const heapIndices = new Int32Array(limit);
  let size = 0;

  const weaker = (a, b) =>
    heapValues[a] < heapValues[b] || (heapValues[a] === heapValues[b] && heapIndices[a] > heapIndices[b]);

  function swap(a, b) {
    const value = heapValues[a];
    heapValues[a] = heapValues[b];
    heapValues[b] = value;
    const index = heapIndices[a];
    heapIndices[a] = heapIndices[b];
    heapIndices[b] = index;
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
    // `> 0` is false for NaN, so non-finite pixels other than +Infinity drop
    // out here; +Infinity is no reading either.
    if (!(value > 0) || value === Number.POSITIVE_INFINITY) continue;
    if (size === limit && value <= heapValues[0]) continue;
    if (ignore && ignore(index)) continue;
    if (size < limit) {
      heapValues[size] = value;
      heapIndices[size] = index;
      size += 1;
      siftUp(size - 1);
    } else {
      heapValues[0] = value;
      heapIndices[0] = index;
      siftDown(0);
    }
  }

  const found = [];
  for (let slot = 0; slot < size; slot += 1) {
    const index = heapIndices[slot];
    found.push({ x: index % width, y: Math.floor(index / width), index, value: heapValues[slot] });
  }
  found.sort((a, b) => b.value - a.value || a.index - b.index);
  return found;
}
