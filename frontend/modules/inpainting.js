/**
 * Inpainting: the detector's gaps and defective pixels filled from their
 * neighbours, so an image reads as one picture.
 *
 * ALBIS knows which pixels to fill -- the pixel mask marks the gaps between
 * modules (bit 0) and the defective pixels (bits 1-4), and a CBF marks them -1
 * and -2 in its data -- so nothing is guessed from the image, and there is no
 * radius or threshold to tune. Saturated pixels are filled only when asked:
 * in imaging they are usually real signal, too bright, not defects.
 *
 * Each hole pixel is interpolated across its gap, along its row and along its
 * column, between the valid pixels on either side (each side the median of the
 * five nearest, so a noisy pixel or a small spot at the edge does not set it). The two are combined with
 * weights 1/span: across a narrow gap the short direction wins, so an edge in
 * the sample carries straight through it; where a horizontal and a vertical
 * gap cross, both count; a single dead pixel takes the values around it.
 * A pixel whose row and column both hold no valid pixel -- where
 * two full-length gaps cross -- is filled in a further round, from the pixels
 * the first one filled. Every pixel is visited a fixed number of times: no
 * windows, no iteration to convergence.
 *
 * Mirrors backend/services/inpainting.py, which fills exported files; both
 * are held to the same cases in tests/fixtures/inpainting_cases.json.
 */

import { DATA_DEFECTIVE_VALUE, DATA_GAP_VALUE, usesInDataPixelFlags } from "./roi_stats_engine.js";

const MAX_ROUNDS = 8;
// Pixels each side's value is taken from: the median of five rejects a spot of
// up to two pixels at a gap's edge.
const SIDE_SAMPLES = 5;
const sideScratch = new Float64Array(SIDE_SAMPLES);

// The valid pixels from `edge` outwards (direction `dir`), up to SIDE_SAMPLES,
// stopping at the first hole or the line's end; into sideScratch. Returns how many.
function sideSamples(values, valid, base, step, edge, dir, length) {
  let k = 0;
  for (let pos = edge; k < SIDE_SAMPLES && pos >= 0 && pos < length; pos += dir) {
    const index = base + pos * step;
    if (!valid[index]) break;
    sideScratch[k] = values[index];
    k += 1;
  }
  return k;
}

function medianOf(k) {
  // Insertion sort: five values at most.
  for (let i = 1; i < k; i += 1) {
    const v = sideScratch[i];
    let j = i - 1;
    while (j >= 0 && sideScratch[j] > v) {
      sideScratch[j + 1] = sideScratch[j];
      j -= 1;
    }
    sideScratch[j + 1] = v;
  }
  const mid = k >> 1;
  return k % 2 ? sideScratch[mid] : (sideScratch[mid - 1] + sideScratch[mid]) / 2;
}

/**
 * Which pixels to fill: 1 for a gap or defective pixel (and a saturated one
 * when `fillSaturated`), 0 otherwise.
 *
 * Same classification as the ROI statistics: the mask wins where there is one;
 * without one, a signed integer frame's -1 and -2 (`inDataFlags`).
 */
export function inpaintHoles({
  data,
  mask = null,
  inDataFlags = false,
  satMax = null,
  fillSaturated = false,
  isSaturatedValue = () => false,
}) {
  const n = data.length;
  const holes = new Uint8Array(n);
  let count = 0;
  const checkSaturated = fillSaturated && satMax !== null;
  const useMask = mask !== null && mask.length === n;
  for (let i = 0; i < n; i += 1) {
    const value = data[i];
    let hole = false;
    if (useMask && mask[i] & 0x1f) hole = true;
    else if (!useMask && inDataFlags && (value === DATA_GAP_VALUE || value === DATA_DEFECTIVE_VALUE)) hole = true;
    else if (checkSaturated && isSaturatedValue(value, satMax)) hole = true;
    else if (!Number.isFinite(value)) hole = true;
    if (hole) {
      holes[i] = 1;
      count += 1;
    }
  }
  return { holes, count };
}

// One direction's contribution: for every hole along each line, the value
// interpolated between the valid pixels on either side, weighted 1/span.
// `step` walks along a line, `lineStep` from one line to the next.
function addDirection(values, valid, holes, num, den, lines, length, step, lineStep) {
  for (let line = 0; line < lines; line += 1) {
    const base = line * lineStep;
    let pos = 0;
    while (pos < length) {
      if (!holes[base + pos * step]) {
        pos += 1;
        continue;
      }
      const start = pos;
      while (pos < length && holes[base + pos * step]) pos += 1;
      const left = start - 1;
      const right = pos;
      const hasLeft = left >= 0;
      const hasRight = right < length;
      if (!hasLeft && !hasRight) continue;
      // Each side the median of up to SIDE_SAMPLES valid pixels beyond the
      // gap's edge, placed at their middle: a spot of a pixel or two next to
      // the gap does not streak across it, and a gradient stays exact.
      let leftValue = 0;
      let rightValue = 0;
      let leftAt = left;
      let rightAt = right;
      if (hasLeft) {
        const k = sideSamples(values, valid, base, step, left, -1, length);
        leftValue = medianOf(k);
        leftAt = left - (k - 1) / 2;
      }
      if (hasRight) {
        const k = sideSamples(values, valid, base, step, right, 1, length);
        rightValue = medianOf(k);
        rightAt = right + (k - 1) / 2;
      }
      const span = hasLeft && hasRight ? right - left : 0;
      for (let k = start; k < right; k += 1) {
        const index = base + k * step;
        let value;
        let weight;
        if (hasLeft && hasRight) {
          const t = (k - leftAt) / (rightAt - leftAt);
          value = leftValue + (rightValue - leftValue) * t;
          weight = 1 / span;
        } else {
          // Open to one side only (the image's edge): the nearest value,
          // weighted by the distance to it.
          value = hasLeft ? leftValue : rightValue;
          weight = 1 / (hasLeft ? k - left + 1 : right - k + 1);
        }
        num[index] += value * weight;
        den[index] += weight;
      }
    }
  }
}

/**
 * A filled copy of `data` (Float32Array), or null when nothing needs filling.
 * The input is never changed: the same array is held by the frame cache and
 * the live history.
 */
export function inpaint(data, width, height, holes) {
  const n = width * height;
  if (!data || data.length < n || !holes) return null;
  const values = new Float32Array(n);
  const valid = new Uint8Array(n);
  const remaining = new Uint8Array(n);
  let left = 0;
  for (let i = 0; i < n; i += 1) {
    if (holes[i]) {
      remaining[i] = 1;
      left += 1;
    } else {
      values[i] = data[i];
      valid[i] = 1;
    }
  }
  if (!left) return null;
  const num = new Float64Array(n);
  const den = new Float64Array(n);
  for (let round = 0; round < MAX_ROUNDS && left > 0; round += 1) {
    num.fill(0);
    den.fill(0);
    addDirection(values, valid, remaining, num, den, height, width, 1, width);
    addDirection(values, valid, remaining, num, den, width, height, width, 1);
    let filledNow = 0;
    for (let i = 0; i < n; i += 1) {
      if (remaining[i] && den[i] > 0) {
        values[i] = num[i] / den[i];
        remaining[i] = 0;
        filledNow += 1;
      }
    }
    // Valid only from the next round on: within a round every hole is filled
    // from measured (or earlier-round) values, never from its neighbours'
    // fresh guesses.
    for (let i = 0; i < n; i += 1) {
      if (holes[i] && !remaining[i]) valid[i] = 1;
    }
    left -= filledNow;
    if (!filledNow) break;
  }
  // Nothing measured anywhere in reach (a frame that is all mask): zero.
  if (left > 0) {
    for (let i = 0; i < n; i += 1) if (remaining[i]) values[i] = 0;
  }
  return values;
}

/**
 * The frame as shown with inpainting on, worked out once per frame and
 * setting: `get(params)` hands back the same result while the frame, its
 * mask and the options are unchanged.
 */
export function createInpaintCache() {
  let key = null;
  let result = { data: null, filled: 0 };
  return {
    get({ data, width, height, mask = null, inDataFlags = false, satMax = null, fillSaturated = false, isSaturatedValue }) {
      if (!data || !width || !height) return { data: null, filled: 0 };
      const next = [data, width, height, mask, inDataFlags, satMax, fillSaturated];
      if (key && next.every((part, i) => part === key[i])) return result;
      const { holes, count } = inpaintHoles({ data, mask, inDataFlags, satMax, fillSaturated, isSaturatedValue });
      result = { data: count ? inpaint(data, width, height, holes) : null, filled: count, holes: count ? holes : null };
      key = next;
      return result;
    },
    clear() {
      key = null;
      result = { data: null, filled: 0 };
    },
  };
}

/**
 * The viewer's inpainted frame: `frame()` is null while inpainting is off,
 * else `{ data, holes, filled }` for the frame on screen -- `data` null when
 * it has nothing to fill. Everything that shows or measures the image (the
 * renderer, the ROI statistics, the PNG and GIF exports) asks here, so they
 * all see the same frame, worked out once.
 */
export function createInpaintView({ state, getActiveSaturationMax, isSaturatedValue }) {
  const cache = createInpaintCache();
  return {
    frame() {
      if (!state.inpaintEnabled || !state.dataRaw) return null;
      return cache.get({ ...inpaintParams(state, state.dataRaw, getActiveSaturationMax()), isSaturatedValue });
    },
    /** The same, for another frame of the same source (the GIF export). */
    frameFor(data, satMax) {
      if (!state.inpaintEnabled || !data) return null;
      const { holes, count } = inpaintHoles({ ...inpaintParams(state, data, satMax), isSaturatedValue });
      return count ? inpaint(data, state.width, state.height, holes) : null;
    },
    clear() {
      cache.clear();
    },
  };
}

function inpaintParams(state, data, satMax) {
  const hasMask =
    state.maskAvailable &&
    state.maskRaw &&
    state.maskShape &&
    state.maskShape[0] === state.height &&
    state.maskShape[1] === state.width;
  return {
    data,
    width: state.width,
    height: state.height,
    mask: hasMask ? state.maskRaw : null,
    inDataFlags: !state.maskAvailable && usesInDataPixelFlags(data),
    satMax: Number.isFinite(satMax) ? satMax : null,
    fillSaturated: Boolean(state.inpaintSaturated),
  };
}
