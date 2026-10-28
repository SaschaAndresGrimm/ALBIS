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

import { applyFlat, flatFits } from "./flatfield.js";
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

export const MAX_INPAINT_MARGIN = 5;

/** How holes are filled: interpolated from their neighbours, or one value. */
export const INPAINT_METHODS = ["interpolate", "fixed"];

export function normalizeInpaintMethod(value) {
  return INPAINT_METHODS.includes(value) ? value : "interpolate";
}

/** A fixed fill value: any finite number, else 0. */
export function normalizeInpaintValue(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

/**
 * A filled copy of `data` (Float32Array) by `method`: "interpolate" (see
 * inpaint) or "fixed", every hole set to `value`. Holes as from inpaintHoles.
 */
export function fillHoles(data, width, height, holes, method = "interpolate", value = 0) {
  if (normalizeInpaintMethod(method) === "fixed") {
    const out = Float32Array.from(data.subarray ? data.subarray(0, width * height) : data);
    const fill = normalizeInpaintValue(value);
    for (let i = 0; i < out.length; i += 1) if (holes[i]) out[i] = fill;
    return out;
  }
  return inpaint(data, width, height, holes);
}

/** Clamp a margin to whole pixels in 0..MAX_INPAINT_MARGIN. */
export function normalizeInpaintMargin(value) {
  const n = Math.round(Number(value));
  return Number.isFinite(n) ? Math.max(0, Math.min(MAX_INPAINT_MARGIN, n)) : 0;
}

/**
 * Widen `holes` (in place) by `margin` pixels all round -- a square, so the 8
 * pixels around a flagged one at margin 1. The pixels next to a flagged one
 * are often unreliable too. Along the rows, then along the columns: the cost
 * is the frame's size, whatever the margin. Returns the new hole count.
 */
export function growHoles(holes, width, height, margin) {
  const r = normalizeInpaintMargin(margin);
  if (!r) {
    let count = 0;
    for (let i = 0; i < holes.length; i += 1) count += holes[i];
    return count;
  }
  // Along the rows: within r pixels of a flagged one in the same row.
  const rows = new Uint8Array(holes.length);
  // Whole numbers for "none yet" (further than r away), not +-Infinity:
  // a float in the loop slowed it threefold.
  for (let y = 0; y < height; y += 1) {
    const base = y * width;
    let last = -r - 1;
    for (let x = 0; x < width; x += 1) {
      if (holes[base + x]) last = x;
      rows[base + x] = x - last <= r ? 1 : 0;
    }
    let next = width + r + 1;
    for (let x = width - 1; x >= 0; x -= 1) {
      if (holes[base + x]) next = x;
      if (next - x <= r) rows[base + x] = 1;
    }
  }
  // Along the columns: within r rows of such a pixel, so the OR of the rows
  // around it, a whole row at a time. Flags are 0 or 1 a byte, so where the
  // rows are whole 32-bit words they are ORed and counted four at a time.
  if (width % 4 === 0 && holes.byteOffset % 4 === 0) {
    const words = width / 4;
    const holeWords = new Uint32Array(holes.buffer, holes.byteOffset, (width * height) / 4);
    const rowWords = new Uint32Array(rows.buffer, 0, (width * height) / 4);
    let count = 0;
    for (let y = 0; y < height; y += 1) {
      const base = y * words;
      const from = Math.max(0, y - r) * words;
      const to = Math.min(height - 1, y + r) * words;
      for (let x = 0; x < words; x += 1) {
        let word = rowWords[from + x];
        for (let other = from + words; other <= to; other += words) word |= rowWords[other + x];
        holeWords[base + x] = word;
        // Four 0/1 bytes summed into the top byte.
        count += Math.imul(word, 0x01010101) >>> 24;
      }
    }
    return count;
  }
  let count = 0;
  for (let y = 0; y < height; y += 1) {
    const base = y * width;
    const from = Math.max(0, y - r) * width;
    const to = Math.min(height - 1, y + r) * width;
    for (let x = 0; x < width; x += 1) holes[base + x] = rows[from + x];
    for (let other = from + width; other <= to; other += width) {
      for (let x = 0; x < width; x += 1) holes[base + x] |= rows[other + x];
    }
    for (let x = 0; x < width; x += 1) count += holes[base + x];
  }
  return count;
}

/**
 * Which pixels to fill: 1 for a gap or defective pixel (and a saturated one
 * when `fillSaturated`), widened by `margin` pixels, 0 otherwise.
 *
 * Same classification as the ROI statistics: the mask wins where there is one;
 * without one, a signed integer frame's -1 and -2 (`inDataFlags`).
 */
export function inpaintHoles({
  data,
  width = 0,
  height = 0,
  margin = 0,
  // Pixels to fill besides the flagged ones (a flat's unusable pixels).
  extraHoles = null,
  // The holes found for these data without margin, saturated or extra
  // pixels, when already in hand (the flatfield's): only what comes on top
  // is looked for.
  baseHoles = null,
  mask = null,
  inDataFlags = false,
  satMax = null,
  fillSaturated = false,
  isSaturatedValue = () => false,
}) {
  const n = data.length;
  let count = 0;
  const checkSaturated = fillSaturated && satMax !== null;
  if (baseHoles && baseHoles.length === n) {
    const holes = baseHoles.slice();
    for (let i = 0; i < n; i += 1) {
      if (holes[i]) count += 1;
      else if (checkSaturated && isSaturatedValue(data[i], satMax)) {
        holes[i] = 1;
        count += 1;
      }
    }
    return finishHoles(holes, count, extraHoles, width, height, margin);
  }
  const holes = new Uint8Array(n);
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
  return finishHoles(holes, count, extraHoles, width, height, margin);
}

// The extra pixels added, then the margin.
function finishHoles(holes, count, extraHoles, width, height, margin) {
  const n = holes.length;
  if (extraHoles && extraHoles.length === n) {
    for (let i = 0; i < n; i += 1) {
      if (extraHoles[i] && !holes[i]) {
        holes[i] = 1;
        count += 1;
      }
    }
  }
  if (count && margin > 0 && width * height === n) count = growHoles(holes, width, height, margin);
  return { holes, count };
}

// One run of holes along a line -- `start` to `end - 1` at positions along it,
// `slots` its accumulator slots in order -- adds its value interpolated
// between the valid pixels on either side, weighted 1/span. `base` and
// `step` place a position in the frame, `length` is the line's.
function addRun(values, valid, num, den, slots, first, base, step, start, end, length) {
  const left = start - 1;
  const right = end;
  const hasLeft = left >= 0;
  const hasRight = right < length;
  if (!hasLeft && !hasRight) return;
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
    const slot = slots[first + k - start];
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
    num[slot] += value * weight;
    den[slot] += weight;
  }
}

// The slots of `holes` (row-major pixel indices) in column-major order: a
// counting sort by column, stable, so each column's holes stay top to bottom.
function columnOrder(holes, count, width, order, starts) {
  starts.fill(0);
  for (let j = 0; j < count; j += 1) starts[(holes[j] % width) + 1] += 1;
  for (let c = 0; c < width; c += 1) starts[c + 1] += starts[c];
  for (let j = 0; j < count; j += 1) order[starts[holes[j] % width]++] = j;
}

/**
 * A filled copy of `data` (Float32Array), or null when nothing needs filling.
 * The input is never changed: the same array is held by the frame cache and
 * the live history.
 *
 * The work follows the holes, not the frame: they are listed once, their
 * runs along the rows found from that list and along the columns from the
 * same list in column order, and their sums kept per hole. A frame with a
 * few percent of holes is filled in about half the time it took when every
 * pass went over every pixel, with the same arithmetic, so the same values.
 */
export function inpaint(data, width, height, holes) {
  const n = width * height;
  if (!data || data.length < n || !holes) return null;
  const values = new Float32Array(n);
  const valid = new Uint8Array(n);
  let count = 0;
  for (let i = 0; i < n; i += 1) {
    if (holes[i]) {
      count += 1;
    } else {
      values[i] = data[i];
      valid[i] = 1;
    }
  }
  if (!count) return null;
  // The holes still to fill, row-major, and per slot their sums.
  const list = new Int32Array(count);
  for (let i = 0, j = 0; j < count; i += 1) if (holes[i]) list[j++] = i;
  const num = new Float64Array(count);
  const den = new Float64Array(count);
  const order = new Int32Array(count);
  const starts = new Int32Array(width + 1);
  const identity = new Int32Array(count);
  for (let j = 0; j < count; j += 1) identity[j] = j;
  for (let round = 0; round < MAX_ROUNDS && count > 0; round += 1) {
    num.fill(0, 0, count);
    den.fill(0, 0, count);
    // Along the rows: a run is consecutive pixels of one row.
    for (let j = 0; j < count; ) {
      const first = j;
      const row = (list[j] / width) | 0;
      const start = list[j] - row * width;
      j += 1;
      while (j < count && list[j] === list[j - 1] + 1 && list[j] - row * width < width) j += 1;
      addRun(values, valid, num, den, identity, first, row * width, 1, start, start + (j - first), width);
    }
    // Along the columns: the same holes in column order.
    columnOrder(list, count, width, order, starts);
    for (let q = 0; q < count; ) {
      const first = q;
      const col = list[order[q]] % width;
      const start = (list[order[q]] - col) / width;
      q += 1;
      while (q < count && list[order[q]] === list[order[q - 1]] + width) q += 1;
      addRun(values, valid, num, den, order, first, col, width, start, start + (q - first), height);
    }
    // Every run of this round is summed: what is filled now is valid from
    // the next round on, never from a neighbour's fresh guess within this one.
    let kept = 0;
    for (let j = 0; j < count; j += 1) {
      if (den[j] > 0) {
        values[list[j]] = num[j] / den[j];
        valid[list[j]] = 1;
      } else {
        list[kept++] = list[j];
      }
    }
    if (kept === count) break;
    count = kept;
  }
  // Nothing measured anywhere in reach (a frame that is all mask): zero.
  for (let j = 0; j < count; j += 1) values[list[j]] = 0;
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
    get({ data, width, height, margin = 0, method = "interpolate", value = 0, mask = null, inDataFlags = false, satMax = null, fillSaturated = false, isSaturatedValue }) {
      if (!data || !width || !height) return { data: null, filled: 0 };
      const next = [data, width, height, margin, method, value, mask, inDataFlags, satMax, fillSaturated];
      if (key && next.every((part, i) => part === key[i])) return result;
      const { holes, count } = inpaintHoles({ data, width, height, margin, mask, inDataFlags, satMax, fillSaturated, isSaturatedValue });
      result = {
        data: count ? fillHoles(data, width, height, holes, method, value) : null,
        filled: count,
        holes: count ? holes : null,
      };
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
 * The frame as the viewer shows it: raw, divided by the flat when flatfield
 * correction is on, then inpainted when inpainting is on. `frame()` is null
 * while both are off, else `{ data, holes, filled, flatfielded, inpainted }`:
 * `data` the processed frame (null when there is nothing to change), `holes`
 * the pixels inpainting filled. Everything that shows or measures the image --
 * the renderer, the histogram and contrast, the ROI statistics, the PNG and
 * GIF exports -- asks here, so they all see the same frame, worked out once
 * per frame and setting.
 */
export function createInpaintView({ state, getActiveSaturationMax, isSaturatedValue, getFlat = () => null }) {
  let key = null;
  let result = null;

  function compute(raw, satMax) {
    const params = inpaintParams(state, raw, satMax);
    const flat = state.flatEnabled ? getFlat() : null;
    const flatOn = flatFits(flat, state.width, state.height);
    let data = null;
    let rawHoles = null;
    if (flatOn) {
      // The frame's own flagged pixels have no value to divide: NaN, like the
      // flat's holes, unless inpainting fills them below.
      rawHoles = inpaintHoles({ ...params, margin: 0, fillSaturated: false, isSaturatedValue }).holes;
      data = applyFlat(raw, flat, state.flatScale, rawHoles);
    }
    if (!state.inpaintEnabled) {
      return { data, holes: null, filled: 0, flatfielded: flatOn, inpainted: false };
    }
    const { holes, count } = inpaintHoles({
      ...params,
      extraHoles: flatOn ? flat.holes : null,
      baseHoles: rawHoles,
      isSaturatedValue,
    });
    if (!count) return { data, holes: null, filled: 0, flatfielded: flatOn, inpainted: true };
    return {
      data: fillHoles(data || raw, state.width, state.height, holes, params.method, params.value),
      holes,
      filled: count,
      flatfielded: flatOn,
      inpainted: true,
    };
  }

  return {
    frame() {
      if ((!state.inpaintEnabled && !state.flatEnabled) || !state.dataRaw) return null;
      const satMax = getActiveSaturationMax();
      const params = inpaintParams(state, state.dataRaw, satMax);
      const next = [
        state.dataRaw, params.mask, params.satMax, state.inpaintEnabled, params.fillSaturated, params.margin,
        params.method, params.value, state.flatEnabled, getFlat(), state.flatScale,
      ];
      if (key && next.every((part, i) => part === key[i])) return result;
      result = compute(state.dataRaw, satMax);
      key = next;
      return result;
    },
    /** The same, for another frame of the same source (the GIF export). */
    frameFor(data, satMax) {
      if ((!state.inpaintEnabled && !state.flatEnabled) || !data) return null;
      return compute(data, satMax).data;
    },
    /** The same, with the filled pixels too: `{ data, holes }` or null. */
    frameForFull(data, satMax) {
      if ((!state.inpaintEnabled && !state.flatEnabled) || !data) return null;
      return compute(data, satMax);
    },
    clear() {
      key = null;
      result = null;
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
    margin: normalizeInpaintMargin(state.inpaintMargin),
    method: normalizeInpaintMethod(state.inpaintMethod),
    value: normalizeInpaintValue(state.inpaintValue),
  };
}
