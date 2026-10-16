/**
 * Flatfield correction: every image divided by a flat -- an image of the beam
 * with no sample in it -- so the beam's profile and what is left of the
 * pixels' differing sensitivity cancel out, and only the sample remains.
 *
 *   counts:        corrected = raw / flat × mean(flat)   (the image's own scale)
 *   transmission:  corrected = raw / flat                (1 = nothing absorbed)
 *
 * No dark image is subtracted: a photon-counting detector has no dark current.
 * Flat pixels that cannot be divided by -- masked, not finite, zero or negative
 * (a CBF's -1 and -2), at their type's maximum (a gap or an overflow), or below
 * MIN_FLAT_FRACTION of the flat's mean -- are holes: inpainting fills them when
 * it is on, and they are left out of the statistics otherwise.
 *
 * Mirrors backend/services/flatfield.py, which corrects exported files.
 */

export const FLAT_SCALES = ["counts", "transmission"];
// A flat pixel this far below the flat's mean has too few counts to divide by:
// one count in a flat of 10 000 would multiply its pixel ten thousandfold.
export const MIN_FLAT_FRACTION = 0.05;

export function normalizeFlatScale(value) {
  return FLAT_SCALES.includes(value) ? value : "counts";
}

/**
 * A flat made ready to divide by: `{ data, width, height, mean, holes, count }`,
 * with `holes` its unusable pixels and `mean` that of the usable ones. `mask`
 * is the flat source's pixel mask, if it has one (bit 0 gap, 1-4 defective).
 */
// The largest value of an unsigned integer frame: a gap or an overflow there.
function typeMax(data) {
  if (data instanceof Uint8Array) return 0xff;
  if (data instanceof Uint16Array) return 0xffff;
  if (data instanceof Uint32Array) return 0xffffffff;
  return Number.POSITIVE_INFINITY;
}

export function prepareFlat(data, width, height, mask = null) {
  const n = width * height;
  if (!data || data.length < n || !n) return null;
  const useMask = mask && mask.length === n;
  const top = typeMax(data);
  let sum = 0;
  let usable = 0;
  for (let i = 0; i < n; i += 1) {
    const v = data[i];
    if ((useMask && mask[i] & 0x1f) || !Number.isFinite(v) || v <= 0 || v >= top) continue;
    sum += v;
    usable += 1;
  }
  if (!usable) return null;
  const firstMean = sum / usable;
  const floor = firstMean * MIN_FLAT_FRACTION;
  const holes = new Uint8Array(n);
  const values = new Float32Array(n);
  sum = 0;
  usable = 0;
  let count = 0;
  for (let i = 0; i < n; i += 1) {
    const v = data[i];
    if ((useMask && mask[i] & 0x1f) || !Number.isFinite(v) || v < floor || v <= 0 || v >= top) {
      holes[i] = 1;
      count += 1;
      continue;
    }
    values[i] = v;
    sum += v;
    usable += 1;
  }
  return { data: values, width, height, mean: sum / usable, holes, count };
}

/** Whether a prepared flat fits a frame of this size. */
export function flatFits(flat, width, height) {
  return Boolean(flat) && flat.width === width && flat.height === height;
}

/**
 * `raw` divided by the flat (Float32Array), on the chosen scale. The flat's
 * holes, and the frame's own flagged pixels (`rawHoles`: gaps, defective),
 * come out NaN: there is no value to divide there, and the histogram, the
 * contrast and the statistics all leave non-finite values out. Inpainting,
 * when on, fills them. `raw` is not changed.
 */
export function applyFlat(raw, flat, scale = "counts", rawHoles = null) {
  const n = flat.width * flat.height;
  const out = new Float32Array(n);
  const factor = normalizeFlatScale(scale) === "transmission" ? 1 : flat.mean;
  const divisor = flat.data;
  const holes = flat.holes;
  for (let i = 0; i < n; i += 1) {
    out[i] = holes[i] || (rawHoles && rawHoles[i]) ? Number.NaN : (raw[i] / divisor[i]) * factor;
  }
  return out;
}
