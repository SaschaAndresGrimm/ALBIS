/**
 * Pure ROI statistics helpers for histogram and mask-aware aggregation.
 */

export const ROI_HISTOGRAM_MIN_FIXED_BINS = 2;
export const ROI_HISTOGRAM_MAX_FIXED_BINS = 512;
export const ROI_HISTOGRAM_DEFAULT_FIXED_BINS = 128;

export function normalizeRoiHistogramBinCount(value) {
  const count = Math.round(Number(value));
  if (!Number.isFinite(count)) return ROI_HISTOGRAM_DEFAULT_FIXED_BINS;
  return Math.max(ROI_HISTOGRAM_MIN_FIXED_BINS, Math.min(ROI_HISTOGRAM_MAX_FIXED_BINS, count));
}

/** The two values detector data uses in place of a separate mask.
 *
 * A PILATUS CBF marks an inter-module gap `-1` and a bad pixel `-2` in the
 * pixel data itself, and ALBIS's own exports write the same two values. There
 * is no mask array to consult for such a frame -- `/api/mask` only answers for
 * HDF5 -- so a CBF's gap pixels were reported as none at all: a real frame in
 * `testdata/` holds 16,558 of them and the panel said zero.
 */
export const DATA_GAP_VALUE = -1;
export const DATA_DEFECTIVE_VALUE = -2;

/** Whether this frame's values can carry the flags above.
 *
 * Signed integer data only. An unsigned type cannot hold either value, and a
 * float frame is not this convention -- a flatfield or an averaged frame may
 * hold exactly -1.0 as a real measurement, and counting that as a gap would be
 * inventing defects.
 */
export function usesInDataPixelFlags(data) {
  return data instanceof Int8Array || data instanceof Int16Array || data instanceof Int32Array;
}

export function getMaskFlags(maskValue) {
  if (!Number.isFinite(maskValue)) {
    return { gap: false, defective: false };
  }
  return {
    gap: Boolean(maskValue & 1),
    defective: Boolean(maskValue & 0x1e),
  };
}

/** Gap and defective flags from the mask bits and the value together.
 *
 * The mask wins where there is one: a frame with a real mask array is being
 * told authoritatively, and `-1` in such a frame is a measurement rather than
 * a marker. `gap` takes precedence over `defective` so the two counts never
 * double-count one pixel.
 */
export function getPixelFlags(value, maskValue, inDataFlags = false) {
  const flags = getMaskFlags(maskValue);
  if (flags.gap || flags.defective) {
    return { gap: flags.gap, defective: !flags.gap && flags.defective };
  }
  if (inDataFlags) {
    if (value === DATA_GAP_VALUE) return { gap: true, defective: false };
    if (value === DATA_DEFECTIVE_VALUE) return { gap: false, defective: true };
  }
  return { gap: false, defective: false };
}

export function applyMaskToValue(value, maskValue, options = {}) {
  const {
    satMax = null,
    maskSaturatedEnabled = false,
    isSaturatedValue = () => false,
    inDataFlags = false,
  } = options;
  if (Number.isFinite(maskValue)) {
    if (maskValue & 1) {
      return { value: 0, skip: true };
    }
    if (maskValue & 0x1e) {
      return { value: 0, skip: true };
    }
  } else if (inDataFlags && (value === DATA_GAP_VALUE || value === DATA_DEFECTIVE_VALUE)) {
    // A frame whose flags live in the pixels, with no mask array to consult:
    // -1 and -2 are a missing pixel and a broken one, not measurements.
    return { value: 0, skip: true };
  }
  if (maskSaturatedEnabled && isSaturatedValue(value, satMax)) {
    return { value: 0, skip: true };
  }
  return { value, skip: false };
}

export function createRoiPixelCounters() {
  return { total: 0, gap: 0, defective: 0, saturated: 0 };
}

export function accumulateRoiPixelCounters(
  counters,
  sampled,
  satMax,
  isSaturatedValue,
  inDataFlags = false
) {
  if (!counters || !sampled) return;
  counters.total += 1;
  const flags = getPixelFlags(sampled.raw, sampled.maskValue, inDataFlags);
  if (flags.gap) {
    counters.gap += 1;
  } else if (flags.defective) {
    counters.defective += 1;
  }
  if (satMax !== null && isSaturatedValue(sampled.raw, satMax) && !flags.gap && !flags.defective) {
    counters.saturated += 1;
  }
}

function buildFixedRoiHistogram(finite, min, max, binCount, isIntegerSeries) {
  const counts = new Float64Array(binCount);
  if (!(max > min)) {
    const step = isIntegerSeries ? 1 : Math.max(1e-6, Math.abs(min) * 1e-6);
    const centerIdx = Math.floor(binCount / 2);
    counts[centerIdx] = finite.length;
    return {
      data: Array.from(counts),
      xStart: min - centerIdx * step,
      xStep: step,
      xTickMode: isIntegerSeries ? "integer" : "",
    };
  }

  const step = (max - min) / binCount;
  finite.forEach((value) => {
    const t = (value - min) / step;
    const idx = Math.max(0, Math.min(binCount - 1, Math.floor(t)));
    counts[idx] += 1;
  });
  return {
    data: Array.from(counts),
    xStart: min + step * 0.5,
    xStep: step,
    xTickMode: "",
  };
}

export function buildRoiHistogram(values, options = {}) {
  const finite = [];
  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;
  let isIntegerSeries = true;
  (values || []).forEach((value) => {
    if (!Number.isFinite(value)) return;
    finite.push(value);
    if (value < min) min = value;
    if (value > max) max = value;
    if (isIntegerSeries && Math.abs(value - Math.round(value)) >= 1e-9) {
      isIntegerSeries = false;
    }
  });
  if (!finite.length || !Number.isFinite(min) || !Number.isFinite(max)) return null;

  const binMode = options?.mode === "fixed" ? "fixed" : "auto";
  if (binMode === "fixed") {
    return buildFixedRoiHistogram(
      finite,
      min,
      max,
      normalizeRoiHistogramBinCount(options?.count),
      isIntegerSeries,
    );
  }

  if (isIntegerSeries) {
    const iMin = Math.floor(min);
    const iMax = Math.ceil(max);
    const intSpan = iMax - iMin + 1;
    if (intSpan > 0 && intSpan <= 512) {
      const counts = new Float64Array(intSpan);
      finite.forEach((value) => {
        const idx = Math.max(0, Math.min(intSpan - 1, Math.round(value) - iMin));
        counts[idx] += 1;
      });
      return {
        data: Array.from(counts),
        xStart: iMin,
        xStep: 1,
        xTickMode: "integer",
      };
    }
  }

  const binCount = Math.max(16, Math.min(192, Math.round(Math.sqrt(finite.length) * 1.5)));
  if (!(max > min)) {
    return {
      data: [finite.length],
      xStart: min,
      xStep: 1,
      xTickMode: isIntegerSeries ? "integer" : "",
    };
  }

  const step = (max - min) / binCount;
  const counts = new Float64Array(binCount);
  finite.forEach((value) => {
    const t = (value - min) / step;
    const idx = Math.max(0, Math.min(binCount - 1, Math.floor(t)));
    counts[idx] += 1;
  });
  return {
    data: Array.from(counts),
    xStart: min + step * 0.5,
    xStep: step,
    xTickMode: "",
  };
}

// Widest value span the counting median will allocate for: 2^21 bins is 8 MB of
// Uint32, and detector counts are far below that in practice. Anything wider
// falls back to selection.
const COUNTING_MEDIAN_MAX_SPAN = 1 << 21;

// Reused across calls. Whole-image statistics run on every frame step, so
// allocating either of these per frame is what made stepping through a large
// series expensive -- an EIGER 16M produced hundreds of megabytes of garbage per
// frame purely to compute one median.
let countingBins = null;
let selectionScratch = null;

/** Values from an integer TypedArray are integral; a float one may not be.
 *
 * Deliberately a type check rather than an inspection of the data. Testing
 * `Number.isInteger(min) && Number.isInteger(max)` looks equivalent and is not:
 * float extremes land on exact integers surprisingly often -- `sin(i) * 1000`
 * stored as float32 has min and max of exactly -1000 and 1000 -- and every
 * fractional value in between would then be truncated into the wrong bin.
 */
function hasIntegralValues(data) {
  return (
    ArrayBuffer.isView(data) &&
    !(data instanceof Float32Array) &&
    !(data instanceof Float64Array)
  );
}

// Gap (bit 0) and defective (bits 1-4) together. Matches applyMaskToValue.
const MASK_SKIP_BITS = 0x1f;

/** Exact median of integer data in one counting pass, no sorting or selection.
 *
 * The skip test is inlined rather than delegated to a predicate: this loop runs
 * once per pixel per frame, and a call per pixel cost more than the counting
 * saved on a masked frame -- which is the usual case, since a mask found in the
 * file enables itself. Only reached for integer TypedArrays, so every value is
 * finite by construction and needs no check.
 */
function countingMedian(data, min, span, count, skip) {
  const { maskBits, maskSaturatedEnabled, isSaturatedValue, satMax, inDataFlags = false } = skip;
  if (!countingBins || countingBins.length < span) {
    countingBins = new Uint32Array(span);
  } else {
    countingBins.fill(0, 0, span);
  }
  const checkSaturated = maskSaturatedEnabled && satMax !== null;
  if (maskBits === null && !checkSaturated && !inDataFlags) {
    for (let i = 0; i < data.length; i += 1) {
      countingBins[data[i] - min] += 1;
    }
  } else if (!checkSaturated && !inDataFlags) {
    for (let i = 0; i < data.length; i += 1) {
      if (maskBits[i] & MASK_SKIP_BITS) continue;
      countingBins[data[i] - min] += 1;
    }
  } else {
    // `min` is the smallest *accepted* value, so a skipped pixel would index
    // before the start of the bins. A typed array drops such a write silently,
    // which would leave the bin total short of `count` and walk the wrong
    // order statistic -- the skips here have to match the counting loop exactly.
    for (let i = 0; i < data.length; i += 1) {
      if (maskBits !== null && maskBits[i] & MASK_SKIP_BITS) continue;
      const value = data[i];
      if (inDataFlags && (value === DATA_GAP_VALUE || value === DATA_DEFECTIVE_VALUE)) continue;
      if (checkSaturated && isSaturatedValue(value, satMax)) continue;
      countingBins[value - min] += 1;
    }
  }
  // For an even count the median averages the two central order statistics, so
  // both are located in the same walk over the bins.
  const lowerK = (count - 1) >> 1;
  const upperK = count >> 1;
  let seen = 0;
  let lower = min;
  let foundLower = false;
  for (let bin = 0; bin < span; bin += 1) {
    const binCount = countingBins[bin];
    if (!binCount) continue;
    seen += binCount;
    if (!foundLower && seen > lowerK) {
      lower = min + bin;
      foundLower = true;
    }
    if (seen > upperK) {
      return (lower + (min + bin)) * 0.5;
    }
  }
  return lower;
}

export function computeGlobalStats(params) {
  const {
    dataRaw,
    width,
    height,
    maskAvailable,
    maskRaw,
    maskShape,
    maskEnabled,
    maskSaturatedEnabled,
    satMax,
    isSaturatedValue,
    computeMedian,
  } = params;
  if (!dataRaw) return null;

  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;
  let sum = 0;
  let count = 0;
  let mean = 0;
  let m2 = 0;
  let gapPixels = 0;
  let defectivePixels = 0;
  let saturatedPixels = 0;
  const hasMask = maskAvailable && maskRaw && maskShape && maskShape[0] === height && maskShape[1] === width;
  // Resolved once: the check is a handful of instanceof tests and this loop
  // runs per pixel per frame. Only consulted where there is no mask to trust.
  const inDataFlags = !hasMask && usesInDataPixelFlags(dataRaw);
  // `inDataFlags` switches masking on by itself. For a frame with a mask array
  // the mask enables itself when found, so excluding flagged pixels is already
  // the default there; a PILATUS frame carries the same information in its
  // values and was the only kind left counting gaps as data.
  const useMasking = (maskEnabled && hasMask) || maskSaturatedEnabled || inDataFlags;

  // Whether pixel `i` contributes. Shared by this pass and the median pass so
  // the two can never disagree about which pixels are in the sample.
  const isAccepted = (i) => {
    const value = dataRaw[i];
    if (!Number.isFinite(value)) return false;
    if (!useMasking) return true;
    return !applyMaskToValue(value, hasMask ? maskRaw[i] : null, {
      satMax,
      maskSaturatedEnabled,
      isSaturatedValue,
      inDataFlags,
    }).skip;
  };

  // getMaskFlags and applyMaskToValue both return a fresh object, and this loop
  // visits every pixel of every frame -- on a 16M detector that was 36 million
  // short-lived allocations per frame. Their logic is inlined here instead; the
  // exported helpers stay as the readable definition, and the parity tests hold
  // this loop to them.
  for (let i = 0; i < dataRaw.length; i += 1) {
    const value = dataRaw[i];
    // maskRaw is a Uint32Array, so its entries are always finite; absent mask
    // means no bits set, which is what getMaskFlags(null) reports.
    const maskBits = hasMask ? maskRaw[i] : 0;
    // Inlined rather than delegated to getPixelFlags for the same reason the
    // mask helpers are: a fresh object per pixel was 36 million allocations a
    // frame on a 16M detector. The parity tests hold this to that helper.
    const gap = (maskBits & 1) !== 0 || (inDataFlags && value === DATA_GAP_VALUE);
    const defective =
      (maskBits & 0x1e) !== 0 || (inDataFlags && value === DATA_DEFECTIVE_VALUE);
    if (gap) {
      gapPixels += 1;
    } else if (defective) {
      defectivePixels += 1;
    }
    if (satMax !== null && isSaturatedValue(value, satMax) && !gap && !defective) {
      saturatedPixels += 1;
    }
    if (!Number.isFinite(value)) continue;
    if (useMasking) {
      // `gap || defective` already covers every MASK_SKIP_BITS bit as well as
      // the two in-data values, so it replaces the bit test rather than
      // joining it.
      if (gap || defective) continue;
      if (maskSaturatedEnabled && isSaturatedValue(value, satMax)) continue;
    }
    count += 1;
    sum += value;
    if (value < min) min = value;
    if (value > max) max = value;
    const delta = value - mean;
    mean += delta / count;
    m2 += delta * (value - mean);
  }

  if (count === 0) {
    return {
      count: 0,
      sum: 0,
      mean: 0,
      median: 0,
      min: 0,
      max: 0,
      std: 0,
      totalPixels: dataRaw.length,
      gapPixels,
      defectivePixels,
      saturatedPixels,
    };
  }
  const std = count > 1 ? Math.sqrt(m2 / (count - 1)) : 0;

  // `min`/`max` here already exclude masked pixels, which matters: a real EIGER
  // frame marks overflow pixels 0xFFFFFFFF, and with the mask on they drop out
  // and leave a span narrow enough to count directly.
  const span = max - min + 1;
  let median;
  if (hasIntegralValues(dataRaw) && span >= 1 && span <= COUNTING_MEDIAN_MAX_SPAN) {
    median = countingMedian(dataRaw, min, span, count, {
      // Mask bits gate on `hasMask`, not `maskEnabled`, to match
      // applyMaskToValue: it is handed the mask value whenever one exists.
      maskBits: useMasking && hasMask ? maskRaw : null,
      maskSaturatedEnabled,
      isSaturatedValue,
      satMax,
      inDataFlags,
    });
  } else {
    // Wide-ranging or floating-point data: fall back to selection, but over a
    // reused typed buffer rather than a fresh JS array of every pixel.
    if (!selectionScratch || selectionScratch.length < count) {
      selectionScratch = new Float64Array(count);
    }
    let written = 0;
    for (let i = 0; i < dataRaw.length && written < count; i += 1) {
      if (!isAccepted(i)) continue;
      selectionScratch[written] = dataRaw[i];
      written += 1;
    }
    median = written ? computeMedian(selectionScratch.subarray(0, written)) : 0;
  }
  return {
    count,
    sum,
    mean,
    median,
    min,
    max,
    std,
    totalPixels: dataRaw.length,
    gapPixels,
    defectivePixels,
    saturatedPixels,
  };
}
