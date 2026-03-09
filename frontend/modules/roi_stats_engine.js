/**
 * Pure ROI statistics helpers for histogram and mask-aware aggregation.
 */

export function getMaskFlags(maskValue) {
  if (!Number.isFinite(maskValue)) {
    return { gap: false, defective: false };
  }
  return {
    gap: Boolean(maskValue & 1),
    defective: Boolean(maskValue & 0x1e),
  };
}

export function applyMaskToValue(value, maskValue, options = {}) {
  const { satMax = null, maskSaturatedEnabled = false, isSaturatedValue = () => false } = options;
  if (Number.isFinite(maskValue)) {
    if (maskValue & 1) {
      return { value: 0, skip: true };
    }
    if (maskValue & 0x1e) {
      return { value: 0, skip: true };
    }
  }
  if (maskSaturatedEnabled && isSaturatedValue(value, satMax)) {
    return { value: 0, skip: true };
  }
  return { value, skip: false };
}

export function createRoiPixelCounters() {
  return { total: 0, gap: 0, defective: 0, saturated: 0 };
}

export function accumulateRoiPixelCounters(counters, sampled, satMax, isSaturatedValue) {
  if (!counters || !sampled) return;
  counters.total += 1;
  const flags = getMaskFlags(sampled.maskValue);
  if (flags.gap) {
    counters.gap += 1;
  } else if (flags.defective) {
    counters.defective += 1;
  }
  if (satMax !== null && isSaturatedValue(sampled.raw, satMax) && !flags.gap && !flags.defective) {
    counters.saturated += 1;
  }
}

export function buildRoiHistogram(values) {
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
  const samples = [];
  const hasMask = maskAvailable && maskRaw && maskShape && maskShape[0] === height && maskShape[1] === width;
  const useMasking = (maskEnabled && hasMask) || maskSaturatedEnabled;

  for (let i = 0; i < dataRaw.length; i += 1) {
    let value = dataRaw[i];
    const maskValue = hasMask ? maskRaw[i] : null;
    const flags = getMaskFlags(maskValue);
    if (flags.gap) {
      gapPixels += 1;
    } else if (flags.defective) {
      defectivePixels += 1;
    }
    if (satMax !== null && isSaturatedValue(value, satMax) && !flags.gap && !flags.defective) {
      saturatedPixels += 1;
    }
    if (!Number.isFinite(value)) continue;
    if (useMasking) {
      const masked = applyMaskToValue(value, maskValue, {
        satMax,
        maskSaturatedEnabled,
        isSaturatedValue,
      });
      if (masked.skip) continue;
      value = masked.value;
    }
    count += 1;
    sum += value;
    samples.push(value);
    min = Math.min(min, value);
    max = Math.max(max, value);
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
  const median = samples.length ? computeMedian(samples) : 0;
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
