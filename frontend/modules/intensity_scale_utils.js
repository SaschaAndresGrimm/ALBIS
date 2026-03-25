/**
 * Intensity scaling, dtype helpers, histogram/stats, and palette generation.
 */

const AUTO_CONTRAST_LOW = 0.001;
const AUTO_CONTRAST_HIGH = 0.999;
const AUTO_CONTRAST_BINS = 4096;
const ALBULA_LIN_SIZE = 256;
const ALBULA_LOG_SIZE = 768;
const ALBULA_LUT_SIZE = ALBULA_LIN_SIZE + ALBULA_LOG_SIZE;
const ALBULA_LOG_FOREGROUND_FACTOR = 10000;

export function getWebglUnsignedDtypeKey(dtype) {
  const normalized = String(dtype || "").toLowerCase();
  if (normalized === "|u1" || normalized === "<u1" || normalized === "uint8") return "u8";
  if (normalized === "<u2" || normalized === "uint16") return "u16";
  if (normalized === "<u4" || normalized === "uint32") return "u32";
  return null;
}

export function isWebglUnsignedRawCandidate(dtype, data) {
  const key = getWebglUnsignedDtypeKey(dtype);
  if (key === "u8") return data instanceof Uint8Array;
  if (key === "u16") return data instanceof Uint16Array;
  if (key === "u32") return data instanceof Uint32Array;
  return false;
}

export function getWebglUnsignedUploadInfo(gl, key) {
  if (key === "u8") {
    return { internalFormat: gl.R8UI, format: gl.RED_INTEGER, type: gl.UNSIGNED_BYTE };
  }
  if (key === "u16") {
    return { internalFormat: gl.R16UI, format: gl.RED_INTEGER, type: gl.UNSIGNED_SHORT };
  }
  if (key === "u32") {
    return { internalFormat: gl.R32UI, format: gl.RED_INTEGER, type: gl.UNSIGNED_INT };
  }
  return null;
}

export function getDtypeInfo(dtype) {
  if (!dtype) return null;
  if (dtype.length >= 3 && (dtype[0] === "<" || dtype[0] === ">" || dtype[0] === "|")) {
    const kind = dtype[1];
    const bytes = Number.parseInt(dtype.slice(2), 10);
    if (Number.isFinite(bytes) && bytes > 0) {
      return { kind, bits: bytes * 8 };
    }
    return null;
  }
  const lower = dtype.toLowerCase();
  if (lower.startsWith("uint")) {
    const bits = Number.parseInt(lower.slice(4), 10);
    if (Number.isFinite(bits)) {
      return { kind: "u", bits };
    }
  }
  if (lower.startsWith("int")) {
    const bits = Number.parseInt(lower.slice(3), 10);
    if (Number.isFinite(bits)) {
      return { kind: "i", bits };
    }
  }
  if (lower.startsWith("float")) {
    const bits = Number.parseInt(lower.slice(5), 10);
    if (Number.isFinite(bits)) {
      return { kind: "f", bits };
    }
  }
  return null;
}

function normalizeSignedZero(text) {
  return text === "-0" ? "0" : text;
}

function trimFixedLabel(text) {
  return normalizeSignedZero(text.replace(/(\.\d*?[1-9])0+$/, "$1").replace(/\.0+$/, ""));
}

function trimScientificLabel(text) {
  const [mantissa, exponent] = String(text).replace("+", "").split("e");
  if (!exponent) return normalizeSignedZero(mantissa);
  return `${trimFixedLabel(mantissa)}e${exponent}`;
}

function getPixelLabelMaxChars(cellPx) {
  const pixelWidth = Math.max(8, Number(cellPx) || 0);
  return Math.max(1, Math.floor((pixelWidth - 2) / 5.6));
}

function compactIntegerLabel(value) {
  if (!Number.isFinite(value)) return "";
  const abs = Math.abs(value);
  const sign = value < 0 ? "-" : "";
  if (abs >= 1e9) return `${sign}${(abs / 1e9).toFixed(abs >= 1e10 ? 0 : 1).replace(/\.0$/, "")}G`;
  if (abs >= 1e6) return `${sign}${(abs / 1e6).toFixed(abs >= 1e7 ? 0 : 1).replace(/\.0$/, "")}M`;
  if (abs >= 1e3) return `${sign}${(abs / 1e3).toFixed(abs >= 1e4 ? 0 : 1).replace(/\.0$/, "")}k`;
  return `${Math.round(value)}`;
}

function formatScientificPixelLabel(value, maxChars) {
  for (let digits = 4; digits >= 0; digits -= 1) {
    const candidate = trimScientificLabel(Number(value).toExponential(digits));
    if (candidate.length <= maxChars) {
      return candidate;
    }
  }
  return "";
}

function formatIntegerPixelLabel(value, maxChars, { allowScientificFallback = false } = {}) {
  const rounded = Math.round(value);
  const integer = String(rounded);
  const compact = compactIntegerLabel(rounded);
  if (integer.length <= maxChars) return integer;
  if (compact.length <= maxChars) return compact;
  return allowScientificFallback ? formatScientificPixelLabel(value, maxChars) : "";
}

function formatFloatFixedPixelLabel(value, maxChars) {
  for (let digits = 6; digits >= 1; digits -= 1) {
    const candidate = trimFixedLabel(Number(value).toFixed(digits));
    if (!candidate || !candidate.includes(".")) continue;
    if (candidate.length <= maxChars) {
      return candidate;
    }
  }
  return "";
}

export function formatPixelLabelValue(value, cellPx, mode = "auto", dtype = "") {
  if (!Number.isFinite(value)) return "";
  const maxChars = getPixelLabelMaxChars(cellPx);
  const info = getDtypeInfo(dtype);
  const isFloat = info?.kind === "f";

  if (mode === "integer") {
    return formatIntegerPixelLabel(value, maxChars);
  }
  if (mode === "scientific") {
    return formatScientificPixelLabel(value, maxChars);
  }

  if (isFloat && !Number.isInteger(value)) {
    const decimal = formatFloatFixedPixelLabel(value, maxChars);
    if (decimal) return decimal;
    return formatScientificPixelLabel(value, maxChars);
  }

  return formatIntegerPixelLabel(value, maxChars, { allowScientificFallback: true });
}

export function getSaturationMax(dtype, rawMax) {
  const info = getDtypeInfo(dtype);
  if (!info || info.kind === "f") return null;
  if (!Number.isFinite(rawMax)) return null;
  const bits = info.bits;
  if (!Number.isFinite(bits) || bits <= 0 || bits > 52) return null;
  const dtypeMax = info.kind === "u" ? 2 ** bits - 1 : 2 ** (bits - 1) - 1;
  const candidates = [4, 8, 12, 16, 32];
  for (const candBits of candidates) {
    if (candBits > bits) continue;
    const candMax = 2 ** candBits - 1;
    if (rawMax === candMax) {
      return candMax;
    }
  }
  return dtypeMax;
}

export function chooseHistogramBins(count) {
  if (!Number.isFinite(count) || count <= 0) return 256;
  const bins = Math.round(Math.sqrt(count) * 0.5);
  return Math.max(32, Math.min(256, bins));
}

export function getPaletteColorCount(palette) {
  if (!palette || !palette.length) return 1;
  return Math.max(1, Math.floor(palette.length / 4));
}

export function mapAlbulaHdrToNorm(value, bg, fg) {
  // Emulate ALBULA HDR transfer:
  // linear ramp until FG, then logarithmic compression for brighter peaks.
  if (!Number.isFinite(value) || !Number.isFinite(bg) || !Number.isFinite(fg)) {
    return 0;
  }
  const lfg = fg * ALBULA_LOG_FOREGROUND_FACTOR;
  let idx = 0;
  if (value <= bg) {
    idx = 0;
  } else if (value >= lfg) {
    idx = ALBULA_LUT_SIZE - 1;
  } else if (value < fg && fg > bg) {
    const linSlope = ALBULA_LIN_SIZE / (fg - bg);
    idx = Math.floor((value - bg) * linSlope);
    idx = Math.max(0, Math.min(ALBULA_LIN_SIZE - 1, idx));
  } else if (fg > bg && lfg > fg && value > bg) {
    const denom = Math.log((lfg - bg) / (fg - bg));
    if (Number.isFinite(denom) && denom > 0) {
      const logSlope = (ALBULA_LOG_SIZE - 1) / denom;
      const logOffset = -Math.log(fg - bg) * logSlope;
      const x = Math.log(Math.max(value - bg, Number.EPSILON)) * logSlope + logOffset;
      idx = ALBULA_LIN_SIZE + Math.floor(x);
      idx = Math.max(ALBULA_LIN_SIZE, Math.min(ALBULA_LUT_SIZE - 1, idx));
    } else {
      idx = ALBULA_LIN_SIZE;
    }
  }
  return idx / (ALBULA_LUT_SIZE - 1);
}

export function computeHistogram(data, min, max, satMax, bins, logX) {
  const hist = new Uint32Array(bins);
  if (!Number.isFinite(min) || !Number.isFinite(max) || bins <= 0) {
    return hist;
  }
  const range = max - min || 1;
  let mapValue = (v) => (v - min) / range;
  if (logX) {
    const symlog = (v) => Math.sign(v) * Math.log10(1 + Math.abs(v));
    const minMap = symlog(min);
    const maxMap = symlog(max);
    const mapRange = maxMap - minMap || 1;
    mapValue = (v) => (symlog(v) - minMap) / mapRange;
  }

  for (let i = 0; i < data.length; i += 1) {
    const v = data[i];
    if (!Number.isFinite(v)) continue;
    if (v < 0) continue;
    if (satMax !== null && v === satMax) continue;
    const t = mapValue(v);
    if (!Number.isFinite(t)) continue;
    const idx = Math.min(bins - 1, Math.max(0, Math.floor(t * (bins - 1))));
    hist[idx] += 1;
  }
  return hist;
}

export function computeAutoLevels(data, satMaxInput, fallbackStats, dtype) {
  let rawMax = Number.NEGATIVE_INFINITY;
  for (let i = 0; i < data.length; i += 1) {
    const v = data[i];
    if (!Number.isFinite(v)) continue;
    if (v < 0) continue;
    if (v > rawMax) rawMax = v;
  }

  const satMax = satMaxInput ?? getSaturationMax(dtype, rawMax);
  let minLog = Number.POSITIVE_INFINITY;
  let maxLog = Number.NEGATIVE_INFINITY;
  let count = 0;

  for (let i = 0; i < data.length; i += 1) {
    const v = data[i];
    if (!Number.isFinite(v)) continue;
    if (v < 0) continue;
    if (satMax !== null && v === satMax) continue;
    const lv = Math.log1p(v);
    if (lv < minLog) minLog = lv;
    if (lv > maxLog) maxLog = lv;
    count += 1;
  }

  if (!Number.isFinite(minLog) || !Number.isFinite(maxLog) || count === 0) {
    return { min: fallbackStats?.min ?? 0, max: fallbackStats?.max ?? 1 };
  }

  const bins = Math.max(256, Math.min(AUTO_CONTRAST_BINS, Math.round(Math.sqrt(count)) * 4));
  const hist = new Uint32Array(bins);
  const range = maxLog - minLog || 1;

  for (let i = 0; i < data.length; i += 1) {
    const v = data[i];
    if (!Number.isFinite(v)) continue;
    if (v < 0) continue;
    if (satMax !== null && v === satMax) continue;
    const lv = Math.log1p(v);
    const idx = Math.min(bins - 1, Math.max(0, Math.floor(((lv - minLog) / range) * (bins - 1))));
    hist[idx] += 1;
  }

  const lowTarget = count * AUTO_CONTRAST_LOW;
  const highTarget = count * AUTO_CONTRAST_HIGH;
  let cumulative = 0;
  let lowBin = 0;
  for (let i = 0; i < bins; i += 1) {
    cumulative += hist[i];
    if (cumulative >= lowTarget) {
      lowBin = i;
      break;
    }
  }
  cumulative = 0;
  let highBin = bins - 1;
  for (let i = 0; i < bins; i += 1) {
    cumulative += hist[i];
    if (cumulative >= highTarget) {
      highBin = i;
      break;
    }
  }
  if (highBin <= lowBin) {
    highBin = Math.min(bins - 1, lowBin + 1);
  }

  const lowLog = minLog + (lowBin / (bins - 1)) * range;
  const highLog = minLog + (highBin / (bins - 1)) * range;
  const minVal = Math.expm1(lowLog);
  const maxVal = Math.expm1(highLog);
  if (!Number.isFinite(minVal) || !Number.isFinite(maxVal) || minVal >= maxVal) {
    return { min: fallbackStats?.min ?? 0, max: fallbackStats?.max ?? 1 };
  }
  return { min: minVal, max: maxVal };
}

export function computeStats(data, dtype, histLogX) {
  let rawMax = Number.NEGATIVE_INFINITY;

  for (let i = 0; i < data.length; i += 1) {
    const v = data[i];
    if (!Number.isFinite(v)) continue;
    if (v < 0) continue;
    if (v > rawMax) rawMax = v;
  }

  const satMax = getSaturationMax(dtype, rawMax);
  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;

  for (let i = 0; i < data.length; i += 1) {
    const v = data[i];
    if (!Number.isFinite(v)) continue;
    if (v < 0) continue;
    if (satMax !== null && v === satMax) continue;
    if (v < min) min = v;
    if (v > max) max = v;
  }

  if (!Number.isFinite(min) || !Number.isFinite(max)) {
    return { min: 0, max: 1, hist: new Uint32Array(0), satMax, bins: 0 };
  }

  const bins = chooseHistogramBins(data.length);
  const hist = computeHistogram(data, min, max, satMax, bins, histLogX);
  return { min, max, hist, satMax, bins };
}

export function mapValueToNorm(value, params) {
  const {
    min,
    max,
    colormap,
    invert,
  } = params || {};
  if (!Number.isFinite(value)) return 0;
  const minVal = Number.isFinite(min) ? min : 0;
  const maxVal = Number.isFinite(max) ? max : minVal + 1;
  const range = maxVal - minVal || 1;
  const t = (value - minVal) / range;
  let norm = 0;
  if (colormap === "albulaHdr") {
    norm = mapAlbulaHdrToNorm(value, minVal, maxVal);
  } else {
    norm = Math.min(1, Math.max(0, t));
  }
  if (invert) {
    norm = 1 - norm;
  }
  return Math.min(1, Math.max(0, norm));
}

export function buildPalette(name) {
  const paletteSize = name === "albulaHdr" ? ALBULA_LUT_SIZE : 256;
  const palette = new Uint8Array(paletteSize * 4);
  const mixStops = (stops, t) => {
    const scaled = t * (stops.length - 1);
    const idx = Math.floor(scaled);
    const frac = scaled - idx;
    const a = stops[idx];
    const b = stops[Math.min(idx + 1, stops.length - 1)];
    return [
      Math.round(a[0] + (b[0] - a[0]) * frac),
      Math.round(a[1] + (b[1] - a[1]) * frac),
      Math.round(a[2] + (b[2] - a[2]) * frac),
    ];
  };
  for (let i = 0; i < paletteSize; i += 1) {
    const t = paletteSize > 1 ? i / (paletteSize - 1) : 0;
    let r = 0;
    let g = 0;
    let b = 0;
    if (name === "gray") {
      r = g = b = Math.round(t * 255);
    } else if (name === "heat") {
      const tt = t * 3;
      r = Math.min(255, Math.round(255 * Math.min(tt, 1)));
      g = Math.min(255, Math.round(255 * Math.max(0, tt - 1)));
      b = Math.min(255, Math.round(255 * Math.max(0, tt - 2)));
    } else if (name === "viridis") {
      [r, g, b] = mixStops(
        [
          [68, 1, 84],
          [59, 82, 139],
          [33, 145, 140],
          [94, 201, 97],
          [253, 231, 37],
        ],
        t,
      );
    } else if (name === "magma") {
      [r, g, b] = mixStops(
        [
          [0, 0, 4],
          [53, 15, 83],
          [132, 32, 102],
          [196, 66, 74],
          [251, 135, 53],
          [252, 253, 191],
        ],
        t,
      );
    } else if (name === "inferno") {
      [r, g, b] = mixStops(
        [
          [0, 0, 4],
          [51, 13, 81],
          [120, 28, 109],
          [190, 55, 84],
          [249, 101, 49],
          [252, 255, 164],
        ],
        t,
      );
    } else if (name === "cividis") {
      [r, g, b] = mixStops(
        [
          [0, 32, 76],
          [40, 77, 117],
          [92, 125, 127],
          [147, 173, 112],
          [207, 223, 108],
          [253, 231, 37],
        ],
        t,
      );
    } else if (name === "turbo") {
      [r, g, b] = mixStops(
        [
          [48, 18, 59],
          [50, 127, 216],
          [63, 195, 160],
          [189, 211, 57],
          [249, 143, 8],
          [179, 21, 22],
        ],
        t,
      );
    } else if (name === "blueYellowRed") {
      r = Math.round(255 * Math.min(1, Math.max(0, t * 1.2)));
      g = Math.round(255 * Math.min(1, Math.max(0, 1.2 - Math.abs(t - 0.5) * 2)));
      b = Math.round(255 * Math.min(1, Math.max(0, 1 - t * 1.2)));
    } else if (name === "albisHdr") {
      const gamma = Math.pow(t, 0.7);
      r = Math.round(255 * Math.min(1, gamma * 1.1));
      g = Math.round(255 * Math.min(1, gamma * 0.9 + t * 0.3));
      b = Math.round(255 * Math.min(1, (1 - gamma) * 0.4 + t * 0.6));
    } else if (name === "albulaHdr") {
      if (i < ALBULA_LIN_SIZE) {
        const v = 255 - i;
        r = v;
        g = v;
        b = v;
      } else {
        const logIndex = i - ALBULA_LIN_SIZE;
        if (logIndex < 256) {
          r = logIndex;
          g = 0;
          b = 0;
        } else if (logIndex < 512) {
          r = 255;
          g = logIndex - 256;
          b = 0;
        } else {
          r = 255;
          g = 255;
          b = logIndex - 512;
        }
      }
    }
    const base = i * 4;
    palette[base] = r;
    palette[base + 1] = g;
    palette[base + 2] = b;
    palette[base + 3] = 255;
  }
  return palette;
}
