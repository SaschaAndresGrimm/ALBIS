/**
 * Pure helpers for the ROI readouts and profile plots: physical-unit
 * formatting, the resolution-calibration check and the profile peak finder.
 */

// d-spacing axis / feature-detection tuning.
export const PROFILE_PEAK_MAX = 6;
export const PROFILE_PEAK_MIN_PROMINENCE_FRAC = 0.08;
// Radial d-axis only makes physical sense when the ROI is centred on the beam.
// Floor for the beam-offset tolerance (the effective tolerance scales with the
// ROI radius, see beamOffsetTolerance).
export const BEAM_CENTER_TOLERANCE_PX = 6;

// Same calibration requirements as getResolutionAtPixel (planar needs distance +
// pixel size + energy; geometry mode needs energy + a loaded geometry).
export function isResolutionCalibrated(params) {
  if (!params) return false;
  if (params.mode === "geometry") {
    return Number.isFinite(params.energyEv) && params.energyEv > 0 && !!params.geometry;
  }
  // Planar mode measures the radius from the beam centre, so an unknown one
  // is not a calibrated geometry. Geometry mode is exempt above: the panel
  // origins carry the centre, so it does not come from these inputs.
  return (
    Boolean(params.centerKnown) &&
    Number.isFinite(params.distanceMm) && params.distanceMm > 0 &&
    Number.isFinite(params.pixelSizeUm) && params.pixelSizeUm > 0 &&
    Number.isFinite(params.energyEv) && params.energyEv > 0
  );
}

// Format a physical length (mm) with precision that scales with magnitude.
export function formatMm(value) {
  if (!Number.isFinite(value)) return null;
  const abs = Math.abs(value);
  const digits = abs >= 100 ? 1 : abs >= 1 ? 2 : 3;
  return value.toFixed(digits);
}

// Format a physical area (mm^2) with magnitude-aware precision.
export function formatArea(value) {
  if (!Number.isFinite(value) || value < 0) return null;
  const digits = value >= 100 ? 0 : value >= 1 ? 1 : value >= 0.01 ? 3 : 5;
  return value.toFixed(digits);
}

// Cheap 1D peak finder over a projection array. Returns axis-space peak
// positions ({ x, value }) so they survive plot zoom/pan (which re-slices data).
export function detectProfilePeaks(values, xStart = 0, xStep = 1) {
  const n = values.length;
  if (n < 3) return [];
  let maxV = -Infinity;
  let minV = Infinity;
  for (let i = 0; i < n; i += 1) {
    const v = values[i];
    if (Number.isFinite(v)) {
      if (v > maxV) maxV = v;
      if (v < minV) minV = v;
    }
  }
  const range = maxV - minV;
  if (!(range > 0)) return [];
  const minProminence = range * PROFILE_PEAK_MIN_PROMINENCE_FRAC;
  const candidates = [];
  for (let i = 0; i < n; i += 1) {
    const v = values[i];
    if (!Number.isFinite(v)) continue;
    let leftVal = -Infinity;
    for (let k = i - 1; k >= 0; k -= 1) {
      if (Number.isFinite(values[k])) { leftVal = values[k]; break; }
    }
    let rightVal = -Infinity;
    for (let k = i + 1; k < n; k += 1) {
      if (Number.isFinite(values[k])) { rightVal = values[k]; break; }
    }
    if (!(v >= leftVal && v >= rightVal)) continue;
    if (v === leftVal && v === rightVal) continue;
    // Topographic prominence: scan outward until higher ground, tracking the valley.
    let leftValley = v;
    for (let k = i - 1; k >= 0; k -= 1) {
      const val = values[k];
      if (!Number.isFinite(val)) continue;
      if (val > v) break;
      if (val < leftValley) leftValley = val;
    }
    let rightValley = v;
    for (let k = i + 1; k < n; k += 1) {
      const val = values[k];
      if (!Number.isFinite(val)) continue;
      if (val > v) break;
      if (val < rightValley) rightValley = val;
    }
    const prominence = v - Math.max(leftValley, rightValley);
    if (prominence < minProminence) continue;
    candidates.push({ index: i, value: v, prominence });
  }
  candidates.sort((a, b) => b.prominence - a.prominence);
  const minSeparation = Math.max(2, Math.round(n * 0.02));
  const selected = [];
  for (const cand of candidates) {
    if (selected.some((s) => Math.abs(s.index - cand.index) < minSeparation)) continue;
    selected.push(cand);
    if (selected.length >= PROFILE_PEAK_MAX) break;
  }
  selected.sort((a, b) => a.index - b.index);
  return selected.map((p) => ({ x: xStart + p.index * xStep, value: p.value }));
}
