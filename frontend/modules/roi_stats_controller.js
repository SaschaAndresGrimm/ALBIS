/**
 * ROI statistics, plots, and ROI plot interaction helpers.
 */

import {
  applyMaskToValue as applyMaskToValueEngine,
  accumulateRoiPixelCounters as accumulateRoiPixelCountersEngine,
  buildRoiHistogram as buildRoiHistogramEngine,
  computeGlobalStats as computeGlobalStatsEngine,
  createRoiPixelCounters as createRoiPixelCountersEngine,
  getMaskFlags as getMaskFlagsEngine,
  normalizeRoiHistogramBinCount,
} from "./roi_stats_engine.js";
import { renderRoiPlot } from "./roi_plot_renderer.js";
import {
  buildRoiCsvExportPayload,
  roiCsvExportUnavailableReason,
} from "./roi_csv_export.js";
import { t } from "./i18n.js";
import {
  clampCircularRoiInnerRadius,
  getCircularRoiOuterRadius,
  physicalRoiRadius,
} from "./roi_geometry_utils.js";

export function createRoiStatsController(ctx) {
  const {
    state,
    roiState,
    roiCenterXInput,
    roiCenterYInput,
    roiCenterSnapBtn,
    roiParams,
    roiLinePlot,
    roiBoxPlotX,
    roiBoxPlotY,
    roiHistogramPlot,
    roiPlotControls,
    roiRadiusField,
    roiCenterFields,
    roiRingFields,
    roiSizeLabel,
    roiHelp,
    roiModeSelect,
    roiClearBtn,
    roiRadiusInput,
    roiInnerInput,
    roiOuterInput,
    roiStartEl,
    roiEndEl,
    roiSizeEl,
    roiAreaEl,
    roiTotalEl,
    roiGapEl,
    roiDefectiveEl,
    roiSaturatedEl,
    roiMinEl,
    roiMaxEl,
    roiSumEl,
    roiMedianEl,
    roiMeanEl,
    roiStdEl,
    roiLineCanvas,
    roiLineCtx,
    roiXCanvas,
    roiXCtx,
    roiYCanvas,
    roiYCtx,
    roiHistCanvas,
    roiHistCtx,
    roiHistogramToggle,
    roiHistBinsAuto,
    roiHistBinCount,
    roiHistBinChip,
    roiHistBinManualRow,
    roiHistBinPresetBtns,
    updateRoiSectionState,
    drawRoiOverlay,
    getActiveSaturationMax,
    isSaturatedValue,
    computeMedian,
    formatStat,
    formatRoiTick,
    PLOT_THEME,
    setStatus,
    getRingParams,
    getResolutionAtPixel,
  } = ctx;

// d-spacing axis / feature-detection tuning.
const PROFILE_PEAK_MAX = 6;
const PROFILE_PEAK_MIN_PROMINENCE_FRAC = 0.08;
// Radial d-axis only makes physical sense when the ROI is centred on the beam.
// Floor for the beam-offset tolerance (the effective tolerance scales with the
// ROI radius, see beamOffsetTolerance).
const BEAM_CENTER_TOLERANCE_PX = 6;

function setRoiText(el, value) {
  if (!el) return;
  el.textContent = value;
}

// Render a value on two lines (e.g. pixel size above, physical mm below) so the
// combined readout does not overflow the stats column. Falls back to a single
// line when line2 is empty.
function setRoiTwoLine(el, line1, line2) {
  if (!el) return;
  el.textContent = line1;
  if (line2) {
    el.append(document.createElement("br"), document.createTextNode(line2));
  }
}

// Same calibration requirements as getResolutionAtPixel (planar needs distance +
// pixel size + energy; geometry mode needs energy + a loaded geometry).
function isResolutionCalibrated(params) {
  if (!params) return false;
  if (params.mode === "geometry") {
    return Number.isFinite(params.energyEv) && params.energyEv > 0 && !!params.geometry;
  }
  return (
    Number.isFinite(params.distanceMm) && params.distanceMm > 0 &&
    Number.isFinite(params.pixelSizeUm) && params.pixelSizeUm > 0 &&
    Number.isFinite(params.energyEv) && params.energyEv > 0
  );
}

// Format a physical length (mm) with precision that scales with magnitude.
function formatMm(value) {
  if (!Number.isFinite(value)) return null;
  const abs = Math.abs(value);
  const digits = abs >= 100 ? 1 : abs >= 1 ? 2 : 3;
  return value.toFixed(digits);
}

// Format a physical area (mm^2) with magnitude-aware precision.
function formatArea(value) {
  if (!Number.isFinite(value) || value < 0) return null;
  const digits = value >= 100 ? 0 : value >= 1 ? 1 : value >= 0.01 ? 3 : 5;
  return value.toFixed(digits);
}

// Cheap 1D peak finder over a projection array. Returns axis-space peak
// positions ({ x, value }) so they survive plot zoom/pan (which re-slices data).
function detectProfilePeaks(values, xStart = 0, xStep = 1) {
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

function updateRoiCenterInputs() {
  if (!roiCenterXInput || !roiCenterYInput) return;
  if (!roiState.start || (roiState.mode !== "circle" && roiState.mode !== "annulus")) {
    roiCenterXInput.value = "";
    roiCenterYInput.value = "";
    return;
  }
  roiCenterXInput.value = String(Math.round(roiState.start.x));
  roiCenterYInput.value = String(Math.round(roiState.start.y));
}

function applyRoiCenterFromInputs() {
  if (!roiCenterXInput || !roiCenterYInput) return null;
  const xVal = Number(roiCenterXInput.value);
  const yVal = Number(roiCenterYInput.value);
  if (!Number.isFinite(xVal) || !Number.isFinite(yVal)) return null;
  return { x: Math.round(xVal), y: Math.round(yVal) };
}

function getRoiPlotKey(canvasEl) {
  const id = canvasEl?.id || "";
  if (id === "roi-line-canvas") return "line";
  if (id === "roi-x-canvas") return "x";
  if (id === "roi-y-canvas") return "y";
  if (id === "roi-hist-canvas") return "hist";
  return "line";
}

function getRoiPlotLimits(plotKey) {
  return roiState.plotLimits[plotKey] || roiState.plotLimits.line;
}

function getRoiPlotLogMap() {
  if (!roiState.plotLog || typeof roiState.plotLog !== "object") {
    const fallback = Boolean(roiState.log);
    roiState.plotLog = {
      line: fallback,
      x: fallback,
      y: fallback,
      hist: fallback,
    };
  }
  ["line", "x", "y", "hist"].forEach((key) => {
    if (typeof roiState.plotLog[key] !== "boolean") {
      roiState.plotLog[key] = Boolean(roiState.log);
    }
  });
  return roiState.plotLog;
}

function getRoiPlotLog(plotKey) {
  const plotLog = getRoiPlotLogMap();
  return Boolean(plotLog[plotKey] ?? roiState.log);
}

function setRoiPlotLog(plotKey, enabled) {
  if (!["line", "x", "y", "hist"].includes(plotKey)) return;
  const plotLog = getRoiPlotLogMap();
  plotLog[plotKey] = Boolean(enabled);
  roiState.log = ["line", "x", "y", "hist"].some((key) => Boolean(plotLog[key]));
}

function getRoiHistogramBinsConfig() {
  if (!roiState.histogramBins || typeof roiState.histogramBins !== "object") {
    roiState.histogramBins = { mode: "auto", count: 128 };
  }
  const mode = roiState.histogramBins.mode === "fixed" ? "fixed" : "auto";
  const count = normalizeRoiHistogramBinCount(roiState.histogramBins.count);
  roiState.histogramBins.mode = mode;
  roiState.histogramBins.count = count;
  return { mode, count };
}

function syncRoiHistogramBinsUi() {
  const { mode, count } = getRoiHistogramBinsConfig();
  const isFixed = mode === "fixed";
  if (roiHistBinsAuto) {
    roiHistBinsAuto.checked = !isFixed;
    roiHistBinsAuto.disabled = !roiState.enabled;
  }
  if (roiHistBinCount) {
    roiHistBinCount.value = String(count);
    roiHistBinCount.disabled = !roiState.enabled || !isFixed;
  }
  if (roiHistBinManualRow) {
    roiHistBinManualRow.classList.toggle("is-hidden", !isFixed);
  }
  if (roiHistBinChip) {
    roiHistBinChip.textContent = isFixed ? t("roi.histogram.bins_chip", { count }) : "";
    roiHistBinChip.classList.toggle("is-hidden", !isFixed);
  }
  roiHistBinPresetBtns?.forEach((button) => {
    const presetCount = normalizeRoiHistogramBinCount(button.dataset?.bins);
    button.classList.toggle("is-active", isFixed && presetCount === count);
    button.disabled = !roiState.enabled || !isFixed;
  });
}

function syncRoiPlotLimitControls() {}

function setRoiPlotAxisLimits(plotKey, axis, minValue, maxValue) {
  if (axis !== "x" && axis !== "y") return;
  const limits = getRoiPlotLimits(plotKey);
  const minKey = axis === "x" ? "xMin" : "yMin";
  const maxKey = axis === "x" ? "xMax" : "yMax";
  let lo = Number.isFinite(minValue) ? minValue : null;
  let hi = Number.isFinite(maxValue) ? maxValue : null;
  if (lo !== null && hi !== null && lo > hi) {
    [lo, hi] = [hi, lo];
  }
  if (plotKey === "hist" && axis === "y") {
    if (lo !== null) lo = Math.max(0, lo);
    if (hi !== null) hi = Math.max(0, hi);
  }
  limits[minKey] = lo;
  limits[maxKey] = hi;
}

function clearRoiPlotLimitsForKey(plotKey) {
  const limits = getRoiPlotLimits(plotKey);
  if (!limits) return;
  limits.xMin = null;
  limits.xMax = null;
  limits.yMin = null;
  limits.yMax = null;
}

function hasManualRoiPlotLimits(plotKey) {
  const limits = getRoiPlotLimits(plotKey);
  if (!limits) return false;
  return (
    Number.isFinite(limits.xMin) ||
    Number.isFinite(limits.xMax) ||
    Number.isFinite(limits.yMin) ||
    Number.isFinite(limits.yMax)
  );
}

function hasAnyManualRoiPlotLimits() {
  return ["line", "x", "y", "hist"].some((key) => hasManualRoiPlotLimits(key));
}

function updateRoiModeUI() {
  const mode = roiState.mode || "line";
  const enabled = Boolean(roiState.enabled);
  const showPlots = enabled;
  if (roiParams) {
    roiParams.classList.toggle("is-hidden", !enabled);
    roiParams.classList.toggle("is-circle", mode === "circle");
    roiParams.classList.toggle("is-annulus", mode === "annulus");
  }
  if (roiLinePlot) {
    const showLine = enabled && (mode === "line" || mode === "circle" || mode === "annulus");
    roiLinePlot.classList.toggle("is-hidden", !showLine);
    const title = roiLinePlot.querySelector(".roi-plot-title");
    if (title) {
      title.textContent = mode === "line" ? t("roi.plot.line_profile") : t("roi.plot.radial_profile");
    }
  }
  if (roiBoxPlotX) {
    roiBoxPlotX.classList.toggle("is-hidden", !enabled || mode !== "box");
  }
  if (roiBoxPlotY) {
    roiBoxPlotY.classList.toggle("is-hidden", !enabled || mode !== "box");
  }
  if (roiHistogramPlot) {
    const showHistogram = enabled && Boolean(roiState.histogramEnabled);
    roiHistogramPlot.classList.toggle("is-hidden", !showHistogram);
  }
  if (roiPlotControls) {
    roiPlotControls.classList.toggle("is-hidden", !showPlots);
  }
  if (roiRadiusField) {
    roiRadiusField.classList.toggle("is-hidden", !enabled || mode !== "circle");
  }
  if (roiCenterFields) {
    roiCenterFields.classList.toggle("is-hidden", !enabled || (mode !== "circle" && mode !== "annulus"));
  }
  if (roiCenterSnapBtn) {
    roiCenterSnapBtn.classList.toggle("is-hidden", !enabled || (mode !== "circle" && mode !== "annulus"));
  }
  if (roiRingFields) {
    roiRingFields.classList.toggle("is-hidden", !enabled || mode !== "annulus");
  }
  if (roiSizeLabel) {
    if (!enabled) {
      roiSizeLabel.textContent = t("roi.size.image");
    } else if (mode === "line") {
      roiSizeLabel.textContent = t("roi.size.length_px");
    } else if (mode === "box") {
      roiSizeLabel.textContent = t("roi.size.width_height");
    } else if (mode === "circle") {
      roiSizeLabel.textContent = t("roi.size.radius_px");
    } else if (mode === "annulus") {
      roiSizeLabel.textContent = t("roi.size.rin_rout");
    } else {
      roiSizeLabel.textContent = t("roi.size.image");
    }
  }
  if (roiHelp) {
    if (!enabled) {
      roiHelp.textContent = t("roi.help.enable");
    } else if (mode === "annulus") {
      roiHelp.textContent = t("roi.help.annulus");
    } else if (mode === "circle") {
      roiHelp.textContent = t("roi.help.circle");
    } else {
      roiHelp.textContent = t("roi.help.default");
    }
  }
  if (roiModeSelect) {
    roiModeSelect.disabled = !enabled;
  }
  if (roiClearBtn) {
    roiClearBtn.disabled = !enabled;
  }
  if (roiHistogramToggle) {
    roiHistogramToggle.checked = Boolean(roiState.histogramEnabled);
    roiHistogramToggle.disabled = !enabled;
  }
  syncRoiHistogramBinsUi();
  updateRoiCenterInputs();
  syncRoiPlotLimitControls();
  updateRoiSectionState();
}

function clearRoi() {
  roiState.start = null;
  roiState.end = null;
  roiState.active = false;
  roiState.stats = null;
  roiState.lineProfile = null;
  roiState.xProjection = null;
  roiState.yProjection = null;
  roiState.histogramDistribution = null;
  roiState.innerRadius = 0;
  roiState.outerRadius = 0;
  if (roiRadiusInput) roiRadiusInput.value = "";
  if (roiCenterXInput) roiCenterXInput.value = "";
  if (roiCenterYInput) roiCenterYInput.value = "";
  if (roiInnerInput) roiInnerInput.value = "";
  if (roiOuterInput) roiOuterInput.value = "";
  setRoiText(roiStartEl, "-");
  setRoiText(roiEndEl, "-");
  setRoiText(roiSizeEl, "-");
  setRoiText(roiAreaEl, "-");
  setRoiText(roiTotalEl, "-");
  setRoiText(roiGapEl, "-");
  setRoiText(roiDefectiveEl, "-");
  setRoiText(roiSaturatedEl, "-");
  setRoiText(roiMinEl, "-");
  setRoiText(roiMaxEl, "-");
  setRoiText(roiSumEl, "-");
  setRoiText(roiMedianEl, "-");
  setRoiText(roiMeanEl, "-");
  setRoiText(roiStdEl, "-");
  drawRoiPlot(roiLineCanvas, roiLineCtx, null);
  drawRoiPlot(roiXCanvas, roiXCtx, null);
  drawRoiPlot(roiYCanvas, roiYCtx, null);
  drawRoiPlot(roiHistCanvas, roiHistCtx, null);
  drawRoiOverlay();
  updateRoiSectionState();
}

function applyMaskToValue(value, maskValue, satMax = getActiveSaturationMax()) {
  return applyMaskToValueEngine(value, maskValue, {
    satMax,
    maskSaturatedEnabled: state.maskSaturatedEnabled,
    isSaturatedValue,
  });
}

function getMaskFlags(maskValue) {
  return getMaskFlagsEngine(maskValue);
}

function sampleValue(ix, iy) {
  if (!state.dataRaw) return null;
  const idx = iy * state.width + ix;
  const raw = state.dataRaw[idx];
  const satMax = getActiveSaturationMax();
  const hasMask =
    state.maskAvailable &&
    state.maskRaw &&
    state.maskShape &&
    state.maskShape[0] === state.height &&
    state.maskShape[1] === state.width;
  const maskValue = hasMask ? state.maskRaw[idx] : null;
  const useMasking = (state.maskEnabled && hasMask) || state.maskSaturatedEnabled;
  if (useMasking) {
    const masked = applyMaskToValue(raw, maskValue, satMax);
    return { value: masked.value, skip: masked.skip, raw, maskValue, maskingApplied: true };
  }
  return { value: raw, skip: false, raw, maskValue, maskingApplied: false };
}

function isGapMaskedSample(sampled) {
  if (!sampled?.maskingApplied) return false;
  const flags = getMaskFlags(sampled.maskValue);
  return flags.gap;
}

function computeGlobalStats() {
  return computeGlobalStatsEngine({
    dataRaw: state.dataRaw,
    width: state.width,
    height: state.height,
    maskAvailable: state.maskAvailable,
    maskRaw: state.maskRaw,
    maskShape: state.maskShape,
    maskEnabled: state.maskEnabled,
    maskSaturatedEnabled: state.maskSaturatedEnabled,
    satMax: getActiveSaturationMax(),
    isSaturatedValue,
    computeMedian,
  });
}

function updateGlobalStats() {
  state.globalStats = computeGlobalStats();
}

function showRoiTooltip(canvasEl, text, clientX, clientY) {
  if (!canvasEl) return;
  const container = canvasEl.parentElement;
  if (!container) return;
  const tooltip = container.querySelector(".roi-tooltip");
  if (!tooltip) return;
  tooltip.textContent = text;
  tooltip.classList.add("is-visible");
  tooltip.setAttribute("aria-hidden", "false");
  const rect = container.getBoundingClientRect();
  let left = clientX - rect.left + 8;
  let top = clientY - rect.top + 8;
  const maxLeft = rect.width - tooltip.offsetWidth - 6;
  const maxTop = rect.height - tooltip.offsetHeight - 6;
  left = Math.min(maxLeft, Math.max(6, left));
  top = Math.min(maxTop, Math.max(6, top));
  tooltip.style.left = `${left}px`;
  tooltip.style.top = `${top}px`;
}

function hideRoiTooltip(canvasEl) {
  if (!canvasEl) return;
  const container = canvasEl.parentElement;
  const tooltip = container?.querySelector(".roi-tooltip");
  if (!tooltip) return;
  tooltip.classList.remove("is-visible");
  tooltip.setAttribute("aria-hidden", "true");
}

function updateRoiTooltip(event, canvasEl) {
  const plot = canvasEl?._roiPlot;
  if (!plot || !plot.data || plot.data.length === 0) {
    hideRoiTooltip(canvasEl);
    return;
  }
  const rect = canvasEl.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const y = event.clientY - rect.top;
  const plotX = x - plot.padL;
  const plotW = plot.width - plot.padL - plot.padR;
  const plotH = plot.height - plot.padT - plot.padB;
  if (plotX < 0 || plotX > plotW || y < plot.padT || y > plot.padT + plotH) {
    hideRoiTooltip(canvasEl);
    return;
  }
  const xFraction = plotW ? plotX / plotW : 0;
  const idx = Math.max(0, Math.min(plot.data.length - 1, Math.round(xFraction * (plot.data.length - 1))));
  const xValue = plot.xStart + idx * plot.xStep;
  const value = plot.data[idx];
  const xText = plot.xTickMode === "integer" ? `${Math.round(xValue)}` : formatRoiTick(xValue);
  const label = `${plot.xLabel} ${xText}  ${t("roi.tooltip.value")} ${formatStat(value)}`;
  showRoiTooltip(canvasEl, label, event.clientX, event.clientY);
}

function createRoiPixelCounters() {
  return createRoiPixelCountersEngine();
}

function accumulateRoiPixelCounters(counters, sampled, satMax) {
  accumulateRoiPixelCountersEngine(counters, sampled, satMax, isSaturatedValue);
}

function updateRoiPixelCounterFields(counters) {
  setRoiText(roiTotalEl, counters ? `${counters.total}` : "-");
  setRoiText(roiGapEl, counters ? `${counters.gap}` : "-");
  setRoiText(roiDefectiveEl, counters ? `${counters.defective}` : "-");
  setRoiText(roiSaturatedEl, counters ? `${counters.saturated}` : "-");
}

function buildRoiHistogram(values) {
  return buildRoiHistogramEngine(values, getRoiHistogramBinsConfig());
}

function updateRoiHistogramPlot(values) {
  if (!roiHistCanvas || !roiHistCtx || !roiHistogramPlot) return;
  const enabled = Boolean(roiState.enabled && roiState.histogramEnabled);
  roiHistogramPlot.classList.toggle("is-hidden", !enabled);
  if (!enabled) {
    roiState.histogramDistribution = null;
    roiHistCanvas._roiPlotMeta = null;
    drawRoiPlot(roiHistCanvas, roiHistCtx, null);
    return;
  }

  const histogram = buildRoiHistogram(values);
  if (!histogram || !histogram.data?.length) {
    roiState.histogramDistribution = null;
    roiHistCanvas._roiPlotMeta = null;
    drawRoiPlot(roiHistCanvas, roiHistCtx, null);
    return;
  }

  roiState.histogramDistribution = histogram.data;
  roiHistCanvas._roiPlotMeta = {
    xLabel: t("roi.plot.intensity"),
    yLabel: t("roi.plot.count"),
    xStart: histogram.xStart,
    xStep: histogram.xStep,
    xTickMode: histogram.xTickMode,
    seriesType: "histogram",
  };
  drawRoiPlot(roiHistCanvas, roiHistCtx, roiState.histogramDistribution);
}

// Per-axis pixel sizes in mm for physical ROI readouts. X is the reference
// axis; Y = X * pixelAspect (so strixel detectors report true mm). Returns
// nulls when the pixel size is unknown, in which case physical readouts are
// suppressed and only the pixel size is shown.
function getRoiPixelSizesMm() {
  const params = typeof getRingParams === "function" ? getRingParams() : null;
  const pxX =
    params && Number.isFinite(params.pixelSizeUm) && params.pixelSizeUm > 0
      ? params.pixelSizeUm / 1000
      : null;
  if (pxX == null) return { pxXmm: null, pxYmm: null };
  return { pxXmm: pxX, pxYmm: pxX * (state.pixelAspect || 1) };
}

function updateRoiStats() {
  // This function is intentionally central: it computes ROI statistics and
  // updates all derived plots/labels in one pass to keep UI state consistent.
  if (!state.hasFrame) {
    if (roiState.active) {
      clearRoi();
    }
    updateRoiHistogramPlot([]);
    updateRoiSectionState();
    return;
  }
  if (!roiState.enabled) {
    roiState.active = false;
    const stats = state.globalStats || computeGlobalStats();
    setRoiText(roiStartEl, "-");
    setRoiText(roiEndEl, "-");
    if (roiSizeLabel) {
      roiSizeLabel.textContent = t("roi.size.image");
    }
    if (state.width && state.height) {
      const { pxXmm, pxYmm } = getRoiPixelSizesMm();
      if (pxXmm) {
        const wMm = state.width * pxXmm;
        const hMm = state.height * pxYmm;
        setRoiTwoLine(roiSizeEl, `${state.width} × ${state.height} px`, `${formatMm(wMm)} × ${formatMm(hMm)} mm`);
        setRoiText(roiAreaEl, `${formatArea(wMm * hMm)} mm²`);
      } else {
        setRoiText(roiSizeEl, `${state.width} × ${state.height}`);
        setRoiText(roiAreaEl, "-");
      }
    } else {
      setRoiText(roiSizeEl, "-");
      setRoiText(roiAreaEl, "-");
    }
    updateRoiPixelCounterFields(
      stats
        ? {
            total: stats.totalPixels ?? 0,
            gap: stats.gapPixels ?? 0,
            defective: stats.defectivePixels ?? 0,
            saturated: stats.saturatedPixels ?? 0,
          }
        : null
    );
    setRoiText(roiMinEl, stats ? formatStat(stats.min) : "-");
    setRoiText(roiMaxEl, stats ? formatStat(stats.max) : "-");
    setRoiText(roiSumEl, stats ? formatStat(stats.sum) : "-");
    setRoiText(roiMedianEl, stats && Number.isFinite(stats.median) ? formatStat(stats.median) : "-");
    setRoiText(roiMeanEl, stats ? formatStat(stats.mean) : "-");
    setRoiText(roiStdEl, stats ? formatStat(stats.std) : "-");
    if (roiLineCanvas) {
      roiLineCanvas._roiPlotMeta = null;
    }
    if (roiXCanvas) {
      roiXCanvas._roiPlotMeta = null;
    }
    if (roiYCanvas) {
      roiYCanvas._roiPlotMeta = null;
    }
    if (roiHistCanvas) {
      roiHistCanvas._roiPlotMeta = null;
    }
    drawRoiPlot(roiLineCanvas, roiLineCtx, null);
    drawRoiPlot(roiXCanvas, roiXCtx, null);
    drawRoiPlot(roiYCanvas, roiYCtx, null);
    updateRoiHistogramPlot([]);
    drawRoiOverlay();
    updateRoiSectionState();
    return;
  }
  if (!roiState.start || !roiState.end) {
    const stats = state.globalStats;
    setRoiText(roiStartEl, "-");
    setRoiText(roiEndEl, "-");
    setRoiText(roiSizeEl, "-");
    setRoiText(roiAreaEl, "-");
    updateRoiPixelCounterFields(
      stats
        ? {
            total: stats.totalPixels ?? 0,
            gap: stats.gapPixels ?? 0,
            defective: stats.defectivePixels ?? 0,
            saturated: stats.saturatedPixels ?? 0,
          }
        : null
    );
    setRoiText(roiMinEl, stats ? formatStat(stats.min) : "-");
    setRoiText(roiMaxEl, stats ? formatStat(stats.max) : "-");
    setRoiText(roiSumEl, stats ? formatStat(stats.sum) : "-");
    setRoiText(roiMedianEl, stats && Number.isFinite(stats.median) ? formatStat(stats.median) : "-");
    setRoiText(roiMeanEl, stats ? formatStat(stats.mean) : "-");
    setRoiText(roiStdEl, stats ? formatStat(stats.std) : "-");
    drawRoiPlot(roiLineCanvas, roiLineCtx, null);
    drawRoiPlot(roiXCanvas, roiXCtx, null);
    drawRoiPlot(roiYCanvas, roiYCtx, null);
    updateRoiHistogramPlot([]);
    drawRoiOverlay();
    updateRoiSectionState();
    return;
  }
  const circularMode = roiState.mode === "circle" || roiState.mode === "annulus";
  const x0 = circularMode
    ? Math.round(roiState.start.x)
    : Math.max(0, Math.min(state.width - 1, Math.round(roiState.start.x)));
  const y0 = circularMode
    ? Math.round(roiState.start.y)
    : Math.max(0, Math.min(state.height - 1, Math.round(roiState.start.y)));
  const x1 = circularMode
    ? Math.round(roiState.end.x)
    : Math.max(0, Math.min(state.width - 1, Math.round(roiState.end.x)));
  const y1 = circularMode
    ? Math.round(roiState.end.y)
    : Math.max(0, Math.min(state.height - 1, Math.round(roiState.end.y)));
  setRoiText(roiStartEl, `${x0}, ${y0}`);
  setRoiText(roiEndEl, `${x1}, ${y1}`);

  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;
  let sum = 0;
  let count = 0;
  let mean = 0;
  let m2 = 0;
  const statsValues = [];
  const satMax = getActiveSaturationMax();
  const pixelCounters = createRoiPixelCounters();

  if (roiState.mode === "line") {
    const dx = x1 - x0;
    const dy = y1 - y0;
    const steps = Math.max(Math.abs(dx), Math.abs(dy));
    const values = [];
    for (let i = 0; i <= steps; i += 1) {
      const t = steps === 0 ? 0 : i / steps;
      const ix = Math.max(0, Math.min(state.width - 1, Math.round(x0 + dx * t)));
      const iy = Math.max(0, Math.min(state.height - 1, Math.round(y0 + dy * t)));
      const sampled = sampleValue(ix, iy);
      if (!sampled) continue;
      accumulateRoiPixelCounters(pixelCounters, sampled, satMax);
      const v = sampled.value;
      const hideInPlot = sampled.skip || !Number.isFinite(v) || isGapMaskedSample(sampled);
      values.push(hideInPlot ? Number.NaN : v);
      if (sampled.skip || !Number.isFinite(v)) {
        continue;
      }
      statsValues.push(v);
      count += 1;
      sum += v;
      min = Math.min(min, v);
      max = Math.max(max, v);
      const delta = v - mean;
      mean += delta / count;
      m2 += delta * (v - mean);
    }
    const length = Math.hypot(dx, dy);
    roiState.lineProfile = values;
    roiState.xProjection = null;
    roiState.yProjection = null;
    const { pxXmm: linePxX, pxYmm: linePxY } = getRoiPixelSizesMm();
    if (linePxX) {
      // Physical length uses the per-axis pixel sizes for the X/Y components.
      const lengthMm = Math.hypot(dx * linePxX, dy * linePxY);
      setRoiTwoLine(roiSizeEl, `${formatStat(length)} px`, `${formatMm(lengthMm)} mm`);
    } else {
      setRoiText(roiSizeEl, formatStat(length));
    }
    setRoiText(roiAreaEl, "-");
    updateRoiPixelCounterFields(pixelCounters);
    const std = count > 1 ? Math.sqrt(m2 / (count - 1)) : 0;
    const median = statsValues.length ? computeMedian(statsValues) : Number.NaN;
    setRoiText(roiMinEl, count ? formatStat(min) : "-");
    setRoiText(roiMaxEl, count ? formatStat(max) : "-");
    setRoiText(roiSumEl, count ? formatStat(sum) : "-");
    setRoiText(roiMedianEl, count ? formatStat(median) : "-");
    setRoiText(roiMeanEl, count ? formatStat(mean) : "-");
    setRoiText(roiStdEl, count ? formatStat(std) : "-");
    const lineParams = typeof getRingParams === "function" ? getRingParams() : null;
    const lineDSpacingForX =
      isResolutionCalibrated(lineParams) && typeof getResolutionAtPixel === "function"
        ? (xValue) => {
            const frac = steps === 0 ? 0 : xValue / steps;
            const ix = Math.max(0, Math.min(state.width - 1, Math.round(x0 + dx * frac)));
            const iy = Math.max(0, Math.min(state.height - 1, Math.round(y0 + dy * frac)));
            return getResolutionAtPixel(ix, iy);
          }
        : null;
    if (roiLineCanvas) {
      roiLineCanvas._roiPlotMeta = {
        xLabel: t("roi.plot.pixels"),
        yLabel: t("roi.plot.intensity"),
        xStart: 0,
        xStep: 1,
        xTickMode: "integer",
        dSpacingForX: lineDSpacingForX,
        dAxisLabel: t("roi.plot.d_axis"),
        qAxisLabel: t("roi.plot.q_axis"),
        peaks: lineDSpacingForX ? detectProfilePeaks(values, 0, 1) : null,
      };
    }
    drawRoiPlot(roiLineCanvas, roiLineCtx, values);
    drawRoiPlot(roiXCanvas, roiXCtx, null);
    drawRoiPlot(roiYCanvas, roiYCtx, null);
  } else if (roiState.mode === "box") {
    const left = Math.min(x0, x1);
    const right = Math.max(x0, x1);
    const top = Math.min(y0, y1);
    const bottom = Math.max(y0, y1);
    const width = right - left + 1;
    const height = bottom - top + 1;
    const xProj = new Float64Array(width);
    const yProj = new Float64Array(height);
    const xCounts = new Int32Array(width);
    const yCounts = new Int32Array(height);

    for (let y = top; y <= bottom; y += 1) {
      const rowIndex = y - top;
      for (let x = left; x <= right; x += 1) {
        const colIndex = x - left;
        const sampled = sampleValue(x, y);
        if (!sampled) continue;
        accumulateRoiPixelCounters(pixelCounters, sampled, satMax);
        const v = sampled.value;
        const validForStats = !sampled.skip && Number.isFinite(v);
        if (validForStats) {
          statsValues.push(v);
          count += 1;
          sum += v;
          min = Math.min(min, v);
          max = Math.max(max, v);
          const delta = v - mean;
          mean += delta / count;
          m2 += delta * (v - mean);
          if (isGapMaskedSample(sampled)) {
            continue;
          }
          xProj[colIndex] += v;
          yProj[rowIndex] += v;
          xCounts[colIndex] += 1;
          yCounts[rowIndex] += 1;
        }
      }
    }
    for (let i = 0; i < xProj.length; i += 1) {
      xProj[i] = xCounts[i] > 0 ? xProj[i] / xCounts[i] : Number.NaN;
    }
    for (let i = 0; i < yProj.length; i += 1) {
      yProj[i] = yCounts[i] > 0 ? yProj[i] / yCounts[i] : Number.NaN;
    }
    roiState.xProjection = Array.from(xProj);
    roiState.yProjection = Array.from(yProj);
    roiState.lineProfile = null;
    const { pxXmm: boxPxX, pxYmm: boxPxY } = getRoiPixelSizesMm();
    if (boxPxX) {
      const wMm = width * boxPxX;
      const hMm = height * boxPxY;
      setRoiTwoLine(roiSizeEl, `${width} × ${height} px`, `${formatMm(wMm)} × ${formatMm(hMm)} mm`);
      setRoiText(roiAreaEl, `${formatArea(wMm * hMm)} mm²`);
    } else {
      setRoiText(roiSizeEl, `${width} × ${height}`);
      setRoiText(roiAreaEl, "-");
    }
    updateRoiPixelCounterFields(pixelCounters);
    const std = count > 1 ? Math.sqrt(m2 / (count - 1)) : 0;
    const median = statsValues.length ? computeMedian(statsValues) : Number.NaN;
    setRoiText(roiMinEl, count ? formatStat(min) : "-");
    setRoiText(roiMaxEl, count ? formatStat(max) : "-");
    setRoiText(roiSumEl, count ? formatStat(sum) : "-");
    setRoiText(roiMedianEl, count ? formatStat(median) : "-");
    setRoiText(roiMeanEl, count ? formatStat(mean) : "-");
    setRoiText(roiStdEl, count ? formatStat(std) : "-");
    drawRoiPlot(roiLineCanvas, roiLineCtx, null);
    if (roiXCanvas) {
      roiXCanvas._roiPlotMeta = {
        xLabel: t("roi.plot.x_pixel"),
        yLabel: t("roi.plot.mean"),
        xStart: left,
        xStep: 1,
        xTickMode: "integer",
      };
    }
    if (roiYCanvas) {
      roiYCanvas._roiPlotMeta = {
        xLabel: t("roi.plot.y_pixel"),
        yLabel: t("roi.plot.mean"),
        xStart: top,
        xStep: 1,
        xTickMode: "integer",
      };
    }
    drawRoiPlot(roiXCanvas, roiXCtx, roiState.xProjection);
    drawRoiPlot(roiYCanvas, roiYCtx, roiState.yProjection);
  } else if (roiState.mode === "circle" || roiState.mode === "annulus") {
    // A circular/annulus ROI is a physical resolution shell. Radii are stored
    // in X-pixel-equivalent units; physical radius r = sqrt(dx^2 + (dy*aspect)^2)
    // with aspect = y_pixel_size / x_pixel_size. For square pixels aspect = 1
    // and this reduces to the ordinary pixel-space circle.
    const aspect = state.pixelAspect || 1;
    const outerRadius = getCircularRoiOuterRadius(roiState, aspect);
    if (roiState.mode === "circle") {
      roiState.innerRadius = 0;
      roiState.outerRadius = outerRadius;
      if (roiRadiusInput) roiRadiusInput.value = String(outerRadius);
    } else {
      roiState.outerRadius = outerRadius;
      const inner = clampCircularRoiInnerRadius(roiState.innerRadius, outerRadius);
      roiState.innerRadius = inner;
      if (roiInnerInput) roiInnerInput.value = String(inner);
      if (roiOuterInput) roiOuterInput.value = String(outerRadius);
    }

    // The shell is an ellipse in pixel space: full width in X, but only
    // outerRadius / aspect rows tall in Y.
    const yExtent = Math.ceil(outerRadius / aspect);
    const left = Math.max(0, Math.floor(x0 - outerRadius));
    const right = Math.min(state.width - 1, Math.ceil(x0 + outerRadius));
    const top = Math.max(0, Math.floor(y0 - yExtent));
    const bottom = Math.min(state.height - 1, Math.ceil(y0 + yExtent));
    const innerR2 = roiState.innerRadius * roiState.innerRadius;
    const outerR2 = outerRadius * outerRadius;
    const radialSum = new Float64Array(outerRadius + 1);
    const radialCount = new Uint32Array(outerRadius + 1);

    for (let y = top; y <= bottom; y += 1) {
      const dyPix = (y - y0) * aspect;
      for (let x = left; x <= right; x += 1) {
        const dxPix = x - x0;
        const r2 = dxPix * dxPix + dyPix * dyPix;
        if (r2 > outerR2 || r2 < innerR2) continue;
        const sampled = sampleValue(x, y);
        if (!sampled) continue;
        accumulateRoiPixelCounters(pixelCounters, sampled, satMax);
        const v = sampled.value;
        const validForStats = !sampled.skip && Number.isFinite(v);
        if (validForStats) {
          statsValues.push(v);
          count += 1;
          sum += v;
          min = Math.min(min, v);
          max = Math.max(max, v);
          const delta = v - mean;
          mean += delta / count;
          m2 += delta * (v - mean);
          if (isGapMaskedSample(sampled)) {
            continue;
          }
          const r = Math.min(outerRadius, Math.floor(Math.sqrt(r2)));
          radialSum[r] += v;
          radialCount[r] += 1;
        }
      }
    }

    const profile = Array.from(radialSum, (v, i) => (radialCount[i] ? v / radialCount[i] : Number.NaN));
    let displayProfile = profile;
    let displayStart = 0;
    if (roiState.mode === "annulus" && roiState.innerRadius > 0) {
      displayStart = Math.min(roiState.innerRadius, profile.length - 1);
      displayProfile = profile.slice(displayStart);
    }
    roiState.lineProfile = displayProfile;
    roiState.xProjection = null;
    roiState.yProjection = null;
    // Radii are X-pixel-equivalent, so physical radius = radius * pxXmm and the
    // shell area is that of a true (physical) circle / annulus.
    const { pxXmm: circPxX } = getRoiPixelSizesMm();
    if (roiState.mode === "circle") {
      const sizePx = `${outerRadius}`;
      if (circPxX) {
        const rMm = outerRadius * circPxX;
        setRoiTwoLine(roiSizeEl, `${sizePx} px`, `${formatMm(rMm)} mm`);
        setRoiText(roiAreaEl, `${formatArea(Math.PI * rMm * rMm)} mm²`);
      } else {
        setRoiText(roiSizeEl, sizePx);
        setRoiText(roiAreaEl, "-");
      }
    } else {
      const sizePx = `${roiState.innerRadius} → ${outerRadius}`;
      if (circPxX) {
        const rInMm = roiState.innerRadius * circPxX;
        const rOutMm = outerRadius * circPxX;
        setRoiTwoLine(roiSizeEl, `${sizePx} px`, `${formatMm(rInMm)} → ${formatMm(rOutMm)} mm`);
        setRoiText(roiAreaEl, `${formatArea(Math.PI * (rOutMm * rOutMm - rInMm * rInMm))} mm²`);
      } else {
        setRoiText(roiSizeEl, sizePx);
        setRoiText(roiAreaEl, "-");
      }
    }
    updateRoiPixelCounterFields(pixelCounters);
    const std = count > 1 ? Math.sqrt(m2 / (count - 1)) : 0;
    const median = statsValues.length ? computeMedian(statsValues) : Number.NaN;
    setRoiText(roiMinEl, count ? formatStat(min) : "-");
    setRoiText(roiMaxEl, count ? formatStat(max) : "-");
    setRoiText(roiSumEl, count ? formatStat(sum) : "-");
    setRoiText(roiMedianEl, count ? formatStat(median) : "-");
    setRoiText(roiMeanEl, count ? formatStat(mean) : "-");
    setRoiText(roiStdEl, count ? formatStat(std) : "-");
    const radialParams = typeof getRingParams === "function" ? getRingParams() : null;
    // A radius->d mapping is only physical when the radial profile is centred on
    // the beam; otherwise the ring geometry the profile sees is off-axis. Use a
    // radius-relative tolerance so a hand-placed-but-centred circle still
    // qualifies, while an off-in-a-corner ROI (where d would be meaningless) does
    // not. Accuracy is exact at the beam centre and degrades with the offset.
    const beamCentreReady =
      isResolutionCalibrated(radialParams) &&
      typeof getResolutionAtPixel === "function" &&
      Number.isFinite(radialParams.centerX) &&
      Number.isFinite(radialParams.centerY);
    const beamOffsetPx = beamCentreReady
      ? physicalRoiRadius(x0 - radialParams.centerX, y0 - radialParams.centerY, aspect)
      : Number.POSITIVE_INFINITY;
    const beamOffsetTolerance = Math.max(BEAM_CENTER_TOLERANCE_PX, outerRadius * 0.1);
    const radialCentredOnBeam = beamOffsetPx <= beamOffsetTolerance;
    // Radial bins are in X-pixel-equivalent units; label the axis in physical
    // millimetres when the pixel size is known (geometry-independent), else fall
    // back to pixels. pxXmm converts a bin radius to mm via r_mm = r * pxXmm.
    const pxXmm =
      radialParams && Number.isFinite(radialParams.pixelSizeUm) && radialParams.pixelSizeUm > 0
        ? radialParams.pixelSizeUm / 1000
        : null;
    const xStep = pxXmm || 1;
    const radialDSpacingForX = radialCentredOnBeam
      ? (xValue) => {
          const params = getRingParams();
          if (!params || !Number.isFinite(params.centerX) || !Number.isFinite(params.centerY)) {
            return null;
          }
          // Convert the axis value back to an X-equivalent pixel radius, then
          // probe resolution along the X axis (dy = 0) where r_mm = r * pxXmm.
          const radius = pxXmm ? xValue / pxXmm : xValue;
          return getResolutionAtPixel(params.centerX + radius, params.centerY, params);
        }
      : null;
    if (roiLineCanvas) {
      roiLineCanvas._roiPlotMeta = {
        xLabel: pxXmm ? t("roi.size.radius_mm") : t("roi.size.radius_px"),
        yLabel: t("roi.plot.intensity"),
        xStart: displayStart * xStep,
        xStep,
        xTickMode: pxXmm ? "auto" : "integer",
        dSpacingForX: radialDSpacingForX,
        dAxisLabel: t("roi.plot.d_axis"),
        qAxisLabel: t("roi.plot.q_axis"),
        peaks: radialDSpacingForX ? detectProfilePeaks(displayProfile, displayStart * xStep, xStep) : null,
      };
    }
    drawRoiPlot(roiLineCanvas, roiLineCtx, displayProfile);
    drawRoiPlot(roiXCanvas, roiXCtx, null);
    drawRoiPlot(roiYCanvas, roiYCtx, null);
  }
  updateRoiHistogramPlot(statsValues);
  drawRoiOverlay();
  updateRoiSectionState();
}

function drawRoiPlot(canvasEl, ctx, data) {
  const plotKey = getRoiPlotKey(canvasEl);
  renderRoiPlot({
    canvasEl,
    ctx,
    data,
    logScale: getRoiPlotLog(plotKey),
    plotTheme: PLOT_THEME,
    getRoiPlotKey,
    getRoiPlotLimits,
    autoscale: !hasManualRoiPlotLimits(plotKey),
    formatRoiTick,
    resolutionAxisUnit: roiState.resolutionAxisUnit === "q" ? "q" : "d",
  });
}

function redrawRoiPlots() {
  const hasActiveRoi = Boolean(roiState.enabled && roiState.active && roiState.start && roiState.end);
  const showsLineProfile =
    hasActiveRoi &&
    (roiState.mode === "line" || roiState.mode === "circle" || roiState.mode === "annulus");
  const showsBoxProfiles = hasActiveRoi && roiState.mode === "box";
  const showsHistogram = Boolean(
    roiState.enabled &&
    roiState.histogramEnabled &&
    roiState.histogramDistribution &&
    roiState.histogramDistribution.length > 0
  );

  drawRoiPlot(roiLineCanvas, roiLineCtx, showsLineProfile ? roiState.lineProfile : null);
  drawRoiPlot(roiXCanvas, roiXCtx, showsBoxProfiles ? roiState.xProjection : null);
  drawRoiPlot(roiYCanvas, roiYCtx, showsBoxProfiles ? roiState.yProjection : null);
  drawRoiPlot(
    roiHistCanvas,
    roiHistCtx,
    showsHistogram ? roiState.histogramDistribution : null
  );
}

function exportRoiCsv() {
  const unavailable = roiCsvExportUnavailableReason(state, roiState);
  if (unavailable) {
    setStatus(unavailable, { tone: "warning" });
    return;
  }
  const payload = buildRoiCsvExportPayload({
    state,
    roiState,
    lineMeta: roiLineCanvas?._roiPlotMeta,
    xMeta: roiXCanvas?._roiPlotMeta,
    yMeta: roiYCanvas?._roiPlotMeta,
    histMeta: roiHistCanvas?._roiPlotMeta,
  });
  if (!payload) {
    setStatus(t("status.roi.no_projection_data"), { tone: "warning" });
    return;
  }

  const blob = new Blob([payload.content], { type: "text/csv;charset=utf-8" });
  const link = document.createElement("a");
  const url = URL.createObjectURL(blob);
  link.href = url;
  link.download = payload.filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
  setStatus(t("status.roi.exported_csv", { filename: payload.filename }), { tone: "success" });
}

  return {
    updateRoiCenterInputs,
    applyRoiCenterFromInputs,
    getRoiPlotKey,
    getRoiPlotLimits,
    getRoiPlotLog,
    setRoiPlotLog,
    syncRoiPlotLimitControls,
    setRoiPlotAxisLimits,
    clearRoiPlotLimitsForKey,
    hasManualRoiPlotLimits,
    hasAnyManualRoiPlotLimits,
    updateRoiModeUI,
    clearRoi,
    computeGlobalStats,
    updateGlobalStats,
    showRoiTooltip,
    hideRoiTooltip,
    updateRoiTooltip,
    updateRoiStats,
    redrawRoiPlots,
    drawRoiPlot,
    exportRoiCsv,
    roiCsvExportUnavailableReason: () => roiCsvExportUnavailableReason(state, roiState),
  };
}
