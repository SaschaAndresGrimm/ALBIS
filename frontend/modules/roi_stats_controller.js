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
import { buildRoiCsvExportPayload } from "./roi_csv_export.js";
import { t } from "./i18n.js";
import { clampCircularRoiInnerRadius, getCircularRoiOuterRadius } from "./roi_geometry_utils.js";

export function createRoiStatsController(ctx) {
  const {
    state,
    roiState,
    roiCenterXInput,
    roiCenterYInput,
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
    scheduleRoiUpdate,
    updateRoiSectionState,
    drawRoiOverlay,
    getActiveSaturationMax,
    isSaturatedValue,
    computeMedian,
    formatStat,
    formatRoiTick,
    PLOT_THEME,
    setStatus,
  } = ctx;
function setRoiText(el, value) {
  if (!el) return;
  el.textContent = value;
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
    setRoiText(roiSizeEl, state.width && state.height ? `${state.width} × ${state.height}` : "-");
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
    setRoiText(roiSizeEl, formatStat(length));
    updateRoiPixelCounterFields(pixelCounters);
    const std = count > 1 ? Math.sqrt(m2 / (count - 1)) : 0;
    const median = statsValues.length ? computeMedian(statsValues) : Number.NaN;
    setRoiText(roiMinEl, count ? formatStat(min) : "-");
    setRoiText(roiMaxEl, count ? formatStat(max) : "-");
    setRoiText(roiSumEl, count ? formatStat(sum) : "-");
    setRoiText(roiMedianEl, count ? formatStat(median) : "-");
    setRoiText(roiMeanEl, count ? formatStat(mean) : "-");
    setRoiText(roiStdEl, count ? formatStat(std) : "-");
    if (roiLineCanvas) {
      roiLineCanvas._roiPlotMeta = {
        xLabel: t("roi.plot.pixels"),
        yLabel: t("roi.plot.intensity"),
        xStart: 0,
        xStep: 1,
        xTickMode: "integer",
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
    setRoiText(roiSizeEl, `${width} × ${height}`);
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
    const outerRadius = getCircularRoiOuterRadius(roiState);
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

    const left = Math.max(0, Math.floor(x0 - outerRadius));
    const right = Math.min(state.width - 1, Math.ceil(x0 + outerRadius));
    const top = Math.max(0, Math.floor(y0 - outerRadius));
    const bottom = Math.min(state.height - 1, Math.ceil(y0 + outerRadius));
    const innerR2 = roiState.innerRadius * roiState.innerRadius;
    const outerR2 = outerRadius * outerRadius;
    const radialSum = new Float64Array(outerRadius + 1);
    const radialCount = new Uint32Array(outerRadius + 1);

    for (let y = top; y <= bottom; y += 1) {
      const dyPix = y - y0;
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
    if (roiState.mode === "circle") {
      setRoiText(roiSizeEl, `${outerRadius}`);
    } else {
      setRoiText(roiSizeEl, `${roiState.innerRadius} → ${outerRadius}`);
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
    if (roiLineCanvas) {
      roiLineCanvas._roiPlotMeta = {
        xLabel: t("roi.size.radius_px"),
        yLabel: t("roi.plot.intensity"),
        xStart: displayStart,
        xStep: 1,
        xTickMode: "integer",
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
  if (!roiState.enabled || !roiState.active) {
    setStatus(t("status.roi.no_data"));
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
    setStatus(t("status.roi.no_projection_data"));
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
  setStatus(t("status.roi.exported_csv", { filename: payload.filename }));
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
  };
}
