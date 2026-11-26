/**
 * ROI plots: per-plot axis limits and log scale, the histogram bins, the hover
 * tooltip, and drawing the line, box-projection and histogram plots.
 */

import {
  buildRoiHistogram as buildRoiHistogramEngine,
  normalizeRoiHistogramBinCount,
  roiHistogramMaxViewBins,
} from "./roi_histogram_engine.js";
import { renderRoiPlot } from "./roi_plot_renderer.js";
import { roiPlotSampleIndexAt } from "./roi_plot_view_utils.js";
import { composeRoiPlotPng, roiPlotPngCaption, roiPlotPngFilename } from "./roi_plot_png_utils.js";
import { t } from "./i18n.js";

export function createRoiPlotController(ctx) {
  const {
    state,
    roiState,
    roiLinePlot,
    roiBoxPlotX,
    roiBoxPlotY,
    setStatus,
    roiLineCanvas,
    roiLineCtx,
    roiXCanvas,
    roiXCtx,
    roiYCanvas,
    roiYCtx,
    roiHistCanvas,
    roiHistCtx,
    roiHistogramPlot,
    roiHistBinsAuto,
    roiHistBinCount,
    roiHistBinChip,
    roiHistBinManualRow,
    roiHistBinPresetBtns,
    formatStat,
    formatRoiTick,
    PLOT_THEME,
  } = ctx;

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
  const idx = roiPlotSampleIndexAt(plot, x);
  const xValue = plot.xStart + idx * plot.xStep;
  const value = plot.data[idx];
  // A histogram bin of several whole values reads as their range.
  const binWidth = canvasEl._roiPlotMeta?.binWidth || 0;
  let xText = plot.xTickMode === "integer" ? `${Math.round(xValue)}` : formatRoiTick(xValue);
  if (binWidth > 1) {
    const first = Math.round(xValue - (binWidth - 1) / 2);
    xText = `${first}–${first + binWidth - 1}`;
  }
  const label = `${plot.xLabel} ${xText}  ${t("roi.tooltip.value")} ${formatStat(value)}`;
  showRoiTooltip(canvasEl, label, event.clientX, event.clientY);
}

// The histogram card sits under the profiles, its title doubling as its on/off
// box: off, it folds to that title row, so the switch is where the plot opens.
// It is hidden only while the ROI tools are off.
function syncRoiHistogramPanel() {
  if (!roiHistogramPlot) return;
  const open = Boolean(roiState.histogramEnabled);
  roiHistogramPlot.classList.toggle("is-hidden", !roiState.enabled);
  roiHistogramPlot.classList.toggle("is-collapsed", !open);
  // A height dragged with the corner handle is inline and would hold the
  // folded card open; folding gives the card back its default height.
  if (!open) roiHistogramPlot.style.removeProperty("height");
}

const PNG_PLOTS = {
  line: () => [roiLinePlot, roiLineCanvas],
  x: () => [roiBoxPlotX, roiXCanvas],
  y: () => [roiBoxPlotY, roiYCanvas],
  hist: () => [roiHistogramPlot, roiHistCanvas],
};

/** Save one plot as a PNG, as it is on screen, with its title and what it was measured on. */
function exportRoiPlotPng(plotKey) {
  const [container, canvas] = PNG_PLOTS[plotKey]?.() || [];
  if (!canvas?._roiPlotMeta) {
    setStatus?.(t("status.roi.no_projection_data"), { tone: "warning" });
    return;
  }
  const rootStyle = window.getComputedStyle(document.documentElement);
  const image = composeRoiPlotPng({
    source: canvas,
    title: container?.querySelector(".roi-plot-title")?.textContent?.trim() || "",
    caption: roiPlotPngCaption(state, roiState),
    dpr: window.devicePixelRatio || 1,
    colors: {
      background: rootStyle.getPropertyValue("--surface-0").trim() || PLOT_THEME.bg,
      plot: PLOT_THEME.bg,
      text: PLOT_THEME.text,
      muted: PLOT_THEME.axis,
    },
    createCanvas: (width, height) => Object.assign(document.createElement("canvas"), { width, height }),
  });
  if (!image) return;
  const filename = roiPlotPngFilename(state, roiState, plotKey);
  image.toBlob((blob) => {
    if (!blob) return;
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = filename;
    link.click();
    URL.revokeObjectURL(link.href);
    setStatus?.(t("status.roi.exported_png", { filename }), { tone: "success" });
  }, "image/png");
}

function buildRoiHistogram(values, levelSample, integral) {
  return buildRoiHistogramEngine(values, {
    ...getRoiHistogramBinsConfig(),
    levelSample,
    integral,
    maxViewBins: roiHistogramMaxViewBins(roiHistCanvas?.clientWidth),
  });
}

// The note over the plot for pixels outside its x range. Whole-number bins
// have their edges between two values, so the edge is turned into the last
// value left out below and the first one above.
function roiHistogramHiddenNote(integral) {
  const value = (edge, toValue) => (integral ? `${Math.round(edge + toValue)}` : formatRoiTick(edge));
  return ({ below, above, belowEdge, aboveEdge }) => ({
    below: below > 0 ? t("roi.histogram.hidden_below", { count: below, value: value(belowEdge, -0.5) }) : "",
    above: above > 0 ? t("roi.histogram.hidden_above", { count: above, value: value(aboveEdge, 0.5) }) : "",
  });
}

/** `levelSample` and `integral` as the level bands have them, so a table of counts is binned once. */
function updateRoiHistogramPlot(values, levelSample = null, integral = undefined) {
  if (!roiHistCanvas || !roiHistCtx || !roiHistogramPlot) return;
  const enabled = Boolean(roiState.enabled && roiState.histogramEnabled);
  syncRoiHistogramPanel();
  if (!enabled) {
    roiState.histogramDistribution = null;
    roiHistCanvas._roiPlotMeta = null;
    drawRoiPlot(roiHistCanvas, roiHistCtx, null);
    return;
  }

  const histogram = buildRoiHistogram(values, levelSample, integral);
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
    binWidth: histogram.binWidth,
    defaultView: histogram.view,
    outsideBelow: histogram.outsideBelow,
    outsideAbove: histogram.outsideAbove,
    hiddenNote: roiHistogramHiddenNote(histogram.xTickMode === "integer"),
  };
  drawRoiPlot(roiHistCanvas, roiHistCtx, roiState.histogramDistribution);
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

  return {
    getRoiPlotKey,
    getRoiPlotLimits,
    getRoiPlotLog,
    setRoiPlotLog,
    setRoiPlotAxisLimits,
    clearRoiPlotLimitsForKey,
    hasManualRoiPlotLimits,
    hasAnyManualRoiPlotLimits,
    showRoiTooltip,
    hideRoiTooltip,
    updateRoiTooltip,
    drawRoiPlot,
    redrawRoiPlots,
    syncRoiHistogramBinsUi,
    syncRoiHistogramPanel,
    updateRoiHistogramPlot,
    exportRoiPlotPng,
  };
}
