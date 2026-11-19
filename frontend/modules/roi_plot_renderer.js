/**
 * ROI plot renderer kept separate from controller wiring.
 */

import { canvasFont } from "./canvas_fonts.js";
import { pickLinearTicks, pickLogTicks, pickMappedTicks } from "./plot_tick_utils.js";
import { roiPlotXView } from "./roi_plot_view_utils.js";

const D_AXIS_TITLE_FONT = canvasFont(10, 500);
const D_AXIS_TICK_FONT = canvasFont(9, 500);
const PEAK_LABEL_FONT = canvasFont(9, 600);
const PLOT_LABEL_FONT = canvasFont(10, 500);
// Least room between neighbouring ticks: y in px (labels are one line high),
// x in px beyond the widest label.
const Y_TICK_MIN_GAP = 22;
const X_TICK_MIN_GAP = 16;
// Samples at least this far apart get a dot each.
const SAMPLE_DOT_MIN_GAP_PX = 8;
const SAMPLE_DOT_RADIUS = 2;

// Q (nm^-1) = 2*pi / d(nm) = 20*pi / d(Å). Physics/SAXS convention Q = 4*pi*sin(theta)/lambda.
const Q_FROM_D_NM = 20 * Math.PI;

function resolutionDisplayValue(dAngstrom, unit) {
  if (!Number.isFinite(dAngstrom) || dAngstrom <= 0) return Number.NaN;
  return unit === "q" ? Q_FROM_D_NM / dAngstrom : dAngstrom;
}

function formatResolutionLabel(value, unit) {
  if (!Number.isFinite(value) || value <= 0) return "";
  if (value >= 100) return value.toFixed(0);
  if (value >= 10) return value.toFixed(1);
  if (value >= 1) return value.toFixed(2);
  return unit === "q" ? value.toFixed(3) : value.toFixed(2);
}

export function renderRoiPlot({
  canvasEl,
  ctx,
  data,
  logScale,
  plotTheme,
  getRoiPlotKey,
  getRoiPlotLimits,
  autoscale,
  resolutionAxisUnit,
}) {
  const dUnit = resolutionAxisUnit === "q" ? "q" : "d";
  if (!canvasEl || !ctx) return;
  const width = canvasEl.clientWidth || 1;
  const height = canvasEl.clientHeight || 1;
  canvasEl.width = Math.max(1, Math.floor(width * window.devicePixelRatio));
  canvasEl.height = Math.max(1, Math.floor(height * window.devicePixelRatio));
  ctx.setTransform(window.devicePixelRatio, 0, 0, window.devicePixelRatio, 0, 0);
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = plotTheme.bg;
  ctx.fillRect(0, 0, width, height);
  if (!data || data.length === 0) {
    canvasEl._roiPlot = null;
    ctx.strokeStyle = plotTheme.frame;
    ctx.strokeRect(0.5, 0.5, width - 1, height - 1);
    return;
  }

  const plotMeta = canvasEl._roiPlotMeta || {};
  const plotKey = getRoiPlotKey(canvasEl);
  const limits = getRoiPlotLimits(plotKey);
  const xTickMode = plotMeta.xTickMode || "";
  const xStepRaw = Number(plotMeta.xStep ?? 1);
  const xStep = Number.isFinite(xStepRaw) && xStepRaw !== 0 ? xStepRaw : 1;
  let xStart = Number(plotMeta.xStart ?? 0) || 0;
  const totalMinX = xStart;
  const totalMaxX = xStart + (data.length - 1) * xStep;

  // Optional secondary d-spacing axis + detected-feature markers.
  const dSpacingForX = typeof plotMeta.dSpacingForX === "function" ? plotMeta.dSpacingForX : null;
  const profilePeaks = Array.isArray(plotMeta.peaks) ? plotMeta.peaks : null;
  let hasDAxis = false;
  if (dSpacingForX) {
    for (let i = 0; i <= 4; i += 1) {
      const probeX = totalMinX + (i / 4) * (totalMaxX - totalMinX);
      const probeD = dSpacingForX(probeX);
      if (Number.isFinite(probeD) && probeD > 0) {
        hasDAxis = true;
        break;
      }
    }
  }
  const seriesType = plotMeta.seriesType || "line";
  const isHistogramSeries = seriesType === "histogram";
  // The x range asked for, exactly (roi_plot_view_utils.js says why), with
  // one sample past each end so the curve runs to the frame.
  const view = roiPlotXView({
    length: data.length,
    xStart,
    xStep,
    histogram: isHistogramSeries,
    limits,
    autoscale,
  });
  const visibleData = view.lastIdx >= view.firstIdx ? data.slice(view.firstIdx, view.lastIdx + 1) : [];
  xStart += view.firstIdx * xStep;
  const viewMin = view.min;
  const viewMax = view.max;
  const inViewFrom = view.yFirstIdx - view.firstIdx;
  const inViewTo = view.yLastIdx - view.firstIdx;

  if (!visibleData || visibleData.length === 0) {
    canvasEl._roiPlot = null;
    ctx.strokeStyle = plotTheme.frame;
    ctx.strokeRect(0.5, 0.5, width - 1, height - 1);
    return;
  }

  const values = visibleData.map((value) => {
    if (!Number.isFinite(value)) return Number.NaN;
    return logScale ? Math.log10(1 + Math.max(0, value)) : value;
  });
  const finiteValues = values.slice(inViewFrom, inViewTo + 1).filter((value) => Number.isFinite(value));

  const allValuesRaw = data.filter((value) => Number.isFinite(value));
  const totalMinY = isHistogramSeries || !allValuesRaw.length ? 0 : Math.min(...allValuesRaw);
  const totalMaxY = allValuesRaw.length ? Math.max(...allValuesRaw) : 0;

  let minValue = finiteValues.length ? Math.min(...finiteValues) : 0;
  if (!autoscale && Number.isFinite(limits.yMin)) {
    minValue = logScale ? Math.log10(1 + Math.max(0, limits.yMin)) : limits.yMin;
  }
  let maxValue = finiteValues.length ? Math.max(...finiteValues) : minValue + 1;
  if (!autoscale && Number.isFinite(limits.yMax)) {
    maxValue = logScale ? Math.log10(1 + Math.max(0, limits.yMax)) : limits.yMax;
  }
  if (!Number.isFinite(minValue)) minValue = 0;
  // Headroom whenever y follows the data, also with x pinned (zoomed in on
  // x alone): without it the highest and lowest samples, and their dots,
  // were cut in half by the frame.
  const yFollowsData = autoscale || (!Number.isFinite(limits.yMin) && !Number.isFinite(limits.yMax));
  if (yFollowsData && Number.isFinite(minValue) && Number.isFinite(maxValue)) {
    // 3% of headroom so the extremes are not drawn on the frame.
    const baseRange = maxValue - minValue;
    if (baseRange > 0) {
      const unpaddedMin = minValue;
      const pad = baseRange * 0.03;
      minValue -= pad;
      maxValue += pad;
      // Padding below zero invents an axis region the quantity cannot occupy:
      // a profile of photon counts whose minimum is 0.18 was labelled down to
      // -1.9, which reads as the baseline going negative. Clamp only when the
      // data itself is non-negative -- a PILATUS frame writes -1 for a module
      // gap and -2 for a bad pixel, and an ROI covering those has a genuinely
      // negative minimum that the axis must still show.
      if (logScale || unpaddedMin >= 0) {
        minValue = Math.max(0, minValue);
      }
    }
  }
  if (isHistogramSeries) {
    minValue = Math.max(0, minValue);
    maxValue = Math.max(0, maxValue);
  }
  if (!Number.isFinite(maxValue) || maxValue <= minValue) {
    maxValue = minValue + 1;
  }

  const yRange = maxValue - minValue;
  const padR = 8;
  const padT = hasDAxis ? 34 : 8;
  const padB = 30;
  const drawableHeight = Math.max(4, height - padT - padB);

  ctx.fillStyle = plotTheme.text;
  ctx.font = PLOT_LABEL_FONT;
  const measureMaxLabel = (labels) =>
    labels.reduce((currentMax, label) => Math.max(currentMax, ctx.measureText(label).width), 0);

  // Round-number ticks (plot_tick_utils.js); the axis still spans the data.
  const yTicks = logScale
    ? pickLogTicks({ min: minValue, max: maxValue, lengthPx: drawableHeight, minGapPx: Y_TICK_MIN_GAP })
    : pickLinearTicks({ min: minValue, max: maxValue, lengthPx: drawableHeight, minGapPx: Y_TICK_MIN_GAP });
  const yTickLabels = yTicks.labels;
  const yTickPositions = yTicks.values.map((value) => {
    const display = logScale ? Math.log10(1 + Math.max(0, value)) : value;
    return padT + drawableHeight - ((display - minValue) / yRange) * drawableHeight;
  });
  const maxYLabelWidth = measureMaxLabel(yTickLabels);
  const padL = Math.max(40, Math.ceil(maxYLabelWidth + 24));
  const drawableWidth = Math.max(4, width - padL - padR);

  const xSpan = viewMax - viewMin;
  const xToPx = (xValue) => padL + (xSpan ? (xValue - viewMin) / xSpan : 0) * drawableWidth;
  const sampleX = (index) => xStart + index * xStep;
  const xTicks = pickLinearTicks({
    min: viewMin,
    max: viewMax,
    lengthPx: drawableWidth,
    minGapPx: X_TICK_MIN_GAP,
    measure: (label) => ctx.measureText(label).width,
    integer: xTickMode === "integer",
  });

  // The axis strip under the pointer, which a drag pans on its own
  // (roi_plot_bindings.js sets data-pan-zone).
  const axisZone = canvasEl.dataset?.panZone;
  if (axisZone === "x" || axisZone === "y") {
    ctx.fillStyle = plotTheme.axisHover;
    if (axisZone === "x") {
      ctx.fillRect(padL, padT + drawableHeight, drawableWidth, height - padT - drawableHeight);
    } else {
      ctx.fillRect(0, padT, padL, drawableHeight);
    }
    ctx.fillStyle = plotTheme.text;
  }

  ctx.strokeStyle = plotTheme.axis;
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(padL, padT);
  ctx.lineTo(padL, padT + drawableHeight);
  ctx.lineTo(padL + drawableWidth, padT + drawableHeight);
  ctx.stroke();

  ctx.textAlign = "center";
  ctx.textBaseline = "top";
  let lastXLabelRight = -Infinity;
  xTicks.values.forEach((xValue, i) => {
    const x = xToPx(xValue);
    ctx.strokeStyle = plotTheme.grid;
    ctx.beginPath();
    ctx.moveTo(x, padT);
    ctx.lineTo(x, padT + drawableHeight);
    ctx.stroke();
    ctx.strokeStyle = plotTheme.axis;
    ctx.beginPath();
    ctx.moveTo(x, padT + drawableHeight);
    ctx.lineTo(x, padT + drawableHeight + 4);
    ctx.stroke();
    // A label at the edge moves inside the canvas instead of being cut off,
    // and is left out if that would run it into its neighbour.
    const label = xTicks.labels[i];
    const half = ctx.measureText(label).width / 2;
    const labelX = Math.min(Math.max(x, half + 2), width - half - 2);
    if (labelX - half < lastXLabelRight + 4) return;
    ctx.fillText(label, labelX, padT + drawableHeight + 6);
    lastXLabelRight = labelX + half;
  });

  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  for (let i = 0; i < yTickPositions.length; i += 1) {
    const y = yTickPositions[i];
    ctx.strokeStyle = plotTheme.grid;
    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(padL + drawableWidth, y);
    ctx.stroke();
    ctx.strokeStyle = plotTheme.axis;
    ctx.beginPath();
    ctx.moveTo(padL - 4, y);
    ctx.lineTo(padL, y);
    ctx.stroke();
    ctx.fillText(yTickLabels[i] || "", padL - 8, y);
  }

  if (hasDAxis) {
    // Match the primary axes: axis/tick lines in plotTheme.axis, labels in plotTheme.text.
    ctx.strokeStyle = plotTheme.axis;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(padL, padT);
    ctx.lineTo(padL + drawableWidth, padT);
    ctx.stroke();
    ctx.font = D_AXIS_TICK_FONT;
    ctx.fillStyle = plotTheme.text;
    ctx.textAlign = "center";
    ctx.textBaseline = "bottom";
    // d and Q are not linear in x, so the ticks go where the curve crosses
    // a round value rather than above the x ticks.
    const samples = [];
    for (let px = 0; px <= drawableWidth; px += 2) {
      const xValue = viewMin + (px / drawableWidth) * xSpan;
      samples.push({ px: padL + px, value: resolutionDisplayValue(dSpacingForX(xValue), dUnit) });
    }
    const dTicks = pickMappedTicks({
      samples,
      measure: (label) => ctx.measureText(label).width,
      minGapPx: 14,
      uniform: dUnit === "q",
      leftPx: 2,
      rightPx: width - 2,
    });
    for (const tick of dTicks) {
      ctx.strokeStyle = plotTheme.axis;
      ctx.beginPath();
      ctx.moveTo(tick.px, padT);
      ctx.lineTo(tick.px, padT - 4);
      ctx.stroke();
      ctx.fillText(tick.label, tick.px, padT - 6);
    }
    // Axis title centered above the axis, mirroring the bottom xLabel styling.
    const dAxisTitle = dUnit === "q"
      ? (plotMeta.qAxisLabel || "Q (1/nm)")
      : (plotMeta.dAxisLabel || "d (Å)");
    ctx.font = D_AXIS_TITLE_FONT;
    ctx.fillStyle = plotTheme.text;
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    ctx.fillText(dAxisTitle, padL + drawableWidth / 2, 2);
  }

  // The samples just outside the view are drawn too, so the curve meets the
  // frame; the clip keeps them off the axes.
  ctx.save();
  ctx.beginPath();
  ctx.rect(padL, padT, drawableWidth, drawableHeight);
  ctx.clip();

  if (isHistogramSeries) {
    // A bar spans its bin: the x values are bin centres.
    const halfBin = Math.abs(xStep) / 2;
    ctx.fillStyle = "rgba(198, 220, 255, 0.88)";
    values.forEach((value, index) => {
      if (!Number.isFinite(value)) return;
      const yNorm = yRange ? (value - minValue) / yRange : 0;
      const y = padT + drawableHeight - Math.max(0, Math.min(1, yNorm)) * drawableHeight;
      const left = xToPx(sampleX(index) - halfBin);
      const right = xToPx(sampleX(index) + halfBin);
      const w = Math.max(1, Math.abs(right - left) - 1);
      const h = Math.max(1, padT + drawableHeight - y);
      ctx.fillRect(Math.min(left, right) + 0.5, y, w, h);
    });
  } else {
    ctx.strokeStyle = plotTheme.line;
    ctx.lineWidth = 1;
    ctx.shadowColor = plotTheme.lineGlow;
    ctx.shadowBlur = 2;
    ctx.beginPath();
    let hasSegment = false;
    values.forEach((value, index) => {
      if (!Number.isFinite(value)) {
        hasSegment = false;
        return;
      }
      const x = xToPx(sampleX(index));
      // Not clamped to the frame: a sample above or below the view would
      // otherwise bend the line along the edge. The clip cuts it off there.
      const yNorm = yRange ? (value - minValue) / yRange : 0;
      const y = padT + drawableHeight - yNorm * drawableHeight;
      if (!hasSegment) {
        ctx.moveTo(x, y);
        hasSegment = true;
      } else {
        ctx.lineTo(x, y);
      }
    });
    ctx.stroke();
    ctx.shadowBlur = 0;

    // Zoomed in far, mark each sample: on a detector profile it shows which
    // pixels are there, and that the line between them is no data.
    if (xSpan && Math.abs((xStep / xSpan) * drawableWidth) >= SAMPLE_DOT_MIN_GAP_PX) {
      ctx.fillStyle = plotTheme.line;
      values.forEach((value, index) => {
        if (!Number.isFinite(value)) return;
        const yNorm = yRange ? (value - minValue) / yRange : 0;
        ctx.beginPath();
        ctx.arc(xToPx(sampleX(index)), padT + drawableHeight - yNorm * drawableHeight, SAMPLE_DOT_RADIUS, 0, Math.PI * 2);
        ctx.fill();
      });
    }
  }
  ctx.restore();

  if (profilePeaks && profilePeaks.length) {
    const peakColor = plotTheme.peak || plotTheme.line;
    let lastLabelRight = -Infinity;
    profilePeaks.forEach((peak) => {
      if (!peak || !Number.isFinite(peak.x) || !Number.isFinite(peak.value)) return;
      if (peak.x < Math.min(viewMin, viewMax) || peak.x > Math.max(viewMin, viewMax)) return;
      const x = xToPx(peak.x);
      const disp = logScale ? Math.log10(1 + Math.max(0, peak.value)) : peak.value;
      const yNorm = yRange ? (disp - minValue) / yRange : 0;
      const y = padT + drawableHeight - Math.max(0, Math.min(1, yNorm)) * drawableHeight;
      ctx.fillStyle = peakColor;
      ctx.beginPath();
      ctx.arc(x, y, 2.5, 0, Math.PI * 2);
      ctx.fill();
      if (!dSpacingForX) return;
      const d = dSpacingForX(peak.x);
      const label = formatResolutionLabel(resolutionDisplayValue(d, dUnit), dUnit);
      if (!label) return;
      ctx.font = PEAK_LABEL_FONT;
      ctx.textAlign = "center";
      ctx.textBaseline = "bottom";
      const labelWidth = ctx.measureText(label).width;
      // Skip labels that would collide with the previous one.
      if (x - labelWidth / 2 < lastLabelRight + 2) return;
      const labelY = Math.max(padT + 10, y - 5);
      ctx.fillStyle = peakColor;
      ctx.fillText(label, x, labelY);
      lastLabelRight = x + labelWidth / 2;
    });
  }

  const xLabel = plotMeta.xLabel || "Index";
  const yLabel = plotMeta.yLabel || "Value";
  const xMinActual = viewMin;
  const xMaxActual = viewMax;
  const yMinActual = logScale ? Math.max(0, Math.pow(10, minValue) - 1) : minValue;
  const yMaxActual = logScale ? Math.max(0, Math.pow(10, maxValue) - 1) : maxValue;
  ctx.fillStyle = plotTheme.text;
  ctx.font = PLOT_LABEL_FONT;
  ctx.textAlign = "center";
  ctx.fillText(xLabel, padL + drawableWidth / 2, height - 4);
  ctx.save();
  ctx.translate(Math.max(8, padL - maxYLabelWidth - 16), padT + drawableHeight / 2);
  ctx.rotate(-Math.PI / 2);
  ctx.fillText(yLabel, 0, 0);
  ctx.restore();

  canvasEl._roiPlot = {
    data: visibleData,
    log: logScale,
    xLabel,
    yLabel,
    padL,
    padR,
    padT,
    padB,
    width,
    height,
    xStart,
    xStep,
    xTickMode,
    totalXMin: view.domainMin,
    totalXMax: view.domainMax,
    totalYMin: totalMinY,
    totalYMax: totalMaxY,
    xMin: xMinActual,
    xMax: xMaxActual,
    yMin: yMinActual,
    yMax: yMaxActual,
  };

  ctx.strokeStyle = plotTheme.frame;
  ctx.strokeRect(0.5, 0.5, width - 1, height - 1);
}
