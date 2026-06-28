/**
 * ROI plot renderer kept separate from controller wiring.
 */

const D_AXIS_TITLE_FONT = '500 10px "Avenir Next", "Segoe UI", "Helvetica Neue", Arial, sans-serif';
const D_AXIS_TICK_FONT = '500 9px "Avenir Next", "Segoe UI", "Helvetica Neue", Arial, sans-serif';
const PEAK_LABEL_FONT = '600 9px "Avenir Next", "Segoe UI", "Helvetica Neue", Arial, sans-serif';

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
  formatRoiTick,
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
  const formatXAxisTick = (value) => {
    if (!Number.isFinite(value)) return "-";
    if (xTickMode === "integer") return `${Math.round(value)}`;
    return formatRoiTick(value);
  };
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
  let visibleData = data;
  if (!autoscale && data.length > 0) {
    const lo = Number.isFinite(limits.xMin) ? Math.max(totalMinX, limits.xMin) : totalMinX;
    const hi = Number.isFinite(limits.xMax) ? Math.min(totalMaxX, limits.xMax) : totalMaxX;
    if (hi < lo) {
      visibleData = [];
    } else {
      const firstIdx = Math.max(0, Math.ceil((lo - xStart) / xStep));
      const lastIdx = Math.min(data.length - 1, Math.floor((hi - xStart) / xStep));
      if (lastIdx >= firstIdx) {
        visibleData = data.slice(firstIdx, lastIdx + 1);
        xStart += firstIdx * xStep;
      } else {
        visibleData = [];
      }
    }
  }

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
  const finiteValues = values.filter((value) => Number.isFinite(value));
  const seriesType = plotMeta.seriesType || "line";
  const isHistogramSeries = seriesType === "histogram";

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
  if (autoscale && Number.isFinite(minValue) && Number.isFinite(maxValue)) {
    const baseRange = maxValue - minValue;
    if (baseRange > 0) {
      const pad = baseRange * 0.03;
      minValue -= pad;
      maxValue += pad;
      if (logScale) {
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
  ctx.font = '500 10px "Avenir Next", "Segoe UI", "Helvetica Neue", Arial, sans-serif';
  const measureMaxLabel = (labels) =>
    labels.reduce((currentMax, label) => Math.max(currentMax, ctx.measureText(label).width), 0);

  let yTickCount = Math.max(2, Math.min(4, Math.floor(drawableHeight / 50)));
  while (yTickCount > 1) {
    const labels = [];
    for (let i = 0; i <= yTickCount; i += 1) {
      const t = i / yTickCount;
      const displayValue = minValue + t * yRange;
      const actualValue = logScale ? Math.pow(10, displayValue) - 1 : displayValue;
      labels.push(formatRoiTick(actualValue));
    }
    const maxLabel = measureMaxLabel(labels);
    const spacing = drawableHeight / yTickCount;
    if (maxLabel + 6 <= spacing) break;
    yTickCount -= 1;
  }

  const yTickLabels = [];
  for (let i = 0; i <= yTickCount; i += 1) {
    const t = i / yTickCount;
    const displayValue = minValue + t * yRange;
    const actualValue = logScale ? Math.pow(10, displayValue) - 1 : displayValue;
    yTickLabels.push(formatRoiTick(actualValue));
  }
  const maxYLabelWidth = measureMaxLabel(yTickLabels);
  const padL = Math.max(40, Math.ceil(maxYLabelWidth + 24));
  const drawableWidth = Math.max(4, width - padL - padR);

  let xTickCount = Math.max(2, Math.min(4, Math.floor(drawableWidth / 90)));
  while (xTickCount > 1) {
    const labels = [];
    for (let i = 0; i <= xTickCount; i += 1) {
      const t = i / xTickCount;
      const xValue = xStart + t * (values.length - 1) * xStep;
      labels.push(formatXAxisTick(xValue));
    }
    const maxLabel = measureMaxLabel(labels);
    const spacing = drawableWidth / xTickCount;
    if (maxLabel + 6 <= spacing) break;
    xTickCount -= 1;
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
  for (let i = 0; i <= xTickCount; i += 1) {
    const t = i / xTickCount;
    const x = padL + t * drawableWidth;
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
    const xValue = xStart + t * (values.length - 1) * xStep;
    ctx.fillText(formatXAxisTick(xValue), x, padT + drawableHeight + 6);
  }

  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  for (let i = 0; i <= yTickCount; i += 1) {
    const t = i / yTickCount;
    const y = padT + drawableHeight - t * drawableHeight;
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
    for (let i = 0; i <= xTickCount; i += 1) {
      const t = i / xTickCount;
      const x = padL + t * drawableWidth;
      const xValue = xStart + t * (values.length - 1) * xStep;
      const d = dSpacingForX(xValue);
      const displayValue = resolutionDisplayValue(d, dUnit);
      if (!Number.isFinite(displayValue)) continue;
      ctx.strokeStyle = plotTheme.axis;
      ctx.beginPath();
      ctx.moveTo(x, padT);
      ctx.lineTo(x, padT - 4);
      ctx.stroke();
      ctx.fillText(formatResolutionLabel(displayValue, dUnit), x, padT - 6);
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

  if (seriesType === "histogram") {
    const barWidth = drawableWidth / Math.max(1, values.length);
    ctx.fillStyle = "rgba(198, 220, 255, 0.88)";
    values.forEach((value, index) => {
      if (!Number.isFinite(value)) return;
      const yNorm = yRange ? (value - minValue) / yRange : 0;
      const y = padT + drawableHeight - Math.max(0, Math.min(1, yNorm)) * drawableHeight;
      const x = padL + index * barWidth;
      const w = Math.max(1, barWidth - 1);
      const h = Math.max(1, padT + drawableHeight - y);
      ctx.fillRect(x + 0.5, y, w, h);
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
      const x = padL + (index / Math.max(1, values.length - 1)) * drawableWidth;
      const yNorm = yRange ? (value - minValue) / yRange : 0;
      const y = padT + drawableHeight - Math.max(0, Math.min(1, yNorm)) * drawableHeight;
      if (!hasSegment) {
        ctx.moveTo(x, y);
        hasSegment = true;
      } else {
        ctx.lineTo(x, y);
      }
    });
    ctx.stroke();
    ctx.shadowBlur = 0;
  }

  if (profilePeaks && profilePeaks.length) {
    const peakColor = plotTheme.peak || plotTheme.line;
    const lastValueIndex = Math.max(1, values.length - 1);
    let lastLabelRight = -Infinity;
    profilePeaks.forEach((peak) => {
      if (!peak || !Number.isFinite(peak.x) || !Number.isFinite(peak.value)) return;
      const idx = (peak.x - xStart) / xStep;
      if (idx < 0 || idx > values.length - 1) return;
      const x = padL + (idx / lastValueIndex) * drawableWidth;
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
  const xMinActual = xStart;
  const xMaxActual = xStart + (values.length - 1) * xStep;
  const yMinActual = logScale ? Math.max(0, Math.pow(10, minValue) - 1) : minValue;
  const yMaxActual = logScale ? Math.max(0, Math.pow(10, maxValue) - 1) : maxValue;
  ctx.fillStyle = plotTheme.text;
  ctx.font = '500 10px "Avenir Next", "Segoe UI", "Helvetica Neue", Arial, sans-serif';
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
    totalXMin: totalMinX,
    totalXMax: totalMaxX,
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
