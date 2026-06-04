/**
 * ROI plot renderer kept separate from controller wiring.
 */

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
}) {
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
  const padT = 8;
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
