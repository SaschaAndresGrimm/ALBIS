/**
 * ROI statistics, plots, and ROI plot interaction helpers.
 */

export function createRoiStatsController(ctx) {
  const {
    state,
    roiState,
    roiCenterXInput,
    roiCenterYInput,
    roiLimitsEnable,
    roiParams,
    roiLinePlot,
    roiBoxPlotX,
    roiBoxPlotY,
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
  const x = Math.max(0, Math.min(state.width - 1, Math.round(xVal)));
  const y = Math.max(0, Math.min(state.height - 1, Math.round(yVal)));
  return { x, y };
}

function getRoiPlotKey(canvasEl) {
  const id = canvasEl?.id || "";
  if (id === "roi-line-canvas") return "line";
  if (id === "roi-x-canvas") return "x";
  if (id === "roi-y-canvas") return "y";
  return "line";
}

function getRoiPlotLimits(plotKey) {
  return roiState.plotLimits[plotKey] || roiState.plotLimits.line;
}

function clearRoiPlotLimits() {
  ["line", "x", "y"].forEach((key) => {
    const limits = roiState.plotLimits[key];
    if (!limits) return;
    limits.xMin = null;
    limits.xMax = null;
    limits.yMin = null;
    limits.yMax = null;
  });
}

function syncRoiPlotLimitControls() {
  if (roiLimitsEnable) {
    roiLimitsEnable.checked = roiState.plotLimits.autoscale;
  }
}

function updateRoiPlotLimitsEnabled() {
  roiState.plotLimits.autoscale = Boolean(roiLimitsEnable?.checked);
  if (roiState.plotLimits.autoscale) {
    clearRoiPlotLimits();
  }
  syncRoiPlotLimitControls();
  scheduleRoiUpdate();
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
  return ["line", "x", "y"].some((key) => hasManualRoiPlotLimits(key));
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
      title.textContent = mode === "line" ? "Line Profile" : "Radial Profile";
    }
  }
  if (roiBoxPlotX) {
    roiBoxPlotX.classList.toggle("is-hidden", !enabled || mode !== "box");
  }
  if (roiBoxPlotY) {
    roiBoxPlotY.classList.toggle("is-hidden", !enabled || mode !== "box");
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
      roiSizeLabel.textContent = "Image";
    } else if (mode === "line") {
      roiSizeLabel.textContent = "Length (px)";
    } else if (mode === "box") {
      roiSizeLabel.textContent = "Size (WxH)";
    } else if (mode === "circle") {
      roiSizeLabel.textContent = "Radius (px)";
    } else if (mode === "annulus") {
      roiSizeLabel.textContent = "Rin → Rout";
    } else {
      roiSizeLabel.textContent = "Image";
    }
  }
  if (roiHelp) {
    if (!enabled) {
      roiHelp.textContent = "Enable Statistics and ROI to define a region.";
    } else if (mode === "annulus") {
      roiHelp.textContent = "Right‑drag to set outer radius. Adjust inner radius below.";
    } else if (mode === "circle") {
      roiHelp.textContent = "Right‑drag from center to set radius.";
    } else {
      roiHelp.textContent = "Right‑drag on the image to define the ROI.";
    }
  }
  if (roiModeSelect) {
    roiModeSelect.disabled = !enabled;
  }
  if (roiClearBtn) {
    roiClearBtn.disabled = !enabled;
  }
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
  drawRoiPlot(roiLineCanvas, roiLineCtx, null, roiState.log);
  drawRoiPlot(roiXCanvas, roiXCtx, null, roiState.log);
  drawRoiPlot(roiYCanvas, roiYCtx, null, roiState.log);
  drawRoiOverlay();
  updateRoiSectionState();
}

function applyMaskToValue(value, maskValue, satMax = getActiveSaturationMax()) {
  if (Number.isFinite(maskValue)) {
    if (maskValue & 1) {
      return { value: 0, skip: true };
    }
    if (maskValue & 0x1e) {
      return { value: 0, skip: true };
    }
  }
  if (state.maskSaturatedEnabled && isSaturatedValue(value, satMax)) {
    return { value: 0, skip: true };
  }
  return { value, skip: false };
}

function getMaskFlags(maskValue) {
  if (!Number.isFinite(maskValue)) {
    return { gap: false, defective: false };
  }
  return {
    gap: Boolean(maskValue & 1),
    defective: Boolean(maskValue & 0x1e),
  };
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
  if (!state.dataRaw) return null;
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
  const satMax = getActiveSaturationMax();
  const hasMask =
    state.maskAvailable &&
    state.maskRaw &&
    state.maskShape &&
    state.maskShape[0] === state.height &&
    state.maskShape[1] === state.width;
  const useMasking = (state.maskEnabled && hasMask) || state.maskSaturatedEnabled;

  for (let i = 0; i < state.dataRaw.length; i += 1) {
    let v = state.dataRaw[i];
    const maskValue = hasMask ? state.maskRaw[i] : null;
    const flags = getMaskFlags(maskValue);
    if (flags.gap) {
      gapPixels += 1;
    } else if (flags.defective) {
      defectivePixels += 1;
    }
    if (satMax !== null && isSaturatedValue(v, satMax) && !flags.gap && !flags.defective) {
      saturatedPixels += 1;
    }
    if (!Number.isFinite(v)) continue;
    if (useMasking) {
      const masked = applyMaskToValue(v, maskValue, satMax);
      if (masked.skip) continue;
      v = masked.value;
    }
    count += 1;
    sum += v;
    samples.push(v);
    min = Math.min(min, v);
    max = Math.max(max, v);
    const delta = v - mean;
    mean += delta / count;
    m2 += delta * (v - mean);
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
      totalPixels: state.dataRaw.length,
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
    totalPixels: state.dataRaw.length,
    gapPixels,
    defectivePixels,
    saturatedPixels,
  };
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
  const t = plotW ? plotX / plotW : 0;
  const idx = Math.max(0, Math.min(plot.data.length - 1, Math.round(t * (plot.data.length - 1))));
  const xValue = plot.xStart + idx * plot.xStep;
  const value = plot.data[idx];
  const xText = plot.xTickMode === "integer" ? `${Math.round(xValue)}` : formatRoiTick(xValue);
  const label = `${plot.xLabel} ${xText}  Value ${formatStat(value)}`;
  showRoiTooltip(canvasEl, label, event.clientX, event.clientY);
}

function createRoiPixelCounters() {
  return { total: 0, gap: 0, defective: 0, saturated: 0 };
}

function accumulateRoiPixelCounters(counters, sampled, satMax) {
  if (!counters || !sampled) return;
  counters.total += 1;
  const flags = getMaskFlags(sampled.maskValue);
  if (flags.gap) {
    counters.gap += 1;
  } else if (flags.defective) {
    counters.defective += 1;
  }
  if (
    satMax !== null &&
    isSaturatedValue(sampled.raw, satMax) &&
    !flags.gap &&
    !flags.defective
  ) {
    counters.saturated += 1;
  }
}

function updateRoiPixelCounterFields(counters) {
  setRoiText(roiTotalEl, counters ? `${counters.total}` : "-");
  setRoiText(roiGapEl, counters ? `${counters.gap}` : "-");
  setRoiText(roiDefectiveEl, counters ? `${counters.defective}` : "-");
  setRoiText(roiSaturatedEl, counters ? `${counters.saturated}` : "-");
}

function updateRoiStats() {
  // This function is intentionally central: it computes ROI statistics and
  // updates all derived plots/labels in one pass to keep UI state consistent.
  if (!state.hasFrame) {
    if (roiState.active) {
      clearRoi();
    }
    updateRoiSectionState();
    return;
  }
  if (!roiState.enabled) {
    roiState.active = false;
    const stats = state.globalStats || computeGlobalStats();
    setRoiText(roiStartEl, "-");
    setRoiText(roiEndEl, "-");
    if (roiSizeLabel) {
      roiSizeLabel.textContent = "Image";
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
    drawRoiPlot(roiLineCanvas, roiLineCtx, null, roiState.log);
    drawRoiPlot(roiXCanvas, roiXCtx, null, roiState.log);
    drawRoiPlot(roiYCanvas, roiYCtx, null, roiState.log);
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
    drawRoiPlot(roiLineCanvas, roiLineCtx, null, roiState.log);
    drawRoiPlot(roiXCanvas, roiXCtx, null, roiState.log);
    drawRoiPlot(roiYCanvas, roiYCtx, null, roiState.log);
    drawRoiOverlay();
    updateRoiSectionState();
    return;
  }
  const x0 = Math.max(0, Math.min(state.width - 1, roiState.start.x));
  const y0 = Math.max(0, Math.min(state.height - 1, roiState.start.y));
  const x1 = Math.max(0, Math.min(state.width - 1, roiState.end.x));
  const y1 = Math.max(0, Math.min(state.height - 1, roiState.end.y));
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
        xLabel: "Pixels",
        yLabel: "Intensity",
        xStart: 0,
        xStep: 1,
        xTickMode: "integer",
      };
    }
    drawRoiPlot(roiLineCanvas, roiLineCtx, values, roiState.log);
    drawRoiPlot(roiXCanvas, roiXCtx, null, roiState.log);
    drawRoiPlot(roiYCanvas, roiYCtx, null, roiState.log);
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
    drawRoiPlot(roiLineCanvas, roiLineCtx, null, roiState.log);
    if (roiXCanvas) {
      roiXCanvas._roiPlotMeta = {
        xLabel: "X Pixel",
        yLabel: "Mean",
        xStart: left,
        xStep: 1,
        xTickMode: "integer",
      };
    }
    if (roiYCanvas) {
      roiYCanvas._roiPlotMeta = {
        xLabel: "Y Pixel",
        yLabel: "Mean",
        xStart: top,
        xStep: 1,
        xTickMode: "integer",
      };
    }
    drawRoiPlot(roiXCanvas, roiXCtx, roiState.xProjection, roiState.log);
    drawRoiPlot(roiYCanvas, roiYCtx, roiState.yProjection, roiState.log);
  } else if (roiState.mode === "circle" || roiState.mode === "annulus") {
    const dx = x1 - x0;
    const dy = y1 - y0;
    const outerRadius = Math.max(1, Math.round(Math.hypot(dx, dy)));
    if (roiState.mode === "circle") {
      roiState.innerRadius = 0;
      roiState.outerRadius = outerRadius;
      if (roiRadiusInput) roiRadiusInput.value = String(outerRadius);
    } else {
      roiState.outerRadius = outerRadius;
      let inner = Math.max(0, Math.round(roiState.innerRadius || 0));
      if (!inner || inner >= outerRadius) {
        inner = Math.max(0, Math.round(outerRadius * 0.5));
      }
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
        xLabel: "Radius (px)",
        yLabel: "Intensity",
        xStart: displayStart,
        xStep: 1,
        xTickMode: "integer",
      };
    }
    drawRoiPlot(roiLineCanvas, roiLineCtx, displayProfile, roiState.log);
    drawRoiPlot(roiXCanvas, roiXCtx, null, roiState.log);
    drawRoiPlot(roiYCanvas, roiYCtx, null, roiState.log);
  }
  drawRoiOverlay();
  updateRoiSectionState();
}

function drawRoiPlot(canvasEl, ctx, data, logScale) {
  if (!canvasEl || !ctx) return;
  const width = canvasEl.clientWidth || 1;
  const height = canvasEl.clientHeight || 1;
  canvasEl.width = Math.max(1, Math.floor(width * window.devicePixelRatio));
  canvasEl.height = Math.max(1, Math.floor(height * window.devicePixelRatio));
  ctx.setTransform(window.devicePixelRatio, 0, 0, window.devicePixelRatio, 0, 0);
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = PLOT_THEME.bg;
  ctx.fillRect(0, 0, width, height);
  if (!data || data.length === 0) {
    canvasEl._roiPlot = null;
    ctx.strokeStyle = PLOT_THEME.frame;
    ctx.strokeRect(0.5, 0.5, width - 1, height - 1);
    return;
  }
  const plotMeta = canvasEl._roiPlotMeta || {};
  const plotKey = getRoiPlotKey(canvasEl);
  const limits = getRoiPlotLimits(plotKey);
  const autoscale = roiState.plotLimits.autoscale;
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
    ctx.strokeStyle = PLOT_THEME.frame;
    ctx.strokeRect(0.5, 0.5, width - 1, height - 1);
    return;
  }

  const valuesRaw = visibleData;
  const values = valuesRaw.map((v) => {
    if (!Number.isFinite(v)) return Number.NaN;
    return logScale ? Math.log10(1 + Math.max(0, v)) : v;
  });
  const finiteValues = values.filter((v) => Number.isFinite(v));

  // Calculate total Y domain from all data
  const allValuesRaw = data.filter((v) => Number.isFinite(v));
  const totalMinY = allValuesRaw.length ? Math.min(...allValuesRaw) : 0;
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
  if (!Number.isFinite(maxValue) || maxValue <= minValue) {
    maxValue = minValue + 1;
  }
  const yRange = maxValue - minValue;
  const padR = 8;
  const padT = 8;
  const padB = 30;
  const drawableHeight = Math.max(4, height - padT - padB);

  ctx.fillStyle = PLOT_THEME.text;
  ctx.font = '500 10px "Avenir Next", "Segoe UI", "Helvetica Neue", Arial, sans-serif';

  const measureMaxLabel = (labels) =>
    labels.reduce((max, label) => Math.max(max, ctx.measureText(label).width), 0);

  let yTickCount = Math.max(2, Math.min(4, Math.floor(drawableHeight / 50)));
  while (yTickCount > 1) {
    const labels = [];
    for (let i = 0; i <= yTickCount; i += 1) {
      const t = i / yTickCount;
      const displayVal = minValue + t * yRange;
      const actualVal = logScale ? Math.pow(10, displayVal) - 1 : displayVal;
      labels.push(formatRoiTick(actualVal));
    }
    const maxLabel = measureMaxLabel(labels);
    const spacing = drawableHeight / yTickCount;
    if (maxLabel + 6 <= spacing) break;
    yTickCount -= 1;
  }
  const yTickLabels = [];
  for (let i = 0; i <= yTickCount; i += 1) {
    const t = i / yTickCount;
    const displayVal = minValue + t * yRange;
    const actualVal = logScale ? Math.pow(10, displayVal) - 1 : displayVal;
    yTickLabels.push(formatRoiTick(actualVal));
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

  ctx.strokeStyle = PLOT_THEME.axis;
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
    ctx.strokeStyle = PLOT_THEME.grid;
    ctx.beginPath();
    ctx.moveTo(x, padT);
    ctx.lineTo(x, padT + drawableHeight);
    ctx.stroke();
    ctx.strokeStyle = PLOT_THEME.axis;
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
    ctx.strokeStyle = PLOT_THEME.grid;
    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(padL + drawableWidth, y);
    ctx.stroke();
    ctx.strokeStyle = PLOT_THEME.axis;
    ctx.beginPath();
    ctx.moveTo(padL - 4, y);
    ctx.lineTo(padL, y);
    ctx.stroke();
    const yLabelText = yTickLabels[i] || "";
    ctx.fillText(yLabelText, padL - 8, y);
  }

  ctx.strokeStyle = PLOT_THEME.line;
  ctx.lineWidth = 1;
  ctx.shadowColor = PLOT_THEME.lineGlow;
  ctx.shadowBlur = 2;
  ctx.beginPath();
  let hasSegment = false;
  values.forEach((v, i) => {
    if (!Number.isFinite(v)) {
      hasSegment = false;
      return;
    }
    const x = padL + (i / Math.max(1, values.length - 1)) * drawableWidth;
    const yNorm = yRange ? (v - minValue) / yRange : 0;
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

  const xLabel = plotMeta.xLabel || "Index";
  const yLabel = plotMeta.yLabel || "Value";
  const xMinActual = xStart;
  const xMaxActual = xStart + (values.length - 1) * xStep;
  const yMinActual = logScale ? Math.max(0, Math.pow(10, minValue) - 1) : minValue;
  const yMaxActual = logScale ? Math.max(0, Math.pow(10, maxValue) - 1) : maxValue;
  ctx.fillStyle = PLOT_THEME.text;
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
  ctx.strokeStyle = PLOT_THEME.frame;
  ctx.strokeRect(0.5, 0.5, width - 1, height - 1);
}

function exportRoiCsv() {
  if (!roiState.enabled || !roiState.active) {
    setStatus("No ROI data to export");
    return;
  }
  const sections = [];
  const formatNum = (value) => (Number.isFinite(value) ? String(value) : "");

  const addSection = (title, data, meta, allowEmpty = false) => {
    if (!allowEmpty && (!data || !data.length)) return;
    const xLabel = meta?.xLabel || "Index";
    const yLabel = meta?.yLabel || "Value";
    const xStart = Number.isFinite(meta?.xStart) ? meta.xStart : 0;
    const xStep = Number.isFinite(meta?.xStep) && meta.xStep !== 0 ? meta.xStep : 1;
    sections.push(`# ${title}`);
    sections.push(`${xLabel},${yLabel}`);
    if (data && data.length) data.forEach((value, idx) => {
      const xVal = xStart + idx * xStep;
      sections.push(`${formatNum(xVal)},${formatNum(value)}`);
    });
    sections.push("");
  };

  if (roiState.lineProfile && roiState.lineProfile.length) {
    addSection(
      roiState.mode === "line" ? "Line Profile" : "Radial Profile",
      roiState.lineProfile,
      roiLineCanvas?._roiPlotMeta,
    );
  }
  const allowBoxEmpty = roiState.mode === "box";
  if (roiState.xProjection && roiState.xProjection.length) {
    addSection("X Projection", roiState.xProjection, roiXCanvas?._roiPlotMeta, allowBoxEmpty);
  } else if (allowBoxEmpty) {
    addSection("X Projection", roiState.xProjection || [], roiXCanvas?._roiPlotMeta, true);
  }
  if (roiState.yProjection && roiState.yProjection.length) {
    addSection("Y Projection", roiState.yProjection, roiYCanvas?._roiPlotMeta, allowBoxEmpty);
  } else if (allowBoxEmpty) {
    addSection("Y Projection", roiState.yProjection || [], roiYCanvas?._roiPlotMeta, true);
  }

  if (!sections.length) {
    setStatus("No ROI projection data to export");
    return;
  }

  const base = (state.file || "roi").split("/").pop().replace(/\.[^.]+$/, "");
  const thresholdSuffix = state.thresholdCount > 1 ? `_thr${state.thresholdIndex + 1}` : "";
  const filename = `${base}_frame_${state.frameIndex + 1}${thresholdSuffix}_roi_${roiState.mode}.csv`;
  const blob = new Blob([sections.join("\n")], { type: "text/csv;charset=utf-8" });
  const link = document.createElement("a");
  const url = URL.createObjectURL(blob);
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
  setStatus(`Exported ROI CSV: ${filename}`);
}

  return {
    updateRoiCenterInputs,
    applyRoiCenterFromInputs,
    getRoiPlotKey,
    getRoiPlotLimits,
    clearRoiPlotLimits,
    syncRoiPlotLimitControls,
    updateRoiPlotLimitsEnabled,
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
    drawRoiPlot,
    exportRoiCsv,
  };
}
