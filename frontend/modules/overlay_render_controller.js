/**
 * Overlay rendering for pixel labels, peak markers, and resolution rings.
 */

import {
  applyGeometryOverrides,
  buildGeometryRingSegments,
  pickGeometryRingLabelPoint,
} from "./ring_geometry_utils.js";

export function createOverlayRenderController({
  state,
  analysisState,
  elements,
  constants,
  callbacks,
}) {
  const {
    canvasWrap,
    pixelOverlay,
    pixelCtx,
    peakOverlay,
    peakCtx,
    resolutionOverlay,
    resolutionCtx,
  } = elements;

  const {
    pixelLabelDefaultMinCellPx,
    pixelLabelDefaultMaxLabels,
    pixelLabelDenseZoomPx,
    pixelLabelInteractionIdleMs,
    pixelLabelHaloMaxLabels,
  } = constants;

  const {
    syncOverlayCanvas,
    getActiveSaturationMax,
    getEffectiveScrollLeft,
    getEffectiveScrollTop,
    formatPixelLabelValue,
    isSaturatedValue,
    getRingParams,
    updateRingsSectionState,
    getRingInteractionState,
  } = callbacks;

  let pixelOverlayScheduled = false;
  let pixelOverlayInteractionUntil = 0;
  let pixelOverlayResumeTimer = null;
  let peakOverlayScheduled = false;
  let resolutionOverlayScheduled = false;
  let geometryRingCacheKey = "";
  let geometryRingCache = new Map();

  function clearPixelOverlay() {
    if (!pixelOverlay || !pixelCtx) return;
    pixelCtx.clearRect(0, 0, pixelOverlay.width, pixelOverlay.height);
  }

  function isPixelOverlayInteractionActive() {
    if (state.pixelLabelShowDuringDrag) return false;
    return Date.now() < pixelOverlayInteractionUntil;
  }

  function isFloatPixelLabelDtype(dtype) {
    const normalized = String(dtype || "").toLowerCase();
    return normalized.startsWith("float") || /^[<>|]f\d+$/.test(normalized);
  }

  function deferPixelOverlayRedraw(delayMs = pixelLabelInteractionIdleMs) {
    if (state.pixelLabelShowDuringDrag) return;
    const delay = Math.max(0, Number(delayMs) || pixelLabelInteractionIdleMs);
    pixelOverlayInteractionUntil = Date.now() + delay;
    clearPixelOverlay();
    if (pixelOverlayResumeTimer) {
      window.clearTimeout(pixelOverlayResumeTimer);
    }
    pixelOverlayResumeTimer = window.setTimeout(() => {
      pixelOverlayResumeTimer = null;
      schedulePixelOverlay();
    }, delay + 10);
  }

  function drawPixelOverlay() {
    if (!pixelOverlay || !pixelCtx || !canvasWrap) return;
    const metrics = syncOverlayCanvas(pixelOverlay, pixelCtx);
    if (!metrics) return;
    const { width, height } = metrics;
    pixelCtx.clearRect(0, 0, width, height);

    if (!state.hasFrame || !state.dataRaw || !state.pixelLabels) return;
    if (isPixelOverlayInteractionActive()) return;
    const zoom = state.zoom || 1;
    const minCellPx = Math.max(8, Number(state.pixelLabelMinCellPx) || pixelLabelDefaultMinCellPx);
    if (zoom < minCellPx) return;
    const satMax = getActiveSaturationMax();
    const offsetX = state.renderOffsetX || 0;
    const offsetY = state.renderOffsetY || 0;
    const maskReady =
      state.maskEnabled &&
      state.maskAvailable &&
      state.maskRaw &&
      state.maskShape &&
      state.maskShape[0] === state.height &&
      state.maskShape[1] === state.width;

    const viewX = getEffectiveScrollLeft() / zoom;
    const viewY = getEffectiveScrollTop() / zoom;
    const viewW = canvasWrap.clientWidth / zoom;
    const viewH = canvasWrap.clientHeight / zoom;
    let startX = Math.floor(viewX);
    let startY = Math.floor(viewY);
    let endX = Math.ceil(viewX + viewW);
    let endY = Math.ceil(viewY + viewH);
    startX = Math.max(0, startX);
    startY = Math.max(0, startY);
    endX = Math.min(state.width, endX);
    endY = Math.min(state.height, endY);

    const cols = Math.max(0, endX - startX);
    const rows = Math.max(0, endY - startY);
    const cells = cols * rows;
    if (cells === 0) {
      return;
    }
    const formatMode = String(state.pixelLabelFormat || "auto").toLowerCase();
    const isFloatLabelMode = isFloatPixelLabelDtype(state.dtype) && formatMode !== "integer";
    const fontSize = isFloatLabelMode
      ? Math.min(11.5, Math.max(6.5, zoom * 0.44))
      : Math.min(13, Math.max(7, zoom * 0.52));
    pixelCtx.font = `${fontSize}px "Lucida Grande", "Helvetica Neue", Arial, sans-serif`;
    pixelCtx.textAlign = "center";
    pixelCtx.textBaseline = "middle";
    pixelCtx.fillStyle = "rgba(248, 252, 255, 0.95)";
    const maxLabels = Math.max(
      100,
      Number.isFinite(state.pixelLabelMaxLabels) ? Number(state.pixelLabelMaxLabels) : pixelLabelDefaultMaxLabels,
    );
    const denseZoomPx = Math.max(minCellPx + 4, pixelLabelDenseZoomPx);
    const denseLabelBudget = Math.max(maxLabels, 16000);
    const canRenderDense = zoom >= denseZoomPx && cells <= denseLabelBudget;
    if (!canRenderDense && cells > maxLabels) {
      return;
    }

    function resolvePixelLabelText(idx) {
      let text = formatPixelLabelValue(state.dataRaw[idx], zoom, formatMode);
      if (maskReady && state.maskRaw) {
        const maskValue = state.maskRaw[idx];
        if (maskValue & 1) {
          text = "G";
        } else if (maskValue & 0x1e) {
          text = "D";
        }
      }
      if (state.maskSaturatedEnabled && text !== "G" && text !== "D" && isSaturatedValue(state.dataRaw[idx], satMax)) {
        text = "S";
      }
      return text;
    }

    if (isFloatLabelMode) {
      const sampleCols = Math.min(6, Math.max(1, cols));
      const sampleRows = Math.min(4, Math.max(1, rows));
      const stepX = Math.max(1, Math.ceil(cols / sampleCols));
      const stepY = Math.max(1, Math.ceil(rows / sampleRows));
      const widthBudget = Math.max(1, zoom * 0.82);
      let maxTextWidth = 0;
      let sampleCount = 0;
      for (let y = startY; y < endY && sampleCount < 24; y += stepY) {
        const rowOffset = y * state.width;
        for (let x = startX; x < endX && sampleCount < 24; x += stepX) {
          const idx = rowOffset + x;
          const text = resolvePixelLabelText(idx);
          if (!text) continue;
          maxTextWidth = Math.max(maxTextWidth, pixelCtx.measureText(text).width);
          sampleCount += 1;
        }
      }
      if (maxTextWidth > widthBudget) {
        return;
      }
    }

    const useHalo = cells <= pixelLabelHaloMaxLabels;
    if (useHalo) {
      pixelCtx.strokeStyle = isFloatLabelMode ? "rgba(4, 8, 14, 0.96)" : "rgba(6, 10, 16, 0.9)";
      pixelCtx.lineWidth = Math.max(1, Math.min(isFloatLabelMode ? 2.4 : 2, fontSize * (isFloatLabelMode ? 0.28 : 0.2)));
      pixelCtx.lineJoin = "round";
      pixelCtx.miterLimit = 2;
    }

    for (let y = startY; y < endY; y += 1) {
      const rowOffset = y * state.width;
      const screenY = (y - viewY) * zoom + zoom / 2 + offsetY;
      for (let x = startX; x < endX; x += 1) {
        const idx = rowOffset + x;
        const text = resolvePixelLabelText(idx);
        if (!text) continue;
        const screenX = (x - viewX) * zoom + zoom / 2 + offsetX;
        if (useHalo) {
          pixelCtx.strokeText(text, screenX, screenY);
        }
        pixelCtx.fillText(text, screenX, screenY);
      }
    }
  }

  function schedulePixelOverlay() {
    if (isPixelOverlayInteractionActive()) {
      deferPixelOverlayRedraw();
      return;
    }
    if (pixelOverlayScheduled) return;
    pixelOverlayScheduled = true;
    window.requestAnimationFrame(() => {
      pixelOverlayScheduled = false;
      drawPixelOverlay();
    });
  }

  function schedulePeakOverlay() {
    if (!peakOverlay || !peakCtx) return;
    if (peakOverlayScheduled) return;
    peakOverlayScheduled = true;
    window.requestAnimationFrame(() => {
      peakOverlayScheduled = false;
      drawPeakOverlay();
    });
  }

  function drawPeakOverlay() {
    if (!peakOverlay || !peakCtx || !canvasWrap) return;
    const metrics = syncOverlayCanvas(peakOverlay, peakCtx);
    if (!metrics) return;
    const { width, height } = metrics;
    peakCtx.clearRect(0, 0, width, height);
    if (!state.hasFrame) return;

    const zoom = state.zoom || 1;
    const offsetX = state.renderOffsetX || 0;
    const offsetY = state.renderOffsetY || 0;
    const viewX = getEffectiveScrollLeft() / zoom;
    const viewY = getEffectiveScrollTop() / zoom;
    const externalSets = Array.isArray(analysisState.externalPeakSets) ? analysisState.externalPeakSets : [];
    const hasLocalPeaks = analysisState.peaksEnabled && Array.isArray(analysisState.peaks) && analysisState.peaks.length;

    if (!hasLocalPeaks && !externalSets.length) return;

    externalSets.forEach((set) => {
      const color = typeof set?.color === "string" && set.color ? set.color : "#4aa3ff";
      const style = typeof set?.style === "string" ? set.style : "";
      const jfjochSet = style === "jfjoch-indexed" || style === "jfjoch-unindexed";
      const points = Array.isArray(set?.points) ? set.points : [];
      const radius = jfjochSet
        ? Math.max(6, Math.min(14, 8 + Math.log2(Math.max(1, zoom)) * 0.45))
        : Math.max(5, Math.min(11, 7 + Math.log2(Math.max(1, zoom)) * 0.35));
      points.forEach((peak) => {
        const px = Number(peak?.x);
        const py = Number(peak?.y);
        if (!Number.isFinite(px) || !Number.isFinite(py)) return;
        const sx = (px + 0.5 - viewX) * zoom + offsetX;
        const sy = (py + 0.5 - viewY) * zoom + offsetY;
        if (sx < -20 || sy < -20 || sx > width + 20 || sy > height + 20) return;

        if (jfjochSet) {
          peakCtx.setLineDash([]);
          peakCtx.beginPath();
          peakCtx.arc(sx, sy, radius, 0, Math.PI * 2);
          peakCtx.lineWidth = 2.8;
          peakCtx.strokeStyle = "rgba(12, 12, 12, 0.78)";
          peakCtx.stroke();

          peakCtx.beginPath();
          peakCtx.arc(sx, sy, Math.max(3, radius - 2), 0, Math.PI * 2);
          peakCtx.lineWidth = 1.8;
          peakCtx.strokeStyle = color;
          peakCtx.stroke();

          const cross = radius + 3;
          peakCtx.beginPath();
          peakCtx.moveTo(sx - cross, sy);
          peakCtx.lineTo(sx + cross, sy);
          peakCtx.moveTo(sx, sy - cross);
          peakCtx.lineTo(sx, sy + cross);
          peakCtx.lineWidth = 1.6;
          peakCtx.strokeStyle = color;
          peakCtx.stroke();
        } else {
          peakCtx.setLineDash([4, 3]);
          peakCtx.beginPath();
          peakCtx.arc(sx, sy, radius, 0, Math.PI * 2);
          peakCtx.lineWidth = 2.4;
          peakCtx.strokeStyle = "rgba(10, 10, 10, 0.62)";
          peakCtx.stroke();

          peakCtx.beginPath();
          peakCtx.arc(sx, sy, Math.max(3, radius - 1.5), 0, Math.PI * 2);
          peakCtx.lineWidth = 1.35;
          peakCtx.strokeStyle = color;
          peakCtx.stroke();
        }
      });
    });

    if (!hasLocalPeaks) {
      peakCtx.setLineDash([]);
      return;
    }

    analysisState.peaks.forEach((peak, index) => {
      const sx = (peak.x + 0.5 - viewX) * zoom + offsetX;
      const sy = (peak.y + 0.5 - viewY) * zoom + offsetY;
      if (sx < -20 || sy < -20 || sx > width + 20 || sy > height + 20) return;
      const selected = analysisState.selectedPeaks.includes(index);
      const zoomScale = Math.max(0, Math.log2(Math.max(1, zoom)));
      const radius = selected
        ? Math.max(14, Math.min(34, 16 + zoomScale * 2.2))
        : Math.max(8, Math.min(16, 9 + zoomScale * 0.6));

      if (selected) {
        peakCtx.setLineDash([]);
        peakCtx.beginPath();
        peakCtx.arc(sx, sy, radius, 0, Math.PI * 2);
        peakCtx.lineWidth = 3.8;
        peakCtx.strokeStyle = "rgba(18, 18, 18, 0.92)";
        peakCtx.stroke();

        peakCtx.beginPath();
        peakCtx.arc(sx, sy, radius - 1.5, 0, Math.PI * 2);
        peakCtx.lineWidth = 2.6;
        peakCtx.strokeStyle = "rgba(72, 255, 105, 0.98)";
        peakCtx.stroke();

        const cross = radius + 5;
        peakCtx.beginPath();
        peakCtx.moveTo(sx - cross, sy);
        peakCtx.lineTo(sx + cross, sy);
        peakCtx.moveTo(sx, sy - cross);
        peakCtx.lineTo(sx, sy + cross);
        peakCtx.lineWidth = 5.2;
        peakCtx.strokeStyle = "rgba(0, 0, 0, 0.8)";
        peakCtx.stroke();

        peakCtx.beginPath();
        peakCtx.moveTo(sx - cross, sy);
        peakCtx.lineTo(sx + cross, sy);
        peakCtx.moveTo(sx, sy - cross);
        peakCtx.lineTo(sx, sy + cross);
        peakCtx.lineWidth = 2.8;
        peakCtx.strokeStyle = "rgba(72, 255, 105, 0.98)";
        peakCtx.stroke();
      } else {
        peakCtx.setLineDash([5, 4]);
        peakCtx.beginPath();
        peakCtx.arc(sx, sy, radius, 0, Math.PI * 2);
        peakCtx.lineWidth = 1.8;
        peakCtx.strokeStyle = "rgba(255, 255, 255, 0.55)";
        peakCtx.stroke();

        peakCtx.beginPath();
        peakCtx.arc(sx, sy, Math.max(3, radius - 2), 0, Math.PI * 2);
        peakCtx.lineWidth = 1.2;
        peakCtx.strokeStyle = "rgba(70, 155, 255, 0.72)";
        peakCtx.stroke();
      }
    });
    peakCtx.setLineDash([]);
  }

  function scheduleResolutionOverlay() {
    if (!resolutionOverlay || !resolutionCtx) return;
    if (resolutionOverlayScheduled) return;
    resolutionOverlayScheduled = true;
    window.requestAnimationFrame(() => {
      resolutionOverlayScheduled = false;
      drawResolutionOverlay();
    });
  }

  function getGeometryCacheKey(params) {
    if (!params || params.mode !== "geometry" || !params.geometry) return "";
    return [
      String(params.geometrySource || params.geometry?.source || ""),
      String(params.energyEv || ""),
      Number.isFinite(params.distanceMm) ? Number(params.distanceMm).toFixed(4) : "",
      Number.isFinite(params.centerX) ? Number(params.centerX).toFixed(4) : "",
      Number.isFinite(params.centerY) ? Number(params.centerY).toFixed(4) : "",
      params.rings.map((value) => Number(value).toFixed(6)).join(","),
    ].join("|");
  }

  function getGeometryRingCache(params) {
    const cacheKey = getGeometryCacheKey(params);
    if (!cacheKey) {
      geometryRingCacheKey = "";
      geometryRingCache = new Map();
      return geometryRingCache;
    }
    if (cacheKey === geometryRingCacheKey) {
      return geometryRingCache;
    }
    geometryRingCacheKey = cacheKey;
    geometryRingCache = new Map();
    const adjustedGeometry = applyGeometryOverrides(params.geometry, {
      centerX: params.centerX,
      centerY: params.centerY,
      distanceMm: params.distanceMm,
    });
    params.rings.forEach((dSpacing) => {
      const segments = buildGeometryRingSegments({
        geometry: adjustedGeometry,
        energyEv: params.energyEv,
        dSpacing,
      });
      geometryRingCache.set(dSpacing, {
        segments,
        labelPoint: pickGeometryRingLabelPoint(segments),
      });
    });
    return geometryRingCache;
  }

  function drawRingLabel(screenPoint, label) {
    if (!screenPoint || !label) return;
    const fontSize = 14;
    const textX = screenPoint.x + 8;
    const textY = screenPoint.y;
    const textWidth = resolutionCtx.measureText(label).width;
    const padX = 6;
    const padY = 3;
    resolutionCtx.fillStyle = "rgba(10, 20, 40, 0.55)";
    resolutionCtx.fillRect(textX - padX, textY - fontSize / 2 - padY, textWidth + padX * 2, fontSize + padY * 2);
    resolutionCtx.lineWidth = 3;
    resolutionCtx.strokeStyle = "rgba(0, 0, 0, 0.7)";
    resolutionCtx.strokeText(label, textX, textY);
    resolutionCtx.fillStyle = "rgba(230, 240, 255, 0.98)";
    resolutionCtx.fillText(label, textX, textY);
  }

  function strokeRingPath(points) {
    if (!Array.isArray(points) || points.length < 2) return;
    resolutionCtx.beginPath();
    resolutionCtx.moveTo(points[0].x, points[0].y);
    for (let index = 1; index < points.length; index += 1) {
      resolutionCtx.lineTo(points[index].x, points[index].y);
    }
    resolutionCtx.lineWidth = 3.5;
    resolutionCtx.strokeStyle = "rgba(255, 255, 255, 0.45)";
    resolutionCtx.stroke();
    resolutionCtx.lineWidth = 2;
    resolutionCtx.strokeStyle = "rgba(20, 80, 170, 0.95)";
    resolutionCtx.stroke();
  }

  function drawBeamCenterMarker(centerX, centerY, zoom, highlight = false) {
    if (!Number.isFinite(centerX) || !Number.isFinite(centerY)) return;
    const arm = Math.max(10, Math.min(22, 10 + Math.log2(Math.max(1, zoom)) * 4));
    resolutionCtx.setLineDash([]);
    if (highlight) {
      resolutionCtx.beginPath();
      resolutionCtx.arc(centerX, centerY, arm + 4, 0, Math.PI * 2);
      resolutionCtx.fillStyle = "rgba(255, 65, 65, 0.16)";
      resolutionCtx.fill();
      resolutionCtx.lineWidth = 1.5;
      resolutionCtx.strokeStyle = "rgba(255, 65, 65, 0.85)";
      resolutionCtx.stroke();
    }
    resolutionCtx.beginPath();
    resolutionCtx.moveTo(centerX - arm, centerY);
    resolutionCtx.lineTo(centerX + arm, centerY);
    resolutionCtx.moveTo(centerX, centerY - arm);
    resolutionCtx.lineTo(centerX, centerY + arm);
    resolutionCtx.lineWidth = 4;
    resolutionCtx.strokeStyle = "rgba(0, 0, 0, 0.72)";
    resolutionCtx.stroke();
    resolutionCtx.beginPath();
    resolutionCtx.moveTo(centerX - arm, centerY);
    resolutionCtx.lineTo(centerX + arm, centerY);
    resolutionCtx.moveTo(centerX, centerY - arm);
    resolutionCtx.lineTo(centerX, centerY + arm);
    resolutionCtx.lineWidth = 2.2;
    resolutionCtx.strokeStyle = "rgba(255, 65, 65, 0.96)";
    resolutionCtx.stroke();
  }

  function drawResolutionOverlay() {
    if (!resolutionOverlay || !resolutionCtx || !canvasWrap) return;
    const metrics = syncOverlayCanvas(resolutionOverlay, resolutionCtx);
    if (!metrics) return;
    const { width, height } = metrics;
    resolutionCtx.clearRect(0, 0, width, height);
    updateRingsSectionState();
    if (!analysisState.ringsEnabled || !state.hasFrame) return;
    const params = getRingParams();
    if (!params.energyEv) return;
    if (params.mode !== "geometry" && (!params.distanceMm || !params.pixelSizeUm)) return;
    const lambda = 12398.4193 / params.energyEv;
    if (!Number.isFinite(lambda) || lambda <= 0) return;
    const zoom = state.zoom || 1;
    const offsetX = state.renderOffsetX || 0;
    const offsetY = state.renderOffsetY || 0;
    const viewX = getEffectiveScrollLeft() / zoom;
    const viewY = getEffectiveScrollTop() / zoom;
    const centerX = (params.centerX - viewX) * zoom + offsetX;
    const centerY = (params.centerY - viewY) * zoom + offsetY;
    const pixelSizeMm = params.pixelSizeUm / 1000;
    const activeHandle = getRingInteractionState?.().handle || null;
    const centerActive = activeHandle?.type === "center";
    const isRingActive = (d) =>
      activeHandle?.type === "ring" &&
      Number.isFinite(activeHandle.d) &&
      Math.abs(activeHandle.d - d) <= Math.max(0.01, d * 0.005);

    resolutionCtx.save();
    resolutionCtx.setLineDash([6, 6]);
    resolutionCtx.lineJoin = "round";
    resolutionCtx.lineCap = "round";
    const fontSize = 14;
    resolutionCtx.font = `${fontSize}px 'Avenir', 'Segoe UI', sans-serif`;
    resolutionCtx.textBaseline = "middle";
    const labelAngle = -Math.PI / 6;
    if (params.mode === "geometry" && params.geometry) {
      const geometryCache = getGeometryRingCache(params);
      params.rings.forEach((d) => {
        const entry = geometryCache.get(d);
        if (!entry || !Array.isArray(entry.segments) || !entry.segments.length) return;
        entry.segments.forEach((segment) => {
          const screenPoints = segment.map((point) => ({
            x: (point.x - viewX) * zoom + offsetX,
            y: (point.y - viewY) * zoom + offsetY,
          }));
          strokeRingPath(screenPoints);
        });
        const labelPoint = entry.labelPoint
          ? {
              x: (entry.labelPoint.x - viewX) * zoom + offsetX,
              y: (entry.labelPoint.y - viewY) * zoom + offsetY,
            }
          : null;
        const label = Number.isFinite(d) ? `${d.toFixed(2).replace(/\.00$/, "")} \u00C5` : "\u00C5";
        drawRingLabel(labelPoint, label);
      });
      if (params.centerKnown) {
        drawBeamCenterMarker(centerX, centerY, zoom, centerActive);
      }
      resolutionCtx.restore();
      return;
    }
    if (!Number.isFinite(pixelSizeMm) || pixelSizeMm <= 0) {
      resolutionCtx.restore();
      return;
    }
    params.rings.forEach((d) => {
      const sinArg = lambda / (2 * d);
      if (!Number.isFinite(sinArg) || sinArg <= 0 || sinArg >= 1) return;
      const twoTheta = 2 * Math.asin(sinArg);
      const radiusMm = params.distanceMm * Math.tan(twoTheta);
      const radiusPx = radiusMm / pixelSizeMm;
      if (!Number.isFinite(radiusPx) || radiusPx <= 0) return;
      const screenRadius = radiusPx * zoom;
      if (screenRadius < 5) return;
      const active = isRingActive(d);
      resolutionCtx.beginPath();
      resolutionCtx.arc(centerX, centerY, screenRadius, 0, Math.PI * 2);
      resolutionCtx.lineWidth = active ? 4.5 : 3.5;
      resolutionCtx.strokeStyle = "rgba(255, 255, 255, 0.45)";
      resolutionCtx.stroke();
      resolutionCtx.lineWidth = active ? 3 : 2;
      resolutionCtx.strokeStyle = active ? "rgba(90, 160, 255, 1)" : "rgba(20, 80, 170, 0.95)";
      resolutionCtx.stroke();

      const labelX = centerX + Math.cos(labelAngle) * screenRadius;
      const labelY = centerY + Math.sin(labelAngle) * screenRadius;
      const label = Number.isFinite(d) ? `${d.toFixed(2).replace(/\.00$/, "")} \u00C5` : "\u00C5";
      drawRingLabel({ x: labelX, y: labelY }, label);
    });

    if (params.centerKnown) {
      drawBeamCenterMarker(centerX, centerY, zoom, centerActive);
    }
    resolutionCtx.restore();
  }

  return {
    schedulePixelOverlay,
    drawPixelOverlay,
    clearPixelOverlay,
    deferPixelOverlayRedraw,
    schedulePeakOverlay,
    drawPeakOverlay,
    scheduleResolutionOverlay,
    drawResolutionOverlay,
  };
}
