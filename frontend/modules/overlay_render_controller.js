/**
 * Overlay rendering for pixel labels, peak markers, and resolution rings.
 */

import {
  buildGeometryRingCache,
  paintPeakMarkers,
  paintResolutionRings,
  screenView,
} from "./overlay_painters.js";

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
    const zoomY = zoom * (state.pixelAspect || 1);
    const minCellPx = Math.max(8, Number(state.pixelLabelMinCellPx) || pixelLabelDefaultMinCellPx);
    // Gate on the smaller cell dimension so labels only appear once each pixel
    // is large enough on both axes (matters for anisotropic pixels).
    if (Math.min(zoom, zoomY) < minCellPx) return;
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
    const viewY = getEffectiveScrollTop() / zoomY;
    const viewW = canvasWrap.clientWidth / zoom;
    const viewH = canvasWrap.clientHeight / zoomY;
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
      const screenY = (y - viewY) * zoomY + zoomY / 2 + offsetY;
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

  // The image->screen map the painters take: zoom, the anisotropic-pixel
  // stretch, the scroll position and the centring offset, in one affine map.
  function currentView() {
    const zoom = state.zoom || 1;
    return screenView({
      zoom,
      zoomY: zoom * (state.pixelAspect || 1),
      scrollX: getEffectiveScrollLeft(),
      scrollY: getEffectiveScrollTop(),
      offsetX: state.renderOffsetX || 0,
      offsetY: state.renderOffsetY || 0,
    });
  }

  function drawPeakOverlay() {
    if (!peakOverlay || !peakCtx || !canvasWrap) return;
    const metrics = syncOverlayCanvas(peakOverlay, peakCtx);
    if (!metrics) return;
    const { width, height } = metrics;
    peakCtx.clearRect(0, 0, width, height);
    if (!state.hasFrame) return;

    const externalPeakSets = Array.isArray(analysisState.externalPeakSets)
      ? analysisState.externalPeakSets
      : [];
    const peaks =
      analysisState.peaksEnabled && Array.isArray(analysisState.peaks) ? analysisState.peaks : [];
    if (!peaks.length && !externalPeakSets.length) return;

    paintPeakMarkers(peakCtx, {
      peaks,
      selectedPeaks: analysisState.selectedPeaks || [],
      externalPeakSets,
      view: currentView(),
      width,
      height,
    });
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
    geometryRingCache = buildGeometryRingCache(params);
    return geometryRingCache;
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
    paintResolutionRings(resolutionCtx, {
      params,
      view: currentView(),
      geometryCache: getGeometryRingCache(params),
      pixelAspect: state.pixelAspect || 1,
      activeHandle: getRingInteractionState?.().handle || null,
    });
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
