/**
 * Overview rendering and viewport interaction orchestration.
 */

export function createOverviewViewportController({
  state,
  overviewState,
  elements,
  constants,
  theme,
  callbacks,
}) {
  const {
    canvasWrap,
    canvas,
    overviewCanvas,
    overviewCtx,
    zoomRange,
    zoomValue,
    viewerFooterEl,
  } = elements;

  const {
    MIN_ZOOM,
    MAX_ZOOM,
    VIEWPORT_INTERACTION_IDLE_MS,
  } = constants;

  const {
    PLOT_THEME,
  } = theme;

  const {
    deferPixelOverlayRedraw,
    schedulePixelOverlay,
    scheduleRoiOverlay,
    scheduleResolutionOverlay,
    schedulePeakOverlay,
    requestFrame,
    cancelActiveFrameLoad,
    hasPendingFrameRequest: hasPendingFrameRequestCallback,
    consumePendingFrameRequest: consumePendingFrameRequestCallback,
    isFrameLoading: isFrameLoadingCallback,
    updateViewerFooter,
  } = callbacks;

  function hasPendingFrameRequest() {
    if (hasPendingFrameRequestCallback) {
      return hasPendingFrameRequestCallback();
    }
    return state.pendingFrame !== null;
  }

  function consumePendingFrameRequest() {
    if (consumePendingFrameRequestCallback) {
      return consumePendingFrameRequestCallback();
    }
    if (state.pendingFrame === null) return null;
    const next = state.pendingFrame;
    state.pendingFrame = null;
    return next;
  }

  function isFrameLoading() {
    if (isFrameLoadingCallback) {
      return isFrameLoadingCallback();
    }
    return Boolean(state.isLoading);
  }

  let overviewScheduled = false;
  let overviewRect = null;
  let viewportInteractionUntil = 0;
  let viewportInteractionResumeTimer = null;
  let touchGestureActive = false;
  let touchGestureDistance = 0;
  let touchGestureMid = null;

  function getMinZoom() {
    if (!canvasWrap || !state.width || !state.height) {
      return MIN_ZOOM;
    }
    const fitScale = Math.min(
      canvasWrap.clientWidth / state.width,
      canvasWrap.clientHeight / state.height,
    );
    if (!Number.isFinite(fitScale) || fitScale <= 0) {
      return MIN_ZOOM;
    }
    // Allow zooming out beyond fit-to-window for better context.
    return Math.max(MIN_ZOOM, Math.min(1, fitScale * 0.1));
  }

  function getEffectiveScrollLeft() {
    if (!canvasWrap) return 0;
    return (canvasWrap.scrollLeft || 0) - (state.panOffsetX || 0);
  }

  function getEffectiveScrollTop() {
    if (!canvasWrap) return 0;
    return (canvasWrap.scrollTop || 0) - (state.panOffsetY || 0);
  }

  function applyCanvasTransform() {
    if (!canvas) return;
    const zoom = Number.isFinite(state.zoom) ? state.zoom : 1;
    const tx = (state.renderOffsetX || 0) + (state.panOffsetX || 0);
    const ty = (state.renderOffsetY || 0) + (state.panOffsetY || 0);
    canvas.style.transform = `translate(${tx}px, ${ty}px) scale(${zoom})`;
  }

  function clampToRange(value, min, max) {
    if (!Number.isFinite(value)) return min;
    if (!Number.isFinite(min) || !Number.isFinite(max) || min > max) {
      return value;
    }
    return Math.max(min, Math.min(max, value));
  }

  function getEffectiveScrollBounds() {
    if (!canvasWrap || !state.width || !state.height) {
      return { minX: 0, maxX: 0, minY: 0, maxY: 0 };
    }
    const zoom = state.zoom || 1;
    const scaledW = state.width * zoom;
    const scaledH = state.height * zoom;
    const viewW = canvasWrap.clientWidth || 0;
    const viewH = canvasWrap.clientHeight || 0;
    const baseX = state.renderOffsetX || 0;
    const baseY = state.renderOffsetY || 0;

    const computeBounds = (scaled, view, base, leadInset = 0, trailInset = 0) => {
      if (!(scaled > 0) || !(view > 0)) {
        return { min: 0, max: 0 };
      }
      const insetLead = Math.max(0, Number(leadInset) || 0);
      const insetTrail = Math.max(0, Number(trailInset) || 0);
      const availableView = Math.max(1, view - insetLead - insetTrail);
      // Keep a small sliver visible so the image cannot be dragged away entirely,
      // but do not scale that guard with zoom or large images will "snap" to an edge.
      const minVisible = Math.min(scaled, Math.max(1, Math.min(48, availableView * 0.5)));
      const min = base + minVisible + insetTrail - view;
      const max = base + scaled - insetLead - minVisible;
      if (!Number.isFinite(min) || !Number.isFinite(max) || min > max) {
        return { min: 0, max: 0 };
      }
      return { min, max };
    };

    const footerInset = viewerFooterEl
      ? Math.max(0, Math.round(viewerFooterEl.getBoundingClientRect().height) + 8)
      : 0;
    const bx = computeBounds(scaledW, viewW, baseX, 0, 0);
    const by = computeBounds(scaledH, viewH, baseY, 0, footerInset);
    return { minX: bx.min, maxX: bx.max, minY: by.min, maxY: by.max };
  }

  function clampEffectiveScroll(targetX, targetY) {
    const bounds = getEffectiveScrollBounds();
    const desiredX = Number.isFinite(targetX) ? targetX : 0;
    const desiredY = Number.isFinite(targetY) ? targetY : 0;
    return {
      x: clampToRange(desiredX, bounds.minX, bounds.maxX),
      y: clampToRange(desiredY, bounds.minY, bounds.maxY),
    };
  }

  function applyEffectiveScroll(targetX, targetY, schedule = true, clamp = true) {
    if (!canvasWrap) {
      return { x: 0, y: 0 };
    }
    let desiredX = Number.isFinite(targetX) ? targetX : 0;
    let desiredY = Number.isFinite(targetY) ? targetY : 0;
    if (clamp) {
      const clamped = clampEffectiveScroll(desiredX, desiredY);
      desiredX = clamped.x;
      desiredY = clamped.y;
    }
    const maxScrollLeft = Math.max(0, (canvasWrap.scrollWidth || 0) - (canvasWrap.clientWidth || 0));
    const maxScrollTop = Math.max(0, (canvasWrap.scrollHeight || 0) - (canvasWrap.clientHeight || 0));
    const nextScrollLeft = clampToRange(desiredX, 0, maxScrollLeft);
    const nextScrollTop = clampToRange(desiredY, 0, maxScrollTop);
    canvasWrap.scrollLeft = nextScrollLeft;
    canvasWrap.scrollTop = nextScrollTop;
    const appliedScrollLeft = canvasWrap.scrollLeft || 0;
    const appliedScrollTop = canvasWrap.scrollTop || 0;
    state.panOffsetX = appliedScrollLeft - desiredX;
    state.panOffsetY = appliedScrollTop - desiredY;
    applyCanvasTransform();
    if (schedule) {
      deferPixelOverlayRedraw();
      syncViewportOverlays();
    }
    return { x: desiredX, y: desiredY };
  }

  function clampPanOffsetsToBounds() {
    if (!canvasWrap) return false;
    const currentX = getEffectiveScrollLeft();
    const currentY = getEffectiveScrollTop();
    const clamped = clampEffectiveScroll(currentX, currentY);
    const changed = clamped.x !== currentX || clamped.y !== currentY;
    applyEffectiveScroll(clamped.x, clamped.y, false, false);
    return changed;
  }

  function scheduleOverview() {
    if (overviewScheduled) return;
    overviewScheduled = true;
    window.requestAnimationFrame(() => {
      overviewScheduled = false;
      drawOverview();
    });
  }

  function syncViewportOverlays() {
    scheduleOverview();
    schedulePixelOverlay();
    scheduleRoiOverlay();
    scheduleResolutionOverlay();
    schedulePeakOverlay();
  }

  function isViewportInteractionActive() {
    return Date.now() < viewportInteractionUntil;
  }

  function flushViewportDeferredWork() {
    if (!hasPendingFrameRequest() || isFrameLoading()) return;
    const next = consumePendingFrameRequest();
    if (next !== null) {
      requestFrame(next);
    }
  }

  function deferViewportInteraction(delayMs = VIEWPORT_INTERACTION_IDLE_MS) {
    const delay = Math.max(60, Number(delayMs) || VIEWPORT_INTERACTION_IDLE_MS);
    viewportInteractionUntil = Date.now() + delay;
    deferPixelOverlayRedraw(delay);
    if (state.playing && isFrameLoading()) {
      cancelActiveFrameLoad();
    }
    if (viewportInteractionResumeTimer) {
      window.clearTimeout(viewportInteractionResumeTimer);
    }
    viewportInteractionResumeTimer = window.setTimeout(() => {
      viewportInteractionResumeTimer = null;
      flushViewportDeferredWork();
    }, delay + 12);
  }

  function setEffectiveScroll(targetX, targetY, schedule = true, clamp = true) {
    applyEffectiveScroll(targetX, targetY, schedule, clamp);
  }

  function updatePanCapability() {
    if (!canvasWrap) return;
    const canPan = Boolean(state.hasFrame && state.width && state.height);
    canvasWrap.classList.toggle("can-pan", canPan);
  }

  function getOverviewMetrics() {
    if (!overviewCanvas) return null;
    const wrap = overviewCanvas.parentElement;
    const width = wrap?.clientWidth || 1;
    const height = wrap?.clientHeight || 1;
    const imgW = state.width || 1;
    const imgH = state.height || 1;
    const scale = Math.min(width / imgW, height / imgH);
    const drawW = imgW * scale;
    const drawH = imgH * scale;
    const offsetX = (width - drawW) / 2;
    const offsetY = (height - drawH) / 2;
    return { width, height, imgW, imgH, scale, offsetX, offsetY };
  }

  function getViewRect() {
    const imgW = state.width;
    const imgH = state.height;
    if (!imgW || !imgH) return null;
    const zoom = state.zoom || 1;
    const scaleX = zoom;
    const scaleY = zoom;
    const viewW = canvasWrap.clientWidth / scaleX;
    const viewH = canvasWrap.clientHeight / scaleY;
    const viewWClamped = Math.min(viewW, imgW);
    const viewHClamped = Math.min(viewH, imgH);
    let viewX = getEffectiveScrollLeft() / scaleX;
    let viewY = getEffectiveScrollTop() / scaleY;
    viewX = Math.max(0, Math.min(imgW - viewWClamped, viewX));
    viewY = Math.max(0, Math.min(imgH - viewHClamped, viewY));
    return { viewX, viewY, viewW: viewWClamped, viewH: viewHClamped, scaleX, scaleY };
  }

  function overviewEventToImage(event) {
    if (!overviewCanvas || !state.hasFrame || !state.width || !state.height) return null;
    const metrics = getOverviewMetrics();
    if (!metrics) return null;
    const rect = overviewCanvas.getBoundingClientRect();
    const x = (event.clientX - rect.left) * (metrics.width / rect.width);
    const y = (event.clientY - rect.top) * (metrics.height / rect.height);
    const imgX = (x - metrics.offsetX) / metrics.scale;
    const imgY = (y - metrics.offsetY) / metrics.scale;
    return {
      x: Math.max(0, Math.min(metrics.imgW, imgX)),
      y: Math.max(0, Math.min(metrics.imgH, imgY)),
    };
  }

  function overviewEventToOverview(event) {
    if (!overviewCanvas) return null;
    const metrics = getOverviewMetrics();
    if (!metrics) return null;
    const rect = overviewCanvas.getBoundingClientRect();
    const x = (event.clientX - rect.left) * (metrics.width / rect.width);
    const y = (event.clientY - rect.top) * (metrics.height / rect.height);
    return { x, y, metrics };
  }

  function panToImageCenter(x, y) {
    const view = getViewRect();
    if (!view) return;
    const maxX = Math.max(0, state.width - view.viewW);
    const maxY = Math.max(0, state.height - view.viewH);
    const targetX = Math.max(0, Math.min(maxX, x - view.viewW / 2));
    const targetY = Math.max(0, Math.min(maxY, y - view.viewH / 2));
    setEffectiveScroll(targetX * view.scaleX, targetY * view.scaleY);
  }

  function scrollToView(viewX, viewY) {
    if (!state.width || !state.height) return;
    const zoom = state.zoom || 1;
    setEffectiveScroll(viewX * zoom, viewY * zoom);
  }

  function getOverviewHandleAt(point) {
    if (!overviewRect) return null;
    const handleSize = overviewRect.handleSize || 8;
    const threshold = handleSize;
    for (const handle of overviewRect.handles) {
      if (Math.abs(point.x - handle.x) <= threshold && Math.abs(point.y - handle.y) <= threshold) {
        return handle.name;
      }
    }
    return null;
  }

  function getAnchorForHandle(view, handle, keepCenter) {
    if (keepCenter) {
      return { x: view.viewX + view.viewW / 2, y: view.viewY + view.viewH / 2 };
    }
    switch (handle) {
      case "nw":
        return { x: view.viewX + view.viewW, y: view.viewY + view.viewH };
      case "ne":
        return { x: view.viewX, y: view.viewY + view.viewH };
      case "se":
        return { x: view.viewX, y: view.viewY };
      case "sw":
        return { x: view.viewX + view.viewW, y: view.viewY };
      default:
        return null;
    }
  }

  function resizeViewFromHandle(point, handle, keepCenter) {
    if (!overviewState.anchor || !state.width || !state.height) return;
    const anchor = overviewState.anchor;
    const aspect = canvasWrap.clientWidth / canvasWrap.clientHeight || 1;
    let width;
    let height;

    if (keepCenter) {
      const dx = Math.abs(point.x - anchor.x);
      const dy = Math.abs(point.y - anchor.y);
      if (handle === "n" || handle === "s") {
        height = dy * 2;
        width = height * aspect;
      } else if (handle === "e" || handle === "w") {
        width = dx * 2;
        height = width / aspect;
      } else {
        width = dx * 2;
        height = dy * 2;
        if (width / height > aspect) {
          height = width / aspect;
        } else {
          width = height * aspect;
        }
      }
    } else if (handle === "n" || handle === "s") {
      height = Math.abs(point.y - anchor.y);
      width = height * aspect;
    } else if (handle === "e" || handle === "w") {
      width = Math.abs(point.x - anchor.x);
      height = width / aspect;
    } else {
      width = Math.abs(point.x - anchor.x);
      height = Math.abs(point.y - anchor.y);
      if (width / height > aspect) {
        height = width / aspect;
      } else {
        width = height * aspect;
      }
    }

    if (!isFinite(width) || !isFinite(height) || width <= 0 || height <= 0) return;
    const minViewW = Math.max(30, state.width * 0.02);
    if (width < minViewW) {
      width = minViewW;
      height = width / aspect;
    }
    width = Math.min(width, state.width);
    height = Math.min(height, state.height);

    let viewX;
    let viewY;
    if (keepCenter) {
      viewX = anchor.x - width / 2;
      viewY = anchor.y - height / 2;
    } else {
      switch (handle) {
        case "nw":
          viewX = anchor.x - width;
          viewY = anchor.y - height;
          break;
        case "ne":
          viewX = anchor.x;
          viewY = anchor.y - height;
          break;
        case "se":
          viewX = anchor.x;
          viewY = anchor.y;
          break;
        case "sw":
          viewX = anchor.x - width;
          viewY = anchor.y;
          break;
        case "n":
          viewX = anchor.x - width / 2;
          viewY = anchor.y - height;
          break;
        case "s":
          viewX = anchor.x - width / 2;
          viewY = anchor.y;
          break;
        case "e":
          viewX = anchor.x;
          viewY = anchor.y - height / 2;
          break;
        case "w":
          viewX = anchor.x - width;
          viewY = anchor.y - height / 2;
          break;
        default:
          return;
      }
    }

    viewX = Math.max(0, Math.min(state.width - width, viewX));
    viewY = Math.max(0, Math.min(state.height - height, viewY));

    const zoomX = canvasWrap.clientWidth / width;
    const zoomY = canvasWrap.clientHeight / height;
    const zoom = Math.min(6, Math.max(0.5, Math.min(zoomX, zoomY)));
    setZoom(zoom);
    window.requestAnimationFrame(() => {
      scrollToView(viewX, viewY);
      scheduleOverview();
    });
  }

  function drawOverview() {
    if (!overviewCanvas || !overviewCtx) return;
    const metrics = getOverviewMetrics();
    if (!metrics) return;
    const { width, height, imgW, imgH, scale, offsetX, offsetY } = metrics;
    const dpr = window.devicePixelRatio || 1;
    overviewCanvas.width = Math.max(1, Math.floor(width * dpr));
    overviewCanvas.height = Math.max(1, Math.floor(height * dpr));
    overviewCtx.setTransform(dpr, 0, 0, dpr, 0, 0);
    overviewCtx.fillStyle = PLOT_THEME.bg;
    overviewCtx.fillRect(0, 0, width, height);

    if (!state.hasFrame || !state.width || !state.height) {
      overviewCtx.strokeStyle = PLOT_THEME.frame;
      overviewCtx.strokeRect(0.5, 0.5, width - 1, height - 1);
      overviewCtx.fillStyle = "rgba(220, 232, 250, 0.7)";
      overviewCtx.font = '500 10px "Avenir Next", "Segoe UI", "Helvetica Neue", Arial, sans-serif';
      overviewCtx.textAlign = "center";
      overviewCtx.textBaseline = "middle";
      overviewCtx.fillText("No image", width / 2, height / 2);
      overviewRect = null;
      return;
    }

    const drawW = imgW * scale;
    const drawH = imgH * scale;
    overviewCtx.drawImage(canvas, 0, 0, imgW, imgH, offsetX, offsetY, drawW, drawH);

    const view = getViewRect();
    if (!view) {
      overviewRect = null;
      return;
    }
    const rectX = offsetX + view.viewX * scale;
    const rectY = offsetY + view.viewY * scale;
    const rectW = view.viewW * scale;
    const rectH = view.viewH * scale;

    overviewCtx.fillStyle = "rgba(0, 0, 0, 0.35)";
    overviewCtx.fillRect(offsetX, offsetY, drawW, drawH);
    overviewCtx.drawImage(
      canvas,
      view.viewX,
      view.viewY,
      view.viewW,
      view.viewH,
      rectX,
      rectY,
      rectW,
      rectH,
    );

    overviewCtx.strokeStyle = "rgba(0, 0, 0, 0.65)";
    overviewCtx.lineWidth = 3;
    overviewCtx.strokeRect(rectX, rectY, rectW, rectH);
    overviewCtx.strokeStyle = "rgba(140, 210, 255, 0.95)";
    overviewCtx.lineWidth = 1.5;
    overviewCtx.strokeRect(rectX, rectY, rectW, rectH);
    overviewCtx.fillStyle = "rgba(110, 181, 255, 0.12)";
    overviewCtx.fillRect(rectX, rectY, rectW, rectH);

    const handleSize = 7;
    const half = handleSize / 2;
    const handles = [
      { name: "nw", x: rectX, y: rectY },
      { name: "ne", x: rectX + rectW, y: rectY },
      { name: "se", x: rectX + rectW, y: rectY + rectH },
      { name: "sw", x: rectX, y: rectY + rectH },
    ];
    overviewCtx.fillStyle = "rgba(220, 245, 255, 0.95)";
    overviewCtx.strokeStyle = "rgba(10, 20, 30, 0.8)";
    overviewCtx.lineWidth = 1;
    handles.forEach((handle) => {
      overviewCtx.fillRect(handle.x - half, handle.y - half, handleSize, handleSize);
      overviewCtx.strokeRect(handle.x - half, handle.y - half, handleSize, handleSize);
    });

    overviewRect = { rectX, rectY, rectW, rectH, handles, handleSize, view, metrics };
  }

  function setZoom(value, options = {}) {
    const { clampPan = true } = options;
    const minZoom = getMinZoom();
    const clamped = Math.max(minZoom, Math.min(MAX_ZOOM, Number(value)));
    state.zoom = clamped;
    const offsetX =
      canvasWrap && state.width
        ? Math.max(0, (canvasWrap.clientWidth - state.width * clamped) / 2)
        : 0;
    const offsetY =
      canvasWrap && state.height
        ? Math.max(0, (canvasWrap.clientHeight - state.height * clamped) / 2)
        : 0;
    state.renderOffsetX = Number.isFinite(offsetX) ? offsetX : 0;
    state.renderOffsetY = Number.isFinite(offsetY) ? offsetY : 0;
    if (clampPan) {
      clampPanOffsetsToBounds();
    }
    applyCanvasTransform();
    updatePanCapability();
    if (zoomRange) {
      zoomRange.min = String(minZoom);
      zoomRange.value = String(clamped);
    }
    if (zoomValue) {
      const zoomLabel = `${clamped.toFixed(1)}x`;
      if ("value" in zoomValue) {
        zoomValue.value = zoomLabel;
      } else {
        zoomValue.textContent = zoomLabel;
      }
    }
    updateViewerFooter();
    schedulePixelOverlay();
    scheduleRoiOverlay();
    scheduleResolutionOverlay();
    schedulePeakOverlay();
  }

  function zoomAt(clientX, clientY, nextZoom) {
    if (!canvasWrap) {
      setZoom(nextZoom);
      return;
    }
    const rect = canvasWrap.getBoundingClientRect();
    const x = Math.max(0, Math.min(rect.width || 0, clientX - rect.left));
    const y = Math.max(0, Math.min(rect.height || 0, clientY - rect.top));
    const prevZoom = state.zoom || 1;
    const prevOffsetX = state.renderOffsetX || 0;
    const prevOffsetY = state.renderOffsetY || 0;
    const prevEffectiveLeft = getEffectiveScrollLeft();
    const prevEffectiveTop = getEffectiveScrollTop();
    const focusX = x;
    const focusY = y;
    const rawWorldX = (prevEffectiveLeft + focusX - prevOffsetX) / prevZoom;
    const rawWorldY = (prevEffectiveTop + focusY - prevOffsetY) / prevZoom;
    const worldX = Number.isFinite(state.width)
      ? Math.max(0, Math.min(state.width, rawWorldX))
      : rawWorldX;
    const worldY = Number.isFinite(state.height)
      ? Math.max(0, Math.min(state.height, rawWorldY))
      : rawWorldY;

    setZoom(nextZoom, { clampPan: false });

    const newOffsetX = state.renderOffsetX || 0;
    const newOffsetY = state.renderOffsetY || 0;
    const targetEffectiveX = worldX * state.zoom - focusX + newOffsetX;
    const targetEffectiveY = worldY * state.zoom - focusY + newOffsetY;
    setEffectiveScroll(targetEffectiveX, targetEffectiveY, false, false);
    syncViewportOverlays();
  }

  function normalizeWheelDelta(event) {
    let delta = event.deltaY;
    if (event.deltaMode === 1) {
      delta *= 16;
    } else if (event.deltaMode === 2) {
      delta *= 120;
    }
    if (!Number.isFinite(delta)) return 0;
    return Math.max(-200, Math.min(200, delta));
  }

  function touchDistance(t0, t1) {
    const dx = t1.clientX - t0.clientX;
    const dy = t1.clientY - t0.clientY;
    return Math.hypot(dx, dy);
  }

  function touchMidpoint(t0, t1) {
    return {
      x: (t0.clientX + t1.clientX) * 0.5,
      y: (t0.clientY + t1.clientY) * 0.5,
    };
  }

  function startTouchGesture(touches) {
    if (!canvasWrap || touches.length < 2) return;
    const t0 = touches[0];
    const t1 = touches[1];
    touchGestureDistance = touchDistance(t0, t1);
    touchGestureMid = touchMidpoint(t0, t1);
    touchGestureActive = true;
    canvasWrap.classList.add("is-panning");
    deferViewportInteraction();
  }

  function stopTouchGesture() {
    touchGestureActive = false;
    touchGestureDistance = 0;
    touchGestureMid = null;
    if (canvasWrap) {
      canvasWrap.classList.remove("is-panning");
    }
  }

  function updateTouchGesture(touches) {
    if (!canvasWrap || touches.length < 2) return;
    const t0 = touches[0];
    const t1 = touches[1];
    const nextDistance = touchDistance(t0, t1);
    const nextMid = touchMidpoint(t0, t1);
    if (!Number.isFinite(nextDistance) || nextDistance <= 0) return;
    deferViewportInteraction();

    if (!touchGestureActive || !touchGestureMid || touchGestureDistance <= 0) {
      touchGestureDistance = nextDistance;
      touchGestureMid = nextMid;
      touchGestureActive = true;
      return;
    }

    const minZoom = getMinZoom();
    const scale = Math.max(0.25, Math.min(4, nextDistance / touchGestureDistance));
    const nextZoom = Math.max(minZoom, Math.min(MAX_ZOOM, (state.zoom || 1) * scale));
    zoomAt(nextMid.x, nextMid.y, nextZoom);

    const dx = nextMid.x - touchGestureMid.x;
    const dy = nextMid.y - touchGestureMid.y;
    if (dx || dy) {
      const nextEffectiveX = getEffectiveScrollLeft() - dx;
      const nextEffectiveY = getEffectiveScrollTop() - dy;
      setEffectiveScroll(nextEffectiveX, nextEffectiveY, true, false);
    }

    touchGestureDistance = nextDistance;
    touchGestureMid = nextMid;
  }

  function queueWheelZoom(delta, clientX, clientY) {
    if (!delta) return;
    const zoomBase = state.zoom ?? 1;
    const factor = Math.exp(-delta * 0.002);
    const minZoom = getMinZoom();
    const nextZoom = Math.max(minZoom, Math.min(MAX_ZOOM, zoomBase * factor));
    zoomAt(clientX, clientY, nextZoom);
  }

  function fitImageToView() {
    if (!canvasWrap || !state.width || !state.height) return;
    const scale = Math.min(
      canvasWrap.clientWidth / state.width,
      canvasWrap.clientHeight / state.height,
    );
    if (!Number.isFinite(scale) || scale <= 0) return;
    setZoom(scale);
    state.panOffsetX = 0;
    state.panOffsetY = 0;
    setEffectiveScroll(0, 0, false);
    syncViewportOverlays();
  }

  function isTouchGestureActive() {
    return touchGestureActive;
  }

  return {
    getMinZoom,
    getEffectiveScrollLeft,
    getEffectiveScrollTop,
    setEffectiveScroll,
    updatePanCapability,
    applyCanvasTransform,
    isViewportInteractionActive,
    deferViewportInteraction,
    setZoom,
    zoomAt,
    normalizeWheelDelta,
    queueWheelZoom,
    startTouchGesture,
    updateTouchGesture,
    stopTouchGesture,
    isTouchGestureActive,
    fitImageToView,
    scheduleOverview,
    drawOverview,
    getViewRect,
    overviewEventToImage,
    overviewEventToOverview,
    getOverviewHandleAt,
    getAnchorForHandle,
    resizeViewFromHandle,
    panToImageCenter,
  };
}
