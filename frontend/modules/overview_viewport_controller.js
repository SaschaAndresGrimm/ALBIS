/**
 * Overview rendering and viewport interaction orchestration.
 */

import { canvasFont } from "./canvas_fonts.js";
import { syncRangeFill } from "./range_fill.js";
import { zoomToSlider } from "./zoom_slider.js";
import {
  axisScroll,
  centerFromScroll,
  centerInBox,
  clampAxisCenter,
  fitZoom,
  visibleAxisSpan,
  zoomAxisAbout,
} from "./viewport_math.js";

export function createOverviewViewportController({
  state,
  overviewState,
  elements,
  constants,
  theme,
  callbacks,
  core,
}) {
  const {
    canvasWrap,
    canvas,
    overviewCanvas,
    overviewCtx,
    zoomRange,
    zoomValue,
    viewerFooterEl,
    toolsPanel,
    appChrome,
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
    scheduleRoiOverlay,
    requestFrame,
    hasPendingFrameRequest: hasPendingFrameRequestCallback,
    consumePendingFrameRequest: consumePendingFrameRequestCallback,
    isFrameLoading: isFrameLoadingCallback,
    updateViewerFooter,
    onViewportChanged,
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
  let viewportChangeSuppressCount = 0;

  function getMinZoom() {
    if (!canvasWrap || !state.width || !state.height) {
      return MIN_ZOOM;
    }
    const aspect = state.pixelAspect || 1;
    const fitScale = Math.min(
      canvasWrap.clientWidth / state.width,
      canvasWrap.clientHeight / (state.height * aspect),
    );
    if (!Number.isFinite(fitScale) || fitScale <= 0) {
      return MIN_ZOOM;
    }
    // Allow zooming out beyond fit-to-window for better context.
    return Math.max(MIN_ZOOM, Math.min(1, fitScale * 0.1));
  }

  /**
   * The part of the canvas the image is laid out in, per axis, in canvas
   * pixels. Horizontally it leaves out a side panel floating over the canvas;
   * vertically Fit centres in the full height, but the strip of image the pan
   * limit keeps in view must be above the footer floating over it. Measured
   * whenever the zoom is set (which a resize and a panel change both do) and
   * kept, because the overlays ask for the scroll position many times a frame.
   */
  let viewArea = null;

  /** Room left between the image and a control floating over the canvas. */
  const FIT_GAP_PX = 8;

  function measureViewArea() {
    if (!canvasWrap) return null;
    const width = canvasWrap.clientWidth || 0;
    const height = canvasWrap.clientHeight || 0;
    const insets = panelInsets();
    const footerInset = footerTopInset();
    viewArea = {
      x: { start: insets.left, end: Math.max(insets.left, width - insets.right) },
      y: { start: 0, end: height, edgeEnd: Math.max(0, height - footerInset) },
    };
    return viewArea;
  }

  /**
   * Canvas pixels from the top of the footer to the bottom of the canvas, plus
   * a gap; the footer floats a little above the bottom edge, so its height
   * alone falls short of what it covers.
   */
  function footerTopInset() {
    if (!viewerFooterEl) return 0;
    const footer = viewerFooterEl.getBoundingClientRect();
    if (!(footer.height > 0)) return 0;
    const wrap = canvasWrap.getBoundingClientRect();
    const covered = Number.isFinite(footer.top) && wrap.height > 0
      ? Math.round(wrap.bottom - footer.top)
      : Math.round(footer.height);
    return covered > 0 ? covered + FIT_GAP_PX : 0;
  }

  function getViewArea() {
    return viewArea || measureViewArea();
  }

  function viewScales() {
    const zoom = Number.isFinite(state.zoom) && state.zoom > 0 ? state.zoom : 1;
    return { x: zoom, y: zoom * (state.pixelAspect || 1) };
  }

  /** The image point at the middle of the visible area; the image centre by default. */
  function currentCenter() {
    return {
      x: Number.isFinite(state.viewCenterX) ? state.viewCenterX : (state.width || 0) / 2,
      y: Number.isFinite(state.viewCenterY) ? state.viewCenterY : (state.height || 0) / 2,
    };
  }

  function clampCenter(center, area = getViewArea()) {
    const scales = viewScales();
    return {
      x: clampAxisCenter(center.x, { size: state.width, scale: scales.x, axis: area.x }),
      y: clampAxisCenter(center.y, { size: state.height, scale: scales.y, axis: area.y }),
    };
  }

  /**
   * Screen pixels between the canvas origin and image pixel 0, negated -- the
   * "effective scroll" the overlays map with. The canvas no longer scrolls;
   * the image is placed by a transform alone, and this is derived from the
   * view centre.
   */
  function getEffectiveScrollLeft() {
    const area = getViewArea();
    if (!area || !state.width) return 0;
    return axisScroll(currentCenter().x, { scale: viewScales().x, axis: area.x });
  }

  function getEffectiveScrollTop() {
    const area = getViewArea();
    if (!area || !state.height) return 0;
    return axisScroll(currentCenter().y, { scale: viewScales().y, axis: area.y });
  }

  function applyCanvasTransform() {
    if (!canvas) return;
    const zoom = Number.isFinite(state.zoom) ? state.zoom : 1;
    const aspect = state.pixelAspect || 1;
    // Something (a focus, the browser restoring a position) may scroll the
    // container; the transform alone places the image, so undo it.
    if (canvasWrap && (canvasWrap.scrollLeft || canvasWrap.scrollTop)) {
      canvasWrap.scrollLeft = 0;
      canvasWrap.scrollTop = 0;
    }
    const tx = -getEffectiveScrollLeft();
    const ty = -getEffectiveScrollTop();
    // Non-uniform scale stretches each data pixel to its physical aspect ratio
    // (X is the reference axis; Y is scaled by pixelAspect).
    canvas.style.transform = `translate(${tx}px, ${ty}px) scale(${zoom}, ${zoom * aspect})`;
  }

  function notifyViewportChanged(reason) {
    if (viewportChangeSuppressCount > 0) return;
    if (!state.hasFrame || !state.width || !state.height) return;
    onViewportChanged?.({ reason });
  }

  function withViewportChangeSuppressed(callback) {
    viewportChangeSuppressCount += 1;
    try {
      return callback();
    } finally {
      viewportChangeSuppressCount = Math.max(0, viewportChangeSuppressCount - 1);
    }
  }

  /**
   * Put an image point at the middle of the view, as near as the pan limits
   * allow. Every change of position ends here.
   */
  function applyViewCenter(center, { schedule = true, reason = "pan" } = {}) {
    if (!state.width || !state.height || !getViewArea()) return;
    const clamped = clampCenter(center);
    state.viewCenterX = clamped.x;
    state.viewCenterY = clamped.y;
    state.renderOffsetX = 0;
    state.renderOffsetY = 0;
    applyCanvasTransform();
    if (schedule) {
      core.overlayRender.deferPixelOverlayRedraw();
      syncViewportOverlays();
    }
    notifyViewportChanged(reason);
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
    core.overlayRender.schedulePixelOverlay();
    scheduleRoiOverlay();
    core.overlayRender.scheduleResolutionOverlay();
    core.overlayRender.schedulePeakOverlay();
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
    core.overlayRender.deferPixelOverlayRedraw(delay);
    if (state.playing && isFrameLoading()) {
      core.fileDataPipeline?.cancelActiveLoad();
    }
    if (viewportInteractionResumeTimer) {
      window.clearTimeout(viewportInteractionResumeTimer);
    }
    viewportInteractionResumeTimer = window.setTimeout(() => {
      viewportInteractionResumeTimer = null;
      flushViewportDeferredWork();
    }, delay + 12);
  }

  function setEffectiveScroll(targetX, targetY, schedule = true) {
    const area = getViewArea();
    if (!area) return;
    const scales = viewScales();
    applyViewCenter(
      {
        x: centerFromScroll(Number(targetX) || 0, { scale: scales.x, axis: area.x }),
        y: centerFromScroll(Number(targetY) || 0, { scale: scales.y, axis: area.y }),
      },
      { schedule },
    );
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
    const aspect = state.pixelAspect || 1;
    // Fit the physically stretched image (height scaled by aspect) into the
    // overview box. scale maps data-X pixels; scaleY maps data-Y pixels.
    const scale = Math.min(width / imgW, height / (imgH * aspect));
    const scaleY = scale * aspect;
    const drawW = imgW * scale;
    const drawH = imgH * scaleY;
    const offsetX = (width - drawW) / 2;
    const offsetY = (height - drawH) / 2;
    return { width, height, imgW, imgH, scale, scaleY, offsetX, offsetY };
  }

  /** The part of the image in view, in image pixels: what the minimap outlines. */
  function getViewRect() {
    const imgW = state.width;
    const imgH = state.height;
    const area = getViewArea();
    if (!imgW || !imgH || !area) return null;
    const scales = viewScales();
    const center = currentCenter();
    const x = visibleAxisSpan(center.x, { size: imgW, scale: scales.x, axis: area.x });
    const y = visibleAxisSpan(center.y, { size: imgH, scale: scales.y, axis: area.y });
    return {
      viewX: x.from,
      viewY: y.from,
      viewW: x.length,
      viewH: y.length,
      scaleX: scales.x,
      scaleY: scales.y,
    };
  }

  function overviewEventToImage(event) {
    if (!overviewCanvas || !state.hasFrame || !state.width || !state.height) return null;
    const metrics = getOverviewMetrics();
    if (!metrics) return null;
    const rect = overviewCanvas.getBoundingClientRect();
    const x = (event.clientX - rect.left) * (metrics.width / rect.width);
    const y = (event.clientY - rect.top) * (metrics.height / rect.height);
    const imgX = (x - metrics.offsetX) / metrics.scale;
    const imgY = (y - metrics.offsetY) / metrics.scaleY;
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
    applyViewCenter({ x, y });
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
      // An edge resizes from the middle of the side across from it.
      case "n":
        return { x: view.viewX + view.viewW / 2, y: view.viewY + view.viewH };
      case "s":
        return { x: view.viewX + view.viewW / 2, y: view.viewY };
      case "e":
        return { x: view.viewX, y: view.viewY + view.viewH / 2 };
      case "w":
        return { x: view.viewX + view.viewW, y: view.viewY + view.viewH / 2 };
      default:
        return null;
    }
  }

  function resizeViewFromHandle(point, handle, keepCenter) {
    if (!overviewState.anchor || !state.width || !state.height) return;
    const anchor = overviewState.anchor;
    const pixelAspect = state.pixelAspect || 1;
    const area = measureViewArea();
    if (!area) return;
    const areaWidth = area.x.end - area.x.start;
    const areaHeight = area.y.end - area.y.start;
    // anchor/point are in data-pixel space; the view rectangle should keep the
    // visible area's on-screen shape, so the data-space aspect folds in pixelAspect.
    const aspect = (areaWidth / areaHeight || 1) * pixelAspect;
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

    // The same zoom range as every other control, and zoom and position in
    // one step, so the rectangle follows the handle without a jump.
    const zoom = Math.min(areaWidth / width, areaHeight / (height * pixelAspect));
    setZoom(zoom, { center: { x: viewX + width / 2, y: viewY + height / 2 } });
  }

  function drawOverview() {
    if (!overviewCanvas || !overviewCtx) return;
    const metrics = getOverviewMetrics();
    if (!metrics) return;
    const { width, height, imgW, imgH, scale, scaleY, offsetX, offsetY } = metrics;
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
      overviewCtx.font = canvasFont(10, 500);
      overviewCtx.textAlign = "center";
      overviewCtx.textBaseline = "middle";
      overviewCtx.fillText("No image", width / 2, height / 2);
      overviewRect = null;
      return;
    }

    const drawW = imgW * scale;
    const drawH = imgH * scaleY;
    overviewCtx.drawImage(canvas, 0, 0, imgW, imgH, offsetX, offsetY, drawW, drawH);

    const view = getViewRect();
    if (!view) {
      overviewRect = null;
      return;
    }
    const rectX = offsetX + view.viewX * scale;
    const rectY = offsetY + view.viewY * scaleY;
    const rectW = view.viewW * scale;
    const rectH = view.viewH * scaleY;

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
      { name: "n", x: rectX + rectW / 2, y: rectY },
      { name: "e", x: rectX + rectW, y: rectY + rectH / 2 },
      { name: "s", x: rectX + rectW / 2, y: rectY + rectH },
      { name: "w", x: rectX, y: rectY + rectH / 2 },
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

  /**
   * How much of the canvas the side panel is sitting on top of, per edge.
   *
   * The panel has two modes. Docked, it owns a grid column, so `canvasWrap`
   * already ends where the panel begins and nothing is covered. In
   * canvas-first mode it is `position: absolute` over the canvas, and
   * `clientWidth` still counts the strip underneath it -- so centring the
   * image in the full width slid it under the panel and left an equal band of
   * empty background on the opposite side. Measuring the real overlap handles
   * both modes, the collapsed state, and the narrow-viewport case where the
   * panel spans everything.
   */
  function panelInsets() {
    const none = { left: 0, right: 0 };
    if (!toolsPanel || !canvasWrap) return none;
    if (typeof window === "undefined" || typeof window.getComputedStyle !== "function") return none;
    const style = window.getComputedStyle(toolsPanel);
    // Only a panel taken out of flow can overlap; a docked one cannot.
    if (style.position !== "absolute" && style.position !== "fixed") return none;
    if (style.display === "none" || style.visibility === "hidden") return none;
    const panel = toolsPanel.getBoundingClientRect();
    const wrap = canvasWrap.getBoundingClientRect();
    if (!(panel.width > 0) || !(wrap.width > 0)) return none;
    const overlap = Math.min(wrap.right, panel.right) - Math.max(wrap.left, panel.left);
    if (!(overlap > 0)) return none;
    // Charge the overlap to whichever edge the panel is anchored nearer, and
    // concede everything from the panel to that edge: the gutter between them
    // is no use to the image either, and leaving it out let Fit run the image
    // under the panel by the gutter's width.
    const anchoredRight = panel.left - wrap.left >= wrap.right - panel.right;
    const reach = anchoredRight ? wrap.right - panel.left : panel.right - wrap.left;
    // Never concede more than half the canvas. Past that the correction costs
    // more room than the occlusion it is compensating for.
    const conceded = Math.min(reach, wrap.width / 2);
    return anchoredRight ? { left: 0, right: conceded } : { left: conceded, right: 0 };
  }

  /**
   * Canvas pixels the menu bar and toolbar cover at the top of the canvas
   * when they float over it (the canvas-first layout); 0 when they sit above.
   */
  function chromeTopInset() {
    if (!appChrome || !canvasWrap) return 0;
    const bar = appChrome.getBoundingClientRect();
    const wrap = canvasWrap.getBoundingClientRect();
    if (!(bar.height > 0) || !(wrap.height > 0)) return 0;
    if (!(Math.min(wrap.right, bar.right) > Math.max(wrap.left, bar.left))) return 0;
    const covered = bar.bottom - wrap.top;
    return covered > 0 ? Math.min(covered, wrap.height / 2) : 0;
  }

  function clampZoom(value) {
    const zoom = Number(value);
    const safe = Number.isFinite(zoom) && zoom > 0 ? zoom : state.zoom || 1;
    return Math.max(getMinZoom(), Math.min(MAX_ZOOM, safe));
  }

  /**
   * Set the zoom about the middle of the visible area -- or about `center`, an
   * image point to put there -- and re-measure the area, so a resize or a
   * panel change keeps the same image point in the middle.
   */
  function setZoom(value, { center = null } = {}) {
    const minZoom = getMinZoom();
    const clamped = clampZoom(value);
    const target = center || currentCenter();
    state.zoom = clamped;
    measureViewArea();
    withViewportChangeSuppressed(() => {
      applyViewCenter(target, { schedule: false });
    });
    applyCanvasTransform();
    updatePanCapability();
    if (zoomRange) {
      zoomRange.max = String(zoomToSlider(MAX_ZOOM));
      zoomRange.min = String(zoomToSlider(minZoom));
      zoomRange.value = String(zoomToSlider(clamped));
      syncRangeFill(zoomRange);
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
    syncViewportOverlays();
    notifyViewportChanged("zoom");
  }

  /**
   * Zoom keeping the image point under (clientX, clientY) where it is, also
   * when that point is off the image; only the pan limits can move it.
   */
  function zoomAt(clientX, clientY, nextZoom) {
    if (!canvasWrap) {
      setZoom(nextZoom);
      return;
    }
    const rect = canvasWrap.getBoundingClientRect();
    const x = Math.max(0, Math.min(rect.width || 0, clientX - rect.left));
    const y = Math.max(0, Math.min(rect.height || 0, clientY - rect.top));
    const area = measureViewArea();
    const from = viewScales();
    const center = currentCenter();
    const zoom = clampZoom(nextZoom);
    const toY = zoom * (state.pixelAspect || 1);
    setZoom(zoom, {
      center: {
        x: zoomAxisAbout(center.x, { point: x, fromScale: from.x, toScale: zoom, axis: area.x }),
        y: zoomAxisAbout(center.y, { point: y, fromScale: from.y, toScale: toY, axis: area.y }),
      },
    });
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
      setEffectiveScroll(nextEffectiveX, nextEffectiveY, true);
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
    // Without a pointer (the minimap), about the middle of the view.
    if (Number.isFinite(clientX) && Number.isFinite(clientY)) {
      zoomAt(clientX, clientY, nextZoom);
    } else {
      setZoom(nextZoom);
    }
  }

  /**
   * The part of the view area nothing floats over: below the menu bar and
   * toolbar, above the footer, beside the panel. A side with a control on it
   * keeps FIT_GAP_PX from that control; a bare canvas edge keeps none.
   */
  function measureFitBox(area) {
    const width = canvasWrap.clientWidth || 0;
    const top = chromeTopInset();
    const gap = (covered) => (covered ? FIT_GAP_PX : 0);
    const x = {
      start: area.x.start + gap(area.x.start > 0),
      end: area.x.end - gap(area.x.end < width),
    };
    // edgeEnd already stops short of the footer by its own gap.
    const y = { start: top > 0 ? top + FIT_GAP_PX : 0, end: area.y.edgeEnd ?? area.y.end };
    // A box squeezed to nothing would fit nothing; fall back to the whole area.
    return {
      x: x.end - x.start > 0 ? x : area.x,
      y: y.end - y.start > 0 ? y : area.y,
    };
  }

  /** The whole image, centred in the part of the view nothing floats over. */
  function fitImageToView() {
    if (!canvasWrap || !state.width || !state.height) return;
    const area = measureViewArea();
    const box = measureFitBox(area);
    const aspect = state.pixelAspect || 1;
    const zoom = fitZoom({
      imageWidth: state.width,
      imageHeight: state.height,
      pixelAspect: aspect,
      width: box.x.end - box.x.start,
      height: box.y.end - box.y.start,
    });
    if (!zoom) return;
    const center = {
      x: centerInBox({ size: state.width, scale: zoom, axis: area.x, box: box.x }),
      y: centerInBox({ size: state.height, scale: zoom * aspect, axis: area.y, box: box.y }),
    };
    withViewportChangeSuppressed(() => {
      setZoom(zoom, { center });
    });
    notifyViewportChanged("fit");
  }

  function getViewCenter() {
    if (!state.width || !state.height) return null;
    const center = currentCenter();
    return { centerX: center.x, centerY: center.y, zoom: state.zoom || 1 };
  }

  function setViewCenter(centerX, centerY, zoom = state.zoom) {
    setZoom(zoom, { center: { x: centerX, y: centerY } });
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
    getViewCenter,
    setViewCenter,
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
