/**
 * ROI edit interactions and overlay rendering.
 */

import {
  applyCircularRoiGeometry,
  clampCircularRoiCenterDelta,
  clampCircularRoiInnerRadius,
  getCircularRoiDirection,
  getCircularRoiOuterRadius,
  getVisibleCircularHandlePoint,
  physicalRoiRadius,
} from "./roi_geometry_utils.js";

export function createRoiInteractionController({
  state,
  roiState,
  elements,
  callbacks,
}) {
  const {
    canvasWrap,
    roiOverlay,
    roiCtx,
    roiRadiusInput,
    roiInnerInput,
    roiOuterInput,
  } = elements;

  const {
    getEffectiveScrollLeft,
    getEffectiveScrollTop,
    syncOverlayCanvas,
    updateRoiCenterInputs,
    updateRoiStats,
    handleRoiChanged,
  } = callbacks;

  let roiOverlayScheduled = false;
  let roiUpdateScheduled = false;
  let roiEditing = false;
  let roiEditHandle = null;
  let roiEditStart = null;
  let roiEditSnapshot = null;

  function getPointerCanvasPos(event) {
    if (!canvasWrap) return null;
    const rect = canvasWrap.getBoundingClientRect();
    return { x: event.clientX - rect.left, y: event.clientY - rect.top };
  }

  function getRoiScreenGeometry() {
    if (!canvasWrap || !roiState.start || !roiState.end) return null;
    const zoom = state.zoom || 1;
    const zoomY = zoom * (state.pixelAspect || 1);
    const offsetX = state.renderOffsetX || 0;
    const offsetY = state.renderOffsetY || 0;
    const viewX = getEffectiveScrollLeft() / zoom;
    const viewY = getEffectiveScrollTop() / zoomY;
    const x0 = (roiState.start.x - viewX) * zoom + offsetX;
    const y0 = (roiState.start.y - viewY) * zoomY + offsetY;
    const x1 = (roiState.end.x - viewX) * zoom + offsetX;
    const y1 = (roiState.end.y - viewY) * zoomY + offsetY;
    return { x0, y0, x1, y1, zoom, zoomY };
  }

  function getVisibleImageScreenRect(canvasWidth, canvasHeight) {
    if (!canvasWrap || !state.width || !state.height) return null;
    const zoom = state.zoom || 1;
    const zoomY = zoom * (state.pixelAspect || 1);
    const offsetX = state.renderOffsetX || 0;
    const offsetY = state.renderOffsetY || 0;
    const viewX = getEffectiveScrollLeft() / zoom;
    const viewY = getEffectiveScrollTop() / zoomY;
    const imageLeft = (-viewX) * zoom + offsetX;
    const imageTop = (-viewY) * zoomY + offsetY;
    const imageRight = imageLeft + state.width * zoom;
    const imageBottom = imageTop + state.height * zoomY;
    const rect = {
      left: Math.max(0, imageLeft),
      top: Math.max(0, imageTop),
      right: Math.min(canvasWidth, imageRight),
      bottom: Math.min(canvasHeight, imageBottom),
    };
    if (rect.left > rect.right || rect.top > rect.bottom) return null;
    return rect;
  }

  function getBoxScreenBounds(x0, y0, x1, y1, zoom, zoomY = zoom) {
    // x0/y0/x1/y1 are pixel-cell origins in screen space (top-left corners).
    // Box ROI is inclusive in pixel indices, so the visual boundary must extend
    // one full pixel beyond the max index (one cell wide in X, one tall in Y).
    return {
      left: Math.min(x0, x1),
      top: Math.min(y0, y1),
      right: Math.max(x0, x1) + zoom,
      bottom: Math.max(y0, y1) + zoomY,
    };
  }

  function getLineScreenEndpoints(x0, y0, x1, y1, zoom, zoomY = zoom) {
    return {
      xStart: x0 + zoom * 0.5,
      yStart: y0 + zoomY * 0.5,
      xEnd: x1 + zoom * 0.5,
      yEnd: y1 + zoomY * 0.5,
    };
  }

  function getCircularHandleScreenPoints(x0, y0, x1, y1, zoom, canvasWidth, canvasHeight) {
    const visibleRect = getVisibleImageScreenRect(canvasWidth, canvasHeight);
    // The shell is a true circle on the isotropic screen, so its screen radius
    // is the (X-pixel-equivalent) radius * zoom. `end` only supplies direction.
    const aspect = state.pixelAspect || 1;
    const direction = { x: x1 - x0, y: y1 - y0 };
    const outerRadius = getCircularRoiOuterRadius(roiState, aspect) * zoom;
    const mag = Math.hypot(direction.x, direction.y) || 1;
    const fallback = {
      x: x0 + (direction.x / mag) * outerRadius,
      y: y0 + (direction.y / mag) * outerRadius,
    };
    const outer =
      getVisibleCircularHandlePoint({ x: x0, y: y0 }, outerRadius, direction, visibleRect) ||
      fallback;
    let inner = null;
    if (roiState.mode === "annulus" && roiState.innerRadius > 0) {
      inner =
        getVisibleCircularHandlePoint(
          { x: x0, y: y0 },
          roiState.innerRadius * zoom,
          direction,
          visibleRect,
        ) ||
        null;
    }
    return { outer, inner };
  }

  function getRoiHandleAt(event) {
    if (!roiState.enabled || !roiState.active) return null;
    const pointer = getPointerCanvasPos(event);
    const geom = getRoiScreenGeometry();
    if (!pointer || !geom) return null;
    const { x0, y0, x1, y1, zoom, zoomY } = geom;
    const rect = canvasWrap.getBoundingClientRect();
    const hit = (x, y) => Math.abs(pointer.x - x) <= 6 && Math.abs(pointer.y - y) <= 6;

    if (roiState.mode === "line") {
      const lineGeom = getLineScreenEndpoints(x0, y0, x1, y1, zoom, zoomY);
      if (hit(lineGeom.xStart, lineGeom.yStart)) return "line-start";
      if (hit(lineGeom.xEnd, lineGeom.yEnd)) return "line-end";
      return null;
    }
    if (roiState.mode === "box") {
      const { left, top, right, bottom } = getBoxScreenBounds(x0, y0, x1, y1, zoom, zoomY);
      if (hit(left, top)) return "box-nw";
      if (hit(right, top)) return "box-ne";
      if (hit(right, bottom)) return "box-se";
      if (hit(left, bottom)) return "box-sw";
      return null;
    }
    if (roiState.mode === "circle" || roiState.mode === "annulus") {
      if (hit(x0, y0)) return "center";
      const handles = getCircularHandleScreenPoints(x0, y0, x1, y1, zoom, rect.width, rect.height);
      if (handles.outer && hit(handles.outer.x, handles.outer.y)) return "outer";
      if (roiState.mode === "annulus" && handles.inner) {
        if (hit(handles.inner.x, handles.inner.y)) {
          return "inner";
        }
      }
    }
    return null;
  }

  function pointToSegmentDistance(p, a, b) {
    const vx = b.x - a.x;
    const vy = b.y - a.y;
    const wx = p.x - a.x;
    const wy = p.y - a.y;
    const c1 = vx * wx + vy * wy;
    if (c1 <= 0) return Math.hypot(wx, wy);
    const c2 = vx * vx + vy * vy;
    if (c2 <= c1) return Math.hypot(p.x - b.x, p.y - b.y);
    const t = c1 / c2;
    const projX = a.x + t * vx;
    const projY = a.y + t * vy;
    return Math.hypot(p.x - projX, p.y - projY);
  }

  function isPointInRoi(point) {
    if (!point || !roiState.start || !roiState.end) return false;
    const x0 = roiState.start.x;
    const y0 = roiState.start.y;
    const x1 = roiState.end.x;
    const y1 = roiState.end.y;
    if (roiState.mode === "box") {
      const left = Math.min(x0, x1);
      const right = Math.max(x0, x1);
      const top = Math.min(y0, y1);
      const bottom = Math.max(y0, y1);
      return point.x >= left && point.x <= right && point.y >= top && point.y <= bottom;
    }
    if (roiState.mode === "circle" || roiState.mode === "annulus") {
      // Membership is a physical resolution shell (a circle in physical space,
      // an ellipse in pixel space): compare X-pixel-equivalent radii.
      const aspect = state.pixelAspect || 1;
      const dist = physicalRoiRadius(point.x - x0, point.y - y0, aspect);
      const outer = getCircularRoiOuterRadius(roiState, aspect);
      return dist <= outer;
    }
    if (roiState.mode === "line") {
      const zoom = state.zoom || 1;
      const tol = 6 / zoom;
      const dist = pointToSegmentDistance(point, roiState.start, roiState.end);
      return dist <= tol;
    }
    return false;
  }

  function clampRoiDelta(dx, dy, baseStart = roiState.start, baseEnd = roiState.end) {
    if (!baseStart || !baseEnd || !state.width || !state.height) {
      return { dx, dy };
    }
    if (roiState.mode === "circle" || roiState.mode === "annulus") {
      return clampCircularRoiCenterDelta(dx, dy, baseStart, state.width, state.height);
    }
    const minX = Math.min(baseStart.x, baseEnd.x);
    const maxX = Math.max(baseStart.x, baseEnd.x);
    const minY = Math.min(baseStart.y, baseEnd.y);
    const maxY = Math.max(baseStart.y, baseEnd.y);
    if (minX + dx < 0) dx = -minX;
    if (maxX + dx > state.width - 1) dx = (state.width - 1) - maxX;
    if (minY + dy < 0) dy = -minY;
    if (maxY + dy > state.height - 1) dy = (state.height - 1) - maxY;
    return { dx, dy };
  }

  function startRoiEdit(handle, point) {
    if (!roiState.start || !roiState.end) return;
    roiEditing = true;
    roiEditHandle = handle || "move";
    roiEditStart = point;
    roiEditSnapshot = {
      start: { ...roiState.start },
      end: { ...roiState.end },
      innerRadius: roiState.innerRadius || 0,
      outerRadius: getCircularRoiOuterRadius(roiState, state.pixelAspect || 1),
    };
    canvasWrap.classList.add("is-roi");
  }

  function applyRoiEdit(point) {
    if (!roiEditing || !roiEditSnapshot || !roiEditStart || !point) return;
    const snap = roiEditSnapshot;
    const dxRaw = point.x - roiEditStart.x;
    const dyRaw = point.y - roiEditStart.y;
    if (roiEditHandle === "move" || roiEditHandle === "center") {
      const clamped = clampRoiDelta(
        dxRaw,
        dyRaw,
        snap.start,
        snap.end,
      );
      const dx = clamped.dx;
      const dy = clamped.dy;
      if (roiState.mode === "circle" || roiState.mode === "annulus") {
        applyCircularRoiGeometry(
          roiState,
          { x: snap.start.x + dx, y: snap.start.y + dy },
          snap.outerRadius,
          getCircularRoiDirection(snap.start, snap.end),
        );
        roiState.innerRadius = clampCircularRoiInnerRadius(snap.innerRadius, snap.outerRadius);
      } else {
        roiState.start = { x: snap.start.x + dx, y: snap.start.y + dy };
        roiState.end = { x: snap.end.x + dx, y: snap.end.y + dy };
        roiState.innerRadius = snap.innerRadius;
        roiState.outerRadius = snap.outerRadius;
      }
      updateRoiCenterInputs();
    } else if (roiState.mode === "box") {
      const anchor = snap;
      if (roiEditHandle === "box-nw") {
        roiState.start = { x: point.x, y: point.y };
        roiState.end = { ...anchor.end };
      } else if (roiEditHandle === "box-ne") {
        roiState.start = { x: anchor.start.x, y: point.y };
        roiState.end = { x: point.x, y: anchor.end.y };
      } else if (roiEditHandle === "box-se") {
        roiState.start = { ...anchor.start };
        roiState.end = { x: point.x, y: point.y };
      } else if (roiEditHandle === "box-sw") {
        roiState.start = { x: point.x, y: anchor.start.y };
        roiState.end = { x: anchor.end.x, y: point.y };
      }
    } else if (roiState.mode === "line") {
      if (roiEditHandle === "line-start") {
        roiState.start = { x: point.x, y: point.y };
        roiState.end = { ...snap.end };
      } else if (roiEditHandle === "line-end") {
        roiState.start = { ...snap.start };
        roiState.end = { x: point.x, y: point.y };
      }
    } else if (roiState.mode === "circle" || roiState.mode === "annulus") {
      const aspect = state.pixelAspect || 1;
      if (roiEditHandle === "outer") {
        const outer = physicalRoiRadius(point.x - snap.start.x, point.y - snap.start.y, aspect);
        applyCircularRoiGeometry(
          roiState,
          snap.start,
          outer,
          { x: point.x - snap.start.x, y: point.y - snap.start.y },
        );
        if (roiState.mode === "circle") {
          roiState.innerRadius = 0;
          if (roiRadiusInput) roiRadiusInput.value = String(outer);
        } else {
          roiState.innerRadius = clampCircularRoiInnerRadius(snap.innerRadius, outer);
          if (roiOuterInput) roiOuterInput.value = String(outer);
          if (roiInnerInput) roiInnerInput.value = String(roiState.innerRadius);
        }
      } else if (roiEditHandle === "inner" && roiState.mode === "annulus") {
        const outer = getCircularRoiOuterRadius(snap, aspect);
        const inner = clampCircularRoiInnerRadius(
          physicalRoiRadius(point.x - snap.start.x, point.y - snap.start.y, aspect),
          outer,
        );
        applyCircularRoiGeometry(
          roiState,
          snap.start,
          outer,
          getCircularRoiDirection(snap.start, snap.end),
        );
        roiState.innerRadius = inner;
        if (roiInnerInput) roiInnerInput.value = String(inner);
        if (roiOuterInput) roiOuterInput.value = String(Math.round(outer));
      }
      updateRoiCenterInputs();
    }
    roiState.active = true;
    scheduleRoiOverlay();
    scheduleRoiUpdate();
    handleRoiChanged?.("roi");
  }

  function stopRoiEdit(event) {
    if (!roiEditing) return;
    roiEditing = false;
    roiEditHandle = null;
    roiEditStart = null;
    roiEditSnapshot = null;
    canvasWrap.classList.remove("is-roi");
    if (event && canvasWrap.hasPointerCapture(event.pointerId)) {
      canvasWrap.releasePointerCapture(event.pointerId);
    }
    scheduleRoiOverlay();
    scheduleRoiUpdate();
    handleRoiChanged?.("roi");
  }

  function drawRoiHandles(ctx, x0, y0, x1, y1, zoom, canvasWidth, canvasHeight, zoomY = zoom) {
    const handleSize = 8;
    const half = handleSize / 2;
    ctx.save();
    ctx.lineWidth = 1;
    ctx.setLineDash([]);
    ctx.fillStyle = "rgba(255, 255, 255, 0.95)";
    ctx.strokeStyle = "rgba(0, 0, 0, 0.8)";

    const drawHandle = (x, y) => {
      ctx.fillRect(x - half, y - half, handleSize, handleSize);
      ctx.strokeRect(x - half, y - half, handleSize, handleSize);
    };

    const drawCross = (x, y) => {
      ctx.strokeStyle = "rgba(255, 255, 255, 0.95)";
      ctx.lineWidth = 1.5;
      ctx.beginPath();
      ctx.moveTo(x - 6, y);
      ctx.lineTo(x + 6, y);
      ctx.moveTo(x, y - 6);
      ctx.lineTo(x, y + 6);
      ctx.stroke();
      ctx.strokeStyle = "rgba(0, 0, 0, 0.8)";
    };

    if (roiState.mode === "line") {
      const lineGeom = getLineScreenEndpoints(x0, y0, x1, y1, zoom, zoomY);
      drawHandle(lineGeom.xStart, lineGeom.yStart);
      drawHandle(lineGeom.xEnd, lineGeom.yEnd);
    } else if (roiState.mode === "box") {
      const { left, top, right, bottom } = getBoxScreenBounds(x0, y0, x1, y1, zoom, zoomY);
      drawHandle(left, top);
      drawHandle(right, top);
      drawHandle(right, bottom);
      drawHandle(left, bottom);
    } else if (roiState.mode === "circle" || roiState.mode === "annulus") {
      drawCross(x0, y0);
      const handles = getCircularHandleScreenPoints(x0, y0, x1, y1, zoom, canvasWidth, canvasHeight);
      if (handles.outer) {
        drawHandle(handles.outer.x, handles.outer.y);
      }
      if (handles.inner) {
        drawHandle(handles.inner.x, handles.inner.y);
      }
    }
    ctx.restore();
  }

  function drawRoiOverlay() {
    if (!roiOverlay || !roiCtx || !canvasWrap) return;
    const metrics = syncOverlayCanvas(roiOverlay, roiCtx);
    if (!metrics) return;
    const { width, height } = metrics;
    roiCtx.clearRect(0, 0, width, height);
    if (!roiState.enabled || !roiState.active || !roiState.start || !roiState.end) return;
    const zoom = state.zoom || 1;
    const zoomY = zoom * (state.pixelAspect || 1);
    const offsetX = state.renderOffsetX || 0;
    const offsetY = state.renderOffsetY || 0;
    const viewX = getEffectiveScrollLeft() / zoom;
    const viewY = getEffectiveScrollTop() / zoomY;
    const x0 = (roiState.start.x - viewX) * zoom + offsetX;
    const y0 = (roiState.start.y - viewY) * zoomY + offsetY;
    const x1 = (roiState.end.x - viewX) * zoom + offsetX;
    const y1 = (roiState.end.y - viewY) * zoomY + offsetY;

    roiCtx.save();
    roiCtx.setLineDash([6, 4]);
    roiCtx.lineJoin = "round";
    roiCtx.lineCap = "round";
    const strokeWithHalo = () => {
      roiCtx.lineWidth = 4;
      roiCtx.strokeStyle = "rgba(0, 0, 0, 0.7)";
      roiCtx.stroke();
      roiCtx.lineWidth = 2;
      roiCtx.strokeStyle = "rgba(255, 255, 255, 0.95)";
      roiCtx.stroke();
    };
    if (roiState.mode === "line") {
      const lineGeom = getLineScreenEndpoints(x0, y0, x1, y1, zoom, zoomY);
      roiCtx.beginPath();
      roiCtx.moveTo(lineGeom.xStart, lineGeom.yStart);
      roiCtx.lineTo(lineGeom.xEnd, lineGeom.yEnd);
      strokeWithHalo();
    } else if (roiState.mode === "box") {
      const { left, top, right, bottom } = getBoxScreenBounds(x0, y0, x1, y1, zoom, zoomY);
      const w = Math.max(0, right - left);
      const h = Math.max(0, bottom - top);
      if (w > 0 && h > 0) {
        roiCtx.save();
        roiCtx.setLineDash([]);
        roiCtx.fillStyle = "rgba(160, 160, 160, 0.08)";
        roiCtx.fillRect(left, top, w, h);
        roiCtx.restore();
      }
      roiCtx.beginPath();
      roiCtx.rect(left, top, w, h);
      strokeWithHalo();
    } else if (roiState.mode === "circle" || roiState.mode === "annulus") {
      // A circular/annulus ROI is a physical resolution shell: a true circle in
      // physical space and therefore a true circle on the (isotropic) display,
      // coinciding with the resolution rings. Its radius is stored in
      // X-pixel-equivalent units, so the on-screen radius is radius * zoom.
      const aspect = state.pixelAspect || 1;
      const radius = getCircularRoiOuterRadius(roiState, aspect) * zoom;
      const inner = (roiState.mode === "annulus" ? roiState.innerRadius || 0 : 0) * zoom;
      if (radius > 0) {
        roiCtx.save();
        roiCtx.setLineDash([]);
        roiCtx.fillStyle = "rgba(160, 160, 160, 0.08)";
        roiCtx.beginPath();
        roiCtx.arc(x0, y0, radius, 0, Math.PI * 2);
        if (inner > 0) {
          roiCtx.moveTo(x0 + inner, y0);
          roiCtx.arc(x0, y0, inner, 0, Math.PI * 2);
          try {
            roiCtx.fill("evenodd");
          } catch {
            roiCtx.fill();
          }
        } else {
          roiCtx.fill();
        }
        roiCtx.restore();
      }
      roiCtx.beginPath();
      roiCtx.arc(x0, y0, radius, 0, Math.PI * 2);
      strokeWithHalo();
      if (inner > 0) {
        roiCtx.beginPath();
        roiCtx.arc(x0, y0, inner, 0, Math.PI * 2);
        strokeWithHalo();
      }
    }
    roiCtx.restore();

    drawRoiHandles(roiCtx, x0, y0, x1, y1, zoom, width, height, zoomY);
  }

  function scheduleRoiOverlay() {
    if (roiOverlayScheduled) return;
    roiOverlayScheduled = true;
    window.requestAnimationFrame(() => {
      roiOverlayScheduled = false;
      drawRoiOverlay();
    });
  }

  function scheduleRoiUpdate() {
    if (roiUpdateScheduled) return;
    roiUpdateScheduled = true;
    window.requestAnimationFrame(() => {
      roiUpdateScheduled = false;
      updateRoiStats();
    });
  }

  function isRoiEditing() {
    return roiEditing;
  }

  return {
    getRoiHandleAt,
    isPointInRoi,
    startRoiEdit,
    applyRoiEdit,
    stopRoiEdit,
    scheduleRoiOverlay,
    drawRoiOverlay,
    scheduleRoiUpdate,
    isRoiEditing,
  };
}
