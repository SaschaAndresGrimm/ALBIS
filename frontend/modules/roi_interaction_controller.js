/**
 * ROI edit interactions and overlay rendering.
 */

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
    const offsetX = state.renderOffsetX || 0;
    const offsetY = state.renderOffsetY || 0;
    const viewX = getEffectiveScrollLeft() / zoom;
    const viewY = getEffectiveScrollTop() / zoom;
    const x0 = (roiState.start.x - viewX) * zoom + offsetX;
    const y0 = (roiState.start.y - viewY) * zoom + offsetY;
    const x1 = (roiState.end.x - viewX) * zoom + offsetX;
    const y1 = (roiState.end.y - viewY) * zoom + offsetY;
    return { x0, y0, x1, y1, zoom };
  }

  function getRoiHandleAt(event) {
    if (!roiState.enabled || !roiState.active) return null;
    const pointer = getPointerCanvasPos(event);
    const geom = getRoiScreenGeometry();
    if (!pointer || !geom) return null;
    const { x0, y0, x1, y1, zoom } = geom;
    const hit = (x, y) => Math.abs(pointer.x - x) <= 6 && Math.abs(pointer.y - y) <= 6;

    if (roiState.mode === "line") {
      if (hit(x0, y0)) return "line-start";
      if (hit(x1, y1)) return "line-end";
      return null;
    }
    if (roiState.mode === "box") {
      if (hit(x0, y0)) return "box-nw";
      if (hit(x1, y0)) return "box-ne";
      if (hit(x1, y1)) return "box-se";
      if (hit(x0, y1)) return "box-sw";
      return null;
    }
    if (roiState.mode === "circle" || roiState.mode === "annulus") {
      if (hit(x0, y0)) return "center";
      const dx = x1 - x0;
      const dy = y1 - y0;
      const outer = Math.hypot(dx, dy);
      const ux = outer > 0 ? dx / outer : 1;
      const uy = outer > 0 ? dy / outer : 0;
      if (hit(x1, y1)) return "outer";
      if (roiState.mode === "annulus" && roiState.innerRadius > 0) {
        if (hit(x0 + roiState.innerRadius * zoom * ux, y0 + roiState.innerRadius * zoom * uy)) {
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
      const dx = point.x - x0;
      const dy = point.y - y0;
      const dist = Math.hypot(dx, dy);
      const outer = Math.hypot(x1 - x0, y1 - y0);
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

  function clampRoiDelta(dx, dy, baseStart = roiState.start, baseEnd = roiState.end, baseOuter = roiState.outerRadius) {
    if (!baseStart || !baseEnd || !state.width || !state.height) {
      return { dx, dy };
    }
    let minX = Math.min(baseStart.x, baseEnd.x);
    let maxX = Math.max(baseStart.x, baseEnd.x);
    let minY = Math.min(baseStart.y, baseEnd.y);
    let maxY = Math.max(baseStart.y, baseEnd.y);
    if (roiState.mode === "circle" || roiState.mode === "annulus") {
      const r =
        baseOuter ||
        Math.hypot(baseEnd.x - baseStart.x, baseEnd.y - baseStart.y);
      minX = baseStart.x - r;
      maxX = baseStart.x + r;
      minY = baseStart.y - r;
      maxY = baseStart.y + r;
    }
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
      outerRadius: roiState.outerRadius || Math.hypot(roiState.end.x - roiState.start.x, roiState.end.y - roiState.start.y),
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
        snap.outerRadius,
      );
      const dx = clamped.dx;
      const dy = clamped.dy;
      roiState.start = { x: snap.start.x + dx, y: snap.start.y + dy };
      roiState.end = { x: snap.end.x + dx, y: snap.end.y + dy };
      roiState.innerRadius = snap.innerRadius;
      roiState.outerRadius = snap.outerRadius;
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
      if (roiEditHandle === "outer") {
        roiState.start = { ...snap.start };
        roiState.end = { x: point.x, y: point.y };
        const outer = Math.max(0, Math.round(Math.hypot(point.x - snap.start.x, point.y - snap.start.y)));
        roiState.outerRadius = outer;
        if (roiState.mode === "circle") {
          roiState.innerRadius = 0;
          if (roiRadiusInput) roiRadiusInput.value = String(outer);
        } else {
          roiState.innerRadius = Math.min(snap.innerRadius, outer);
          if (roiOuterInput) roiOuterInput.value = String(outer);
          if (roiInnerInput) roiInnerInput.value = String(roiState.innerRadius);
        }
      } else if (roiEditHandle === "inner" && roiState.mode === "annulus") {
        roiState.start = { ...snap.start };
        roiState.end = { ...snap.end };
        const outer = snap.outerRadius || Math.hypot(snap.end.x - snap.start.x, snap.end.y - snap.start.y);
        const inner = Math.max(0, Math.min(Math.round(Math.hypot(point.x - snap.start.x, point.y - snap.start.y)), outer));
        roiState.outerRadius = outer;
        roiState.innerRadius = inner;
        if (roiInnerInput) roiInnerInput.value = String(inner);
        if (roiOuterInput) roiOuterInput.value = String(Math.round(outer));
      }
      updateRoiCenterInputs();
    }
    roiState.active = true;
    scheduleRoiOverlay();
    scheduleRoiUpdate();
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
  }

  function drawRoiHandles(ctx, x0, y0, x1, y1, zoom) {
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
      drawHandle(x0, y0);
      drawHandle(x1, y1);
    } else if (roiState.mode === "box") {
      drawHandle(x0, y0);
      drawHandle(x1, y0);
      drawHandle(x1, y1);
      drawHandle(x0, y1);
    } else if (roiState.mode === "circle" || roiState.mode === "annulus") {
      drawCross(x0, y0);
      const dx = x1 - x0;
      const dy = y1 - y0;
      const outer = Math.hypot(dx, dy);
      const ux = outer > 0 ? dx / outer : 1;
      const uy = outer > 0 ? dy / outer : 0;
      drawHandle(x1, y1);
      if (roiState.mode === "annulus" && roiState.innerRadius > 0) {
        drawHandle(x0 + roiState.innerRadius * zoom * ux, y0 + roiState.innerRadius * zoom * uy);
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
    const offsetX = state.renderOffsetX || 0;
    const offsetY = state.renderOffsetY || 0;
    const viewX = getEffectiveScrollLeft() / zoom;
    const viewY = getEffectiveScrollTop() / zoom;
    const x0 = (roiState.start.x - viewX) * zoom + offsetX;
    const y0 = (roiState.start.y - viewY) * zoom + offsetY;
    const x1 = (roiState.end.x - viewX) * zoom + offsetX;
    const y1 = (roiState.end.y - viewY) * zoom + offsetY;

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
      roiCtx.beginPath();
      roiCtx.moveTo(x0, y0);
      roiCtx.lineTo(x1, y1);
      strokeWithHalo();
    } else if (roiState.mode === "box") {
      const left = Math.min(x0, x1);
      const top = Math.min(y0, y1);
      const w = Math.abs(x1 - x0);
      const h = Math.abs(y1 - y0);
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
      const radius = Math.hypot(x1 - x0, y1 - y0);
      if (radius > 0) {
        roiCtx.save();
        roiCtx.setLineDash([]);
        roiCtx.fillStyle = "rgba(160, 160, 160, 0.08)";
        roiCtx.beginPath();
        roiCtx.arc(x0, y0, radius, 0, Math.PI * 2);
        if (roiState.mode === "annulus" && roiState.innerRadius > 0) {
          const inner = roiState.innerRadius * zoom;
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
      if (roiState.mode === "annulus" && roiState.innerRadius > 0) {
        roiCtx.beginPath();
        roiCtx.arc(x0, y0, roiState.innerRadius * zoom, 0, Math.PI * 2);
        strokeWithHalo();
      }
    }
    roiCtx.restore();

    drawRoiHandles(roiCtx, x0, y0, x1, y1, zoom);
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
