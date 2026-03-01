/**
 * Canvas viewport and gesture interaction bindings.
 */

export function bindViewportInteractions({
  state,
  roiState,
  constants,
  elements,
  callbacks,
}) {
  const { MAX_ZOOM } = constants;
  const {
    panelResizer,
    appLayout,
    toolsPanel,
    canvasWrap,
    roiRadiusInput,
    roiInnerInput,
    roiOuterInput,
  } = elements;

  const {
    applyPanelState,
    setPanelWidth,
    scheduleHistogram,
    deferViewportInteraction,
    normalizeWheelDelta,
    queueWheelZoom,
    scheduleOverview,
    schedulePixelOverlay,
    scheduleRoiOverlay,
    scheduleResolutionOverlay,
    schedulePeakOverlay,
    startTouchGesture,
    updateTouchGesture,
    stopTouchGesture,
    isTouchGestureActive,
    getEffectiveScrollLeft,
    getEffectiveScrollTop,
    setEffectiveScroll,
    getImagePointFromEvent,
    updateRoiCenterInputs,
    getRoiHandleAt,
    isPointInRoi,
    startRoiEdit,
    updateCursorOverlay,
    isRoiEditing,
    applyRoiEdit,
    stopRoiEdit,
    hideCursorOverlay,
    getMinZoom,
    zoomAt,
    getRoiDragging,
    setRoiDragging,
    scheduleRoiUpdate,
  } = callbacks;

  let panning = false;
  let panStart = { x: 0, y: 0, effectiveLeft: 0, effectiveTop: 0 };
  let touchDragActive = false;
  let touchDragStart = null;

  function stopTouchDrag() {
    touchDragActive = false;
    touchDragStart = null;
  }

  function stopPan(event) {
    if (!panning) return;
    panning = false;
    canvasWrap.classList.remove("is-panning");
    schedulePixelOverlay();
    if (event && canvasWrap.hasPointerCapture(event.pointerId)) {
      canvasWrap.releasePointerCapture(event.pointerId);
    }
  }

  function stopRoi(event) {
    if (!getRoiDragging()) return;
    setRoiDragging(false);
    canvasWrap.classList.remove("is-roi");
    if (event && canvasWrap.hasPointerCapture(event.pointerId)) {
      canvasWrap.releasePointerCapture(event.pointerId);
    }
    scheduleRoiOverlay();
    scheduleRoiUpdate();
  }

  function updateRoiDrag(point) {
    roiState.end = point;
    if (roiState.mode === "circle" || roiState.mode === "annulus") {
      const dx = roiState.end.x - roiState.start.x;
      const dy = roiState.end.y - roiState.start.y;
      const outer = Math.max(0, Math.round(Math.hypot(dx, dy)));
      roiState.outerRadius = outer;
      if (roiState.mode === "circle") {
        if (roiRadiusInput) roiRadiusInput.value = String(outer);
      } else {
        if (roiOuterInput) roiOuterInput.value = String(outer);
        if (!roiState.innerRadius || roiState.innerRadius >= outer) {
          roiState.innerRadius = Math.max(0, Math.round(outer * 0.5));
          if (roiInnerInput) roiInnerInput.value = String(roiState.innerRadius);
        }
      }
    }
    scheduleRoiOverlay();
    scheduleRoiUpdate();
  }

  panelResizer?.addEventListener("mousedown", (event) => {
    if (!appLayout) return;
    if (state.panelCollapsed) {
      state.panelCollapsed = false;
      applyPanelState();
    }
    const startX = event.clientX;
    const startWidth = toolsPanel?.getBoundingClientRect().width || state.panelWidth;

    function onMove(nextEvent) {
      const delta = startX - nextEvent.clientX;
      setPanelWidth(startWidth + delta);
      scheduleHistogram();
    }

    function onUp(nextEvent) {
      const delta = startX - nextEvent.clientX;
      const finalWidth = startWidth + delta;
      if (finalWidth < 140) {
        state.panelCollapsed = true;
      }
      applyPanelState();
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
      document.body.style.cursor = "";
    }

    document.body.style.cursor = "col-resize";
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  });

  canvasWrap.addEventListener(
    "wheel",
    (event) => {
      event.preventDefault();
      deferViewportInteraction();
      const delta = normalizeWheelDelta(event);
      queueWheelZoom(delta, event.clientX, event.clientY);
    },
    { passive: false }
  );

  canvasWrap.addEventListener("scroll", () => {
    deferViewportInteraction();
    scheduleOverview();
    schedulePixelOverlay();
    scheduleRoiOverlay();
    scheduleResolutionOverlay();
    schedulePeakOverlay();
  });

  canvasWrap.addEventListener("contextmenu", (event) => {
    event.preventDefault();
  });

  canvasWrap.addEventListener(
    "touchstart",
    (event) => {
      if (event.touches.length >= 2) {
        stopTouchDrag();
        startTouchGesture(event.touches);
        event.preventDefault();
      } else if (event.touches.length === 1) {
        const touch = event.touches[0];
        touchDragStart = {
          x: touch.clientX,
          y: touch.clientY,
          effectiveLeft: getEffectiveScrollLeft(),
          effectiveTop: getEffectiveScrollTop(),
        };
        touchDragActive = true;
      }
    },
    { passive: false }
  );

  canvasWrap.addEventListener(
    "touchmove",
    (event) => {
      if (event.touches.length >= 2) {
        if (!isTouchGestureActive()) return;
        updateTouchGesture(event.touches);
        event.preventDefault();
        return;
      }

      if (event.touches.length === 1 && touchDragActive && touchDragStart) {
        deferViewportInteraction();
        const touch = event.touches[0];
        const dx = touch.clientX - touchDragStart.x;
        const dy = touch.clientY - touchDragStart.y;
        const nextEffectiveX = touchDragStart.effectiveLeft - dx;
        const nextEffectiveY = touchDragStart.effectiveTop - dy;
        setEffectiveScroll(nextEffectiveX, nextEffectiveY);
        event.preventDefault();
      }
    },
    { passive: false }
  );

  canvasWrap.addEventListener("touchend", (event) => {
    if (event.touches.length >= 2) {
      startTouchGesture(event.touches);
      return;
    }
    stopTouchGesture();
    stopTouchDrag();
  });

  canvasWrap.addEventListener("touchcancel", () => {
    stopTouchGesture();
    stopTouchDrag();
  });

  canvasWrap.addEventListener("pointerdown", (event) => {
    if (event.pointerType === "touch") return;
    const isRightClick = event.button === 2 || event.buttons === 2 || event.which === 3;
    const isCtrlClick = event.button === 0 && event.ctrlKey;
    const roiTrigger = roiState.enabled && (isRightClick || isCtrlClick);
    if (roiTrigger) {
      const point = getImagePointFromEvent(event);
      if (!point) return;
      setRoiDragging(true);
      roiState.active = true;
      roiState.start = point;
      roiState.end = point;
      if (roiState.mode === "circle" || roiState.mode === "annulus") {
        updateRoiCenterInputs();
        roiState.outerRadius = 0;
        if (roiState.mode === "circle") {
          roiState.innerRadius = 0;
          if (roiRadiusInput) roiRadiusInput.value = "0";
        } else {
          if (!roiState.innerRadius) {
            roiState.innerRadius = 0;
          }
          if (roiInnerInput) roiInnerInput.value = String(roiState.innerRadius || 0);
          if (roiOuterInput) roiOuterInput.value = "0";
        }
      }
      canvasWrap.classList.add("is-roi");
      canvasWrap.setPointerCapture(event.pointerId);
      event.preventDefault();
      scheduleRoiOverlay();
      scheduleRoiUpdate();
      return;
    }

    if (event.button !== 0) return;
    if (event.target.closest(".loading")) return;

    if (roiState.enabled && roiState.active) {
      const point = getImagePointFromEvent(event);
      if (point) {
        const handle = getRoiHandleAt(event);
        if (handle || isPointInRoi(point)) {
          startRoiEdit(handle || "move", point);
          canvasWrap.setPointerCapture(event.pointerId);
          event.preventDefault();
          return;
        }
      }
    }

    panning = true;
    panStart = {
      x: event.clientX,
      y: event.clientY,
      effectiveLeft: getEffectiveScrollLeft(),
      effectiveTop: getEffectiveScrollTop(),
    };
    canvasWrap.classList.add("is-panning");
    deferViewportInteraction();
    canvasWrap.setPointerCapture(event.pointerId);
    event.preventDefault();
  });

  canvasWrap.addEventListener("pointermove", (event) => {
    if (event.pointerType === "touch") return;
    updateCursorOverlay(event);

    if (isRoiEditing()) {
      const point = getImagePointFromEvent(event);
      if (!point) return;
      applyRoiEdit(point);
      return;
    }

    if (getRoiDragging()) {
      const point = getImagePointFromEvent(event);
      if (!point) return;
      updateRoiDrag(point);
      return;
    }

    if (!panning) return;

    deferViewportInteraction();
    const dx = event.clientX - panStart.x;
    const dy = event.clientY - panStart.y;
    const nextEffectiveX = panStart.effectiveLeft - dx;
    const nextEffectiveY = panStart.effectiveTop - dy;
    setEffectiveScroll(nextEffectiveX, nextEffectiveY);
  });

  canvasWrap.addEventListener("pointerup", (event) => {
    stopRoiEdit(event);
    stopRoi(event);
    stopPan(event);
  });

  canvasWrap.addEventListener("pointercancel", (event) => {
    stopRoiEdit(event);
    stopRoi(event);
    stopPan(event);
  });

  canvasWrap.addEventListener("pointerleave", () => {
    stopRoi();
    hideCursorOverlay();
  });

  canvasWrap.addEventListener("dblclick", (event) => {
    event.preventDefault();
    const minZoom = getMinZoom();
    const next = Math.min(MAX_ZOOM, Math.max(minZoom, state.zoom * 2));
    zoomAt(event.clientX, event.clientY, next);
  });
}
