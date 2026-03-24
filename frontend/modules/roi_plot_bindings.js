/**
 * ROI plot pan/zoom/resize interaction bindings.
 */

function isInsideRoiPlotViewport(plot, x, y) {
  if (!plot) return false;
  const minX = plot.padL;
  const maxX = plot.width - plot.padR;
  const minY = plot.padT;
  const maxY = plot.height - plot.padB;
  return x >= minX && x <= maxX && y >= minY && y <= maxY;
}

export function bindRoiPlotInteractions({
  roiState,
  elements,
  callbacks,
}) {
  const {
    roiLineCanvas,
    roiXCanvas,
    roiYCanvas,
    roiHistCanvas,
    roiLinePlot,
    roiBoxPlotX,
    roiBoxPlotY,
    roiHistogramPlot,
    roiLimitsEnable,
  } = elements;

  const {
    updateRoiTooltip,
    hideRoiTooltip,
    getRoiPlotKey,
    getRoiPlotLimits,
    setRoiPlotAxisLimits,
    syncRoiPlotLimitControls,
    redrawRoiPlots,
    clearRoiPlotLimitsForKey,
    hasAnyManualRoiPlotLimits,
    normalizeWheelDelta,
  } = callbacks;

  const roiPlotCanvases = [roiLineCanvas, roiXCanvas, roiYCanvas, roiHistCanvas].filter(Boolean);
  let roiPlotResizing = null;
  let roiPlotResizeStart = { x: 0, y: 0, height: 0, container: null };
  let roiPlotPanning = null;
  let roiPlotRedrawScheduled = false;

  function scheduleRoiPlotRedraw() {
    if (roiPlotRedrawScheduled) return;
    roiPlotRedrawScheduled = true;
    window.requestAnimationFrame(() => {
      roiPlotRedrawScheduled = false;
      redrawRoiPlots();
    });
  }

  function updateRoiPlotPanReadyState(canvasEl, clientX, clientY) {
    if (!canvasEl) return;
    if (roiPlotPanning && roiPlotPanning.canvasEl === canvasEl) return;
    const plot = canvasEl._roiPlot;
    if (!plot) {
      canvasEl.classList.remove("is-pan-ready");
      return;
    }
    const rect = canvasEl.getBoundingClientRect();
    const x = clientX - rect.left;
    const y = clientY - rect.top;
    canvasEl.classList.toggle("is-pan-ready", isInsideRoiPlotViewport(plot, x, y));
  }

  function beginRoiPlotPan(event, canvasEl) {
    if (!canvasEl || event.button !== 0 || roiPlotResizing) return false;
    const plot = canvasEl._roiPlot;
    if (!plot) return false;
    const rect = canvasEl.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const y = event.clientY - rect.top;
    if (!isInsideRoiPlotViewport(plot, x, y)) return false;

    const plotKey = getRoiPlotKey(canvasEl);
    const currentLimits = getRoiPlotLimits(plotKey);
    roiPlotPanning = {
      canvasEl,
      pointerId: event.pointerId,
      startClientX: event.clientX,
      startClientY: event.clientY,
      xMin: Number.isFinite(currentLimits.xMin) ? currentLimits.xMin : plot.xMin,
      xMax: Number.isFinite(currentLimits.xMax) ? currentLimits.xMax : plot.xMax,
      yMin: Number.isFinite(currentLimits.yMin) ? currentLimits.yMin : plot.yMin,
      yMax: Number.isFinite(currentLimits.yMax) ? currentLimits.yMax : plot.yMax,
      domainXMin: Number.isFinite(plot.totalXMin) ? plot.totalXMin : null,
      domainXMax: Number.isFinite(plot.totalXMax) ? plot.totalXMax : null,
      domainYMin: Number.isFinite(plot.totalYMin) ? plot.totalYMin : null,
      domainYMax: Number.isFinite(plot.totalYMax) ? plot.totalYMax : null,
      hasMoved: false,
    };

    canvasEl.classList.remove("is-pan-ready");
    canvasEl.classList.add("is-panning");
    if (typeof canvasEl.setPointerCapture === "function") {
      canvasEl.setPointerCapture(event.pointerId);
    }
    event.preventDefault();
    return true;
  }

  function moveRoiPlotPan(event, canvasEl) {
    if (!roiPlotPanning || roiPlotPanning.canvasEl !== canvasEl || roiPlotPanning.pointerId !== event.pointerId) {
      return;
    }
    const plot = canvasEl?._roiPlot;
    if (!plot) return;

    const plotKey = getRoiPlotKey(canvasEl);
    const plotWidth = Math.max(1, plot.width - plot.padL - plot.padR);
    const plotHeight = Math.max(1, plot.height - plot.padT - plot.padB);
    const xRange = roiPlotPanning.xMax - roiPlotPanning.xMin;
    const yRange = roiPlotPanning.yMax - roiPlotPanning.yMin;
    if (!(xRange > 0) && !(yRange > 0)) return;

    const dx = event.clientX - roiPlotPanning.startClientX;
    const dy = event.clientY - roiPlotPanning.startClientY;

    if (!roiPlotPanning.hasMoved && (Math.abs(dx) > 2 || Math.abs(dy) > 2)) {
      roiPlotPanning.hasMoved = true;
      if (roiState.plotLimits.autoscale) {
        roiState.plotLimits.autoscale = false;
        if (roiLimitsEnable) roiLimitsEnable.checked = false;
      }
      const limits = getRoiPlotLimits(plotKey);
      if (!Number.isFinite(limits.xMin) || !Number.isFinite(limits.xMax)) {
        setRoiPlotAxisLimits(plotKey, "x", plot.xMin, plot.xMax);
      }
      if (!Number.isFinite(limits.yMin) || !Number.isFinite(limits.yMax)) {
        setRoiPlotAxisLimits(plotKey, "y", plot.yMin, plot.yMax);
      }
    }

    if (!roiPlotPanning.hasMoved) return;

    let nextXMin = roiPlotPanning.xMin - (dx / plotWidth) * xRange;
    let nextXMax = roiPlotPanning.xMax - (dx / plotWidth) * xRange;
    const domainXMin = roiPlotPanning.domainXMin;
    const domainXMax = roiPlotPanning.domainXMax;
    if (Number.isFinite(domainXMin) && Number.isFinite(domainXMax)) {
      const domainRange = domainXMax - domainXMin;
      if (xRange >= domainRange) {
        nextXMin = domainXMin;
        nextXMax = domainXMax;
      } else {
        if (nextXMin < domainXMin) {
          const shift = domainXMin - nextXMin;
          nextXMin += shift;
          nextXMax += shift;
        }
        if (nextXMax > domainXMax) {
          const shift = nextXMax - domainXMax;
          nextXMin -= shift;
          nextXMax -= shift;
        }
      }
    }

    let nextYMin = roiPlotPanning.yMin + (dy / plotHeight) * yRange;
    let nextYMax = roiPlotPanning.yMax + (dy / plotHeight) * yRange;
    if (nextYMin > nextYMax) {
      [nextYMin, nextYMax] = [nextYMax, nextYMin];
    }

    const domainYMin = roiPlotPanning.domainYMin;
    const domainYMax = roiPlotPanning.domainYMax;
    if (Number.isFinite(domainYMin) && Number.isFinite(domainYMax)) {
      const domainRange = domainYMax - domainYMin;
      if (yRange >= domainRange) {
        nextYMin = domainYMin;
        nextYMax = domainYMax;
      } else {
        if (nextYMin < domainYMin) {
          const shift = domainYMin - nextYMin;
          nextYMin += shift;
          nextYMax += shift;
        }
        if (nextYMax > domainYMax) {
          const shift = nextYMax - domainYMax;
          nextYMin -= shift;
          nextYMax -= shift;
        }
      }
    }

    setRoiPlotAxisLimits(plotKey, "x", nextXMin, nextXMax);
    setRoiPlotAxisLimits(plotKey, "y", nextYMin, nextYMax);
    syncRoiPlotLimitControls();
    scheduleRoiPlotRedraw();
    event.preventDefault();
  }

  function endRoiPlotPan(event, canvasEl) {
    if (!roiPlotPanning || roiPlotPanning.canvasEl !== canvasEl) return;
    const pointerId = roiPlotPanning.pointerId;
    roiPlotPanning = null;
    canvasEl.classList.remove("is-panning");
    if (event) {
      updateRoiPlotPanReadyState(canvasEl, event.clientX, event.clientY);
    } else {
      canvasEl.classList.remove("is-pan-ready");
    }
    if (Number.isFinite(pointerId) && typeof canvasEl.releasePointerCapture === "function") {
      if (canvasEl.hasPointerCapture(pointerId)) {
        canvasEl.releasePointerCapture(pointerId);
      }
    }
  }

  roiPlotCanvases.forEach((canvasEl) => {
    if (!canvasEl) return;

    canvasEl.addEventListener("mousemove", (event) => {
      updateRoiTooltip(event, canvasEl);
      updateRoiPlotPanReadyState(canvasEl, event.clientX, event.clientY);
    });

    canvasEl.addEventListener("mouseleave", () => {
      hideRoiTooltip(canvasEl);
      if (!(roiPlotPanning && roiPlotPanning.canvasEl === canvasEl)) {
        canvasEl.classList.remove("is-pan-ready");
      }
    });

    canvasEl.addEventListener("pointerdown", (event) => {
      beginRoiPlotPan(event, canvasEl);
    });

    canvasEl.addEventListener("pointermove", (event) => {
      if (roiPlotPanning && roiPlotPanning.canvasEl === canvasEl) {
        moveRoiPlotPan(event, canvasEl);
        return;
      }
      updateRoiPlotPanReadyState(canvasEl, event.clientX, event.clientY);
    });

    canvasEl.addEventListener("pointerup", (event) => {
      endRoiPlotPan(event, canvasEl);
    });

    canvasEl.addEventListener("pointercancel", (event) => {
      endRoiPlotPan(event, canvasEl);
    });

    canvasEl.addEventListener("dblclick", (event) => {
      event.preventDefault();
      const plotKey = getRoiPlotKey(canvasEl);
      clearRoiPlotLimitsForKey(plotKey);
      if (!hasAnyManualRoiPlotLimits()) {
        roiState.plotLimits.autoscale = true;
      }
      syncRoiPlotLimitControls();
      scheduleRoiPlotRedraw();
    });

    canvasEl.addEventListener(
      "wheel",
      (event) => {
        event.preventDefault();
        const plot = canvasEl._roiPlot;
        if (!plot) return;
        const rect = canvasEl.getBoundingClientRect();
        const x = event.clientX - rect.left;
        const y = event.clientY - rect.top;
        const inYAxis = x <= plot.padL;
        const inXAxis = y >= plot.height - plot.padB;
        const inPlotArea = isInsideRoiPlotViewport(plot, x, y);
        if (!inYAxis && !inXAxis && !inPlotArea) return;

        const delta = normalizeWheelDelta(event);
        if (!delta) return;
        const factor = Math.exp(-delta * 0.002);
        const plotKey = getRoiPlotKey(canvasEl);

        if (roiState.plotLimits.autoscale) {
          roiState.plotLimits.autoscale = false;
          if (roiLimitsEnable) roiLimitsEnable.checked = false;
        }

        if (inYAxis) {
          const yRange = plot.yMax - plot.yMin;
          const cursorFrac = (plot.height - plot.padB - y) / (plot.height - plot.padB - plot.padT);
          const cursorValue = plot.yMin + cursorFrac * yRange;
          const newRange = yRange / factor;
          const newMin = cursorValue - cursorFrac * newRange;
          const newMax = cursorValue + (1 - cursorFrac) * newRange;
          setRoiPlotAxisLimits(plotKey, "y", newMin, newMax);
        } else if (inXAxis) {
          const xRange = plot.xMax - plot.xMin;
          const cursorFrac = (x - plot.padL) / (plot.width - plot.padL - plot.padR);
          const cursorValue = plot.xMin + cursorFrac * xRange;
          const newRange = xRange / factor;
          const newMin = cursorValue - cursorFrac * newRange;
          const newMax = cursorValue + (1 - cursorFrac) * newRange;
          setRoiPlotAxisLimits(plotKey, "x", newMin, newMax);
        } else if (inPlotArea) {
          const xRange = plot.xMax - plot.xMin;
          const yRange = plot.yMax - plot.yMin;

          const xCursorFrac = (x - plot.padL) / (plot.width - plot.padL - plot.padR);
          const xCursorValue = plot.xMin + xCursorFrac * xRange;
          const newXRange = xRange / factor;
          const newXMin = xCursorValue - xCursorFrac * newXRange;
          const newXMax = xCursorValue + (1 - xCursorFrac) * newXRange;

          const yCursorFrac = (plot.height - plot.padB - y) / (plot.height - plot.padB - plot.padT);
          const yCursorValue = plot.yMin + yCursorFrac * yRange;
          const newYRange = yRange / factor;
          const newYMin = yCursorValue - yCursorFrac * newYRange;
          const newYMax = yCursorValue + (1 - yCursorFrac) * newYRange;

          setRoiPlotAxisLimits(plotKey, "x", newXMin, newXMax);
          setRoiPlotAxisLimits(plotKey, "y", newYMin, newYMax);
        }

        syncRoiPlotLimitControls();
        scheduleRoiPlotRedraw();
      },
      { passive: false }
    );
  });

  if (typeof window.ResizeObserver === "function" && roiPlotCanvases.length) {
    const roiPlotResizeObserver = new window.ResizeObserver((entries) => {
      if (!entries?.length) return;
      scheduleRoiPlotRedraw();
    });
    roiPlotCanvases.forEach((canvasEl) => {
      roiPlotResizeObserver.observe(canvasEl);
    });
  }

  [roiLinePlot, roiBoxPlotX, roiBoxPlotY, roiHistogramPlot].forEach((plotContainer) => {
    if (!plotContainer) return;
    const resizeHandle = plotContainer.querySelector(".roi-resize-handle");
    if (!resizeHandle) return;

    resizeHandle.addEventListener("pointerdown", (event) => {
      if (event.button !== 0) return;
      event.preventDefault();
      roiPlotResizing = plotContainer;
      const rect = plotContainer.getBoundingClientRect();
      roiPlotResizeStart = {
        x: event.clientX,
        y: event.clientY,
        height: rect.height,
        container: plotContainer,
      };
      document.addEventListener("pointermove", onRoiPlotResizeMove);
      document.addEventListener("pointerup", onRoiPlotResizeEnd);
    });
  });

  function onRoiPlotResizeMove(event) {
    if (!roiPlotResizing) return;
    const dy = event.clientY - roiPlotResizeStart.y;
    const newHeight = Math.max(80, roiPlotResizeStart.height + dy);
    roiPlotResizing.style.height = `${newHeight}px`;
  }

  function onRoiPlotResizeEnd() {
    if (roiPlotResizing) {
      const canvas = roiPlotResizing.querySelector("canvas");
      if (canvas) {
        scheduleRoiPlotRedraw();
      }
    }
    roiPlotResizing = null;
    document.removeEventListener("pointermove", onRoiPlotResizeMove);
    document.removeEventListener("pointerup", onRoiPlotResizeEnd);
  }
}
