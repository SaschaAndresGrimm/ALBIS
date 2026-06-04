/**
 * ROI plot pan/zoom/resize interaction bindings.
 */

import { t } from "./i18n.js";

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
  } = elements;

  const {
    updateRoiTooltip,
    hideRoiTooltip,
    getRoiPlotKey,
    getRoiPlotLimits,
    getRoiPlotLog,
    setRoiPlotLog,
    setRoiPlotAxisLimits,
    syncRoiPlotLimitControls,
    redrawRoiPlots,
    clearRoiPlotLimitsForKey,
    hasManualRoiPlotLimits,
    hasAnyManualRoiPlotLimits,
    normalizeWheelDelta,
  } = callbacks;

  const roiPlotCanvases = [roiLineCanvas, roiXCanvas, roiYCanvas, roiHistCanvas].filter(Boolean);
  const roiPlotEntries = [
    { key: "line", container: roiLinePlot, canvas: roiLineCanvas },
    { key: "x", container: roiBoxPlotX, canvas: roiXCanvas },
    { key: "y", container: roiBoxPlotY, canvas: roiYCanvas },
    { key: "hist", container: roiHistogramPlot, canvas: roiHistCanvas },
  ].filter((entry) => entry.container);
  const roiAxisLimitControls = roiPlotEntries.map((entry) => ({
    ...entry,
    toggle: entry.container.querySelector(".roi-axis-limits-toggle"),
    popover: entry.container.querySelector(".roi-axis-limits-popover"),
    chip: entry.container.querySelector("[data-roi-axis-chip]"),
    logToggle: entry.container.querySelector("[data-roi-plot-log]"),
    autoToggle: entry.container.querySelector("[data-roi-axis-auto]"),
    inputs: [...entry.container.querySelectorAll(".roi-axis-limits-grid input")],
    resetBtn: entry.container.querySelector("[data-roi-axis-reset]"),
  }));
  let roiPlotResizing = null;
  let roiPlotResizeStart = { x: 0, y: 0, height: 0, container: null };
  let roiPlotPanning = null;
  let roiPlotRedrawScheduled = false;
  let roiAxisPopoverRepositionScheduled = false;
  let roiAxisPopoverOpenedAt = 0;

  function scheduleRoiPlotRedraw() {
    if (roiPlotRedrawScheduled) return;
    roiPlotRedrawScheduled = true;
    window.requestAnimationFrame(() => {
      roiPlotRedrawScheduled = false;
      redrawRoiPlots();
    });
  }

  function formatAxisLimitInputValue(value) {
    if (!Number.isFinite(value)) return "";
    const abs = Math.abs(value);
    if (abs !== 0 && (abs >= 1e7 || abs < 1e-4)) {
      return value.toExponential(6).replace(/\.?0+e/, "e");
    }
    return `${Number(value.toPrecision(7))}`;
  }

  function getAxisLimitKey(axis, bound) {
    if (axis === "x") return bound === "min" ? "xMin" : "xMax";
    if (axis === "y") return bound === "min" ? "yMin" : "yMax";
    return "";
  }

  function getAxisControl(plotKey) {
    return roiAxisLimitControls.find((entry) => entry.key === plotKey) || null;
  }

  function isAxisLimitsPopoverOpen(entry) {
    return entry?.toggle?.getAttribute("aria-expanded") === "true";
  }

  function moveRoiAxisPopoverToBody(entry) {
    if (!entry?.popover || entry.popover.parentElement === document.body) return;
    entry.popoverParent = entry.popover.parentElement;
    entry.popoverNextSibling = entry.popover.nextSibling;
    document.body?.appendChild(entry.popover);
  }

  function restoreRoiAxisPopover(entry) {
    if (!entry?.popover || !entry.popoverParent || entry.popover.parentElement === entry.popoverParent) return;
    if (entry.popoverNextSibling && entry.popoverNextSibling.parentElement === entry.popoverParent) {
      entry.popoverParent.insertBefore(entry.popover, entry.popoverNextSibling);
    } else {
      entry.popoverParent.appendChild(entry.popover);
    }
  }

  function resetRoiAxisPopoverPosition(entry) {
    if (!entry?.popover) return;
    entry.popover.style.left = "";
    entry.popover.style.top = "";
    entry.popover.style.right = "";
    entry.popover.style.width = "";
    entry.popover.style.maxHeight = "";
    entry.popover.style.overflowY = "";
  }

  function positionRoiAxisLimitPopover(entry) {
    if (!entry?.toggle || !entry?.popover || !isAxisLimitsPopoverOpen(entry)) return;
    const viewportWidth = window.innerWidth || document.documentElement?.clientWidth || 320;
    const viewportHeight = window.innerHeight || document.documentElement?.clientHeight || 480;
    const margin = 8;
    const gap = 6;
    const toggleRect = entry.toggle.getBoundingClientRect();
    const maxWidth = Math.max(180, viewportWidth - margin * 2);
    const width = Math.min(248, maxWidth);
    const left = Math.min(viewportWidth - margin - width, Math.max(margin, toggleRect.right - width));

    entry.popover.style.width = `${width}px`;
    entry.popover.style.left = `${left}px`;
    entry.popover.style.right = "auto";

    const belowTop = toggleRect.bottom + gap;
    const belowSpace = viewportHeight - belowTop - margin;
    const aboveSpace = toggleRect.top - gap - margin;
    const desiredHeight = Math.min(entry.popover.scrollHeight || 0, viewportHeight - margin * 2);
    const useAbove = aboveSpace > belowSpace && belowSpace < Math.min(desiredHeight, 180);
    const availableHeight = Math.max(120, useAbove ? aboveSpace : belowSpace);
    const top = useAbove
      ? Math.max(margin, toggleRect.top - gap - Math.min(desiredHeight || availableHeight, availableHeight))
      : Math.max(margin, belowTop);

    entry.popover.style.top = `${top}px`;
    entry.popover.style.maxHeight = `${availableHeight}px`;
    entry.popover.style.overflowY = "auto";
  }

  function scheduleRoiAxisPopoverReposition() {
    if (roiAxisPopoverRepositionScheduled) return;
    roiAxisPopoverRepositionScheduled = true;
    window.requestAnimationFrame(() => {
      roiAxisPopoverRepositionScheduled = false;
      roiAxisLimitControls.forEach((entry) => positionRoiAxisLimitPopover(entry));
    });
  }

  function closeRoiAxisLimitPopovers(exceptEntry = null) {
    roiAxisLimitControls.forEach((entry) => {
      if (entry === exceptEntry || !entry.toggle || !entry.popover) return;
      entry.toggle.setAttribute("aria-expanded", "false");
      entry.popover.classList.remove("is-open");
      entry.popover.setAttribute("aria-hidden", "true");
      resetRoiAxisPopoverPosition(entry);
      restoreRoiAxisPopover(entry);
    });
  }

  function setRoiAxisLimitsPopoverOpen(entry, open) {
    if (!entry?.toggle || !entry?.popover) return;
    const isOpen = Boolean(open);
    if (isOpen) {
      closeRoiAxisLimitPopovers(entry);
      syncRoiAxisLimitControls(entry.key);
      moveRoiAxisPopoverToBody(entry);
      roiAxisPopoverOpenedAt = Date.now();
    }
    entry.toggle.setAttribute("aria-expanded", isOpen ? "true" : "false");
    entry.popover.classList.toggle("is-open", isOpen);
    entry.popover.setAttribute("aria-hidden", isOpen ? "false" : "true");
    if (isOpen) {
      positionRoiAxisLimitPopover(entry);
    } else {
      resetRoiAxisPopoverPosition(entry);
      restoreRoiAxisPopover(entry);
    }
  }

  function updateGlobalAutoscaleFromManualLimits() {
    roiState.plotLimits.autoscale = !hasAnyManualRoiPlotLimits();
  }

  function syncRoiAxisLimitControls(plotKey = null, options = {}) {
    const preserveInput = options.preserveInput || null;
    roiAxisLimitControls.forEach((entry) => {
      if (plotKey && entry.key !== plotKey) return;
      const limits = getRoiPlotLimits(entry.key);
      const hasManual = Boolean(hasManualRoiPlotLimits?.(entry.key));
      if (entry.chip) {
        entry.chip.textContent = t("roi.axis.manual_chip");
        entry.chip.classList.toggle("is-hidden", !hasManual);
      }
      if (entry.autoToggle) {
        entry.autoToggle.checked = !hasManual;
      }
      if (entry.resetBtn) {
        entry.resetBtn.disabled = !hasManual;
      }
      if (entry.logToggle) {
        entry.logToggle.checked = Boolean(getRoiPlotLog?.(entry.key));
        entry.logToggle.disabled = roiState.enabled === false;
      }
      entry.inputs.forEach((input) => {
        if (input === preserveInput) return;
        const key = getAxisLimitKey(input.dataset.axis, input.dataset.bound);
        input.value = key ? formatAxisLimitInputValue(limits[key]) : "";
      });
    });
  }

  function seedRoiAxisLimitsFromVisiblePlot(entry) {
    const plot = entry?.canvas?._roiPlot;
    if (!plot) return false;
    setRoiPlotAxisLimits(entry.key, "x", plot.xMin, plot.xMax);
    setRoiPlotAxisLimits(entry.key, "y", plot.yMin, plot.yMax);
    roiState.plotLimits.autoscale = false;
    syncRoiPlotLimitControls();
    syncRoiAxisLimitControls(entry.key);
    scheduleRoiPlotRedraw();
    return true;
  }

  function clearRoiAxisLimitsForEntry(entry) {
    if (!entry) return;
    clearRoiPlotLimitsForKey(entry.key);
    updateGlobalAutoscaleFromManualLimits();
    syncRoiPlotLimitControls();
    syncRoiAxisLimitControls(entry.key);
    scheduleRoiPlotRedraw();
  }

  function applyRoiAxisLimitInput(entry, input, options = {}) {
    if (!entry || !input) return;
    const commit = options.commit !== false;
    const axis = input.dataset.axis;
    const bound = input.dataset.bound;
    if (axis !== "x" && axis !== "y") return;
    const limits = getRoiPlotLimits(entry.key);
    const minKey = getAxisLimitKey(axis, "min");
    const maxKey = getAxisLimitKey(axis, "max");
    let nextMin = Number.isFinite(limits[minKey]) ? limits[minKey] : null;
    let nextMax = Number.isFinite(limits[maxKey]) ? limits[maxKey] : null;
    const raw = String(input.value || "").trim();
    const value = raw ? Number(raw) : null;
    if (raw && !Number.isFinite(value)) {
      if (commit) syncRoiAxisLimitControls(entry.key);
      return;
    }
    if (bound === "min") {
      nextMin = value;
    } else if (bound === "max") {
      nextMax = value;
    } else {
      return;
    }
    setRoiPlotAxisLimits(entry.key, axis, nextMin, nextMax);
    updateGlobalAutoscaleFromManualLimits();
    syncRoiPlotLimitControls();
    syncRoiAxisLimitControls(entry.key, { preserveInput: commit ? null : input });
    scheduleRoiPlotRedraw();
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
    syncRoiAxisLimitControls(plotKey);
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
      syncRoiAxisLimitControls(plotKey);
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
        syncRoiAxisLimitControls(plotKey);
        scheduleRoiPlotRedraw();
      },
      { passive: false }
    );
  });

  roiAxisLimitControls.forEach((entry) => {
    entry.toggle?.addEventListener("pointerdown", (event) => {
      event.stopPropagation();
    });

    entry.toggle?.addEventListener("click", (event) => {
      event.stopPropagation();
      setRoiAxisLimitsPopoverOpen(entry, !isAxisLimitsPopoverOpen(entry));
    });

    entry.popover?.addEventListener("pointerdown", (event) => {
      event.stopPropagation();
    });

    entry.popover?.addEventListener("click", (event) => {
      event.stopPropagation();
    });

    entry.logToggle?.addEventListener("change", () => {
      setRoiPlotLog?.(entry.key, entry.logToggle.checked);
      scheduleRoiPlotRedraw();
      syncRoiAxisLimitControls(entry.key);
    });

    entry.autoToggle?.addEventListener("change", () => {
      if (entry.autoToggle.checked) {
        clearRoiAxisLimitsForEntry(entry);
      } else if (!seedRoiAxisLimitsFromVisiblePlot(entry)) {
        syncRoiAxisLimitControls(entry.key);
      }
    });

    entry.inputs.forEach((input) => {
      input.addEventListener("input", () => {
        applyRoiAxisLimitInput(entry, input, { commit: false });
      });

      input.addEventListener("change", () => {
        applyRoiAxisLimitInput(entry, input, { commit: true });
      });
    });

    entry.resetBtn?.addEventListener("click", () => {
      clearRoiAxisLimitsForEntry(entry);
    });
  });

  document.addEventListener("click", (event) => {
    if (!roiAxisLimitControls.some((entry) => isAxisLimitsPopoverOpen(entry))) return;
    if (Date.now() - roiAxisPopoverOpenedAt < 80) return;
    if (roiAxisLimitControls.some((entry) => entry.popover?.contains(event.target) || entry.toggle?.contains(event.target))) {
      return;
    }
    closeRoiAxisLimitPopovers();
  });

  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") return;
    const openEntry = roiAxisLimitControls.find((entry) => isAxisLimitsPopoverOpen(entry));
    if (!openEntry) return;
    closeRoiAxisLimitPopovers();
    openEntry.toggle?.focus();
  });

  window.addEventListener("resize", scheduleRoiAxisPopoverReposition);
  document.addEventListener("scroll", scheduleRoiAxisPopoverReposition, true);

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
