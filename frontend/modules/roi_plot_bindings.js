/**
 * ROI plot pan/zoom/resize interaction bindings.
 */

import { t } from "./i18n.js";
import { axisChipState, toggleAxisChip } from "./roi_axis_chip_utils.js";
import {
  panAxisRange,
  roiPlotBoxLimits,
  roiPlotHitZone,
  roiPlotPanMode,
  roiPlotWheelAction,
  zoomAxisRange,
} from "./roi_plot_pan_utils.js";
import { createRoiPlotViewHistory } from "./roi_plot_view_history.js";
import { roiPlotSampleIndexAt } from "./roi_plot_view_utils.js";

// Wheel and trackpad events closer together than this are one gesture, and
// the history remembers the view once for it, not once per event.
const WHEEL_GESTURE_GAP_MS = 600;

export function bindRoiPlotInteractions({
  roiState,
  elements,
  callbacks,
  core,
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
    redrawRoiPlots,
    clearRoiPlotLimitsForKey,
    hasManualRoiPlotLimits,
    hasAnyManualRoiPlotLimits,
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
    unitSelect: entry.container.querySelector("[data-roi-resolution-unit]"),
    autoToggle: entry.container.querySelector("[data-roi-axis-auto]"),
    inputs: [...entry.container.querySelectorAll(".roi-axis-limits-grid input")],
    resetBtn: entry.container.querySelector("[data-roi-axis-reset]"),
    backBtn: entry.container.querySelector("[data-roi-view-back]"),
  }));
  roiPlotEntries.forEach((entry) => {
    entry.container.querySelector("[data-roi-plot-png]")?.addEventListener("click", () => {
      core?.roiStats.exportRoiPlotPng(entry.key);
    });
  });

  let roiPlotResizing = null;
  let roiPlotResizeStart = { x: 0, y: 0, height: 0, container: null };
  let roiPlotPanning = null;
  let roiPlotBoxZoom = null;
  const viewHistory = createRoiPlotViewHistory();
  const lastWheelAt = new Map();
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
      if (hasManual) entry.stash = null; // a fresh manual state replaces the remembered one
      if (entry.chip) {
        const state = axisChipState(limits, entry.stash);
        const auto = state === "auto";
        // The keys go on the element as well, so a language switch retranslates
        // the tag in place (applyI18nToDom) rather than waiting for the next zoom.
        entry.chip.dataset.i18n = auto ? "roi.axis.auto_chip" : "roi.axis.manual_chip";
        entry.chip.dataset.i18nTitle = auto ? "roi.axis.chip_hint.auto" : "roi.axis.chip_hint.manual";
        entry.chip.textContent = t(entry.chip.dataset.i18n);
        entry.chip.title = t(entry.chip.dataset.i18nTitle);
        entry.chip.classList.toggle("is-hidden", state === "none");
      }
      if (entry.autoToggle) {
        entry.autoToggle.checked = !hasManual;
      }
      if (entry.backBtn) {
        entry.backBtn.classList.toggle("is-hidden", viewHistory.size(entry.key) === 0);
      }
      if (entry.resetBtn) {
        entry.resetBtn.disabled = !hasManual;
      }
      if (entry.logToggle) {
        entry.logToggle.checked = Boolean(getRoiPlotLog?.(entry.key));
        entry.logToggle.disabled = roiState.enabled === false;
      }
      if (entry.unitSelect) {
        entry.unitSelect.value = roiState.resolutionAxisUnit === "q" ? "q" : "d";
        entry.unitSelect.disabled = roiState.enabled === false;
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
    syncRoiAxisLimitControls(entry.key);
    scheduleRoiPlotRedraw();
    return true;
  }

  function toggleRoiAxisChip(entry) {
    const next = toggleAxisChip(getRoiPlotLimits(entry.key), entry.stash);
    if (!next) return;
    entry.stash = next.stash;
    setRoiPlotAxisLimits(entry.key, "x", next.limits.xMin, next.limits.xMax);
    setRoiPlotAxisLimits(entry.key, "y", next.limits.yMin, next.limits.yMax);
    updateGlobalAutoscaleFromManualLimits();
    syncRoiAxisLimitControls(entry.key);
    scheduleRoiPlotRedraw();
  }

  function clearRoiAxisLimitsForEntry(entry) {
    if (!entry) return;
    entry.stash = null;
    clearRoiPlotLimitsForKey(entry.key);
    updateGlobalAutoscaleFromManualLimits();
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
    syncRoiAxisLimitControls(entry.key, { preserveInput: commit ? null : input });
    scheduleRoiPlotRedraw();
  }

  // The zone under the pointer, as data-pan-zone ("plot", "x" or "y"): the
  // stylesheet picks the cursor from it and the renderer tints an axis strip.
  function setRoiPlotPanZone(canvasEl, zone) {
    const previous = canvasEl.dataset.panZone || null;
    if (previous === zone) return;
    if (zone) {
      canvasEl.dataset.panZone = zone;
    } else {
      delete canvasEl.dataset.panZone;
    }
    if (previous === "x" || previous === "y" || zone === "x" || zone === "y") {
      scheduleRoiPlotRedraw();
    }
  }

  function roiPlotZoneAt(canvasEl, clientX, clientY) {
    const plot = canvasEl?._roiPlot;
    if (!plot) return null;
    const rect = canvasEl.getBoundingClientRect();
    return roiPlotHitZone(plot, clientX - rect.left, clientY - rect.top);
  }

  function updateRoiPlotPanReadyState(canvasEl, clientX, clientY) {
    if (!canvasEl) return;
    if (roiPlotPanning && roiPlotPanning.canvasEl === canvasEl) return;
    const mode = roiPlotPanMode(roiPlotZoneAt(canvasEl, clientX, clientY));
    setRoiPlotPanZone(canvasEl, mode === "both" ? "plot" : mode);
  }

  function beginRoiPlotPan(event, canvasEl) {
    if (!canvasEl || event.button !== 0 || roiPlotResizing) return false;
    const plot = canvasEl._roiPlot;
    if (!plot) return false;
    const mode = roiPlotPanMode(roiPlotZoneAt(canvasEl, event.clientX, event.clientY));
    if (!mode) return false;

    const plotKey = getRoiPlotKey(canvasEl);
    const currentLimits = getRoiPlotLimits(plotKey);
    roiPlotPanning = {
      canvasEl,
      mode,
      log: Boolean(plot.log),
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

    setRoiPlotPanZone(canvasEl, mode === "both" ? "plot" : mode);
    canvasEl.classList.add("is-panning");
    if (typeof canvasEl.setPointerCapture === "function") {
      canvasEl.setPointerCapture(event.pointerId);
    }
    event.preventDefault();
    return true;
  }

  function moveRoiPlotPan(event, canvasEl) {
    const pan = roiPlotPanning;
    if (!pan || pan.canvasEl !== canvasEl || pan.pointerId !== event.pointerId) {
      return;
    }
    const plot = canvasEl?._roiPlot;
    if (!plot) return;

    const plotKey = getRoiPlotKey(canvasEl);
    const plotWidth = Math.max(1, plot.width - plot.padL - plot.padR);
    const plotHeight = Math.max(1, plot.height - plot.padT - plot.padB);
    const movesX = pan.mode !== "y";
    const movesY = pan.mode !== "x";
    const dx = movesX ? event.clientX - pan.startClientX : 0;
    const dy = movesY ? event.clientY - pan.startClientY : 0;

    if (!pan.hasMoved && (Math.abs(dx) > 2 || Math.abs(dy) > 2)) {
      pan.hasMoved = true;
      rememberView(plotKey);
      if (roiState.plotLimits.autoscale) {
        roiState.plotLimits.autoscale = false;
      }
      // Pin the axis left alone at what is shown, so turning autoscale off
      // does not move it.
      const limits = getRoiPlotLimits(plotKey);
      if (!Number.isFinite(limits.xMin) || !Number.isFinite(limits.xMax)) {
        setRoiPlotAxisLimits(plotKey, "x", plot.xMin, plot.xMax);
      }
      if (!Number.isFinite(limits.yMin) || !Number.isFinite(limits.yMax)) {
        setRoiPlotAxisLimits(plotKey, "y", plot.yMin, plot.yMax);
      }
    }

    if (!pan.hasMoved) return;

    // The data follows the pointer: right moves x to smaller values, down
    // moves y to larger ones.
    if (movesX) {
      const next = panAxisRange(
        { min: pan.xMin, max: pan.xMax, domainMin: pan.domainXMin, domainMax: pan.domainXMax },
        dx / plotWidth
      );
      if (next) setRoiPlotAxisLimits(plotKey, "x", next.min, next.max);
    }
    if (movesY) {
      const next = panAxisRange(
        {
          min: pan.yMin,
          max: pan.yMax,
          domainMin: pan.domainYMin,
          domainMax: pan.domainYMax,
          log: pan.log,
          bound: "overlap",
        },
        -dy / plotHeight
      );
      if (next) setRoiPlotAxisLimits(plotKey, "y", next.min, next.max);
    }
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
      setRoiPlotPanZone(canvasEl, null);
    }
    if (Number.isFinite(pointerId) && typeof canvasEl.releasePointerCapture === "function") {
      if (canvasEl.hasPointerCapture(pointerId)) {
        canvasEl.releasePointerCapture(pointerId);
      }
    }
  }

  function rememberView(plotKey) {
    viewHistory.push(plotKey, getRoiPlotLimits(plotKey));
  }

  function rememberViewOncePerGesture(plotKey) {
    const now = Date.now();
    if (now - (lastWheelAt.get(plotKey) ?? 0) > WHEEL_GESTURE_GAP_MS) rememberView(plotKey);
    lastWheelAt.set(plotKey, now);
  }

  function goToPreviousView(plotKey) {
    const view = viewHistory.pop(plotKey);
    if (!view) return;
    setRoiPlotAxisLimits(plotKey, "x", view.xMin, view.xMax);
    setRoiPlotAxisLimits(plotKey, "y", view.yMin, view.yMax);
    roiState.plotLimits.autoscale = !hasAnyManualRoiPlotLimits();
    syncRoiAxisLimitControls(plotKey);
    scheduleRoiPlotRedraw();
  }

  // The sample under the pointer, which the renderer marks: the tooltip's
  // number alone does not say which point on a dense profile it belongs to.
  function updateRoiHoverSample(canvasEl, clientX, clientY) {
    const plot = canvasEl._roiPlot;
    let next = null;
    if (plot && !roiPlotBoxZoom && roiPlotZoneAt(canvasEl, clientX, clientY) === "plot") {
      const idx = roiPlotSampleIndexAt(plot, clientX - canvasEl.getBoundingClientRect().left);
      if (idx >= 0) next = plot.xStart + idx * plot.xStep;
    }
    if (next === (canvasEl._roiHoverX ?? null)) return;
    canvasEl._roiHoverX = next;
    scheduleRoiPlotRedraw();
  }

  function boxPointFor(canvasEl, event) {
    const plot = canvasEl._roiPlot;
    const rect = canvasEl.getBoundingClientRect();
    return {
      x: Math.max(plot.padL, Math.min(plot.width - plot.padR, event.clientX - rect.left)),
      y: Math.max(plot.padT, Math.min(plot.height - plot.padB, event.clientY - rect.top)),
    };
  }

  // Right-drag (or Ctrl-drag, for a one-button trackpad) in the plot draws a
  // box and zooms into it on release: the same gesture that draws an ROI on
  // the image. It was Shift-drag, which on the image sets the contrast.
  function beginRoiPlotBoxZoom(event, canvasEl) {
    const isRightClick = event.button === 2;
    const isCtrlClick = event.button === 0 && event.ctrlKey;
    if (!(isRightClick || isCtrlClick) || roiPlotResizing || !canvasEl._roiPlot) return false;
    if (roiPlotZoneAt(canvasEl, event.clientX, event.clientY) !== "plot") return false;
    const { x, y } = boxPointFor(canvasEl, event);
    roiPlotBoxZoom = { canvasEl, pointerId: event.pointerId };
    canvasEl._roiZoomBox = { x0: x, y0: y, x1: x, y1: y };
    canvasEl._roiHoverX = null;
    canvasEl.classList.add("is-box-zooming");
    if (typeof canvasEl.setPointerCapture === "function") {
      canvasEl.setPointerCapture(event.pointerId);
    }
    event.preventDefault();
    return true;
  }

  function moveRoiPlotBoxZoom(event, canvasEl) {
    const box = canvasEl._roiZoomBox;
    if (!box || !canvasEl._roiPlot || roiPlotBoxZoom.pointerId !== event.pointerId) return;
    const { x, y } = boxPointFor(canvasEl, event);
    box.x1 = x;
    box.y1 = y;
    scheduleRoiPlotRedraw();
    event.preventDefault();
  }

  function endRoiPlotBoxZoom(event, canvasEl, { apply }) {
    const box = canvasEl._roiZoomBox;
    const pointerId = roiPlotBoxZoom?.pointerId;
    roiPlotBoxZoom = null;
    canvasEl._roiZoomBox = null;
    canvasEl.classList.remove("is-box-zooming");
    if (Number.isFinite(pointerId) && canvasEl.hasPointerCapture?.(pointerId)) {
      canvasEl.releasePointerCapture(pointerId);
    }
    const plot = canvasEl._roiPlot;
    if (apply && plot && box) {
      const next = roiPlotBoxLimits(plot, box, { minXSpan: 2 * Math.abs(plot.xStep || 1) });
      if (next.x || next.y) {
        const plotKey = getRoiPlotKey(canvasEl);
        rememberView(plotKey);
        roiState.plotLimits.autoscale = false;
        // A flat or thin box zooms one direction; the other stays as shown.
        const x = next.x ?? { min: plot.xMin, max: plot.xMax };
        const y = next.y ?? { min: plot.yMin, max: plot.yMax };
        setRoiPlotAxisLimits(plotKey, "x", x.min, x.max);
        setRoiPlotAxisLimits(plotKey, "y", y.min, y.max);
        syncRoiAxisLimitControls(plotKey);
      }
    }
    scheduleRoiPlotRedraw();
  }

  roiPlotCanvases.forEach((canvasEl) => {
    if (!canvasEl) return;

    canvasEl.addEventListener("mousemove", (event) => {
      updateRoiTooltip(event, canvasEl);
      updateRoiHoverSample(canvasEl, event.clientX, event.clientY);
      updateRoiPlotPanReadyState(canvasEl, event.clientX, event.clientY);
    });

    canvasEl.addEventListener("mouseleave", () => {
      hideRoiTooltip(canvasEl);
      if (canvasEl._roiHoverX != null) {
        canvasEl._roiHoverX = null;
        scheduleRoiPlotRedraw();
      }
      if (!(roiPlotPanning && roiPlotPanning.canvasEl === canvasEl)) {
        setRoiPlotPanZone(canvasEl, null);
      }
    });

    // The right button draws a zoom box here, as it draws an ROI on the image.
    canvasEl.addEventListener("contextmenu", (event) => {
      event.preventDefault();
    });

    canvasEl.addEventListener("pointerdown", (event) => {
      if (beginRoiPlotBoxZoom(event, canvasEl)) return;
      beginRoiPlotPan(event, canvasEl);
    });

    canvasEl.addEventListener("pointermove", (event) => {
      if (roiPlotBoxZoom && roiPlotBoxZoom.canvasEl === canvasEl) {
        moveRoiPlotBoxZoom(event, canvasEl);
        return;
      }
      if (roiPlotPanning && roiPlotPanning.canvasEl === canvasEl) {
        moveRoiPlotPan(event, canvasEl);
        return;
      }
      updateRoiPlotPanReadyState(canvasEl, event.clientX, event.clientY);
    });

    canvasEl.addEventListener("pointerup", (event) => {
      if (roiPlotBoxZoom && roiPlotBoxZoom.canvasEl === canvasEl) {
        endRoiPlotBoxZoom(event, canvasEl, { apply: true });
        return;
      }
      endRoiPlotPan(event, canvasEl);
    });

    canvasEl.addEventListener("pointercancel", (event) => {
      if (roiPlotBoxZoom && roiPlotBoxZoom.canvasEl === canvasEl) {
        endRoiPlotBoxZoom(event, canvasEl, { apply: false });
        return;
      }
      endRoiPlotPan(event, canvasEl);
    });

    canvasEl.addEventListener("dblclick", (event) => {
      event.preventDefault();
      const plotKey = getRoiPlotKey(canvasEl);
      const mode = roiPlotPanMode(roiPlotZoneAt(canvasEl, event.clientX, event.clientY));
      const dblEntry = roiAxisLimitControls.find((e) => e.key === plotKey);
      if (dblEntry) dblEntry.stash = null;
      if (hasManualRoiPlotLimits?.(plotKey)) rememberView(plotKey);
      if (mode === "x" || mode === "y") {
        // On an axis, reset that axis only.
        setRoiPlotAxisLimits(plotKey, mode, null, null);
      } else {
        clearRoiPlotLimitsForKey(plotKey);
      }
      if (!hasAnyManualRoiPlotLimits()) {
        roiState.plotLimits.autoscale = true;
      }
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
        const zone = roiPlotHitZone(plot, x, y);
        const inYAxis = zone === "y-axis";
        const inXAxis = zone === "x-axis";
        const inPlotArea = zone === "plot";
        if (!zone) return;

        const action = roiPlotWheelAction(event);
        if (!action) return;
        // A sideways swipe pans x; over the y axis there is no x to pan.
        if (action.kind === "pan-x" && inYAxis) return;
        const plotKey = getRoiPlotKey(canvasEl);
        rememberViewOncePerGesture(plotKey);

        if (roiState.plotLimits.autoscale) {
          roiState.plotLimits.autoscale = false;
        }

        // Each notch starts from the range last set, not from what was drawn,
        // so nothing is lost between notches.
        const limits = getRoiPlotLimits(plotKey) || {};
        const pinned = (lo, hi) => Number.isFinite(lo) && Number.isFinite(hi) && hi > lo;
        const plotW = Math.max(1, plot.width - plot.padL - plot.padR);
        const plotH = Math.max(1, plot.height - plot.padT - plot.padB);
        const xRange = () => {
          const useLimits = pinned(limits.xMin, limits.xMax);
          return {
            min: useLimits ? limits.xMin : plot.xMin,
            max: useLimits ? limits.xMax : plot.xMax,
            domainMin: Number.isFinite(plot.totalXMin) ? plot.totalXMin : null,
            domainMax: Number.isFinite(plot.totalXMax) ? plot.totalXMax : null,
          };
        };

        if (action.kind === "pan-x") {
          // Like a drag, a swipe keeps y where it is rather than refitting it
          // to every step.
          if (!pinned(limits.yMin, limits.yMax)) setRoiPlotAxisLimits(plotKey, "y", plot.yMin, plot.yMax);
          // Swiping right shows what lies to the right, as a scrollbar does.
          const next = panAxisRange(xRange(), -action.px / plotW);
          if (next) setRoiPlotAxisLimits(plotKey, "x", next.min, next.max);
        } else {
          if (inXAxis || inPlotArea) {
            const next = zoomAxisRange(
              // Two samples across at the most: closer than that is empty space.
              { ...xRange(), minSpan: 2 * Math.abs(plot.xStep || 1) },
              (x - plot.padL) / plotW,
              action.factor
            );
            if (next) setRoiPlotAxisLimits(plotKey, "x", next.min, next.max);
          }
          if (inYAxis || inPlotArea) {
            const useLimits = pinned(limits.yMin, limits.yMax);
            const next = zoomAxisRange(
              {
                min: useLimits ? limits.yMin : plot.yMin,
                max: useLimits ? limits.yMax : plot.yMax,
                log: Boolean(plot.log),
              },
              (plot.height - plot.padB - y) / plotH,
              action.factor
            );
            if (next) setRoiPlotAxisLimits(plotKey, "y", next.min, next.max);
          }
        }

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

    entry.chip?.addEventListener("click", () => toggleRoiAxisChip(entry));

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

    entry.unitSelect?.addEventListener("change", () => {
      roiState.resolutionAxisUnit = entry.unitSelect.value === "q" ? "q" : "d";
      scheduleRoiPlotRedraw();
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

    entry.backBtn?.addEventListener("click", () => {
      goToPreviousView(entry.key);
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
