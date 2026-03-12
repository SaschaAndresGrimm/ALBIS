/**
 * Histogram min/max drag and hover tooltip bindings.
 */

import { t } from "./i18n.js";

export function bindHistogramDragInteractions({
  state,
  elements,
  callbacks,
}) {
  const {
    histCanvas,
    minInput,
    maxInput,
    autoScaleToggle,
  } = elements;

  const {
    snapHistogramValue,
    formatValue,
    redraw,
    scheduleHistogram,
    histogramValueToX,
    histogramXToValue,
    getHistTooltipPosition,
    showHistTooltip,
    hideHistTooltip,
  } = callbacks;

  let histDragging = false;
  let histDragTarget = null;
  let histDragRaf = null;
  let histDragPendingValue = null;

  function applyHistogramDragValue(value) {
    if (!state.stats) return;
    const snapped = snapHistogramValue(value);
    if (!Number.isFinite(snapped)) return;
    const minVal = state.stats.min;
    const maxVal = state.stats.max;
    if (histDragTarget === "min") {
      const clamped = Math.max(minVal, Math.min(snapped, state.max));
      state.min = clamped;
      minInput.value = formatValue(state.min);
    } else if (histDragTarget === "max") {
      const clamped = Math.min(maxVal, Math.max(snapped, state.min));
      state.max = clamped;
      maxInput.value = formatValue(state.max);
    } else {
      return;
    }
    state.autoScale = false;
    autoScaleToggle.checked = false;
    redraw();
    scheduleHistogram();
  }

  function flushHistogramDragFrame() {
    if (histDragRaf) {
      window.cancelAnimationFrame(histDragRaf);
      histDragRaf = null;
    }
    if (histDragPendingValue !== null) {
      applyHistogramDragValue(histDragPendingValue);
      histDragPendingValue = null;
    }
  }

  function scheduleHistogramDragFrame(value) {
    histDragPendingValue = value;
    if (histDragRaf) return;
    histDragRaf = window.requestAnimationFrame(() => {
      histDragRaf = null;
      if (histDragPendingValue === null) return;
      const nextValue = histDragPendingValue;
      histDragPendingValue = null;
      applyHistogramDragValue(nextValue);
    });
  }

  function stopHistDrag(event) {
    flushHistogramDragFrame();
    if (!histDragging) return;
    histDragging = false;
    histDragTarget = null;
    histCanvas.style.cursor = "";
    hideHistTooltip();
    if (event && histCanvas.hasPointerCapture(event.pointerId)) {
      histCanvas.releasePointerCapture(event.pointerId);
    }
  }

  histCanvas.addEventListener("pointerdown", (event) => {
    if (!state.stats) return;
    const rect = histCanvas.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const width = histCanvas.clientWidth;
    const minX = histogramValueToX(state.min, width);
    const maxX = histogramValueToX(state.max, width);
    const threshold = 6;
    const distMin = Math.abs(x - minX);
    const distMax = Math.abs(x - maxX);
    if (Math.min(distMin, distMax) > threshold) return;

    histDragTarget = distMin <= distMax ? "min" : "max";
    histDragging = true;
    histCanvas.setPointerCapture(event.pointerId);
    histCanvas.style.cursor = "ew-resize";
    hideHistTooltip();
    event.preventDefault();
  });

  histCanvas.addEventListener("pointermove", (event) => {
    if (!state.stats) return;
    const rect = histCanvas.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const width = histCanvas.clientWidth;

    if (!histDragging) {
      const minX = histogramValueToX(state.min, width);
      const maxX = histogramValueToX(state.max, width);
      const threshold = 6;
      const distMin = Math.abs(x - minX);
      const distMax = Math.abs(x - maxX);
      if (Math.min(distMin, distMax) <= threshold) {
        histCanvas.style.cursor = "ew-resize";
      } else {
        histCanvas.style.cursor = "";
      }
      const value = snapHistogramValue(histogramXToValue(x, width));
      if (Number.isFinite(value)) {
        const { left, top } = getHistTooltipPosition(rect, x);
        showHistTooltip(t("histogram.tooltip.value", { value: formatValue(value) }), left, top);
      } else {
        hideHistTooltip();
      }
      return;
    }

    const value = histogramXToValue(x, width);
    scheduleHistogramDragFrame(value);
    hideHistTooltip();
    event.preventDefault();
  });

  histCanvas.addEventListener("pointerup", (event) => {
    stopHistDrag(event);
  });

  histCanvas.addEventListener("pointercancel", (event) => {
    stopHistDrag(event);
  });

  histCanvas.addEventListener("pointerleave", () => {
    if (!histDragging) {
      histCanvas.style.cursor = "";
      hideHistTooltip();
    }
  });
}
