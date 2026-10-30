/**
 * The colour bar under the histogram as a window/level control: drag to move
 * the BG-FG window along the values, keeping its width; scroll to widen or
 * narrow it around its centre; double-click for Auto Contrast.
 *
 * The bar is drawn on the histogram's own value axis (linear or log), so moves
 * happen in that axis's pixels and map back through it: a drag follows the
 * pointer whichever scale is shown. Changes go through the same path as
 * dragging the BG and FG markers -- Auto off, both fields, redraw.
 */

const WHEEL_STEP = 0.0015; // relative width change per wheel delta unit
const MIN_WIDTH_PX = 2;

export function bindColorbarWindowInteractions({ state, elements, callbacks }) {
  const { colorbar, minInput, maxInput, autoScaleToggle } = elements;
  const {
    histogramValueToX,
    histogramXToValue,
    snapHistogramValue,
    formatValue,
    redraw,
    scheduleHistogram,
    handleContrastChanged,
  } = callbacks;
  if (!colorbar) return null;

  let drag = null;
  let pending = null;
  let frame = 0;

  const widthOf = () => colorbar.clientWidth || 1;

  /** BG and FG for a window spanning x0..x1 on the bar, in its pixels. */
  function windowFromX(x0, x1) {
    const width = widthOf();
    const lo = Math.max(0, Math.min(width, Math.min(x0, x1)));
    const hi = Math.max(0, Math.min(width, Math.max(x0, x1)));
    const min = snapHistogramValue(histogramXToValue(lo, width));
    const max = snapHistogramValue(histogramXToValue(hi, width));
    return Number.isFinite(min) && Number.isFinite(max) && max > min ? { min, max } : null;
  }

  function apply(next) {
    if (!next) return;
    state.min = next.min;
    state.max = next.max;
    minInput.value = formatValue(state.min);
    maxInput.value = formatValue(state.max);
    state.autoScale = false;
    autoScaleToggle.checked = false;
    redraw();
    scheduleHistogram();
    handleContrastChanged?.("levels");
  }

  function schedule(next) {
    pending = next;
    if (frame) return;
    frame = window.requestAnimationFrame(() => {
      frame = 0;
      const value = pending;
      pending = null;
      apply(value);
    });
  }

  colorbar.addEventListener("pointerdown", (event) => {
    if (!state.stats || event.button !== 0) return;
    const width = widthOf();
    drag = {
      startX: event.clientX,
      x0: histogramValueToX(state.min, width),
      x1: histogramValueToX(state.max, width),
    };
    colorbar.setPointerCapture?.(event.pointerId);
    colorbar.classList.add("is-dragging");
    event.preventDefault();
  });

  colorbar.addEventListener("pointermove", (event) => {
    if (!drag) return;
    const width = widthOf();
    const span = drag.x1 - drag.x0;
    // Keep the window on the bar: it slides to an end and stops there.
    const dx = Math.max(-drag.x0, Math.min(width - drag.x1, event.clientX - drag.startX));
    if (span < MIN_WIDTH_PX) return;
    schedule(windowFromX(drag.x0 + dx, drag.x1 + dx));
  });

  const stop = (event) => {
    if (!drag) return;
    drag = null;
    colorbar.classList.remove("is-dragging");
    if (event && colorbar.hasPointerCapture?.(event.pointerId)) colorbar.releasePointerCapture(event.pointerId);
  };
  colorbar.addEventListener("pointerup", stop);
  colorbar.addEventListener("pointercancel", stop);

  colorbar.addEventListener(
    "wheel",
    (event) => {
      if (!state.stats) return;
      event.preventDefault();
      const width = widthOf();
      const x0 = histogramValueToX(state.min, width);
      const x1 = histogramValueToX(state.max, width);
      const centre = (x0 + x1) / 2;
      const half = Math.max(MIN_WIDTH_PX / 2, ((x1 - x0) / 2) * Math.exp(event.deltaY * WHEEL_STEP));
      schedule(windowFromX(centre - half, centre + half));
    },
    { passive: false },
  );

  // Double-click turns Auto Contrast back on, as the Auto toggle does.
  colorbar.addEventListener("dblclick", () => {
    if (!autoScaleToggle.checked) autoScaleToggle.click();
  });

  return { windowFromX };
}
