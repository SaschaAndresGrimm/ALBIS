/**
 * The Inpainting section of the Analysis tab: its switch, the saturated-pixels
 * choice, the margin around flagged pixels, what it filled in this frame, and
 * the footer's "Inpainted" chip, so nobody mistakes a filled image for a
 * measured one.
 *
 * Turning it on or off redraws the image and recomputes the statistics; the
 * filled frame itself is worked out once per frame by inpainting.js.
 */

import { t } from "./i18n.js";
import { normalizeInpaintMargin, normalizeInpaintMethod, normalizeInpaintValue } from "./inpainting.js";

// A contrast level as a fill value. Auto-contrast levels are percentiles, so
// a background of 8 counts arrives as 7.99992: for integer data the nearest
// whole count, for float data four significant figures.
export function levelValue(level, integral = true) {
  const n = Number(level);
  if (!Number.isFinite(n)) return 0;
  return integral ? Math.round(n) : +n.toPrecision(4);
}

function integralData(state) {
  return /[iu]\d+$/.test(String(state.dtype || "")) || !(state.dataRaw instanceof Float32Array || state.dataRaw instanceof Float64Array);
}

export function createInpaintingController({ state, elements, callbacks }) {
  const {
    toggle,
    saturatedToggle,
    marginInput,
    methodSelect,
    valueRow,
    valueInput,
    backgroundBtn,
    foregroundBtn,
    stateEl,
    summaryEl,
    footerChip,
  } = elements;
  const {
    getInpaintedFrame,
    redraw,
    updateGlobalStats,
    scheduleRoiUpdate,
    setSectionBadgeState,
    setSummaryChip,
  } = callbacks;

  function updateState() {
    if (footerChip) footerChip.hidden = !(state.inpaintEnabled && state.hasFrame !== false && state.dataRaw);
    if (!state.inpaintEnabled) {
      setSectionBadgeState(stateEl, "empty", t("inpaint.state.enable_hint"));
      setSummaryChip(summaryEl, t("summary.off"));
      return;
    }
    if (!state.dataRaw) {
      setSectionBadgeState(stateEl, "empty", t("inpaint.state.load_frame"));
      setSummaryChip(summaryEl, t("summary.waiting_frame"));
      return;
    }
    const filled = getInpaintedFrame()?.filled || 0;
    if (filled) {
      const count = filled.toLocaleString();
      const message =
        state.inpaintMethod === "fixed"
          ? t("inpaint.state.set", { count, value: normalizeInpaintValue(state.inpaintValue).toLocaleString() })
          : t("inpaint.state.filled", { count });
      setSectionBadgeState(stateEl, "active", message);
      setSummaryChip(summaryEl, t("inpaint.summary.filled", { count: filled.toLocaleString() }), "active");
    } else {
      setSectionBadgeState(stateEl, "active", t("inpaint.state.nothing"));
      setSummaryChip(summaryEl, t("inpaint.summary.nothing"), "active");
    }
  }

  function apply() {
    state.inpaintEnabled = Boolean(toggle?.checked);
    state.inpaintSaturated = Boolean(saturatedToggle?.checked);
    if (marginInput) {
      state.inpaintMargin = normalizeInpaintMargin(marginInput.value);
      marginInput.value = String(state.inpaintMargin);
    }
    if (methodSelect) state.inpaintMethod = normalizeInpaintMethod(methodSelect.value);
    if (valueInput) {
      state.inpaintValue = normalizeInpaintValue(valueInput.value);
      valueInput.value = String(state.inpaintValue);
    }
    if (valueRow) valueRow.hidden = state.inpaintMethod !== "fixed";
    redraw();
    updateGlobalStats();
    scheduleRoiUpdate();
    updateState();
  }

  toggle?.addEventListener("change", apply);
  saturatedToggle?.addEventListener("change", apply);
  marginInput?.addEventListener("change", apply);
  methodSelect?.addEventListener("change", apply);
  valueInput?.addEventListener("change", apply);
  // The contrast's low or high end, copied once: the fill stays put when the
  // contrast changes later, so the same settings always export the same file.
  const pick = (button, level, key) => {
    if (!button) return;
    const label = () => {
      button.title = t(key, { value: levelValue(level(), integralData(state)).toLocaleString() });
    };
    button.addEventListener("pointerenter", label);
    button.addEventListener("focus", label);
    button.addEventListener("click", () => {
      if (valueInput) valueInput.value = String(levelValue(level(), integralData(state)));
      apply();
    });
  };
  pick(backgroundBtn, () => state.min, "inpaint.value.background_title");
  pick(foregroundBtn, () => state.max, "inpaint.value.foreground_title");
  if (toggle) toggle.checked = Boolean(state.inpaintEnabled);
  if (saturatedToggle) saturatedToggle.checked = Boolean(state.inpaintSaturated);
  if (marginInput) marginInput.value = String(normalizeInpaintMargin(state.inpaintMargin));
  if (methodSelect) methodSelect.value = normalizeInpaintMethod(state.inpaintMethod);
  if (valueInput) valueInput.value = String(normalizeInpaintValue(state.inpaintValue));
  if (valueRow) valueRow.hidden = normalizeInpaintMethod(state.inpaintMethod) !== "fixed";
  updateState();

  return { updateState };
}
