/**
 * The Inpainting section of the Analysis tab: its switch, the "also fill
 * saturated pixels" choice, what it filled in this frame, and the footer's
 * "Inpainted" chip, so nobody mistakes a filled image for a measured one.
 *
 * Turning it on or off redraws the image and recomputes the statistics; the
 * filled frame itself is worked out once per frame by inpainting.js.
 */

import { t } from "./i18n.js";

export function createInpaintingController({ state, elements, callbacks }) {
  const { toggle, saturatedToggle, stateEl, summaryEl, footerChip } = elements;
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
      setSectionBadgeState(stateEl, "active", t("inpaint.state.filled", { count: filled.toLocaleString() }));
      setSummaryChip(summaryEl, t("inpaint.summary.filled", { count: filled.toLocaleString() }), "active");
    } else {
      setSectionBadgeState(stateEl, "active", t("inpaint.state.nothing"));
      setSummaryChip(summaryEl, t("inpaint.summary.nothing"), "active");
    }
  }

  function apply() {
    state.inpaintEnabled = Boolean(toggle?.checked);
    state.inpaintSaturated = Boolean(saturatedToggle?.checked);
    redraw();
    updateGlobalStats();
    scheduleRoiUpdate();
    updateState();
  }

  toggle?.addEventListener("change", apply);
  saturatedToggle?.addEventListener("change", apply);
  if (toggle) toggle.checked = Boolean(state.inpaintEnabled);
  if (saturatedToggle) saturatedToggle.checked = Boolean(state.inpaintSaturated);
  updateState();

  return { updateState };
}
