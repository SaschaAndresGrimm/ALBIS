/**
 * ROI control input bindings.
 */

import { t } from "./i18n.js";
import {
  applyCircularRoiGeometry,
  clampCircularRoiInnerRadius,
  getCircularRoiDirection,
  getCircularRoiOuterRadius,
} from "./roi_geometry_utils.js";
import { normalizeRoiHistogramBinCount } from "./roi_stats_engine.js";

export function bindRoiControlInteractions({
  roiState,
  elements,
  callbacks,
}) {
  const {
    roiEnableToggle,
    roiModeSelect,
    roiHistogramToggle,
    roiHistBinsAuto,
    roiHistBinCount,
    roiHistBinPresetBtns,
    roiClearBtn,
    roiExportCsvBtn,
    roiRadiusInput,
    roiInnerInput,
    roiOuterInput,
    roiCenterXInput,
    roiCenterYInput,
    canvasWrap,
  } = elements;

  const {
    setRoiDragging,
    stopRoiEdit,
    updateRoiModeUI,
    scheduleRoiOverlay,
    scheduleRoiUpdate,
    clearRoi,
    setStatus,
    exportRoiCsv,
    applyRoiCenterFromInputs,
    updateRoiCenterInputs,
  } = callbacks;

  function updateRoiHistogramBinSettings(mode, count = roiState.histogramBins?.count) {
    const nextMode = mode === "fixed" ? "fixed" : "auto";
    roiState.histogramBins = {
      mode: nextMode,
      count: normalizeRoiHistogramBinCount(count),
    };
    updateRoiModeUI();
    scheduleRoiUpdate();
  }

  roiEnableToggle?.addEventListener("change", () => {
    roiState.enabled = Boolean(roiEnableToggle.checked);
    if (!roiState.enabled) {
      setRoiDragging(false);
      stopRoiEdit();
      canvasWrap?.classList.remove("is-roi");
    } else {
      roiState.active = Boolean(roiState.start && roiState.end);
    }
    updateRoiModeUI();
    scheduleRoiOverlay();
    scheduleRoiUpdate();
  });

  roiModeSelect?.addEventListener("change", () => {
    roiState.mode = roiModeSelect.value || "line";
    updateRoiModeUI();
    if (roiState.mode === "circle") {
      roiState.innerRadius = 0;
    }
    roiState.active = Boolean(roiState.start && roiState.end);
    scheduleRoiOverlay();
    scheduleRoiUpdate();
  });

  roiHistogramToggle?.addEventListener("change", () => {
    roiState.histogramEnabled = Boolean(roiHistogramToggle.checked);
    updateRoiModeUI();
    scheduleRoiUpdate();
  });

  roiHistBinsAuto?.addEventListener("change", () => {
    updateRoiHistogramBinSettings(roiHistBinsAuto.checked ? "auto" : "fixed");
  });

  roiHistBinCount?.addEventListener("change", () => {
    const count = normalizeRoiHistogramBinCount(roiHistBinCount.value);
    roiHistBinCount.value = String(count);
    updateRoiHistogramBinSettings("fixed", count);
  });

  roiHistBinPresetBtns?.forEach((button) => {
    button.addEventListener("click", () => {
      const count = normalizeRoiHistogramBinCount(button.dataset?.bins);
      if (roiHistBinsAuto) roiHistBinsAuto.checked = false;
      if (roiHistBinCount) roiHistBinCount.value = String(count);
      updateRoiHistogramBinSettings("fixed", count);
    });
  });

  roiClearBtn?.addEventListener("click", () => {
    clearRoi();
    setStatus(t("status.roi.cleared"));
  });

  roiExportCsvBtn?.addEventListener("click", exportRoiCsv);

  roiRadiusInput?.addEventListener("change", () => {
    if (roiState.mode !== "circle") return;
    if (!roiState.start) {
      const center = applyRoiCenterFromInputs();
      if (center) {
        roiState.start = center;
        roiState.end = center;
      }
    }
    if (!roiState.start) return;
    const radius = Math.max(0, Math.round(Number(roiRadiusInput.value || 0)));
    applyCircularRoiGeometry(
      roiState,
      roiState.start,
      radius,
      getCircularRoiDirection(roiState.start, roiState.end),
    );
    roiState.active = true;
    updateRoiCenterInputs();
    scheduleRoiOverlay();
    scheduleRoiUpdate();
  });

  roiInnerInput?.addEventListener("change", () => {
    if (roiState.mode !== "annulus") return;
    if (!roiState.start) {
      const center = applyRoiCenterFromInputs();
      if (center) {
        roiState.start = center;
        roiState.end = center;
      }
    }
    if (!roiState.start) return;
    const inner = clampCircularRoiInnerRadius(
      Number(roiInnerInput.value || 0),
      getCircularRoiOuterRadius(roiState),
    );
    roiState.innerRadius = inner;
    if (roiInnerInput) roiInnerInput.value = String(inner);
    roiState.active = true;
    updateRoiCenterInputs();
    scheduleRoiOverlay();
    scheduleRoiUpdate();
  });

  roiOuterInput?.addEventListener("change", () => {
    if (roiState.mode !== "annulus") return;
    if (!roiState.start) {
      const center = applyRoiCenterFromInputs();
      if (center) {
        roiState.start = center;
        roiState.end = center;
      }
    }
    if (!roiState.start) return;
    const outer = Math.max(0, Math.round(Number(roiOuterInput.value || 0)));
    applyCircularRoiGeometry(
      roiState,
      roiState.start,
      outer,
      getCircularRoiDirection(roiState.start, roiState.end),
    );
    roiState.innerRadius = clampCircularRoiInnerRadius(roiState.innerRadius, outer);
    if (roiInnerInput) roiInnerInput.value = String(roiState.innerRadius);
    roiState.active = true;
    updateRoiCenterInputs();
    scheduleRoiOverlay();
    scheduleRoiUpdate();
  });

  [roiCenterXInput, roiCenterYInput].forEach((input) => {
    input?.addEventListener("change", () => {
      if (roiState.mode !== "circle" && roiState.mode !== "annulus") return;
      const center = applyRoiCenterFromInputs();
      if (!center) return;
      const direction = getCircularRoiDirection(roiState.start, roiState.end);
      const outer =
        roiState.mode === "circle"
          ? Math.max(0, Math.round(Number(roiRadiusInput?.value || getCircularRoiOuterRadius(roiState))))
          : Math.max(0, Math.round(Number(roiOuterInput?.value || getCircularRoiOuterRadius(roiState))));
      applyCircularRoiGeometry(
        roiState,
        center,
        outer,
        direction,
      );
      if (roiState.mode === "annulus") {
        roiState.innerRadius = clampCircularRoiInnerRadius(roiState.innerRadius, outer);
        if (roiInnerInput) roiInnerInput.value = String(roiState.innerRadius);
      }
      roiState.active = true;
      updateRoiCenterInputs();
      scheduleRoiOverlay();
      scheduleRoiUpdate();
    });
  });
}
