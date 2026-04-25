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

export function bindRoiControlInteractions({
  roiState,
  elements,
  callbacks,
}) {
  const {
    roiEnableToggle,
    roiModeSelect,
    roiLogToggle,
    roiHistogramToggle,
    roiLimitsEnable,
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
    updateRoiPlotLimitsEnabled,
    clearRoi,
    setStatus,
    exportRoiCsv,
    applyRoiCenterFromInputs,
    updateRoiCenterInputs,
  } = callbacks;

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

  roiLogToggle?.addEventListener("change", () => {
    roiState.log = roiLogToggle.checked;
    scheduleRoiUpdate();
  });

  roiHistogramToggle?.addEventListener("change", () => {
    roiState.histogramEnabled = Boolean(roiHistogramToggle.checked);
    updateRoiModeUI();
    scheduleRoiUpdate();
  });

  roiLimitsEnable?.addEventListener("change", updateRoiPlotLimitsEnabled);

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
