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
    roiCenterSnapBtn,
    canvasWrap,
  } = elements;

  const {
    setRoiDragging,
    stopRoiEdit,
    updateRoiModeUI,
    scheduleRoiOverlay,
    scheduleRoiUpdate,
    handleRoiChanged,
    clearRoi,
    setStatus,
    exportRoiCsv,
    applyRoiCenterFromInputs,
    updateRoiCenterInputs,
    getRingParams,
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
    handleRoiChanged?.("roi");
  });

  roiModeSelect?.addEventListener("change", () => {
    roiState.mode = roiModeSelect.value || "line";
    updateRoiModeUI();
    if (roiState.mode === "circle") {
      roiState.innerRadius = 0;
    }
    if (
      (roiState.mode === "circle" || roiState.mode === "annulus") &&
      roiState.start &&
      roiState.end &&
      !(Number(roiState.outerRadius) > 0)
    ) {
      // Switching from a line/box carries over start/end but leaves outerRadius
      // at its 0 default, so the radial profile reads radius 0 and comes back
      // empty until the ROI is nudged. Seed the circle geometry from the current
      // start/end (the radius the overlay already shows) so the profile is
      // populated immediately.
      applyCircularRoiGeometry(
        roiState,
        roiState.start,
        Math.round(Math.hypot(
          Number(roiState.end.x) - Number(roiState.start.x),
          Number(roiState.end.y) - Number(roiState.start.y),
        )),
        getCircularRoiDirection(roiState.start, roiState.end),
      );
    }
    roiState.active = Boolean(roiState.start && roiState.end);
    scheduleRoiOverlay();
    scheduleRoiUpdate();
    handleRoiChanged?.("roi");
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
    handleRoiChanged?.("roi");
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
    handleRoiChanged?.("roi");
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
    handleRoiChanged?.("roi");
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
    handleRoiChanged?.("roi");
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
      handleRoiChanged?.("roi");
    });
  });

  roiCenterSnapBtn?.addEventListener("click", () => {
    if (roiState.mode !== "circle" && roiState.mode !== "annulus") return;
    const params = typeof getRingParams === "function" ? getRingParams() : null;
    if (
      !params ||
      !params.centerKnown ||
      !Number.isFinite(params.centerX) ||
      !Number.isFinite(params.centerY)
    ) {
      setStatus?.(t("status.roi.snap_no_beam"));
      return;
    }
    const center = { x: Math.round(params.centerX), y: Math.round(params.centerY) };
    if (!roiState.start) {
      roiState.start = center;
      roiState.end = center;
    }
    const outer = Math.max(0, Math.round(getCircularRoiOuterRadius(roiState)));
    const direction = getCircularRoiDirection(roiState.start, roiState.end);
    applyCircularRoiGeometry(roiState, center, outer, direction);
    if (roiState.mode === "annulus") {
      roiState.innerRadius = clampCircularRoiInnerRadius(roiState.innerRadius, outer);
      if (roiInnerInput) roiInnerInput.value = String(roiState.innerRadius);
    }
    roiState.active = true;
    updateRoiCenterInputs();
    scheduleRoiOverlay();
    scheduleRoiUpdate();
    handleRoiChanged?.("roi");
    setStatus?.(t("status.roi.snapped_beam", { x: center.x, y: center.y }));
  });
}
