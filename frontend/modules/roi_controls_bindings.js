/**
 * ROI control input bindings.
 */

export function bindRoiControlInteractions({
  state,
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
    setStatus("ROI cleared");
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
    roiState.outerRadius = radius;
    roiState.end = { x: Math.min(state.width - 1, roiState.start.x + radius), y: roiState.start.y };
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
    const inner = Math.max(0, Math.round(Number(roiInnerInput.value || 0)));
    roiState.innerRadius = inner;
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
    roiState.outerRadius = outer;
    roiState.end = { x: Math.min(state.width - 1, roiState.start.x + outer), y: roiState.start.y };
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
      roiState.start = center;
      const outer =
        roiState.mode === "circle"
          ? Math.max(0, Math.round(Number(roiRadiusInput?.value || roiState.outerRadius || 0)))
          : Math.max(0, Math.round(Number(roiOuterInput?.value || roiState.outerRadius || 0)));
      roiState.outerRadius = outer;
      roiState.end = { x: Math.min(state.width - 1, center.x + outer), y: center.y };
      roiState.active = true;
      updateRoiCenterInputs();
      scheduleRoiOverlay();
      scheduleRoiUpdate();
    });
  });
}
