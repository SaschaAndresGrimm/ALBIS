/**
 * Runtime bootstrap helpers for initial UI defaults and startup sequencing.
 */

import { t } from "./i18n.js";

export function initializeUiDefaults({
  state,
  roiState,
  backendIsLocal,
  elements,
  callbacks,
}) {
  const {
    filesystemMode,
    fpsSelect,
    frameStep,
    histLogX,
    histLogY,
    colormapSelect,
    roiEnableToggle,
    roiModeSelect,
    roiLogToggle,
    roiHistogramToggle,
  } = elements;

  const {
    restoreFilesystemMode,
    showSplash,
    drawSplash,
    setFps,
    setFrameStep,
    updateRoiModeUI,
    updateRoiPlotLimitsEnabled,
  } = callbacks;

  restoreFilesystemMode();

  if (backendIsLocal && filesystemMode) {
    filesystemMode.parentElement?.classList.add("is-hidden");
  }

  showSplash();
  drawSplash();

  if (fpsSelect) {
    setFps(Number(fpsSelect.value));
  }
  if (frameStep) {
    setFrameStep(frameStep.value);
  }
  if (histLogX) {
    state.histLogX = histLogX.checked;
  }
  if (histLogY) {
    state.histLogY = histLogY.checked;
  }
  if (colormapSelect) {
    colormapSelect.value = state.colormap;
  }
  if (roiEnableToggle) {
    roiState.enabled = roiEnableToggle.checked;
  }
  if (roiModeSelect) {
    roiState.mode = roiModeSelect.value || "line";
    roiState.active = false;
  }
  if (roiLogToggle) {
    roiState.log = roiLogToggle.checked;
  }
  if (roiHistogramToggle) {
    roiState.histogramEnabled = roiHistogramToggle.checked;
  }
  updateRoiModeUI();

  updateRoiPlotLimitsEnabled();
}

export function finalizeRuntimeBootstrap({
  state,
  callbacks,
}) {
  const {
    getMaxPanelWidth,
    nearestMobilePanelSnap,
    setMobilePanelSnap,
    applyPanelState,
    applyCanvasTransform,
    updatePanCapability,
    loadAutoloadSettings,
    updatePlayButtons,
    updateViewerFooter,
    setDataSourceSectionState,
    updateFullscreenUi,
    updateAboutVersion,
    initHelpTooltips,
    startBackendHeartbeat,
    bootstrapApp,
    setSplashStatus,
    setStatus,
    showSplash,
    setLoading,
  } = callbacks;

  try {
    const storedWidth = Number(localStorage.getItem("albis.panelWidth"));
    const storedCollapsed = localStorage.getItem("albis.panelCollapsed");
    const storedMobileSnap = Number(localStorage.getItem("albis.mobilePanelSnap"));
    if (storedWidth) {
      state.panelWidth = Math.max(220, Math.min(getMaxPanelWidth(), storedWidth));
    }
    if (storedCollapsed !== null) {
      state.panelCollapsed = storedCollapsed === "true";
    } else if (window.innerWidth < 900) {
      state.panelCollapsed = true;
    }
    if (Number.isFinite(storedMobileSnap) && storedMobileSnap > 0) {
      setMobilePanelSnap(nearestMobilePanelSnap(storedMobileSnap));
    }
  } catch {
    // Ignore storage errors and continue with defaults.
  }

  applyPanelState();
  applyCanvasTransform();
  updatePanCapability();
  loadAutoloadSettings();
  updatePlayButtons();
  updateViewerFooter();
  if (!state.file) {
    setDataSourceSectionState("empty", t("data_source.choose_to_begin"));
  }
  updateFullscreenUi();
  updateAboutVersion();
  initHelpTooltips();
  startBackendHeartbeat();

  void bootstrapApp().catch((err) => {
    console.error(err);
    setSplashStatus(t("splash.status.initialization_failed"));
    setStatus(t("status.app.initialization_failed"));
    showSplash();
    setLoading(false);
  });
}
