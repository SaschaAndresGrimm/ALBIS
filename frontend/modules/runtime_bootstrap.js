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
    colormapSelect,
    roiEnableToggle,
    roiModeSelect,
    roiHistogramToggle,
  } = elements;

  const {
    restoreFilesystemMode,
    showSplash,
    drawSplash,
    setFps,
    setFrameStep,
    updateRoiModeUI,
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
  if (roiHistogramToggle) {
    roiState.histogramEnabled = roiHistogramToggle.checked;
  }
  updateRoiModeUI();
}

export function finalizeRuntimeBootstrap({
  state,
  callbacks,
  core,
}) {
  const {
    getMaxPanelWidth,
    nearestMobilePanelSnap,
    applyPanelState,
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
      core.panelLayout?.setMobilePanelSnap(nearestMobilePanelSnap(storedMobileSnap));
    }
  } catch {
    // Ignore storage errors and continue with defaults.
  }

  applyPanelState();
  core.overviewViewport?.applyCanvasTransform();
  core.overviewViewport?.updatePanCapability();
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

  void bootstrapApp()
    // Both of these read the URL fragment and both reopen a file through the
    // ordinary path, so they run after bootstrap rather than inside it, and
    // need the app fully wired.
    //
    // A file the desktop environment asked ALBIS to open comes first, because
    // the clone restore strips the whole fragment once it finds its own token.
    .then(() => core.windowClone.openLaunchTargetFromHash())
    .then(() => core.windowClone.restoreCloneFromHash())
    .catch((err) => {
      console.error(err);
      setSplashStatus("splash.status.initialization_failed");
      setStatus(t("status.app.initialization_failed"), { tone: "error" });
      showSplash();
      setLoading(false);
    });
}
