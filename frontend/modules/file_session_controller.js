/**
 * File/session teardown helpers.
 */

export function createFileSessionController({
  state,
  analysisState,
  elements,
  callbacks,
}) {
  const {
    fileSelect,
    datasetSelect,
    minInput,
    maxInput,
    metaShape,
    metaDtype,
    metaRange,
    canvas,
  } = elements;

  const {
    stopPlayback,
    clearMaskState,
    clearImageHeader,
    updateToolbar,
    setDataSourceSectionState,
    setStatus,
    setLoading,
    hideUploadProgress,
    hideProcessingProgress,
    showSplash,
    setSplashStatus,
    updateInspectorHeaderVisibility,
    updateFrameControls,
    updateThresholdOptions,
    applyCanvasTransform,
    updatePanCapability,
    clearHistogram,
    renderPeakList,
    schedulePeakOverlay,
    syncSeriesSumOutputPath,
    clearRoi,
    updateRingsSectionState,
    updatePeaksSectionState,
    updatePlayButtons,
  } = callbacks;

  function closeCurrentFile() {
    stopPlayback();
    state.file = "";
    state.dataset = "";
    state.shape = [];
    state.dtype = "";
    state.frameCount = 1;
    state.frameIndex = 0;
    state.thresholdCount = 1;
    state.thresholdIndex = 0;
    state.thresholdEnergies = [];
    state.dataRaw = null;
    state.dataFloat = null;
    state.histogram = null;
    state.stats = null;
    state.hasFrame = false;
    state.panOffsetX = 0;
    state.panOffsetY = 0;
    state.renderOffsetX = 0;
    state.renderOffsetY = 0;
    state.globalStats = null;
    analysisState.peaks = [];
    analysisState.selectedPeaks = [];
    analysisState.peakSelectionAnchor = null;
    clearMaskState();
    clearImageHeader();
    updateToolbar();
    setDataSourceSectionState("empty", "No file loaded.");
    setStatus("No file loaded");
    setLoading(false);
    hideUploadProgress();
    hideProcessingProgress();
    showSplash();
    setSplashStatus("Ready. Open a file to begin.");
    updateInspectorHeaderVisibility("");

    if (fileSelect) {
      fileSelect.selectedIndex = 0;
    }
    if (datasetSelect) {
      datasetSelect.innerHTML = "";
    }
    updateFrameControls();
    updateThresholdOptions();
    if (minInput) minInput.value = "";
    if (maxInput) maxInput.value = "";
    if (metaShape) metaShape.textContent = "-";
    if (metaDtype) metaDtype.textContent = "-";
    if (metaRange) metaRange.textContent = "-";

    if (canvas) {
      canvas.width = 1;
      canvas.height = 1;
      const ctx = canvas.getContext("2d");
      if (ctx) {
        ctx.clearRect(0, 0, 1, 1);
      }
    }
    applyCanvasTransform();
    updatePanCapability();
    clearHistogram();
    renderPeakList();
    schedulePeakOverlay();
    syncSeriesSumOutputPath(true);
    clearRoi();
    updateRingsSectionState();
    updatePeaksSectionState();
    updatePlayButtons();
  }

  return {
    closeCurrentFile,
  };
}
