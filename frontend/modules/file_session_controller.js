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
    option,
    setDataControlsForImage,
    setDataControlsForSeries,
    buildNegativeMask,
    updateMaskUI,
    getRenderer,
    isWebglUnsignedRawCandidate,
    toFloat32,
    computeStats,
    updateGlobalStats,
    computeAutoLevels,
    formatValue,
    alignMaskToFrame,
    syncMaskAvailability,
    redraw,
    fitImageToView,
    hideSplash,
    scheduleOverview,
    scheduleRoiUpdate,
    schedulePixelOverlay,
    scheduleResolutionOverlay,
    schedulePeakFinder,
    scheduleHistogram,
  } = callbacks;

  const PLAYBACK_STATS_REFRESH_MS = 220;
  let lastStatsRefreshAt = 0;

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

  function applyFrame(data, width, height, dtype) {
    state.dataRaw = data;
    const activeRenderer = getRenderer();
    state.dataFloat =
      activeRenderer?.type === "webgl" && !isWebglUnsignedRawCandidate(dtype, data) ? toFloat32(data) : null;
    state.width = width;
    state.height = height;
    state.dtype = dtype;
    const now = typeof performance !== "undefined" && typeof performance.now === "function"
      ? performance.now()
      : Date.now();
    const refreshStats =
      !state.playing ||
      !state.hasFrame ||
      !state.stats ||
      now - lastStatsRefreshAt >= PLAYBACK_STATS_REFRESH_MS;
    if (refreshStats) {
      state.stats = computeStats(data);
      state.histogram = state.stats.hist;
      updateGlobalStats();
      lastStatsRefreshAt = now;
      scheduleHistogram();
    }

    if (state.autoScale && state.stats && refreshStats) {
      const levels = computeAutoLevels(data, state.stats.satMax ?? null);
      state.min = levels.min;
      state.max = levels.max;
      if (minInput) minInput.value = formatValue(state.min);
      if (maxInput) maxInput.value = formatValue(state.max);
    }

    if (metaRange && state.stats) {
      metaRange.textContent = `${formatValue(state.stats.min)} → ${formatValue(state.stats.max)}`;
    }
    alignMaskToFrame();
    syncMaskAvailability(false);
    redraw();
    if (!state.hasFrame) {
      fitImageToView();
    }
    state.hasFrame = true;
    updatePanCapability();
    hideSplash();
    updatePlayButtons();
    scheduleOverview();
    scheduleRoiUpdate();
    schedulePixelOverlay();
    scheduleResolutionOverlay();
    schedulePeakFinder();
  }

  function applyExternalFrame(data, shape, dtype, label, fitView, preserveMask = false, options = {}) {
    if (!Array.isArray(shape) || shape.length < 2) return;
    const keepPlaying = Boolean(options.keepPlaying);
    if (!(keepPlaying && state.playing)) {
      stopPlayback();
    }
    const preserveSeries = Boolean(options.preserveSeries);
    if (fitView) {
      state.hasFrame = false;
    }
    if (!preserveSeries) {
      state.file = label;
      state.dataset = "";
      state.seriesFiles = [];
      state.seriesLabel = "";
      state.frameCount = 1;
      state.frameIndex = 0;
      state.thresholdCount = 1;
      state.thresholdIndex = 0;
      state.thresholdEnergies = [];
      updateFrameControls();
      updateThresholdOptions();
      if (datasetSelect) {
        datasetSelect.innerHTML = "";
        datasetSelect.appendChild(option("Single image", ""));
        datasetSelect.value = "";
      }
      setDataControlsForImage();
    } else {
      state.dataset = "";
      state.thresholdCount = 1;
      state.thresholdIndex = 0;
      state.thresholdEnergies = [];
      updateFrameControls();
      updateThresholdOptions();
      if (datasetSelect) {
        datasetSelect.innerHTML = "";
        datasetSelect.appendChild(option("Series image", ""));
        datasetSelect.value = "";
      }
      setDataControlsForSeries();
    }
    const height = shape[0];
    const width = shape[1];
    if (!preserveMask) {
      clearMaskState();
    }
    if (options.autoMask) {
      const autoMask = buildNegativeMask(data);
      if (autoMask) {
        state.maskRaw = autoMask;
        state.maskShape = [height, width];
        state.maskAuto = true;
        state.maskFile = options.maskKey || `auto:${label}`;
        updateMaskUI();
      }
    }
    if (metaShape) metaShape.textContent = `${width} × ${height}`;
    if (metaDtype) metaDtype.textContent = dtype;
    applyFrame(data, width, height, dtype);
    setDataSourceSectionState("active", preserveSeries ? "Series image loaded." : "Image loaded.");
    updateToolbar();
  }

  return {
    closeCurrentFile,
    applyFrame,
    applyExternalFrame,
  };
}
