/**
 * File/session teardown helpers.
 */

import { t } from "./i18n.js";

export function createFileSessionController({
  state,
  analysisState,
  elements,
  callbacks,
  core,
}) {
  const {
    fileSelect,
    datasetSelect,
    minInput,
    maxInput,
    metaShape,
    metaDtype,
    metaRange,
    metaSaturation,
    canvas,
  } = elements;

  const {
    clearGeometrySource,
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
    getContrastData = () => null,
    formatValue,
    alignMaskToFrame,
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
    core.framePlayback?.stopPlayback();
    core.fileDataPipeline?.resetTransientLoadState();
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
    state.pixelAspect = 1;
    state.globalStats = null;
    analysisState.peaks = [];
    analysisState.selectedPeaks = [];
    analysisState.peakSelectionAnchor = null;
    analysisState.ringMode = "planar";
    analysisState.ringGeometry = null;
    analysisState.ringGeometrySource = "";
    analysisState.ringGeometryKey = "";
    analysisState.geometryOverrideActive = false;
    // The closed file's metadata goes; the user's geometry override stays,
    // since it is meant for every file until switched off.
    clearGeometrySource?.();
    core.sourceMetadata?.clearImageGeometry();
    clearMaskState();
    clearImageHeader();
    updateToolbar();
    setDataSourceSectionState("empty", t("status.file.no_file_loaded"));
    setStatus(t("status.file.no_file_loaded"));
    setLoading(false);
    hideUploadProgress();
    hideProcessingProgress();
    showSplash();
    setSplashStatus("splash.status.ready_open_file");
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
    if (metaSaturation) metaSaturation.textContent = "-";

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
    const canUseUnsignedWebglTextures =
      activeRenderer?.type === "webgl" &&
      activeRenderer?.supportsUnsignedTextures &&
      isWebglUnsignedRawCandidate(dtype, data);
    state.dataFloat =
      activeRenderer?.type === "webgl" && !canUseUnsignedWebglTextures ? toFloat32(data) : null;
    state.width = width;
    state.height = height;
    state.dtype = dtype;
    // Before the statistics, not after: `updateGlobalStats` is mask-aware, and
    // a mask fetched while no frame was loaded yet is still marked unavailable
    // at this point because `syncMaskAvailability` had no frame shape to match
    // it against. Computing the global statistics first reported the first
    // frame of a series as having no gap and no defective pixels at all, and
    // only stepping to the next frame corrected it.
    alignMaskToFrame();
    core.maskCursor.syncMaskAvailability(false);
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
      // The saturation value comes with the stats, so Mask saturated can only
      // say whether it applies now: refreshed earlier, it stayed disabled and
      // unticked while saturated pixels were already being masked.
      updateMaskUI();
      refreshDisplayContrast(state.autoScale);
      updateGlobalStats();
      lastStatsRefreshAt = now;
      scheduleHistogram();
    }

    if (metaRange && state.stats) {
      metaRange.textContent = `${formatValue(state.stats.min)} → ${formatValue(state.stats.max)}`;
    }
    if (metaSaturation && state.stats) {
      metaSaturation.textContent = Number.isFinite(state.stats.satMax)
        ? formatValue(state.stats.satMax)
        : t("common.none");
    }
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

  /**
   * The histogram and, when `updateLevels`, the auto contrast, from the frame
   * as shown. That is the raw frame, except with flatfield correction on: its
   * values can sit on another scale altogether (transmission, 0 to 1), so the
   * contrast must come from the corrected frame. `state.stats` stays the raw
   * frame's, which the saturation value and the image info come from.
   */
  function refreshDisplayContrast(updateLevels) {
    if (!state.dataRaw || !state.stats) return;
    const shown = getContrastData();
    state.displayStats = shown ? computeStats(shown) : null;
    state.histogram = (state.displayStats || state.stats).hist;
    if (updateLevels) {
      const levels = computeAutoLevels(shown || state.dataRaw, shown ? null : (state.stats.satMax ?? null));
      state.min = levels.min;
      state.max = levels.max;
      if (minInput) minInput.value = formatValue(state.min);
      if (maxInput) maxInput.value = formatValue(state.max);
    }
  }

  function applyExternalFrame(data, shape, dtype, label, fitView, preserveMask = false, options = {}) {
    if (!Array.isArray(shape) || shape.length < 2) return;
    const keepPlaying = Boolean(options.keepPlaying);
    if (!(keepPlaying && state.playing)) {
      core.framePlayback?.stopPlayback();
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
        const singleImageOption = option(t("data.single_image"), "");
        singleImageOption.dataset.i18n = "data.single_image";
        datasetSelect.appendChild(singleImageOption);
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
        const seriesImageOption = option(t("data.series_image"), "");
        seriesImageOption.dataset.i18n = "data.series_image";
        datasetSelect.appendChild(seriesImageOption);
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
      let autoMask = buildNegativeMask(data);
      if (Array.isArray(options.maskColumns) && options.maskColumns.length) {
        autoMask = core.maskCursor.buildColumnMask(width, height, options.maskColumns, autoMask);
      }
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
    setDataSourceSectionState("active", preserveSeries ? t("status.data.series_image_loaded") : t("status.data.image_loaded"));
    updateToolbar();
  }

  return {
    closeCurrentFile,
    applyFrame,
    applyExternalFrame,
    refreshDisplayContrast,
  };
}
