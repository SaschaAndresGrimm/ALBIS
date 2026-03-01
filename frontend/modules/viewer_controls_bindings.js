/**
 * Viewer controls and toolbar/panel interaction bindings.
 *
 * ROI geometry-edit bindings are intentionally left in app.js for now.
 */

export function bindViewerControls({
  state,
  elements,
  callbacks,
}) {
  const {
    colormapSelect,
    autoScaleToggle,
    minInput,
    maxInput,
    maskToggle,
    maskSaturatedToggle,
    autoContrastBtn,
    invertToggle,
    histLogX,
    histLogY,
    zoomRange,
    resetView,
    prevBtn,
    nextBtn,
    playBtn,
    toolbarPlaybackToggle,
    toolbarMoreToggle,
    toolbarMoreStep,
    toolbarMoreFps,
    toolbarMoreThreshold,
    toolbarMorePanelToggle,
    toolbarMoreFullscreen,
    fullscreenToggle,
    splashOpenFileBtn,
    footerVersionToggleEl,
    panelFab,
    panelCollapseBtn,
    panelSheetHandle,
    canvasWrap,
  } = elements;

  const {
    redraw,
    scheduleHistogram,
    computeAutoLevels,
    formatValue,
    updateGlobalStats,
    scheduleRoiUpdate,
    schedulePeakFinder,
    chooseHistogramBins,
    computeHistogram,
    snapHistogramValue,
    deferViewportInteraction,
    setZoom,
    scheduleOverview,
    zoomAt,
    fitImageToView,
    stopPlayback,
    requestFrame,
    startPlayback,
    toggleToolbarPlaybackPopover,
    toggleToolbarMorePopover,
    setFrameStep,
    setFps,
    setThresholdIndex,
    togglePanel,
    closeToolbarMorePopover,
    toggleFullscreen,
    openFileModal,
    toggleFooterVersionPopover,
    registerChromeActivity,
    updateFullscreenUi,
    startMobilePanelDrag,
    updateMobilePanelDrag,
    stopMobilePanelDrag,
  } = callbacks;

  colormapSelect?.addEventListener("change", () => {
    const value = colormapSelect.value;
    if (value) {
      state.colormap = value;
      redraw();
      scheduleHistogram();
    }
  });

  autoScaleToggle.addEventListener("change", () => {
    state.autoScale = autoScaleToggle.checked;
    if (state.autoScale && state.dataRaw && state.stats) {
      const levels = computeAutoLevels(state.dataRaw, state.stats.satMax ?? null);
      state.min = levels.min;
      state.max = levels.max;
      minInput.value = formatValue(state.min);
      maxInput.value = formatValue(state.max);
    }
    redraw();
    scheduleHistogram();
  });

  maskToggle?.addEventListener("change", () => {
    state.maskEnabled = maskToggle.checked;
    state.maskAuto = false;
    updateGlobalStats();
    redraw();
    scheduleRoiUpdate();
    schedulePeakFinder();
  });

  maskSaturatedToggle?.addEventListener("change", () => {
    state.maskSaturatedEnabled = maskSaturatedToggle.checked;
    updateGlobalStats();
    redraw();
    scheduleRoiUpdate();
    schedulePeakFinder();
  });

  autoContrastBtn.addEventListener("click", () => {
    if (!state.stats || !state.dataRaw) return;
    state.autoScale = true;
    autoScaleToggle.checked = true;
    const levels = computeAutoLevels(state.dataRaw, state.stats.satMax ?? null);
    state.min = levels.min;
    state.max = levels.max;
    minInput.value = formatValue(state.min);
    maxInput.value = formatValue(state.max);
    redraw();
    scheduleHistogram();
  });

  invertToggle.addEventListener("change", () => {
    state.invert = invertToggle.checked;
    redraw();
    scheduleHistogram();
  });

  [histLogX, histLogY].forEach((toggle) => {
    if (!toggle) return;
    toggle.addEventListener("change", () => {
      state.histLogX = histLogX?.checked ?? state.histLogX;
      state.histLogY = histLogY?.checked ?? state.histLogY;
      if (state.dataRaw && state.stats) {
        const bins = state.stats.bins || chooseHistogramBins(state.dataRaw.length);
        state.histogram = computeHistogram(
          state.dataRaw,
          state.stats.min,
          state.stats.max,
          state.stats.satMax ?? null,
          bins,
          state.histLogX
        );
      }
      scheduleHistogram();
    });
  });

  [minInput, maxInput].forEach((input) => {
    input.addEventListener("change", () => {
      if (!state.dataRaw) return;
      const statsMin = Number.isFinite(state.stats?.min) ? state.stats.min : Math.min(state.min, state.max);
      const statsMax = Number.isFinite(state.stats?.max) ? state.stats.max : Math.max(state.min, state.max);
      let nextMin = snapHistogramValue(Number(minInput.value || state.min));
      let nextMax = snapHistogramValue(Number(maxInput.value || state.max));
      nextMin = Math.max(statsMin, Math.min(statsMax, nextMin));
      nextMax = Math.max(statsMin, Math.min(statsMax, nextMax));
      if (input === minInput && nextMin > nextMax) {
        nextMin = nextMax;
      } else if (input === maxInput && nextMax < nextMin) {
        nextMax = nextMin;
      } else if (nextMin > nextMax) {
        nextMax = nextMin;
      }
      state.min = nextMin;
      state.max = nextMax;
      minInput.value = formatValue(state.min);
      maxInput.value = formatValue(state.max);
      state.autoScale = false;
      autoScaleToggle.checked = false;
      redraw();
      scheduleHistogram();
    });
  });

  zoomRange.addEventListener("input", () => {
    deferViewportInteraction();
    if (!canvasWrap) {
      setZoom(zoomRange.value);
      scheduleOverview();
      return;
    }
    const rect = canvasWrap.getBoundingClientRect();
    zoomAt(rect.left + rect.width / 2, rect.top + rect.height / 2, Number(zoomRange.value));
  });

  resetView.addEventListener("click", () => {
    fitImageToView();
  });

  prevBtn?.addEventListener("click", () => {
    stopPlayback();
    requestFrame(state.frameIndex - Math.max(1, state.step));
  });

  nextBtn?.addEventListener("click", () => {
    stopPlayback();
    requestFrame(state.frameIndex + Math.max(1, state.step));
  });

  playBtn?.addEventListener("click", () => {
    if (state.playing) {
      stopPlayback();
    } else {
      startPlayback();
    }
  });

  toolbarPlaybackToggle?.addEventListener("click", (event) => {
    event.stopPropagation();
    toggleToolbarPlaybackPopover();
  });

  toolbarMoreToggle?.addEventListener("click", (event) => {
    event.stopPropagation();
    toggleToolbarMorePopover();
  });

  toolbarMoreStep?.addEventListener("change", () => {
    setFrameStep(toolbarMoreStep.value);
  });

  toolbarMoreFps?.addEventListener("change", () => {
    setFps(Number(toolbarMoreFps.value));
  });

  toolbarMoreThreshold?.addEventListener("change", async (event) => {
    const value = Math.max(0, Number(event.target.value || 0));
    await setThresholdIndex(value);
  });

  toolbarMorePanelToggle?.addEventListener("click", () => {
    togglePanel();
    closeToolbarMorePopover();
  });

  toolbarMoreFullscreen?.addEventListener("click", () => {
    void toggleFullscreen();
    closeToolbarMorePopover();
  });

  fullscreenToggle?.addEventListener("click", () => {
    void toggleFullscreen();
  });

  splashOpenFileBtn?.addEventListener("click", () => {
    void openFileModal();
  });

  footerVersionToggleEl?.addEventListener("click", (event) => {
    event.stopPropagation();
    toggleFooterVersionPopover();
    registerChromeActivity();
  });

  document.addEventListener("fullscreenchange", updateFullscreenUi);

  panelFab?.addEventListener("click", () => {
    togglePanel();
  });

  panelCollapseBtn?.addEventListener("click", () => {
    togglePanel();
  });

  panelSheetHandle?.addEventListener("pointerdown", (event) => {
    startMobilePanelDrag(event);
  });

  window.addEventListener("pointermove", (event) => {
    updateMobilePanelDrag(event);
  });

  window.addEventListener("pointerup", (event) => {
    stopMobilePanelDrag(event, false);
  });

  window.addEventListener("pointercancel", (event) => {
    stopMobilePanelDrag(event, true);
  });

  window.addEventListener("pointermove", registerChromeActivity, { passive: true });
  window.addEventListener("pointerdown", registerChromeActivity, { passive: true });
  window.addEventListener("wheel", registerChromeActivity, { passive: true });
  window.addEventListener("touchstart", registerChromeActivity, { passive: true });
}
