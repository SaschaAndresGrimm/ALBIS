/**
 * Viewer controls and toolbar/panel interaction bindings.
 *
 * ROI geometry-edit bindings are intentionally left in app.js for now.
 */

import { sliderToZoom } from "./zoom_slider.js";

// In log2 of the zoom, the slider's own scale.
const ZOOM_SLIDER_KEY_STEPS = {
  ArrowUp: 0.25,
  ArrowRight: 0.25,
  ArrowDown: -0.25,
  ArrowLeft: -0.25,
  PageUp: 1,
  PageDown: -1,
};

export function bindViewerControls({
  state,
  elements,
  callbacks,
  core,
}) {
  const {
    colormapSelect,
    autoScaleToggle,
    minInput,
    maxInput,
    maskToggle,
    maskSaturatedToggle,
    invertToggle,
    zoomRange,
    zoomValue,
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
    getContrastData,
    formatValue,
    schedulePeakFinder,
    snapHistogramValue,
    zoomAt,
    requestFrame,
    startPlayback,
    toggleToolbarPlaybackPopover,
    toggleToolbarMorePopover,
    setFrameStep,
    setFps,
    togglePanel,
    closeToolbarMorePopover,
    toggleFullscreen,
    toggleFooterVersionPopover,
    registerChromeActivity,
    updateFullscreenUi,
    startMobilePanelDrag,
    updateMobilePanelDrag,
  } = callbacks;

  colormapSelect?.addEventListener("change", () => {
    const value = colormapSelect.value;
    if (value) {
      state.colormap = value;
      redraw();
      scheduleHistogram();
      core.viewerSync?.handleContrastChanged("colormap");
    }
  });

  autoScaleToggle.addEventListener("change", () => {
    state.autoScale = autoScaleToggle.checked;
    if (state.autoScale && state.dataRaw && state.stats) {
      const shown = getContrastData?.() || null;
      const levels = computeAutoLevels(shown || state.dataRaw, shown ? null : (state.stats.satMax ?? null));
      state.min = levels.min;
      state.max = levels.max;
      minInput.value = formatValue(state.min);
      maxInput.value = formatValue(state.max);
    }
    redraw();
    scheduleHistogram();
    core.viewerSync?.handleContrastChanged(state.autoScale ? "auto" : "levels");
  });

  maskToggle?.addEventListener("change", () => {
    state.maskEnabled = maskToggle.checked;
    state.maskAuto = false;
    core.roiStats.updateGlobalStats();
    redraw();
    core.roiInteraction.scheduleRoiUpdate();
    schedulePeakFinder();
  });

  maskSaturatedToggle?.addEventListener("change", () => {
    state.maskSaturatedEnabled = maskSaturatedToggle.checked;
    core.roiStats.updateGlobalStats();
    redraw();
    core.roiInteraction.scheduleRoiUpdate();
    schedulePeakFinder();
  });

  invertToggle.addEventListener("change", () => {
    state.invert = invertToggle.checked;
    redraw();
    scheduleHistogram();
    core.viewerSync?.handleContrastChanged("invert");
  });

  // A typed background or foreground is taken as typed, whatever the frame
  // holds: a fixed range across a series, or a foreground above anything in
  // this frame. It used to be clamped to the frame's own minimum and maximum.
  // Only the order is kept, background below foreground.
  [minInput, maxInput].forEach((input) => {
    input.addEventListener("change", () => {
      if (!state.dataRaw) return;
      const typed = (field, fallback) => {
        const value = Number(field.value === "" ? fallback : field.value);
        return Number.isFinite(value) ? snapHistogramValue(value) : fallback;
      };
      let nextMin = typed(minInput, state.min);
      let nextMax = typed(maxInput, state.max);
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
      core.viewerSync?.handleContrastChanged("levels");
    });
  });

  zoomRange.addEventListener("input", () => {
    core.overviewViewport?.deferViewportInteraction();
    const zoom = sliderToZoom(zoomRange.value);
    if (!canvasWrap) {
      core.overviewViewport?.setZoom(zoom);
      core.overviewViewport?.scheduleOverview();
      return;
    }
    const rect = canvasWrap.getBoundingClientRect();
    zoomAt(rect.left + rect.width / 2, rect.top + rect.height / 2, zoom);
  });

  // The slider has no step, so a drag zooms smoothly; the keys get steps of
  // their own, as zoom factors: four arrow presses double the zoom, Page Up
  // doubles it outright.
  zoomRange.addEventListener("keydown", (event) => {
    const step = ZOOM_SLIDER_KEY_STEPS[event.key];
    if (!step || event.altKey || event.ctrlKey || event.metaKey) return;
    event.preventDefault();
    const min = Number(zoomRange.min);
    const max = Number(zoomRange.max);
    const next = Math.max(min, Math.min(max, Number(zoomRange.value) + step));
    zoomRange.value = String(next);
    zoomRange.dispatchEvent(new window.Event("input", { bubbles: true }));
  });

  const parseZoomValue = (rawValue) => {
    if (typeof rawValue !== "string") return Number.NaN;
    const trimmed = rawValue.trim();
    if (!trimmed) return Number.NaN;
    const normalized = trimmed.replace(",", ".");
    if (normalized.endsWith("%")) {
      const percent = Number(normalized.slice(0, -1).trim());
      return Number.isFinite(percent) ? percent / 100 : Number.NaN;
    }
    const zoomText = normalized.endsWith("x") || normalized.endsWith("X")
      ? normalized.slice(0, -1).trim()
      : normalized;
    const zoom = Number(zoomText);
    return Number.isFinite(zoom) ? zoom : Number.NaN;
  };

  const applyManualZoom = () => {
    if (!zoomValue) return;
    const parsedZoom = parseZoomValue(zoomValue.value);
    if (!Number.isFinite(parsedZoom) || parsedZoom <= 0) {
      core.overviewViewport?.setZoom(state.zoom || 1);
      return;
    }
    core.overviewViewport?.deferViewportInteraction();
    if (!canvasWrap) {
      core.overviewViewport?.setZoom(parsedZoom);
      core.overviewViewport?.scheduleOverview();
      return;
    }
    const rect = canvasWrap.getBoundingClientRect();
    zoomAt(rect.left + rect.width / 2, rect.top + rect.height / 2, parsedZoom);
  };

  zoomValue?.addEventListener("change", applyManualZoom);
  zoomValue?.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    applyManualZoom();
    zoomValue.blur();
  });

  resetView.addEventListener("click", () => {
    core.overviewViewport?.fitImageToView();
  });

  prevBtn?.addEventListener("click", () => {
    core.framePlayback?.stopPlayback();
    requestFrame(state.frameIndex - Math.max(1, state.step));
  });

  nextBtn?.addEventListener("click", () => {
    core.framePlayback?.stopPlayback();
    requestFrame(state.frameIndex + Math.max(1, state.step));
  });

  playBtn?.addEventListener("click", () => {
    if (state.playing) {
      core.framePlayback?.stopPlayback();
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
    await core.thresholdPlayback?.setThresholdIndex(value);
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
    void core.fileOpen.openFileModal();
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
    core.panelLayout?.stopMobilePanelDrag(event, false);
  });

  window.addEventListener("pointercancel", (event) => {
    core.panelLayout?.stopMobilePanelDrag(event, true);
  });

  window.addEventListener("pointermove", registerChromeActivity, { passive: true });
  window.addEventListener("pointerdown", registerChromeActivity, { passive: true });
  window.addEventListener("wheel", registerChromeActivity, { passive: true });
  window.addEventListener("touchstart", registerChromeActivity, { passive: true });
}
