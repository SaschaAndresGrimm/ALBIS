/**
 * Analysis controls bindings (rings, peaks, series-sum, pixel labels).
 */

import { t } from "./i18n.js";
import { chooseNativePath } from "./native_picker.js";

function clampFrameIndex(rawValue, total, fallback) {
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(1, Math.min(total, Math.round(parsed)));
}

export function bindAnalysisControlInteractions({
  apiBase,
  state,
  analysisState,
  backendIsLocal,
  elements,
  callbacks,
}) {
  const {
    ringsToggle,
    peaksCountInput,
    peaksCountHint,
    peaksSnrInput,
    peaksSnrHint,
    peaksEnableToggle,
    peaksExportBtn,
    seriesSumOutput,
    seriesSumFrames,
    seriesSumEvery,
    seriesSumOperation,
    seriesSumResult,
    seriesSumStep,
    seriesSumStepHint,
    seriesSumRangeStart,
    seriesSumRangeEnd,
    seriesSumBrowse,
    filesystemMode,
    seriesSumProgress,
    seriesSumStart,
    seriesSumCancel,
    seriesSumUseFlat,
    pixelLabelToggle,
  } = elements;

  const {
    setFieldHint,
    updateRingsSectionState,
    scheduleResolutionOverlay,
    schedulePeakFinder,
    refreshPeakResolutions,
    exportPeakCsv,
    syncSeriesSumOutputPath,
    updateSeriesSumUi,
    validateSeriesStepInput,
    handleLocalFileSelection,
    openFileBrowser,
    openSeriesSumOutputTarget,
    startSeriesSumming,
    cancelSeriesSumming,
    useSeriesOutputAsFlat,
    schedulePixelOverlay,
  } = callbacks;

  // Distance, pixel size, energy and beam centre are edited in Data → Detector
  // Geometry (geometry_params_controller). This section only owns the rings.
  // The ring values themselves are ring_list_controller's; this follows the switch.
  function updateRingsFromInputs() {
    if (ringsToggle) {
      analysisState.ringsEnabled = ringsToggle.checked;
    }
    updateRingsSectionState();
    scheduleResolutionOverlay();
    refreshPeakResolutions?.();
  }

  function validatePeaksCountInput(commit = false) {
    if (!peaksCountInput) return null;
    const raw = String(peaksCountInput.value || "").trim();
    if (!raw) {
      setFieldHint(peaksCountInput, peaksCountHint, t("validation.peaks.count_range"));
      return null;
    }
    const parsed = Number(raw);
    if (!Number.isFinite(parsed)) {
      setFieldHint(peaksCountInput, peaksCountHint, t("validation.peaks.count_range"));
      return null;
    }
    const rounded = Math.round(parsed);
    const clamped = Math.max(1, Math.min(1000, rounded));
    if (commit) {
      peaksCountInput.value = String(clamped);
    }
    if ((clamped !== parsed || rounded !== parsed) && !commit) {
      setFieldHint(peaksCountInput, peaksCountHint, t("validation.peaks.using_nearest"));
    } else {
      setFieldHint(peaksCountInput, peaksCountHint, "");
    }
    return clamped;
  }

  function validatePeaksSnrInput(commit = false) {
    if (!peaksSnrInput) return null;
    const raw = String(peaksSnrInput.value || "").trim();
    if (!raw) {
      setFieldHint(peaksSnrInput, peaksSnrHint, t("validation.peaks.snr_range"));
      return null;
    }
    const parsed = Number(raw);
    if (!Number.isFinite(parsed)) {
      setFieldHint(peaksSnrInput, peaksSnrHint, t("validation.peaks.snr_range"));
      return null;
    }
    const clamped = Math.max(0, Math.min(50, parsed));
    if (commit) {
      peaksSnrInput.value = String(clamped);
    }
    if (clamped !== parsed && !commit) {
      setFieldHint(peaksSnrInput, peaksSnrHint, t("validation.peaks.snr_range"));
    } else {
      setFieldHint(peaksSnrInput, peaksSnrHint, "");
    }
    return clamped;
  }

  updateRingsFromInputs();

  ringsToggle?.addEventListener("change", updateRingsFromInputs);

  if (peaksCountInput) {
    const initial = Math.max(1, Math.min(1000, Math.round(Number(peaksCountInput.value || 25))));
    analysisState.peakCount = initial;
    peaksCountInput.value = String(initial);
    setFieldHint(peaksCountInput, peaksCountHint, "");
    peaksCountInput.addEventListener("input", () => {
      validatePeaksCountInput(false);
    });
    peaksCountInput.addEventListener("change", () => {
      const next = validatePeaksCountInput(true);
      if (!Number.isFinite(next)) return;
      analysisState.peakCount = next;
      schedulePeakFinder();
    });
  }

  if (peaksSnrInput) {
    const fallback = Number.isFinite(analysisState.peakMinSnr) ? analysisState.peakMinSnr : 5;
    const initial = Math.max(0, Math.min(50, Number(peaksSnrInput.value || fallback)));
    analysisState.peakMinSnr = initial;
    peaksSnrInput.value = String(initial);
    setFieldHint(peaksSnrInput, peaksSnrHint, "");
    peaksSnrInput.addEventListener("input", () => {
      validatePeaksSnrInput(false);
    });
    peaksSnrInput.addEventListener("change", () => {
      const next = validatePeaksSnrInput(true);
      if (!Number.isFinite(next)) return;
      analysisState.peakMinSnr = next;
      schedulePeakFinder();
    });
  }

  if (peaksEnableToggle) {
    analysisState.peaksEnabled = peaksEnableToggle.checked;
    peaksEnableToggle.addEventListener("change", () => {
      analysisState.peaksEnabled = peaksEnableToggle.checked;
      if (!analysisState.peaksEnabled) {
        analysisState.peakSelectionAnchor = null;
      }
      schedulePeakFinder();
    });
  }

  peaksExportBtn?.addEventListener("click", () => {
    exportPeakCsv();
  });

  if (seriesSumOutput && !seriesSumOutput.value.trim()) {
    syncSeriesSumOutputPath(true);
  }

  seriesSumFrames?.addEventListener("change", () => {
    updateSeriesSumUi();
  });

  seriesSumResult?.addEventListener("change", () => {
    updateSeriesSumUi();
  });

  seriesSumEvery?.addEventListener("change", () => {
    const parsed = Math.round(Number(seriesSumEvery.value));
    seriesSumEvery.value = String(Number.isFinite(parsed) && parsed >= 1 ? parsed : 1);
    updateSeriesSumUi();
  });

  seriesSumOperation?.addEventListener("change", () => {
    syncSeriesSumOutputPath();
    updateSeriesSumUi();
  });

  seriesSumStep?.addEventListener("change", () => {
    validateSeriesStepInput(true);
    updateSeriesSumUi();
  });

  seriesSumStep?.addEventListener("input", () => {
    validateSeriesStepInput(false);
    updateSeriesSumUi();
  });

  seriesSumRangeStart?.addEventListener("change", () => {
    const total = Math.max(1, Number(state.frameCount || 1));
    const fallback = clampFrameIndex(seriesSumRangeStart.value || 1, total, 1);
    const value = clampFrameIndex(seriesSumRangeStart.value || 1, total, fallback);
    seriesSumRangeStart.value = String(value);
    updateSeriesSumUi();
  });

  seriesSumRangeEnd?.addEventListener("change", () => {
    seriesSumRangeEnd.dataset.edited = "1";
    const total = Math.max(1, Number(state.frameCount || 1));
    const fallback = total;
    const value = clampFrameIndex(seriesSumRangeEnd.value || total, total, fallback);
    seriesSumRangeEnd.value = String(value);
    updateSeriesSumUi();
  });

  seriesSumBrowse?.addEventListener("click", async () => {
    if (state.seriesSum.running) return;

    if (backendIsLocal) {
      try {
        const path = await chooseNativePath(apiBase, { kind: "folder", purpose: "series_output" });
        if (path && seriesSumOutput) {
          const picked = path.replace(/[\\/]$/, "");
          seriesSumOutput.value = `${picked}/series_sum`;
        }
        return;
      } catch (err) {
        console.error(err);
      }
      openFileBrowser("series-sum", seriesSumOutput);
      return;
    } else if (filesystemMode?.value === "local") {
      handleLocalFileSelection("series-sum");
    } else {
      openFileBrowser("series-sum", seriesSumOutput);
    }
  });

  seriesSumProgress?.addEventListener("click", () => {
    openSeriesSumOutputTarget();
  });

  seriesSumStart?.addEventListener("click", () => {
    startSeriesSumming();
  });

  seriesSumCancel?.addEventListener("click", () => {
    void cancelSeriesSumming();
  });

  seriesSumUseFlat?.addEventListener("click", () => {
    void useSeriesOutputAsFlat();
  });

  // A toolbar toggle beside the magnifier: both are for looking at pixels up
  // close. Pressed state as the magnifier button shows its own.
  if (pixelLabelToggle) {
    state.pixelLabels = pixelLabelToggle.getAttribute("aria-pressed") !== "false";
    pixelLabelToggle.addEventListener("click", () => {
      state.pixelLabels = !state.pixelLabels;
      pixelLabelToggle.setAttribute("aria-pressed", String(state.pixelLabels));
      pixelLabelToggle.classList.toggle("is-active", state.pixelLabels);
      schedulePixelOverlay();
    });
  }

  // Preserve hint state for fields that are only validated on interaction.
  if (seriesSumStep && seriesSumStepHint && !seriesSumStep.value.trim()) {
    setFieldHint(seriesSumStep, seriesSumStepHint, "");
  }
}
