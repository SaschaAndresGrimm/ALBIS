/**
 * Analysis controls bindings (rings, peaks, series-sum, pixel labels).
 */

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
  constants,
  elements,
  callbacks,
}) {
  const { defaultRingCount } = constants;

  const {
    ringsToggle,
    ringsDistance,
    ringsDistanceHint,
    ringsPixel,
    ringsPixelHint,
    ringsEnergy,
    ringsEnergyHint,
    ringsCenterX,
    ringsCenterY,
    ringInputs,
    peaksCountInput,
    peaksCountHint,
    peaksEnableToggle,
    peaksExportBtn,
    seriesSumOutput,
    seriesSumMode,
    seriesSumOperation,
    seriesSumNormalizeEnable,
    seriesSumStep,
    seriesSumStepHint,
    seriesSumRangeStart,
    seriesSumRangeEnd,
    seriesSumNormalizeFrame,
    seriesSumBrowse,
    filesystemMode,
    seriesSumProgress,
    seriesSumStart,
    seriesSumCancel,
    pixelLabelToggle,
  } = elements;

  const {
    setFieldHint,
    updateRingsSectionState,
    scheduleResolutionOverlay,
    schedulePeakFinder,
    exportPeakCsv,
    syncSeriesSumOutputPath,
    updateSeriesSumUi,
    validateSeriesStepInput,
    setStatus,
    handleLocalFileSelection,
    openFileBrowser,
    openSeriesSumOutputTarget,
    startSeriesSumming,
    cancelSeriesSumming,
    schedulePixelOverlay,
  } = callbacks;

  function parsePositiveNumberInput(inputEl, hintEl, label) {
    if (!inputEl) return null;
    const raw = String(inputEl.value || "").trim();
    if (!raw) {
      setFieldHint(inputEl, hintEl, "");
      return null;
    }
    const value = Number(raw);
    if (!Number.isFinite(value) || value <= 0) {
      setFieldHint(inputEl, hintEl, `${label} must be greater than 0.`);
      return null;
    }
    setFieldHint(inputEl, hintEl, "");
    return value;
  }

  function updateRingsFromInputs() {
    if (ringsToggle) {
      analysisState.ringsEnabled = ringsToggle.checked;
    }
    analysisState.ringCount = Math.max(1, Math.min(defaultRingCount, Math.max(1, ringInputs.length)));
    if (ringsDistance) {
      analysisState.distanceMm = parsePositiveNumberInput(ringsDistance, ringsDistanceHint, "Detector distance");
    }
    if (ringsPixel) {
      analysisState.pixelSizeUm = parsePositiveNumberInput(ringsPixel, ringsPixelHint, "Pixel size");
    }
    if (ringsEnergy) {
      analysisState.energyEv = parsePositiveNumberInput(ringsEnergy, ringsEnergyHint, "Photon energy");
    }
    if (ringsCenterX) {
      const value = Number(ringsCenterX.value);
      analysisState.centerX = Number.isFinite(value) ? value : analysisState.centerX;
    }
    if (ringsCenterY) {
      const value = Number(ringsCenterY.value);
      analysisState.centerY = Number.isFinite(value) ? value : analysisState.centerY;
    }
    if (ringInputs.length) {
      analysisState.rings = ringInputs
        .map((input, idx) => {
          const value = Number(input.value || analysisState.rings[idx]);
          return Number.isFinite(value) && value > 0 ? value : null;
        })
        .filter((value) => value !== null);
    }
    ringInputs.forEach((input, idx) => {
      if (!input) return;
      const visible = idx < analysisState.ringCount;
      input.style.display = visible ? "" : "none";
    });
    updateRingsSectionState();
    scheduleResolutionOverlay();
  }

  function validatePeaksCountInput(commit = false) {
    if (!peaksCountInput) return null;
    const raw = String(peaksCountInput.value || "").trim();
    if (!raw) {
      setFieldHint(peaksCountInput, peaksCountHint, "Enter a value from 1 to 1000.");
      return null;
    }
    const parsed = Number(raw);
    if (!Number.isFinite(parsed)) {
      setFieldHint(peaksCountInput, peaksCountHint, "Enter a value from 1 to 1000.");
      return null;
    }
    const rounded = Math.round(parsed);
    const clamped = Math.max(1, Math.min(1000, rounded));
    if (commit) {
      peaksCountInput.value = String(clamped);
    }
    if ((clamped !== parsed || rounded !== parsed) && !commit) {
      setFieldHint(peaksCountInput, peaksCountHint, "Using nearest integer in range 1-1000.");
    } else {
      setFieldHint(peaksCountInput, peaksCountHint, "");
    }
    return clamped;
  }

  updateRingsFromInputs();

  [ringsToggle, ringsDistance, ringsPixel, ringsEnergy, ringsCenterX, ringsCenterY, ...ringInputs]
    .filter(Boolean)
    .forEach((input) => {
      const eventName = input.type === "checkbox" ? "change" : "input";
      input.addEventListener(eventName, updateRingsFromInputs);
    });

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

  seriesSumMode?.addEventListener("change", () => {
    updateSeriesSumUi();
  });

  seriesSumOperation?.addEventListener("change", () => {
    updateSeriesSumUi();
  });

  seriesSumNormalizeEnable?.addEventListener("change", () => {
    updateSeriesSumUi();
  });

  seriesSumStep?.addEventListener("change", () => {
    validateSeriesStepInput(true);
  });

  seriesSumStep?.addEventListener("input", () => {
    validateSeriesStepInput(false);
  });

  seriesSumRangeStart?.addEventListener("change", () => {
    const total = Math.max(1, Number(state.frameCount || 1));
    const fallback = clampFrameIndex(seriesSumRangeStart.value || 1, total, 1);
    const value = clampFrameIndex(seriesSumRangeStart.value || 1, total, fallback);
    seriesSumRangeStart.value = String(value);
  });

  seriesSumRangeEnd?.addEventListener("change", () => {
    const total = Math.max(1, Number(state.frameCount || 1));
    const fallback = total;
    const value = clampFrameIndex(seriesSumRangeEnd.value || total, total, fallback);
    seriesSumRangeEnd.value = String(value);
  });

  seriesSumNormalizeFrame?.addEventListener("change", () => {
    const total = Math.max(1, Number(state.frameCount || 1));
    const value = clampFrameIndex(seriesSumNormalizeFrame.value || 1, total, 1);
    seriesSumNormalizeFrame.value = String(value);
  });

  seriesSumBrowse?.addEventListener("click", async () => {
    if (state.seriesSum.running) return;

    if (backendIsLocal) {
      try {
        const res = await fetch(`${apiBase}/choose-folder`);
        if (res.status === 204) return;
        if (!res.ok) {
          setStatus("Series output picker unavailable");
          return;
        }
        const data = await res.json();
        if (data?.path && seriesSumOutput) {
          const picked = String(data.path).replace(/[\\/]$/, "");
          seriesSumOutput.value = `${picked}/series_sum`;
        }
      } catch (err) {
        console.error(err);
        setStatus("Series output picker failed");
      }
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

  if (pixelLabelToggle) {
    state.pixelLabels = pixelLabelToggle.checked;
    pixelLabelToggle.addEventListener("change", () => {
      state.pixelLabels = pixelLabelToggle.checked;
      schedulePixelOverlay();
    });
  }

  // Preserve hint state for fields that are only validated on interaction.
  if (seriesSumStep && seriesSumStepHint && !seriesSumStep.value.trim()) {
    setFieldHint(seriesSumStep, seriesSumStepHint, "");
  }
}
