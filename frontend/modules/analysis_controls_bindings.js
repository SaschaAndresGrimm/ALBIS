/**
 * Analysis controls bindings (rings, peaks, series-sum, pixel labels).
 */

import { t } from "./i18n.js";
import { getActiveSourceScopeKey, getGeometryScopeKey, isExptPath } from "./geometry_override_utils.js";

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
    ringsGeometryFile,
    ringsGeometryFileHint,
    ringsGeometryBrowse,
    ringsGeometryClear,
    ringsGeometryLockReset,
    ringInputs,
    peaksCountInput,
    peaksCountHint,
    peaksEnableToggle,
    peaksExportBtn,
    seriesSumOutput,
    seriesSumMode,
    seriesSumOperation,
    seriesSumNormalizeMethod,
    seriesSumStep,
    seriesSumStepHint,
    seriesSumRangeStart,
    seriesSumRangeEnd,
    seriesSumNormalizeFrame,
    seriesSumNormalizeScalar,
    seriesSumNormalizeImage,
    seriesSumNormalizeImageBrowse,
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
    openFileDialog,
    applyGeometryOverridePath,
    clearGeometryOverridePath,
    updateGeometryLockUi,
    resetGeometryLock,
    openSeriesSumOutputTarget,
    startSeriesSumming,
    cancelSeriesSumming,
    schedulePixelOverlay,
  } = callbacks;

  function isTiffPath(path) {
    const lower = String(path || "").toLowerCase();
    return lower.endsWith(".tif") || lower.endsWith(".tiff");
  }

  function currentGeometryScopeKey() {
    return getGeometryScopeKey(state, state.file || "");
  }

  function parsePositiveNumberInput(inputEl, hintEl, messageKey) {
    if (!inputEl) return null;
    const raw = String(inputEl.value || "").trim();
    if (!raw) {
      setFieldHint(inputEl, hintEl, "");
      return null;
    }
    const value = Number(raw);
    if (!Number.isFinite(value) || value <= 0) {
      setFieldHint(inputEl, hintEl, t(messageKey));
      return null;
    }
    setFieldHint(inputEl, hintEl, "");
    return value;
  }

  function updateRingsFromInputs(evt) {
    const userEdit = Boolean(evt && evt.type);
    const geometryDriven = analysisState.ringMode === "geometry" && analysisState.ringGeometry;
    if (ringsToggle) {
      analysisState.ringsEnabled = ringsToggle.checked;
    }
    analysisState.ringCount = Math.max(1, Math.min(defaultRingCount, Math.max(1, ringInputs.length)));
    if (ringsDistance) {
      const parsedDistance = parsePositiveNumberInput(
        ringsDistance,
        ringsDistanceHint,
        "validation.rings.distance_positive",
      );
      if (geometryDriven) {
        if (Number.isFinite(parsedDistance)) {
          analysisState.distanceMm = parsedDistance;
          analysisState.geometryDistanceManual = true;
          analysisState.geometryManualKey = String(
            analysisState.ringGeometryKey || analysisState.ringGeometrySource || "",
          );
        }
      } else {
        analysisState.distanceMm = parsedDistance;
      }
    }
    if (ringsPixel && !geometryDriven) {
      analysisState.pixelSizeUm = parsePositiveNumberInput(
        ringsPixel,
        ringsPixelHint,
        "validation.rings.pixel_size_positive",
      );
    }
    if (ringsEnergy) {
      analysisState.energyEv = parsePositiveNumberInput(
        ringsEnergy,
        ringsEnergyHint,
        "validation.rings.photon_energy_positive",
      );
    }
    if (ringsCenterX) {
      const raw = String(ringsCenterX.value ?? "").trim();
      const value = raw ? Number(raw) : null;
      if (geometryDriven) {
        if (Number.isFinite(value)) {
          analysisState.centerX = value;
          analysisState.geometryCenterXManual = true;
          analysisState.geometryManualKey = String(
            analysisState.ringGeometryKey || analysisState.ringGeometrySource || "",
          );
        }
      } else {
        analysisState.centerX = Number.isFinite(value) ? value : analysisState.centerX;
      }
    }
    if (ringsCenterY) {
      const raw = String(ringsCenterY.value ?? "").trim();
      const value = raw ? Number(raw) : null;
      if (geometryDriven) {
        if (Number.isFinite(value)) {
          analysisState.centerY = value;
          analysisState.geometryCenterYManual = true;
          analysisState.geometryManualKey = String(
            analysisState.ringGeometryKey || analysisState.ringGeometrySource || "",
          );
        }
      } else {
        analysisState.centerY = Number.isFinite(value) ? value : analysisState.centerY;
      }
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
    // A manual correction to any geometry field while a live source is running
    // locks the whole geometry block so incoming frames stop overwriting it.
    // Geometry-file mode keeps its own per-field override handling above.
    if (userEdit && evt?.target !== ringsToggle && !geometryDriven && state.autoload?.running) {
      analysisState.geometryLocked = true;
      analysisState.geometryLockKey = getActiveSourceScopeKey(state);
      updateGeometryLockUi?.();
    }
    updateRingsSectionState();
    scheduleResolutionOverlay();
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

  updateRingsFromInputs();

  [ringsToggle, ringsDistance, ringsPixel, ringsEnergy, ringsCenterX, ringsCenterY, ...ringInputs]
    .filter(Boolean)
    .forEach((input) => {
      const eventName = input.type === "checkbox" ? "change" : "input";
      input.addEventListener(eventName, updateRingsFromInputs);
    });

  ringsGeometryFile?.addEventListener("change", async () => {
    const raw = String(ringsGeometryFile.value || "").trim();
    if (!raw) {
      setFieldHint(ringsGeometryFile, ringsGeometryFileHint, "");
      await clearGeometryOverridePath();
      return;
    }
    if (!currentGeometryScopeKey()) {
      setStatus(t("status.file.no_file_loaded"));
      return;
    }
    if (!isExptPath(raw)) {
      setFieldHint(
        ringsGeometryFile,
        ringsGeometryFileHint,
        t("validation.rings.geometry_expt_required"),
      );
      return;
    }
    setFieldHint(ringsGeometryFile, ringsGeometryFileHint, "");
    await applyGeometryOverridePath(raw);
  });

  ringsGeometryBrowse?.addEventListener("click", async () => {
    if (!currentGeometryScopeKey()) {
      setStatus(t("status.file.no_file_loaded"));
      return;
    }
    if (backendIsLocal) {
      try {
        const res = await fetch(`${apiBase}/choose-file?exts=.expt`);
        if (res.status === 204) return;
        if (!res.ok) {
          setStatus(
            res.status === 409
              ? t("status.file_picker.unavailable")
              : t("status.analysis.geometry_picker_failed"),
          );
          return;
        }
        const data = await res.json();
        const pickedPath = String(data?.path || "");
        if (!pickedPath) return;
        if (!isExptPath(pickedPath)) {
          setFieldHint(
            ringsGeometryFile,
            ringsGeometryFileHint,
            t("validation.rings.geometry_expt_required"),
          );
          return;
        }
        if (ringsGeometryFile) {
          ringsGeometryFile.value = pickedPath;
        }
        setFieldHint(ringsGeometryFile, ringsGeometryFileHint, "");
        await applyGeometryOverridePath(pickedPath);
        return;
      } catch (err) {
        console.error(err);
        setStatus(t("status.analysis.geometry_picker_failed"));
        return;
      }
    }
    try {
      const selectedPath = await openFileDialog({ exts: ".expt" });
      if (!selectedPath) return;
      if (!isExptPath(selectedPath)) {
        setFieldHint(
          ringsGeometryFile,
          ringsGeometryFileHint,
          t("validation.rings.geometry_expt_required"),
        );
        return;
      }
      if (ringsGeometryFile) {
        ringsGeometryFile.value = String(selectedPath);
      }
      setFieldHint(ringsGeometryFile, ringsGeometryFileHint, "");
      await applyGeometryOverridePath(String(selectedPath));
    } catch (err) {
      console.error(err);
      setStatus(t("status.analysis.geometry_picker_failed"));
    }
  });

  ringsGeometryClear?.addEventListener("click", async () => {
    if (ringsGeometryFile) {
      ringsGeometryFile.value = "";
    }
    setFieldHint(ringsGeometryFile, ringsGeometryFileHint, "");
    await clearGeometryOverridePath();
  });

  ringsGeometryLockReset?.addEventListener("click", () => {
    resetGeometryLock?.();
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
    syncSeriesSumOutputPath();
    updateSeriesSumUi();
  });

  seriesSumNormalizeMethod?.addEventListener("change", () => {
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
    const total = Math.max(1, Number(state.frameCount || 1));
    const fallback = total;
    const value = clampFrameIndex(seriesSumRangeEnd.value || total, total, fallback);
    seriesSumRangeEnd.value = String(value);
    updateSeriesSumUi();
  });

  seriesSumNormalizeFrame?.addEventListener("change", () => {
    const total = Math.max(1, Number(state.frameCount || 1));
    const value = clampFrameIndex(seriesSumNormalizeFrame.value || 1, total, 1);
    seriesSumNormalizeFrame.value = String(value);
  });

  seriesSumNormalizeScalar?.addEventListener("change", () => {
    const parsed = Number(seriesSumNormalizeScalar.value || "1");
    if (!Number.isFinite(parsed)) {
      seriesSumNormalizeScalar.value = "1";
      return;
    }
    seriesSumNormalizeScalar.value = String(parsed);
  });

  seriesSumNormalizeImageBrowse?.addEventListener("click", async () => {
    if (state.seriesSum.running) return;

    if (backendIsLocal) {
      try {
        const res = await fetch(`${apiBase}/choose-file`);
        if (res.status === 204) return;
        if (res.ok) {
          const data = await res.json();
          const pickedPath = String(data?.path || "");
          if (!pickedPath) return;
          if (!isTiffPath(pickedPath)) {
            setStatus(t("status.analysis.normalization_tiff_required"));
            return;
          }
          if (seriesSumNormalizeImage) {
            seriesSumNormalizeImage.value = pickedPath;
          }
          return;
        }
      } catch (err) {
        console.error(err);
      }
      try {
        const selectedPath = await openFileDialog();
        if (!selectedPath) return;
        if (!isTiffPath(selectedPath)) {
          setStatus(t("status.analysis.normalization_tiff_required"));
          return;
        }
        if (seriesSumNormalizeImage) {
          seriesSumNormalizeImage.value = String(selectedPath);
        }
      } catch (err) {
        console.error(err);
        setStatus(t("status.analysis.normalization_picker_failed"));
      }
      return;
    }

    if (filesystemMode?.value === "local") {
      setStatus(t("status.analysis.normalization_picker_unavailable"));
      return;
    }

    try {
      const selectedPath = await openFileDialog();
      if (!selectedPath) return;
      if (!isTiffPath(selectedPath)) {
        setStatus(t("status.analysis.normalization_tiff_required"));
        return;
      }
      if (seriesSumNormalizeImage) {
        seriesSumNormalizeImage.value = String(selectedPath);
      }
    } catch (err) {
      console.error(err);
      setStatus(t("status.analysis.normalization_picker_failed"));
    }
  });

  seriesSumBrowse?.addEventListener("click", async () => {
    if (state.seriesSum.running) return;

    if (backendIsLocal) {
      try {
        const res = await fetch(`${apiBase}/choose-folder`);
        if (res.status === 204) return;
        if (res.ok) {
          const data = await res.json();
          if (data?.path && seriesSumOutput) {
            const picked = String(data.path).replace(/[\\/]$/, "");
            seriesSumOutput.value = `${picked}/series_sum`;
          }
          return;
        }
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
