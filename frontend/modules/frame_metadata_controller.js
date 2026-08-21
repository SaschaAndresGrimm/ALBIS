/**
 * File list and frame metadata orchestration.
 */

import { t } from "./i18n.js";
import { getGeometryScopeKey } from "./geometry_override_utils.js";

export function createFrameMetadataController({
  apiBase,
  state,
  analysisState,
  elements,
  callbacks,
}) {
  const {
    autoloadDir,
    autoloadDirList,
    fileSelect,
    metaShape,
    metaDtype,
    ringsDistance,
    ringsPixel,
    ringsEnergy,
    ringsCenterX,
    ringsCenterY,
    ringInputs,
  } = elements;

  const {
    fetchJSON,
    option,
    fileLabel,
    setDataControlsForHdf5,
    setDataSourceSectionState,
    setStatus,
    stopPlayback,
    onWriterPresenceChange,
    updateToolbar,
    showSplash,
    setSplashStatus,
    setLoading,
    showProcessingProgress,
    hideProcessingProgress,
    getDefaultThresholdIndex,
    syncSeriesSumOutputPath,
    updateFrameControls,
    updateThresholdOptions,
    loadMask,
    loadFrame,
    isHdf5File,
    getDefaultCenter,
    loadImageGeometry,
    resetTransientFrameLoadState,
    scheduleResolutionOverlay,
  } = callbacks;

  async function loadAutoloadFolders() {
    if (!autoloadDirList) return;
    try {
      const data = await fetchJSON(`${apiBase}/folders`);
      const folders = Array.isArray(data.folders) ? data.folders : [];
      const current = state.autoload.dir || autoloadDir?.value || "";
      autoloadDirList.innerHTML = "";
      autoloadDirList.appendChild(option(".", ""));
      folders.forEach((name) => autoloadDirList.appendChild(option(name, name)));
      if (current && !folders.includes(current)) {
        autoloadDirList.appendChild(option(current, current));
      }
    } catch (err) {
      console.error(err);
    }
  }

  async function loadFiles() {
    setDataControlsForHdf5();
    setDataSourceSectionState("loading", t("status.files.loading"), true);
    const folder = (autoloadDir?.value || state.autoload.dir || "").trim();
    const url = folder ? `${apiBase}/files?folder=${encodeURIComponent(folder)}` : `${apiBase}/files`;
    try {
      const data = await fetchJSON(url);
      fileSelect.innerHTML = "";
      const existingFile = state.file;
      if (data.files.length > 0) {
        const placeholder = option(t("files.select_placeholder"), "");
        placeholder.dataset.i18n = "files.select_placeholder";
        placeholder.disabled = true;
        placeholder.selected = true;
        fileSelect.appendChild(placeholder);
        data.files.forEach((name) => fileSelect.appendChild(option(fileLabel(name), name)));
        if (existingFile) {
          const hasExisting = data.files.includes(existingFile);
          if (!hasExisting) {
            fileSelect.appendChild(option(fileLabel(existingFile), existingFile));
          }
          fileSelect.value = existingFile;
          setDataSourceSectionState(
            data.truncated ? "warning" : "active",
            data.truncated ? t("status.files.truncated") : t("status.files.list_loaded"),
          );
        } else {
          state.file = "";
          state.dataset = "";
          setStatus(t("status.frame.select_file_to_begin"));
          updateToolbar();
          showSplash();
          setSplashStatus("splash.status.ready_open_file");
          setLoading(false);
          if (data.truncated) {
            setDataSourceSectionState("warning", t("status.files.truncated"));
          } else {
            setDataSourceSectionState("empty", t("status.frame.select_file_to_begin"));
          }
        }
        loadAutoloadFolders();
      } else {
        data.files.forEach((name) => fileSelect.appendChild(option(fileLabel(name), name)));
        if (!existingFile) {
          setStatus(t("status.frame.no_image_files"));
          showSplash();
          setSplashStatus("splash.status.no_image_files_found");
          setLoading(false);
        }
        setDataSourceSectionState("warning", t("status.files.none_in_folder"));
        loadAutoloadFolders();
      }
    } catch (err) {
      console.error(err);
      setStatus(t("status.frame.load_files_failed"), { tone: "error" });
      setDataSourceSectionState("warning", t("status.files.load_failed"));
    }
  }

  async function loadMetadata() {
    if (!state.file || !state.dataset) return false;
    const shouldResetTransientLoadState =
      Boolean(state.hasFrame)
      || state.pendingFrame !== null
      || Boolean(state.isLoading)
      || Boolean(state.playing);
    if (shouldResetTransientLoadState) {
      stopPlayback();
      if (resetTransientFrameLoadState) {
        resetTransientFrameLoadState();
      } else {
        state.pendingFrame = null;
        state.isLoading = false;
      }
    }
    showProcessingProgress(t("status.frame.loading_metadata"));
    setStatus(t("status.frame.loading_metadata"));
    setLoading(true);
    setDataSourceSectionState("loading", t("status.data.loading_dataset_metadata"), true);
    try {
      state.maskAuto = true;
      const data = await fetchJSON(
        `${apiBase}/metadata?file=${encodeURIComponent(state.file)}&dataset=${encodeURIComponent(state.dataset)}`,
      );
      state.shape = data.shape;
      state.dtype = data.dtype;
      if (data.shape.length === 4) {
        state.frameCount = data.shape[0];
        state.thresholdCount = data.shape[1];
        state.thresholdEnergies = Array.isArray(data.threshold_energies) ? data.threshold_energies : [];
      } else {
        state.frameCount = data.shape.length === 3 ? data.shape[0] : 1;
        state.thresholdCount = 1;
        state.thresholdEnergies = [];
      }
      state.thresholdIndex = getDefaultThresholdIndex();
      state.frameIndex = 0;
      syncSeriesSumOutputPath();
      updateFrameControls();
      updateThresholdOptions();
      metaShape.textContent = data.shape.join(" × ");
      metaDtype.textContent = data.dtype;
      updateToolbar();
      await loadAnalysisParams();
      await loadMask(true);
      const loadedFrame = await loadFrame();
      if (!loadedFrame) {
        setDataSourceSectionState("warning", t("status.data.failed_load_frame"));
        return false;
      }
      setDataSourceSectionState("active", t("status.data.metadata_ready"));
      onWriterPresenceChange?.(Boolean(data.writer_present));
      return true;
    } catch (err) {
      console.error(err);
      setDataSourceSectionState("warning", t("status.data.failed_load_metadata"));
      throw err;
    } finally {
      hideProcessingProgress();
    }
  }

  function frameCountFromShape(shape) {
    if (!Array.isArray(shape) || !shape.length) return 1;
    if (shape.length === 4 || shape.length === 3) return shape[0];
    return 1;
  }

  /**
   * Re-read the frame count of the open dataset, and nothing else.
   *
   * `loadMetadata` cannot be used for this: it stops playback, resets the frame
   * index to zero, reloads the mask and refetches the frame. On a timer that
   * would drag the viewer back to the first frame every second. What a growing
   * series needs is only the count, so this touches only the count.
   *
   * Returns whether a writer still holds the file, which is what decides
   * whether asking again is worth anything.
   */
  async function refreshFrameCount() {
    if (!state.file || !state.dataset) return { writerPresent: false, changed: false };
    let data;
    try {
      data = await fetchJSON(
        `${apiBase}/metadata?file=${encodeURIComponent(state.file)}&dataset=${encodeURIComponent(state.dataset)}`,
      );
    } catch (err) {
      console.error(err);
      // A file being written can momentarily refuse a read. Stopping the watch
      // on the first hiccup would be worse than trying again on the next tick.
      return { writerPresent: true, changed: false };
    }
    const nextCount = frameCountFromShape(data.shape);
    const changed = Number.isFinite(nextCount) && nextCount !== state.frameCount;
    if (changed) {
      state.frameCount = nextCount;
      state.shape = data.shape;
      if (metaShape) metaShape.textContent = data.shape.join(" × ");
      updateFrameControls();
      updateToolbar();
    }
    return { writerPresent: Boolean(data.writer_present), changed, frameCount: nextCount };
  }

  async function loadAnalysisParams() {
    // Reset the display aspect to square first; only HDF master files with
    // per-axis pixel sizes override it below.
    state.pixelAspect = 1;
    if (!state.file || !isHdf5File(state.file)) {
      return;
    }
    try {
      const data = await fetchJSON(
        `${apiBase}/analysis/params?file=${encodeURIComponent(state.file)}&dataset=${encodeURIComponent(
          state.dataset || "",
        )}`,
      );
      if (Number.isFinite(data.distance_mm) && ringsDistance) {
        analysisState.distanceMm = data.distance_mm;
        ringsDistance.value = String(Math.round(data.distance_mm));
      }
      if (Number.isFinite(data.pixel_size_um) && ringsPixel) {
        analysisState.pixelSizeUm = data.pixel_size_um;
        ringsPixel.value = data.pixel_size_um.toFixed(2);
      }
      // Derive the display aspect ratio from the per-axis pixel sizes so
      // anisotropic ("strixel") detectors render with the correct geometry.
      // X is the reference axis; Y is stretched by y/x. Defaults to 1 (square)
      // whenever the per-axis sizes are missing or equal.
      const pxX = Number(data.pixel_size_x_um);
      const pxY = Number(data.pixel_size_y_um);
      state.pixelAspect =
        Number.isFinite(pxX) && pxX > 0 && Number.isFinite(pxY) && pxY > 0 ? pxY / pxX : 1;
      if (Number.isFinite(data.energy_ev) && ringsEnergy) {
        analysisState.energyEv = data.energy_ev;
        ringsEnergy.value = String(Math.round(data.energy_ev));
      }
      const fallback = getDefaultCenter();
      const centerX = Number.isFinite(data.center_x_px) ? data.center_x_px : fallback.x;
      const centerY = Number.isFinite(data.center_y_px) ? data.center_y_px : fallback.y;
      analysisState.centerX = centerX;
      analysisState.centerY = centerY;
      if (ringsCenterX) ringsCenterX.value = Math.round(centerX).toString();
      if (ringsCenterY) ringsCenterY.value = Math.round(centerY).toString();
      if (ringInputs.length && ringInputs.every((input) => !input.value)) {
        ringInputs.forEach((input, idx) => {
          const value = analysisState.rings[idx] ?? "";
          if (value) {
            input.value = String(value);
          }
        });
      }
      scheduleResolutionOverlay();
      await loadImageGeometry(state.file, getGeometryScopeKey(state, state.file));
    } catch (err) {
      console.error(err);
    }
  }

  return {
    loadAutoloadFolders,
    loadFiles,
    loadMetadata,
    loadAnalysisParams,
    refreshFrameCount,
  };
}
