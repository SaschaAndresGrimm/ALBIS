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
          setDataSourceSectionState("active", t("status.files.list_loaded"));
        } else {
          state.file = "";
          state.dataset = "";
          setStatus(t("status.frame.select_file_to_begin"));
          updateToolbar();
          showSplash();
          setSplashStatus("splash.status.ready_open_file");
          setLoading(false);
          setDataSourceSectionState("empty", t("status.frame.select_file_to_begin"));
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
      setStatus(t("status.frame.load_files_failed"));
      setDataSourceSectionState("warning", t("status.files.load_failed"));
    }
  }

  async function loadMetadata() {
    if (!state.file || !state.dataset) return;
    showProcessingProgress(t("status.frame.loading_metadata"));
    setStatus(t("status.frame.loading_metadata"));
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
      await loadFrame();
      setDataSourceSectionState("active", t("status.data.metadata_ready"));
    } catch (err) {
      console.error(err);
      setDataSourceSectionState("warning", t("status.data.failed_load_metadata"));
      throw err;
    } finally {
      hideProcessingProgress();
    }
  }

  async function loadAnalysisParams() {
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
  };
}
