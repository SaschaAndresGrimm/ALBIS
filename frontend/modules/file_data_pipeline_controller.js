/**
 * File and frame loading pipeline.
 */

import { t } from "./i18n.js";
import { getGeometryScopeKey } from "./geometry_override_utils.js";
import { createTransientFrameLoadState } from "./transient_frame_load_state.js";

// Generous ceiling for a single binary frame/image transfer. Long enough not to
// trip on large frames over a slow link, short enough that a hung connection no
// longer leaves the loading spinner up forever.
const FRAME_LOAD_TIMEOUT_MS = 120000;

export function createFileDataPipelineController({
  apiBase,
  state,
  elements,
  callbacks,
}) {
  const {
    fileSelect,
    datasetSelect,
    metaShape,
    metaDtype,
  } = elements;

  const {
    fetchJSON,
    option,
    fileLabel,
    isSeriesCapable,
    isHdfFile,
    setDataControlsForHdf5,
    setDataControlsForSeries,
    loadMetadata,
    loadImageGeometry,
    loadInspectorRoot,
    updateFrameControls,
    updatePlayButtons,
    requestFrame,
    parseDtype,
    parseShape,
    typedArrayFrom,
    applyImageMeta,
    applyExternalFrame,
    processPendingFrameRequest,
    currentFrameStatusText,
    setLoading,
    setStatus,
    showSplash,
    setSplashStatus,
    setDataSourceSectionState,
    showProcessingProgress,
    hideProcessingProgress,
    stopPlayback,
    loadMask,
    updateToolbar,
  } = callbacks;

  const transientFrameLoadState = createTransientFrameLoadState(state);
  let activeFrameLoadController = null;

  function cancelActiveLoad() {
    if (!activeFrameLoadController) return;
    try {
      activeFrameLoadController.abort();
    } catch {
      // Ignore abort errors while a replacement request takes over.
    }
  }

  function resetFrameLoadState() {
    stopPlayback();
    if (activeFrameLoadController) {
      try {
        activeFrameLoadController.abort();
      } catch {
        // Ignore abort errors when switching data sources.
      }
      activeFrameLoadController = null;
    }
    transientFrameLoadState.resetTransientFrameLoadState();
    setLoading(false);
  }

  function startFrameLoad() {
    return transientFrameLoadState.startFrameLoad();
  }

  function finishFrameLoad(requestController) {
    if (activeFrameLoadController !== requestController) {
      return false;
    }
    activeFrameLoadController = null;
    transientFrameLoadState.finishFrameLoad();
    setLoading(false);
    return true;
  }

  function resetSeriesState() {
    state.seriesFiles = [];
    state.seriesLabel = "";
  }

  function sortDatasets(datasets) {
    const linkedStack = datasets.find((d) => d.path === "/entry/data");
    if (linkedStack) {
      return [linkedStack, ...datasets.filter((d) => d !== linkedStack)];
    }
    const primary = datasets.find((d) => d.path.includes("/entry/data/data"));
    if (primary) {
      return [primary, ...datasets.filter((d) => d !== primary)];
    }
    return datasets;
  }

  async function loadAutoloadFile(file) {
    const lower = file.toLowerCase();
    if (lower.endsWith(".h5") || lower.endsWith(".hdf5")) {
      state.file = file;
      if (fileSelect) {
        const existing = Array.from(fileSelect.options).some((opt) => opt.value === file);
        if (!existing) {
          fileSelect.appendChild(option(fileLabel(file), file));
        }
        fileSelect.value = file;
      }
      setDataControlsForHdf5();
      const loaded = await loadDatasets();
      if (loaded && state.frameCount > 1) {
        requestFrame(state.frameCount - 1);
      }
      return loaded;
    }
    return loadImageSeries(file);
  }

  async function loadImageSeries(file, { skipSourceSwitchPrep = false } = {}) {
    if (!file) return false;
    if (!skipSourceSwitchPrep) {
      resetFrameLoadState();
    }
    if (!isSeriesCapable(file)) {
      resetSeriesState();
      return loadImageFile(file, { skipSourceSwitchPrep: true });
    }
    try {
      const data = await fetchJSON(`${apiBase}/series?file=${encodeURIComponent(file)}`, {
        timeoutMs: 60000,
      });
      const files = Array.isArray(data.files) ? data.files : [file];
      if (data.series && files.length > 1) {
        state.seriesFiles = files;
        state.seriesLabel = fileLabel(file);
        state.file = file;
        state.dataset = "";
        state.frameCount = files.length;
        state.frameIndex = Math.max(0, Math.min(files.length - 1, Number(data.index || 0)));
        state.hasFrame = false;
        updateFrameControls();
        updatePlayButtons();
        setDataControlsForSeries();
        return loadSeriesFrame();
      }
    } catch (err) {
      console.warn(err);
    }
    resetSeriesState();
    return loadImageFile(file, { skipSourceSwitchPrep: true });
  }

  async function loadImageFile(file, { skipSourceSwitchPrep = false } = {}) {
    if (!skipSourceSwitchPrep) {
      resetFrameLoadState();
    }
    let loaded = false;
    setLoading(true);
    setStatus(t("status.data.loading_image"));
    const geometryPromise = loadImageGeometry(file, getGeometryScopeKey(state, file));
    try {
      const res = await fetch(`${apiBase}/image?file=${encodeURIComponent(file)}`);
      if (!res.ok) {
        setStatus(t("status.data.failed_load_image"), { tone: "error" });
        return false;
      }
      const buffer = await res.arrayBuffer();
      const dtype = parseDtype(res.headers.get("X-Dtype"));
      const shape = parseShape(res.headers.get("X-Shape"));
      const data = typedArrayFrom(buffer, dtype);
      applyImageMeta(res.headers);
      await geometryPromise;
      applyExternalFrame(data, shape, dtype, file, true, false, {
        autoMask: true,
        maskKey: `auto:${file}`,
      });
      loaded = true;
      setStatus(t("status.frame.position", { current: 1, total: 1 }), { frameStatus: true });
    } catch (err) {
      console.error(err);
      setStatus(t("status.data.failed_load_image"), { tone: "error" });
    } finally {
      setLoading(false);
    }
    return loaded;
  }

  async function loadDatasets() {
    if (!state.file) return false;
    resetFrameLoadState();
    if (!isHdfFile(state.file)) {
      return loadImageSeries(state.file, { skipSourceSwitchPrep: true });
    }
    state.hasFrame = false;
    resetSeriesState();
    setDataControlsForHdf5();
    await loadMask(true);
    showProcessingProgress(t("status.data.scanning_datasets"));
    setLoading(true);
    setStatus(t("status.data.scanning_datasets"));
    setDataSourceSectionState("loading", t("status.data.scanning_datasets"), true);
    try {
      const data = await fetchJSON(`${apiBase}/datasets?file=${encodeURIComponent(state.file)}`, {
        timeoutMs: 60000,
      });
      const candidates = data.datasets
        .filter((d) => d.image)
        .sort((a, b) => b.size - a.size);

      datasetSelect.innerHTML = "";
      const ordered = sortDatasets(candidates);
      ordered.forEach((d) => datasetSelect.appendChild(option(`${d.path} (${d.shape.join("x")})`, d.path)));
      await loadInspectorRoot();

      if (ordered.length > 0) {
        state.dataset = ordered[0].path;
        datasetSelect.value = state.dataset;
        const loaded = await loadMetadata();
        if (loaded) {
          setDataSourceSectionState("active", t("status.data.dataset_metadata_loaded"));
        }
        return loaded;
      } else {
        setStatus(t("status.data.no_image_datasets"));
        showSplash();
        setSplashStatus("splash.status.no_image_datasets");
        setLoading(false);
        setDataSourceSectionState("warning", t("status.data.no_image_datasets"));
        return false;
      }
    } catch (err) {
      console.error(err);
      setStatus(t("status.data.failed_scan_datasets"), { tone: "error" });
      showSplash();
      setSplashStatus("splash.status.dataset_scan_failed");
      setLoading(false);
      setDataSourceSectionState("warning", t("status.data.failed_scan_datasets"));
      return false;
    } finally {
      hideProcessingProgress();
    }
  }

  async function loadSeriesFrame() {
    const files = Array.isArray(state.seriesFiles) ? state.seriesFiles : [];
    if (!files.length) return false;
    const file = files[state.frameIndex];
    if (!file) return false;
    if (!startFrameLoad()) return false;
    const showLoading = !state.playing;
    if (showLoading) {
      setLoading(true);
      setStatus(t("status.data.loading_frame"));
    } else {
      setLoading(false);
    }
    let appliedFrame = false;
    let timedOut = false;
    const requestController = new AbortController();
    activeFrameLoadController = requestController;
    const loadTimer = setTimeout(() => {
      timedOut = true;
      requestController.abort();
    }, FRAME_LOAD_TIMEOUT_MS);
    const geometryScopeKey = getGeometryScopeKey(state, file);
    const geometryPromise = loadImageGeometry(state.file || file, geometryScopeKey);
    try {
      const res = await fetch(`${apiBase}/image?file=${encodeURIComponent(file)}`, {
        signal: requestController.signal,
        cache: "no-store",
      });
      if (!res.ok) {
        setStatus(t("status.data.failed_load_image"), { tone: "error" });
        if (!state.hasFrame) {
          showSplash();
        }
        return false;
      }
      const buffer = await res.arrayBuffer();
      const dtype = parseDtype(res.headers.get("X-Dtype"));
      const shape = parseShape(res.headers.get("X-Shape"));
      const data = typedArrayFrom(buffer, dtype);
      applyImageMeta(res.headers);
      await geometryPromise;

      const height = shape[0];
      const width = shape[1];
      metaShape.textContent = `${width} × ${height}`;
      metaDtype.textContent = dtype;

      const seriesKey = state.seriesLabel || state.file || file;
      const reuseMask = Boolean(state.maskRaw && state.maskFile === `auto:${seriesKey}`);
      applyExternalFrame(data, shape, dtype, state.file || file, false, reuseMask, {
        preserveSeries: true,
        keepPlaying: true,
        autoMask: !reuseMask,
        maskKey: `auto:${seriesKey}`,
      });
      appliedFrame = true;
      setStatus(currentFrameStatusText(), { frameStatus: true });
      updateToolbar();
    } catch (err) {
      // A timeout aborts our own controller, so treat it as a surfaced failure
      // rather than the silent navigation-cancel case below.
      if (timedOut || err?.name !== "AbortError") {
        console.error(err);
        setStatus(t("status.data.failed_load_image"), { tone: "error" });
        if (!state.hasFrame) {
          showSplash();
        }
      }
    } finally {
      clearTimeout(loadTimer);
      finishFrameLoad(requestController);
    }
    processPendingFrameRequest(appliedFrame);
    return appliedFrame;
  }

  async function loadFrame() {
    if (Array.isArray(state.seriesFiles) && state.seriesFiles.length > 0) {
      return loadSeriesFrame();
    }
    if (!state.file || !state.dataset) return false;
    if (!startFrameLoad()) return false;
    if (!state.playing) {
      setLoading(true);
      setStatus(t("status.data.loading_frame"));
    } else {
      setLoading(false);
    }
    const url = `${apiBase}/frame?file=${encodeURIComponent(state.file)}&dataset=${encodeURIComponent(
      state.dataset
    )}&index=${state.frameIndex}${
      state.thresholdCount > 1 ? `&threshold=${state.thresholdIndex}` : ""
    }`;
    let appliedFrame = false;
    let timedOut = false;
    const requestController = new AbortController();
    activeFrameLoadController = requestController;
    const loadTimer = setTimeout(() => {
      timedOut = true;
      requestController.abort();
    }, FRAME_LOAD_TIMEOUT_MS);
    try {
      const res = await fetch(url, {
        signal: requestController.signal,
        cache: "no-store",
      });
      if (!res.ok) {
        setStatus(t("status.data.failed_load_frame"), { tone: "error" });
        if (!state.hasFrame) {
          showSplash();
        }
        return false;
      }
      const buffer = await res.arrayBuffer();
      const dtype = parseDtype(res.headers.get("X-Dtype"));
      const shape = parseShape(res.headers.get("X-Shape"));
      const data = typedArrayFrom(buffer, dtype);

      const height = shape[0];
      const width = shape[1];
      metaShape.textContent = `${width} × ${height}`;
      metaDtype.textContent = dtype;

      callbacks.applyFrame(data, width, height, dtype);
      appliedFrame = true;
      setStatus(currentFrameStatusText(), { frameStatus: true });
      updateToolbar();
    } catch (err) {
      // A timeout aborts our own controller, so treat it as a surfaced failure
      // rather than the silent navigation-cancel case below.
      if (timedOut || err?.name !== "AbortError") {
        console.error(err);
        setStatus(t("status.data.failed_load_frame"), { tone: "error" });
        if (!state.hasFrame) {
          showSplash();
        }
      }
    } finally {
      clearTimeout(loadTimer);
      finishFrameLoad(requestController);
    }
    processPendingFrameRequest(appliedFrame);
    return appliedFrame;
  }

  return {
    cancelActiveLoad,
    resetTransientLoadState: resetFrameLoadState,
    queuePendingFrame: transientFrameLoadState.queuePendingFrame,
    hasPendingFrameRequest: transientFrameLoadState.hasPendingFrameRequest,
    consumePendingFrameRequest: transientFrameLoadState.consumePendingFrameRequest,
    isFrameLoading: transientFrameLoadState.isFrameLoading,
    loadAutoloadFile,
    loadImageSeries,
    loadImageFile,
    loadDatasets,
    loadSeriesFrame,
    loadFrame,
  };
}
