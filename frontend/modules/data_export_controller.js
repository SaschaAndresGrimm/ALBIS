/**
 * Data-format export workflow and UI state.
 */

import { t } from "./i18n.js";

function fileStem(path) {
  const normalized = String(path || "").replace(/\\/g, "/");
  const name = normalized.split("/").pop() || "dataset";
  const lower = name.toLowerCase();
  if (lower.endsWith(".cbf.gz")) return name.slice(0, -7);
  const idx = name.lastIndexOf(".");
  return idx > 0 ? name.slice(0, idx) : name;
}

function dirname(path) {
  const normalized = String(path || "").replace(/\\/g, "/");
  const idx = normalized.lastIndexOf("/");
  return idx >= 0 ? normalized.slice(0, idx) : "";
}

function safeName(value, fallback = "dataset") {
  const text = String(value || "")
    .replace(/\\/g, "/")
    .replace(/^\/+|\/+$/g, "")
    .replace(/[^A-Za-z0-9_.-]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^[._-]+|[._-]+$/g, "");
  return text || fallback;
}

function defaultOutputDir(path) {
  const base = fileStem(path);
  const dir = dirname(path);
  return dir ? `${dir}/${base}_export` : `${base}_export`;
}

function defaultOutputPrefix(path, dataset) {
  const source = fileStem(path);
  const datasetTag = safeName(dataset || "", "");
  return datasetTag ? safeName(`${source}_${datasetTag}`) : safeName(source);
}

export function createDataExportController({
  apiBase,
  state,
  elements,
  callbacks,
}) {
  const {
    dataExportModal,
    dataExportClose,
    dataExportSource,
    dataExportFormat,
    dataExportFrameMode,
    dataExportRangeStartField,
    dataExportRangeEndField,
    dataExportRangeStart,
    dataExportRangeEnd,
    dataExportThresholdModeField,
    dataExportThresholdMode,
    dataExportOutputDir,
    dataExportOutputBrowse,
    dataExportPrefix,
    dataExportOverwrite,
    dataExportStart,
    dataExportCancel,
    dataExportProgress,
    dataExportProgressFill,
    dataExportProgressText,
  } = elements;

  const {
    isHdfFile,
    openModal,
    closeModal,
    setStatus,
    fetchJSON,
    fetchJSONWithInit,
    ensureFileMode,
    loadAutoloadFile,
  } = callbacks;

  let pollTimer = null;

  function stopPolling() {
    if (pollTimer) {
      window.clearTimeout(pollTimer);
      pollTimer = null;
    }
  }

  function isReady() {
    return Boolean(state.file && (!isHdfFile(state.file) || state.dataset));
  }

  function sourceKey() {
    return `${state.file || ""}|${state.dataset || ""}`;
  }

  function resolveOpenTarget(outputs) {
    if (!Array.isArray(outputs) || !outputs.length) return "";
    return String(outputs[0] || "");
  }

  function setProgress(progress, text) {
    const value = Number.isFinite(progress) ? Math.max(0, Math.min(1, progress)) : 0;
    state.dataExport.progress = value;
    state.dataExport.message = text || state.dataExport.message || t("data_export.progress.idle");
    if (dataExportProgress) {
      dataExportProgress.classList.toggle("is-loading", Boolean(state.dataExport.running));
    }
    if (dataExportProgressFill) {
      dataExportProgressFill.style.width = `${(value * 100).toFixed(1)}%`;
    }
    if (dataExportProgressText) {
      if (state.dataExport.running) {
        dataExportProgressText.textContent = `${Math.round(value * 100)}%  ${state.dataExport.message}`;
      } else {
        dataExportProgressText.textContent = state.dataExport.message || t("data_export.progress.idle");
      }
    }
  }

  function syncDefaults(force = false) {
    const dir = defaultOutputDir(state.file);
    const prefix = defaultOutputPrefix(state.file, state.dataset);
    if (dataExportOutputDir) {
      const current = String(dataExportOutputDir.value || "").trim();
      if (force || !current || current === state.dataExport.autoOutputDir) {
        dataExportOutputDir.value = dir;
      }
      state.dataExport.autoOutputDir = dir;
    }
    if (dataExportPrefix) {
      const current = String(dataExportPrefix.value || "").trim();
      if (force || !current || current === state.dataExport.autoOutputPrefix) {
        dataExportPrefix.value = prefix;
      }
      state.dataExport.autoOutputPrefix = prefix;
    }
  }

  function clearFinishedResult() {
    if (state.dataExport.running || !state.dataExport.openTarget) return;
    state.dataExport.openTarget = "";
    state.dataExport.outputs = [];
    setProgress(0, t("data_export.progress.idle"));
    updateUi();
  }

  function updateUi() {
    const ready = isReady();
    const running = Boolean(state.dataExport.running);
    const canOpen = !running && Boolean(state.dataExport.openTarget);
    const frameMode = String(dataExportFrameMode?.value || "all").toLowerCase();
    const showRange = frameMode === "range";
    const hasThresholds = Number(state.thresholdCount || 1) > 1;
    if (dataExportSource) {
      dataExportSource.textContent = state.file
        ? `${state.file}${state.dataset ? `  ${state.dataset}` : ""}`
        : t("data_export.source.none");
    }
    if (dataExportRangeStartField) {
      dataExportRangeStartField.classList.toggle("is-hidden", !showRange);
    }
    if (dataExportRangeEndField) {
      dataExportRangeEndField.classList.toggle("is-hidden", !showRange);
    }
    if (dataExportThresholdModeField) {
      dataExportThresholdModeField.classList.toggle("is-hidden", !hasThresholds);
    }
    const totalFrames = Math.max(1, Math.round(Number(state.frameCount || 1)));
    if (dataExportRangeStart) {
      dataExportRangeStart.min = "1";
      dataExportRangeStart.max = String(totalFrames);
      if (!dataExportRangeStart.value) dataExportRangeStart.value = "1";
    }
    if (dataExportRangeEnd) {
      dataExportRangeEnd.min = "1";
      dataExportRangeEnd.max = String(totalFrames);
      if (!dataExportRangeEnd.value) dataExportRangeEnd.value = String(totalFrames);
    }
    [
      dataExportFormat,
      dataExportFrameMode,
      dataExportThresholdMode,
      dataExportOutputDir,
      dataExportOutputBrowse,
      dataExportPrefix,
      dataExportOverwrite,
    ].forEach((element) => {
      if (element) element.disabled = running || !ready;
    });
    if (dataExportRangeStart) dataExportRangeStart.disabled = running || !ready || !showRange;
    if (dataExportRangeEnd) dataExportRangeEnd.disabled = running || !ready || !showRange;
    if (dataExportStart) {
      dataExportStart.disabled = running || (!ready && !canOpen);
      dataExportStart.textContent = running
        ? t("data_export.button.exporting")
        : canOpen
          ? t("data_export.button.open_image")
          : t("data_export.button.start");
    }
    if (dataExportCancel) {
      dataExportCancel.classList.toggle("is-hidden", !running);
      dataExportCancel.disabled = !running || state.dataExport.cancelling || !state.dataExport.jobId;
      dataExportCancel.textContent = state.dataExport.cancelling
        ? t("data_export.button.cancelling")
        : t("data_export.button.cancel");
    }
  }

  async function pollStatus() {
    if (!state.dataExport.jobId) {
      state.dataExport.running = false;
      state.dataExport.cancelling = false;
      updateUi();
      return;
    }
    try {
      const data = await fetchJSON(
        `${apiBase}/export/data/status?job_id=${encodeURIComponent(state.dataExport.jobId)}`
      );
      const status = data.status || "running";
      const progress = Number.isFinite(data.progress) ? Number(data.progress) : state.dataExport.progress;
      const outputs = Array.isArray(data.outputs) ? data.outputs : [];
      state.dataExport.running = status === "queued" || status === "running" || status === "cancelling";
      state.dataExport.cancelling = Boolean(data.cancel_requested) && state.dataExport.running;
      state.dataExport.outputs = outputs;
      state.dataExport.openTarget = state.dataExport.running ? "" : resolveOpenTarget(outputs);
      setProgress(progress, data.message || t("data_export.progress.running"));
      updateUi();
      if (state.dataExport.running) {
        pollTimer = window.setTimeout(pollStatus, 500);
        return;
      }
      if (status === "done") {
        setStatus(t("status.data_export.done", { count: outputs.length }), { tone: "success" });
      } else if (status === "cancelled") {
        setStatus(t("status.data_export.cancelled"));
      } else if (status === "error") {
        setStatus(t("status.data_export.failed"), { tone: "error" });
      }
    } catch (err) {
      console.error(err);
      state.dataExport.running = false;
      state.dataExport.cancelling = false;
      setProgress(1, t("status.data_export.status_query_failed"));
      updateUi();
      setStatus(t("status.data_export.status_failed"));
    }
  }

  async function startExport() {
    if (!state.dataExport.running && state.dataExport.openTarget) {
      await openFirstOutput();
      return;
    }
    if (!isReady() || state.dataExport.running) return;
    const mode = String(dataExportFrameMode?.value || "all").toLowerCase();
    const totalFrames = Math.max(1, Math.round(Number(state.frameCount || 1)));
    const rangeStart = Math.max(1, Math.round(Number(dataExportRangeStart?.value || 1)));
    const rangeEnd = Math.max(1, Math.round(Number(dataExportRangeEnd?.value || totalFrames)));
    if (mode === "range" && rangeStart > rangeEnd) {
      setStatus(t("status.data_export.range_invalid"), { tone: "warning" });
      return;
    }
    const thresholdCount = Math.max(1, Math.round(Number(state.thresholdCount || 1)));
    const thresholdMode = thresholdCount > 1
      ? String(dataExportThresholdMode?.value || "current").toLowerCase()
      : "current";
    const payload = {
      file: state.file,
      dataset: state.dataset || "",
      format: String(dataExportFormat?.value || "tiff").toLowerCase(),
      output_dir: String(dataExportOutputDir?.value || "").trim() || null,
      output_prefix: String(dataExportPrefix?.value || "").trim() || null,
      frame_mode: mode,
      frame_start: mode === "current" ? Number(state.frameIndex || 0) + 1 : mode === "range" ? rangeStart : null,
      frame_end: mode === "range" ? rangeEnd : null,
      threshold_mode: thresholdMode,
      threshold_index: thresholdMode === "current" ? Number(state.thresholdIndex || 0) + 1 : null,
      overwrite: Boolean(dataExportOverwrite?.checked),
    };
    try {
      stopPolling();
      state.dataExport.running = true;
      state.dataExport.cancelling = false;
      state.dataExport.jobId = "";
      state.dataExport.outputs = [];
      state.dataExport.openTarget = "";
      setProgress(0, t("data_export.progress.submitting"));
      updateUi();
      const data = await fetchJSONWithInit(`${apiBase}/export/data/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      state.dataExport.jobId = String(data.job_id || "");
      setProgress(0.01, t("data_export.progress.queued"));
      setStatus(t("status.data_export.started"));
      updateUi();
      pollStatus();
    } catch (err) {
      console.error(err);
      state.dataExport.running = false;
      state.dataExport.cancelling = false;
      setProgress(0, t("data_export.progress.start_failed"));
      updateUi();
      setStatus(t("status.data_export.start_failed"), { tone: "error" });
    }
  }

  async function cancelExport() {
    if (!state.dataExport.running || !state.dataExport.jobId || state.dataExport.cancelling) return;
    state.dataExport.cancelling = true;
    setProgress(state.dataExport.progress, t("data_export.button.cancelling"));
    updateUi();
    try {
      const data = await fetchJSONWithInit(`${apiBase}/export/data/cancel`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ job_id: state.dataExport.jobId }),
      });
      if (data?.accepted) {
        setStatus(t("status.data_export.cancel_requested"));
      } else {
        setStatus(t("status.data_export.already", { status: data?.status || "finished" }));
        state.dataExport.cancelling = false;
      }
      stopPolling();
      pollStatus();
    } catch (err) {
      console.error(err);
      state.dataExport.cancelling = false;
      updateUi();
      setStatus(t("status.data_export.cancel_failed"), { tone: "error" });
    }
  }

  async function openFirstOutput() {
    const target = state.dataExport.openTarget;
    if (state.dataExport.running || !target) return;
    try {
      await ensureFileMode();
      const loaded = await loadAutoloadFile(target);
      if (!loaded) {
        setStatus(t("status.data_export.output_open_failed"), { tone: "error" });
        return;
      }
      setStatus(t("status.data_export.output_opened"));
      state.dataExport.openTarget = "";
      setProgress(0, t("data_export.progress.idle"));
      updateUi();
      closeDialog();
    } catch (err) {
      console.error(err);
      setStatus(t("status.data_export.output_open_failed"), { tone: "error" });
    }
  }

  async function browseOutputDir() {
    if (state.dataExport.running) return;
    try {
      const response = await fetch(`${apiBase}/choose-folder`);
      if (response.status === 204) return;
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      if (data?.path && dataExportOutputDir) {
        dataExportOutputDir.value = String(data.path).replace(/[\\/]$/, "");
      }
    } catch (err) {
      console.error(err);
      setStatus(t("status.data_export.browse_failed"), { tone: "error" });
    }
  }

  function openDialog() {
    if (!state.file) {
      setStatus(t("status.data_export.no_file"), { tone: "warning" });
      return;
    }
    if (isHdfFile(state.file) && !state.dataset) {
      setStatus(t("status.data_export.no_dataset"), { tone: "warning" });
      return;
    }
    const nextSourceKey = sourceKey();
    if (!state.dataExport.running && state.dataExport.sourceKey !== nextSourceKey) {
      state.dataExport.outputs = [];
      state.dataExport.openTarget = "";
      state.dataExport.sourceKey = nextSourceKey;
      setProgress(0, t("data_export.progress.idle"));
    }
    syncDefaults(false);
    updateUi();
    openModal(dataExportModal, { focusTarget: dataExportFormat });
  }

  function closeDialog() {
    closeModal(dataExportModal);
  }

  dataExportClose?.addEventListener("click", closeDialog);
  dataExportModal?.addEventListener("click", (event) => {
    if (event.target === dataExportModal || event.target.classList?.contains("modal-backdrop")) {
      closeDialog();
    }
  });
  dataExportFrameMode?.addEventListener("change", updateUi);
  [
    dataExportFormat,
    dataExportFrameMode,
    dataExportRangeStart,
    dataExportRangeEnd,
    dataExportThresholdMode,
    dataExportOutputDir,
    dataExportPrefix,
    dataExportOverwrite,
  ].forEach((element) => {
    element?.addEventListener("change", clearFinishedResult);
    element?.addEventListener("input", clearFinishedResult);
  });
  dataExportThresholdMode?.addEventListener("change", updateUi);
  dataExportOutputBrowse?.addEventListener("click", browseOutputDir);
  dataExportStart?.addEventListener("click", startExport);
  dataExportCancel?.addEventListener("click", cancelExport);

  return {
    isReady,
    openDialog,
    closeDialog,
    updateUi,
    startExport,
    cancelExport,
    syncDefaults,
    setProgress,
  };
}
