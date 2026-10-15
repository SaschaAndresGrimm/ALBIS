/**
 * Data-format export workflow and UI state.
 */

import { t, getLanguage } from "./i18n.js";
import { canExportData, isLiveExportSource, liveExportEntries } from "./command_availability.js";
import { normalizeInpaintMargin, normalizeInpaintMethod, normalizeInpaintValue } from "./inpainting.js";

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

// The name an export's files share, as the backend makes it: `series_190` for
// series_190_master.h5, `image` for image_00007.cbf, the dataset added only
// when it is not the standard one. The number goes last, after it.
const STANDARD_DATASETS = new Set(["entry/data/data", "data"]);

export function defaultOutputPrefix(path, dataset) {
  let stem = fileStem(path).replace(/_master$/i, "");
  if (!dataset) stem = stem.replace(/[_.-]*\d+$/, "") || stem;
  const tag = String(dataset || "").trim().replace(/^\/+|\/+$/g, "");
  if (tag && !STANDARD_DATASETS.has(tag.toLowerCase())) stem = `${stem}_${tag}`;
  return safeName(stem);
}

// ---- Live sources --------------------------------------------------------
// A live source (SIMPLON monitor, JUNGFRAUJOCH preview, Remote Stream) has no
// file to convert: its frames are the ones in the live history. They are sent
// to the backend one by one and written as a converted file would be.

const LIVE_PREFIX = { simplon: "simplon_monitor", jungfraujoch: "jungfraujoch_preview", remote: "remote_stream" };

function finiteNumber(value) {
  if (value === null || value === undefined || value === "") return undefined;
  const number = Number(value);
  return Number.isFinite(number) ? number : undefined;
}

// An energy, distance or pixel size of 0 means "not known" here (the live
// snapshot fills unknowns with 0): a header says nothing rather than 0.
function positiveNumber(value) {
  const number = finiteNumber(value);
  return number !== undefined && number > 0 ? number : undefined;
}

// What the header can state about one live frame, under the backend's keys.
export function liveFrameMetadata(entry) {
  const snapshot = entry?.snapshot || {};
  const analysis = snapshot.analysis || {};
  const kind = String(entry?.sourceKind || snapshot.sourceKind || "");
  const meta = (kind === "simplon" ? snapshot.simplonMeta : kind === "jungfraujoch" ? snapshot.jfjochMeta : snapshot.remoteMeta) || {};
  const distanceMm = positiveNumber(analysis.distanceMm ?? meta.distanceMm);
  const pixelUm = positiveNumber(analysis.pixelSizeUm);
  // A beam centre of 0,0 is a detector's "not set".
  let centerX = finiteNumber(analysis.centerX ?? meta.centerX);
  let centerY = finiteNumber(analysis.centerY ?? meta.centerY);
  if (!centerX && !centerY) {
    centerX = undefined;
    centerY = undefined;
  }
  const out = {
    source_name: String(entry?.label || ""),
    incident_energy_ev: positiveNumber(analysis.energyEv ?? meta.energyEv),
    wavelength_a: positiveNumber(meta.wavelengthA),
    detector_distance_m: distanceMm === undefined ? undefined : distanceMm / 1000,
    beam_center_x_px: centerX,
    beam_center_y_px: centerY,
    pixel_size_x_m: pixelUm === undefined ? undefined : pixelUm / 1e6,
    pixel_size_y_m: pixelUm === undefined ? undefined : pixelUm / 1e6,
    threshold_energy_ev: positiveNumber(meta.thresholdEv),
    series_number: finiteNumber(meta.series),
    image_number: finiteNumber(meta.image),
    image_datetime: String(meta.date || ""),
  };
  return Object.fromEntries(Object.entries(out).filter(([, value]) => value !== undefined && value !== ""));
}

// The number last, as for any export: "simplon_monitor_s189_00009" when the
// source numbers its images, else by position: "simplon_monitor_00001".
export function liveFrameName(prefix, entry, index) {
  const meta = liveFrameMetadata(entry);
  const base = safeName(prefix, "live");
  const number = (value) => String(Math.max(0, Math.round(value))).padStart(5, "0");
  const series = meta.series_number;
  const image = meta.image_number;
  if (image !== undefined) {
    return series !== undefined ? `${base}_s${Math.round(series)}_${number(image)}` : `${base}_${number(image)}`;
  }
  return `${base}_${number(index + 1)}`;
}

const DTYPE_OF_ARRAY = new Map([
  [Uint8Array, "<u1"],
  [Uint16Array, "<u2"],
  [Uint32Array, "<u4"],
  [Int8Array, "<i1"],
  [Int16Array, "<i2"],
  [Int32Array, "<i4"],
  [Float32Array, "<f4"],
  [Float64Array, "<f8"],
]);

function liveFrameDtype(entry) {
  const declared = String(entry?.dtype || "");
  if (declared) return declared;
  return DTYPE_OF_ARRAY.get(entry?.data?.constructor) || "";
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
    dataExportInpaint,
    dataExportInpaintLabel,
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
  // The Frames choice as the file dialog labels it, restored when the source
  // is a file again.
  const fileFrameLabels = new Map();
  let liveNote = null;

  function stopPolling() {
    if (pollTimer) {
      window.clearTimeout(pollTimer);
      pollTimer = null;
    }
  }

  function isReady() {
    return canExportData(state, isHdfFile);
  }

  function isLive() {
    return isLiveExportSource(state);
  }

  function sourceKey() {
    if (isLive()) return `live|${state.autoload?.mode || ""}|${liveExportEntries(state)[0]?.sourceKey || ""}`;
    return `${state.file || ""}|${state.dataset || ""}`;
  }

  function currentLiveEntry() {
    const entries = liveExportEntries(state);
    if (!entries.length) return null;
    const cursor = Math.round(Number(state.autoload?.historyCursor));
    return entries[Number.isFinite(cursor) ? Math.max(0, Math.min(entries.length - 1, cursor)) : entries.length - 1];
  }

  // Frames: "Current image" and "All images in the live history (4)" for a
  // live source, no range; the file labels otherwise.
  function syncFrameChoices(live) {
    if (!dataExportFrameMode) return;
    const count = liveExportEntries(state).length;
    for (const option of dataExportFrameMode.options || []) {
      if (!fileFrameLabels.has(option.value)) fileFrameLabels.set(option.value, option.textContent);
      if (option.value === "range") option.hidden = live;
      if (option.value === "all") option.textContent = live ? t("data_export.frames.live_all", { count }) : fileFrameLabels.get("all");
      if (option.value === "current") option.textContent = live ? t("data_export.frames.live_current") : fileFrameLabels.get("current");
    }
    if (live && dataExportFrameMode.value === "range") dataExportFrameMode.value = "current";
  }

  // Under the source line, for a live source only: what these images are.
  function syncLiveNote(live) {
    if (!dataExportSource) return;
    if (!liveNote && live) {
      liveNote = document.createElement("p");
      liveNote.className = "data-export-live-note";
      dataExportSource.after?.(liveNote);
    }
    if (liveNote) {
      liveNote.hidden = !live;
      liveNote.textContent = live ? t("data_export.live_note") : "";
    }
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
    // A live source: live_export in the data folder, named after the source.
    const live = isLive();
    const dir = live ? "live_export" : defaultOutputDir(state.file);
    const prefix = live
      ? LIVE_PREFIX[String(state.autoload?.mode || "")] || "live"
      : defaultOutputPrefix(state.file, state.dataset);
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
    const live = isLive();
    const hasThresholds = !live && Number(state.thresholdCount || 1) > 1;
    syncFrameChoices(live);
    syncLiveNote(live);
    if (dataExportSource) {
      if (live) {
        const entry = currentLiveEntry();
        const count = liveExportEntries(state).length;
        dataExportSource.textContent = entry
          ? `${entry.label} · ${t("data_export.source.live_count", { count })}`
          : t("data_export.source.none");
      } else {
        dataExportSource.textContent = state.file
          ? `${state.file}${state.dataset ? `  ${state.dataset}` : ""}`
          : t("data_export.source.none");
      }
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
      dataExportInpaint,
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
      dataExportCancel.disabled = !running || state.dataExport.cancelling || (!state.dataExport.jobId && !state.dataExport.live);
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
    if (isLive()) {
      await startLiveExport();
      return;
    }
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
      inpaint: inpaintChoice(),
      inpaint_margin: inpaintMargin(),
      inpaint_method: normalizeInpaintMethod(state.inpaintMethod),
      inpaint_value: normalizeInpaintValue(state.inpaintValue),
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

  // What to fill instead of writing -1 and -2: the Analysis tab's setting,
  // unless unticked here.
  function inpaintChoice() {
    if (!dataExportInpaint?.checked) return "off";
    return state.inpaintSaturated ? "saturated" : "defects";
  }

  function inpaintMargin() {
    return inpaintChoice() === "off" ? 0 : normalizeInpaintMargin(state.inpaintMargin);
  }

  // The mask a live frame is filled by: the source's, when it has one that
  // fits (the SIMPLON monitor's). Without one, only the values say.
  function liveMask(entry) {
    const [height, width] = entry.shape || [];
    const mask = state.maskRaw;
    if (!state.maskAvailable || !(mask instanceof Uint32Array) || !state.maskShape) return null;
    if (state.maskShape[0] !== height || state.maskShape[1] !== width) return null;
    return mask;
  }

  // The frames chosen, sent one at a time: each is a small request the
  // progress bar can follow, and Cancel takes effect before the next one.
  async function startLiveExport() {
    const mode = String(dataExportFrameMode?.value || "current").toLowerCase();
    const chosen = mode === "all" ? liveExportEntries(state).slice() : [currentLiveEntry()].filter(Boolean);
    if (!chosen.length) return;
    const format = String(dataExportFormat?.value || "tiff").toLowerCase();
    const outputDir = String(dataExportOutputDir?.value || "").trim() || null;
    const prefix = String(dataExportPrefix?.value || "").trim() || LIVE_PREFIX[String(state.autoload?.mode || "")] || "live";
    const overwrite = Boolean(dataExportOverwrite?.checked);
    const inpaint = inpaintChoice();
    stopPolling();
    state.dataExport.running = true;
    state.dataExport.live = true;
    state.dataExport.cancelling = false;
    state.dataExport.jobId = "";
    state.dataExport.outputs = [];
    state.dataExport.openTarget = "";
    setStatus(t("status.data_export.started"));
    const outputs = [];
    let failure = "";
    for (let index = 0; index < chosen.length; index += 1) {
      if (state.dataExport.cancelling) break;
      const entry = chosen[index];
      setProgress(index / chosen.length, t("data_export.progress.live_frame", { n: index + 1, total: chosen.length }));
      updateUi();
      const data = entry.data;
      const form = new FormData();
      form.append(
        "meta",
        JSON.stringify({
          dtype: liveFrameDtype(entry),
          shape: entry.shape,
          name: liveFrameName(prefix, entry, index),
          format,
          output_dir: outputDir,
          overwrite,
          metadata: liveFrameMetadata(entry),
          inpaint,
          inpaint_margin: inpaintMargin(),
          inpaint_method: normalizeInpaintMethod(state.inpaintMethod),
          inpaint_value: normalizeInpaintValue(state.inpaintValue),
        }),
      );
      form.append("pixels", new Blob([new Uint8Array(data.buffer, data.byteOffset, data.byteLength)]), "frame.bin");
      const mask = inpaint === "off" ? null : liveMask(entry);
      if (mask) form.append("mask", new Blob([new Uint8Array(mask.buffer, mask.byteOffset, mask.byteLength)]), "mask.bin");
      try {
        const written = await fetchJSONWithInit(`${apiBase}/export/data/frame`, { method: "POST", body: form });
        outputs.push(String(written?.path || ""));
      } catch (err) {
        console.error(err);
        failure = err?.detail || err?.message || "";
        break;
      }
    }
    const cancelled = state.dataExport.cancelling && !failure;
    state.dataExport.running = false;
    state.dataExport.live = false;
    state.dataExport.cancelling = false;
    state.dataExport.outputs = outputs.filter(Boolean);
    state.dataExport.openTarget = resolveOpenTarget(state.dataExport.outputs);
    if (failure) {
      setProgress(1, t("data_export.progress.live_failed", { reason: failure }));
      setStatus(t("status.data_export.failed"), { tone: "error" });
    } else if (cancelled) {
      setProgress(1, t("status.data_export.cancelled"));
      setStatus(t("status.data_export.cancelled"));
    } else {
      setProgress(1, t("status.data_export.done", { count: outputs.length }));
      setStatus(t("status.data_export.done", { count: outputs.length }), { tone: "success" });
    }
    updateUi();
  }

  async function cancelExport() {
    if (state.dataExport.live && state.dataExport.running && !state.dataExport.cancelling) {
      state.dataExport.cancelling = true;
      setProgress(state.dataExport.progress, t("data_export.button.cancelling"));
      updateUi();
      return;
    }
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
      // Leaves a live source for the file, but keeps the detector's monitor
      // on: another window, or the next live view, may need it.
      await ensureFileMode({ disableMonitor: false });
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
      const response = await fetch(`${apiBase}/choose-folder?purpose=export_output&lang=${encodeURIComponent(getLanguage())}`);
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
    if (isLive() && !liveExportEntries(state).length) {
      setStatus(t("status.data_export.no_live_image"), { tone: "warning" });
      return;
    }
    if (!isLive() && !state.file) {
      setStatus(t("status.data_export.no_file"), { tone: "warning" });
      return;
    }
    if (!isLive() && isHdfFile(state.file) && !state.dataset) {
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
    // Switching between a file and a live source resets folder and prefix.
    const switched = state.dataExport.sourceKey !== undefined && Boolean(state.dataExport.wasLive) !== isLive();
    state.dataExport.wasLive = isLive();
    if (dataExportFrameMode && switched) dataExportFrameMode.value = isLive() ? "current" : "all";
    syncDefaults(switched);
    // Inpainting as on screen: ticked while it is on, and saying what it fills.
    if (dataExportInpaint && !state.dataExport.running) dataExportInpaint.checked = Boolean(state.inpaintEnabled);
    if (dataExportInpaintLabel) {
      const margin = normalizeInpaintMargin(state.inpaintMargin);
      dataExportInpaintLabel.textContent =
        t(state.inpaintSaturated ? "data_export.inpaint_saturated" : "data_export.inpaint") +
        (margin ? t("data_export.inpaint_margin", { margin }) : "") +
        (normalizeInpaintMethod(state.inpaintMethod) === "fixed"
          ? t("data_export.inpaint_fixed", { value: normalizeInpaintValue(state.inpaintValue).toLocaleString() })
          : "");
    }
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
    dataExportInpaint,
  ].forEach((element) => {
    element?.addEventListener("change", clearFinishedResult);
    element?.addEventListener("input", clearFinishedResult);
  });
  dataExportThresholdMode?.addEventListener("change", updateUi);
  dataExportOutputBrowse?.addEventListener("click", browseOutputDir);
  dataExportStart?.addEventListener("click", startExport);
  dataExportCancel?.addEventListener("click", cancelExport);

  return {
    openDialog,
    closeDialog,
    updateUi,
    startExport,
    cancelExport,
    syncDefaults,
    setProgress,
  };
}
