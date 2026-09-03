/**
 * Series summing workflow and UI state.
 */

import { t } from "./i18n.js";
import { canExportData, canStartSeriesOperation } from "./command_availability.js";
import { showConfirmDialog } from "./dialogs.js";

export function createSeriesSumController({
  apiBase,
  state,
  elements,
  callbacks,
}) {
  const {
    seriesSumMode,
    seriesSumOperation,
    seriesSumStepField,
    seriesSumStepLabel,
    seriesSumStep,
    seriesSumRangeStartField,
    seriesSumRangeEndField,
    seriesSumRangeStart,
    seriesSumRangeEnd,
    seriesSumNormalizeMethod,
    seriesSumNormalizeFrameField,
    seriesSumNormalizeFrame,
    seriesSumNormalizeScalarField,
    seriesSumNormalizeScalar,
    seriesSumNormalizeImageField,
    seriesSumNormalizeImage,
    seriesSumNormalizeImageBrowse,
    seriesSumMedianEstimate,
    seriesSumOutput,
    seriesSumBrowse,
    seriesSumFormat,
    seriesSumMask,
    seriesSumStart,
    seriesSumCancel,
    seriesSumProgress,
    seriesSumProgressFill,
    seriesSumProgressText,
  } = elements;

  const {
    isHdfFile,
    validateSeriesStepInput,
    setStatus,
    ensureFileMode,
    loadAutoloadFile,
    fetchJSON,
    fetchJSONWithInit,
    getSeriesSumGeometryContext,
  } = callbacks;

  let pollTimer = null;
  const GIB = 1024 ** 3;

  function isTiffPath(path) {
    const lower = String(path || "").toLowerCase();
    return lower.endsWith(".tif") || lower.endsWith(".tiff");
  }

  function defaultSeriesSumOutputPath(filePath) {
    const operation = (seriesSumOperation?.value || "sum").toLowerCase();
    if (!filePath) return `output/series_${operation}`;
    const normalized = String(filePath).replace(/\\/g, "/");
    const lastSlash = normalized.lastIndexOf("/");
    const lastDot = normalized.lastIndexOf(".");
    const base = lastDot > lastSlash ? normalized.slice(0, lastDot) : normalized;
    return `${base}_series_${operation}`;
  }

  function formatGiB(bytes) {
    const gib = Number(bytes) / GIB;
    if (!Number.isFinite(gib) || gib <= 0) return "0 GiB";
    if (gib >= 100) return `${Math.round(gib)} GiB`;
    if (gib >= 10) return `${gib.toFixed(1)} GiB`;
    return `${gib.toFixed(2)} GiB`;
  }

  function parsePositiveInt(rawValue, fallback = 1) {
    const parsed = Math.round(Number(rawValue));
    if (!Number.isFinite(parsed) || parsed < 1) return fallback;
    return parsed;
  }

  function resolveImageShape() {
    const shape = Array.isArray(state.shape) ? state.shape : [];
    if (shape.length >= 2) {
      const h = parsePositiveInt(shape[shape.length - 2], 0);
      const w = parsePositiveInt(shape[shape.length - 1], 0);
      if (h > 0 && w > 0) return { height: h, width: w };
    }
    const h = parsePositiveInt(state.height || 0, 0);
    const w = parsePositiveInt(state.width || 0, 0);
    if (h > 0 && w > 0) return { height: h, width: w };
    return null;
  }

  function maxFramesPerMedianGroup({ mode, totalFrames, step, rangeStart, rangeEnd }) {
    if (mode === "all") return totalFrames;
    if (mode === "nth") return Math.floor((totalFrames - 1) / step) + 1;
    if (mode === "range") {
      const start = Math.max(1, Math.min(totalFrames, rangeStart));
      const end = Math.max(1, Math.min(totalFrames, rangeEnd));
      if (start > end) return 0;
      const span = end - start + 1;
      return Math.min(step, span);
    }
    return Math.min(step, totalFrames);
  }

  function buildMedianEstimate() {
    const operation = (seriesSumOperation?.value || "sum").toLowerCase();
    if (operation !== "median") return null;
    const dims = resolveImageShape();
    if (!dims) return null;
    const totalFrames = Math.max(1, parsePositiveInt(state.frameCount || 1, 1));
    const mode = (seriesSumMode?.value || "all").toLowerCase();
    const step = parsePositiveInt(seriesSumStep?.value || 1, 1);
    const rangeStart = parsePositiveInt(seriesSumRangeStart?.value || 1, 1);
    const rangeEnd = parsePositiveInt(seriesSumRangeEnd?.value || totalFrames, totalFrames);
    const groupFrames = maxFramesPerMedianGroup({
      mode,
      totalFrames,
      step,
      rangeStart,
      rangeEnd,
    });
    if (groupFrames <= 0) return null;
    const pixelCount = Number(dims.height) * Number(dims.width);
    if (!Number.isFinite(pixelCount) || pixelCount <= 0) return null;
    const rawBytes = pixelCount * groupFrames * 8;
    const peakLowBytes = rawBytes * 2;
    const peakHighBytes = rawBytes * 3;
    const severe = peakHighBytes >= 16 * GIB;
    const warning = peakHighBytes >= 8 * GIB;
    const caution = peakHighBytes >= 4 * GIB;
    const tonePrefix = severe
      ? t("series.median.prefix.high_risk")
      : warning
        ? t("series.median.prefix.warning")
        : caution
          ? t("series.median.prefix.notice")
          : "";
    const thresholdCount = Math.max(1, parsePositiveInt(state.thresholdCount || 1, 1));
    const perThresholdNote = thresholdCount > 1 ? t("series.median.per_threshold") : "";
    const message = [
      tonePrefix,
      t("series.median.raw_stack", {
        perThresholdNote,
        rawBytes: formatGiB(rawBytes),
        groupFrames,
        width: dims.width,
        height: dims.height,
      }),
      t("series.median.peak_range", {
        low: formatGiB(peakLowBytes),
        high: formatGiB(peakHighBytes),
      }),
    ]
      .filter(Boolean)
      .join(" ");
    return {
      message,
      requiresConfirm: warning || severe,
      rawBytes,
      peakLowBytes,
      peakHighBytes,
    };
  }

  function syncSeriesSumOutputPath(force = false) {
    const autoPath = defaultSeriesSumOutputPath(state.file);
    if (!seriesSumOutput) {
      state.seriesSum.autoOutputPath = autoPath;
      return;
    }
    const current = (seriesSumOutput.value || "").trim();
    if (force || !current || current === state.seriesSum.autoOutputPath) {
      seriesSumOutput.value = autoPath;
    }
    state.seriesSum.autoOutputPath = autoPath;
  }

  function updateSeriesSumProgressOpenState() {
    if (!seriesSumProgress) return;
    const canOpen = !state.seriesSum.running && Boolean(state.seriesSum.openTarget);
    seriesSumProgress.classList.toggle("is-clickable", canOpen);
    seriesSumProgress.title = canOpen ? t("series.progress.click_to_open") : "";
  }

  function setSeriesSumProgress(progress, text) {
    const value = Number.isFinite(progress) ? Math.max(0, Math.min(1, progress)) : 0;
    state.seriesSum.progress = value;
    state.seriesSum.message = text || state.seriesSum.message || t("series.progress.idle");
    if (seriesSumProgress) {
      seriesSumProgress.classList.toggle("is-loading", Boolean(state.seriesSum.running));
    }
    if (seriesSumProgressFill) {
      seriesSumProgressFill.style.width = `${(value * 100).toFixed(1)}%`;
    }
    const canOpen = !state.seriesSum.running && Boolean(state.seriesSum.openTarget);
    if (seriesSumProgressText) {
      if (state.seriesSum.running) {
        const pct = `${Math.round(value * 100)}%`;
        seriesSumProgressText.textContent = `${pct}  ${state.seriesSum.message || t("series.progress.running")}`;
      } else {
        let message = state.seriesSum.message || t("series.progress.idle");
        const openHint = t("series.progress.click_to_open_inline");
        if (canOpen && !message.includes(openHint)) {
          message = `${message} — ${openHint}`;
        }
        seriesSumProgressText.textContent = message;
      }
    }
    updateSeriesSumProgressOpenState();
  }

  function updateSeriesSumUi() {
    const mode = (seriesSumMode?.value || "all").toLowerCase();
    const isNth = mode === "nth";
    const isRange = mode === "range";
    const normalizeMethod = (seriesSumNormalizeMethod?.value || "none").toLowerCase();
    const isFrameNorm = normalizeMethod === "frame";
    const isScalarNorm = normalizeMethod === "scalar";
    const isImageNorm = normalizeMethod === "image";
    if (seriesSumStepField) {
      seriesSumStepField.classList.toggle("is-hidden", mode === "all");
    }
    if (seriesSumStepLabel) {
      seriesSumStepLabel.textContent = isNth ? t("series.step_label.nth") : t("series.step_label.chunk");
    }
    if (seriesSumRangeStartField) {
      seriesSumRangeStartField.classList.toggle("is-hidden", !isRange);
    }
    if (seriesSumRangeEndField) {
      seriesSumRangeEndField.classList.toggle("is-hidden", !isRange);
    }
    if (seriesSumNormalizeFrameField) {
      seriesSumNormalizeFrameField.classList.toggle("is-hidden", !isFrameNorm);
    }
    if (seriesSumNormalizeScalarField) {
      seriesSumNormalizeScalarField.classList.toggle("is-hidden", !isScalarNorm);
    }
    if (seriesSumNormalizeImageField) {
      seriesSumNormalizeImageField.classList.toggle("is-hidden", !isImageNorm);
    }
    if (
      isImageNorm
      && seriesSumNormalizeImage
      && !String(seriesSumNormalizeImage.value || "").trim()
      && isTiffPath(state.seriesSum.openTarget)
    ) {
      seriesSumNormalizeImage.value = String(state.seriesSum.openTarget);
    }
    const totalFrames = Math.max(1, Number(state.frameCount || 1));
    if (seriesSumRangeStart) {
      seriesSumRangeStart.min = "1";
      seriesSumRangeStart.max = String(totalFrames);
      const nextStart = Math.max(1, Math.min(totalFrames, Math.round(Number(seriesSumRangeStart.value || 1))));
      seriesSumRangeStart.value = String(nextStart);
    }
    if (seriesSumRangeEnd) {
      seriesSumRangeEnd.min = "1";
      seriesSumRangeEnd.max = String(totalFrames);
      const nextEnd = Math.max(1, Math.min(totalFrames, Math.round(Number(seriesSumRangeEnd.value || totalFrames))));
      seriesSumRangeEnd.value = String(nextEnd);
    }
    if (seriesSumNormalizeFrame) {
      seriesSumNormalizeFrame.min = "1";
      seriesSumNormalizeFrame.max = String(totalFrames);
      const nextNorm = Math.max(1, Math.min(totalFrames, Math.round(Number(seriesSumNormalizeFrame.value || 1))));
      seriesSumNormalizeFrame.value = String(nextNorm);
    }
    const ready = canExportData(state, isHdfFile);
    if (seriesSumStart) {
      seriesSumStart.disabled = !ready || state.seriesSum.running;
      seriesSumStart.textContent = state.seriesSum.cancelling
        ? t("series.button.cancelling")
        : state.seriesSum.running
          ? t("series.button.summing")
          : t("series.button.start");
    }
    if (seriesSumCancel) {
      seriesSumCancel.classList.toggle("is-hidden", !state.seriesSum.running);
      seriesSumCancel.disabled = !state.seriesSum.running || state.seriesSum.cancelling || !state.seriesSum.jobId;
      seriesSumCancel.textContent = state.seriesSum.cancelling ? t("series.button.cancelling") : t("series.button.cancel");
    }
    if (seriesSumBrowse) {
      seriesSumBrowse.disabled = state.seriesSum.running || !ready;
    }
    if (seriesSumMode) {
      seriesSumMode.disabled = state.seriesSum.running || !ready;
    }
    if (seriesSumOperation) {
      seriesSumOperation.disabled = state.seriesSum.running || !ready;
    }
    if (seriesSumStep) {
      seriesSumStep.disabled = state.seriesSum.running || mode === "all" || !ready;
    }
    if (seriesSumRangeStart) {
      seriesSumRangeStart.disabled = state.seriesSum.running || !isRange || !ready;
    }
    if (seriesSumRangeEnd) {
      seriesSumRangeEnd.disabled = state.seriesSum.running || !isRange || !ready;
    }
    if (seriesSumNormalizeMethod) {
      seriesSumNormalizeMethod.disabled = state.seriesSum.running || !ready;
    }
    if (seriesSumNormalizeFrame) {
      seriesSumNormalizeFrame.disabled = state.seriesSum.running || !isFrameNorm || !ready;
    }
    if (seriesSumNormalizeScalar) {
      seriesSumNormalizeScalar.disabled = state.seriesSum.running || !isScalarNorm || !ready;
    }
    if (seriesSumNormalizeImage) {
      seriesSumNormalizeImage.disabled = state.seriesSum.running || !isImageNorm || !ready;
    }
    if (seriesSumNormalizeImageBrowse) {
      seriesSumNormalizeImageBrowse.disabled = state.seriesSum.running || !isImageNorm || !ready;
    }
    if (seriesSumOutput) {
      seriesSumOutput.disabled = state.seriesSum.running || !ready;
    }
    if (seriesSumFormat) {
      seriesSumFormat.disabled = state.seriesSum.running || !ready;
    }
    if (seriesSumMask) {
      seriesSumMask.disabled = state.seriesSum.running || !ready;
    }
    if (seriesSumMedianEstimate) {
      const estimate = buildMedianEstimate();
      if (estimate) {
        seriesSumMedianEstimate.textContent = estimate.message;
        seriesSumMedianEstimate.classList.remove("is-hidden");
      } else {
        seriesSumMedianEstimate.textContent = "";
        seriesSumMedianEstimate.classList.add("is-hidden");
      }
    }
    validateSeriesStepInput(false);
  }

  function stopSeriesSumPolling() {
    if (pollTimer) {
      window.clearTimeout(pollTimer);
      pollTimer = null;
    }
  }

  function resolveSeriesOpenTarget(outputs) {
    if (!Array.isArray(outputs) || !outputs.length) return "";
    return String(outputs[0]);
  }

  async function pollSeriesSumStatus() {
    if (!state.seriesSum.jobId) {
      state.seriesSum.running = false;
      state.seriesSum.cancelling = false;
      updateSeriesSumUi();
      return;
    }
    try {
      const data = await fetchJSON(
        `${apiBase}/analysis/series-sum/status?job_id=${encodeURIComponent(state.seriesSum.jobId)}`
      );
      const status = data.status || "running";
      const progress = Number.isFinite(data.progress) ? Number(data.progress) : state.seriesSum.progress;
      const message = data.message || state.seriesSum.message || t("series.progress.running");
      const outputs = Array.isArray(data.outputs) ? data.outputs : [];
      state.seriesSum.running = status === "queued" || status === "running" || status === "cancelling";
      state.seriesSum.cancelling = Boolean(data.cancel_requested) && state.seriesSum.running;
      state.seriesSum.outputs = outputs;
      state.seriesSum.openTarget = state.seriesSum.running ? "" : resolveSeriesOpenTarget(outputs);
      setSeriesSumProgress(progress, message);
      updateSeriesSumUi();
      if (state.seriesSum.running) {
        pollTimer = window.setTimeout(pollSeriesSumStatus, 500);
        return;
      }
      if (status === "done") {
        const count = state.seriesSum.outputs.length;
        setStatus(t("status.series.done", { count }), { tone: "success" });
      } else if (status === "cancelled") {
        setStatus(t("status.series.cancelled"));
      } else if (status === "error") {
        setStatus(t("status.series.failed"), { tone: "error" });
      }
    } catch (err) {
      console.error(err);
      state.seriesSum.running = false;
      state.seriesSum.cancelling = false;
      state.seriesSum.openTarget = "";
      setSeriesSumProgress(1, t("status.series.status_query_failed"));
      updateSeriesSumUi();
      setStatus(t("status.series.status_failed"));
    }
  }

  async function cancelSeriesSumming() {
    if (!state.seriesSum.running || !state.seriesSum.jobId || state.seriesSum.cancelling) return;
    state.seriesSum.cancelling = true;
    setSeriesSumProgress(state.seriesSum.progress, t("series.button.cancelling"));
    updateSeriesSumUi();
    try {
      const data = await fetchJSONWithInit(`${apiBase}/analysis/series-sum/cancel`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ job_id: state.seriesSum.jobId }),
      });
      if (data?.accepted) {
        setStatus(t("status.series.cancel_requested"));
      } else {
        setStatus(t("status.series.already", { status: data?.status || "finished" }));
        state.seriesSum.cancelling = false;
      }
      stopSeriesSumPolling();
      pollSeriesSumStatus();
    } catch (err) {
      console.error(err);
      state.seriesSum.cancelling = false;
      updateSeriesSumUi();
      setStatus(t("status.series.cancel_failed"), { tone: "error" });
    }
  }

  async function startSeriesSumming() {
    if (!canStartSeriesOperation(state, isHdfFile)) return;
    const mode = (seriesSumMode?.value || "all").toLowerCase();
    const operation = (seriesSumOperation?.value || "sum").toLowerCase();
    const normalizeMethod = (seriesSumNormalizeMethod?.value || "none").toLowerCase();
    const totalFrames = Math.max(1, Math.round(Number(state.frameCount || 1)));

    const parsedStep = validateSeriesStepInput(true);
    if (!Number.isFinite(parsedStep) && mode !== "all") {
      setStatus(t("status.series.step_invalid"), { tone: "warning" });
      return;
    }
    const step = Number.isFinite(parsedStep) ? parsedStep : 1;

    const rangeStart = Math.max(1, Math.round(Number(seriesSumRangeStart?.value || 1)));
    const rangeEnd = Math.max(1, Math.round(Number(seriesSumRangeEnd?.value || totalFrames)));
    if (mode === "range" && rangeStart > rangeEnd) {
      setStatus(t("status.series.range_invalid"), { tone: "warning" });
      return;
    }

    let normalizeFrame = null;
    let normalizeScalar = null;
    let normalizeImage = "";
    if (normalizeMethod === "frame") {
      normalizeFrame = Math.max(1, Math.min(totalFrames, Math.round(Number(seriesSumNormalizeFrame?.value || 1))));
      if (seriesSumNormalizeFrame) {
        seriesSumNormalizeFrame.value = String(normalizeFrame);
      }
    } else if (normalizeMethod === "scalar") {
      const parsedScalar = Number(seriesSumNormalizeScalar?.value || "1");
      if (!Number.isFinite(parsedScalar) || Math.abs(parsedScalar) <= 1e-12) {
        setStatus(t("status.series.scalar_invalid"), { tone: "warning" });
        return;
      }
      normalizeScalar = parsedScalar;
      if (seriesSumNormalizeScalar) {
        seriesSumNormalizeScalar.value = String(parsedScalar);
      }
    } else if (normalizeMethod === "image") {
      normalizeImage = String(seriesSumNormalizeImage?.value || "").trim();
      if (!normalizeImage) {
        setStatus(t("status.series.select_norm_image"), { tone: "warning" });
        return;
      }
      if (!isTiffPath(normalizeImage)) {
        setStatus(t("status.series.norm_image_invalid"), { tone: "warning" });
        return;
      }
    }

    const payload = {
      file: state.file,
      dataset: state.dataset,
      mode,
      step,
      operation,
      normalize_method: normalizeMethod,
      normalize_frame: normalizeFrame,
      normalize_scalar: normalizeScalar,
      normalize_image: normalizeImage || null,
      range_start: mode === "range" ? rangeStart : null,
      range_end: mode === "range" ? rangeEnd : null,
      output_path: (seriesSumOutput?.value || "").trim(),
      format: (seriesSumFormat?.value || "hdf5").toLowerCase(),
      apply_mask: Boolean(seriesSumMask?.checked),
    };
    const geometryContext = typeof getSeriesSumGeometryContext === "function"
      ? getSeriesSumGeometryContext()
      : null;
    if (geometryContext?.geometry) {
      payload.geometry = geometryContext.geometry;
      payload.distance_mm = geometryContext.distanceMm ?? null;
      payload.pixel_size_um = geometryContext.pixelSizeUm ?? null;
      payload.energy_ev = geometryContext.energyEv ?? null;
      payload.center_x_px = geometryContext.centerX ?? null;
      payload.center_y_px = geometryContext.centerY ?? null;
    }
    const medianEstimate = buildMedianEstimate();
    if (medianEstimate?.requiresConfirm) {
      const proceed = await showConfirmDialog({
        title: t("series.confirm.memory_title"),
        message: [medianEstimate.message, "", t("series.confirm.continue")].join("\n"),
        confirmLabel: t("common.confirm"),
        danger: true,
      });
      if (!proceed) {
        setStatus(t("status.series.cancelled_before_start"));
        return;
      }
    }
    try {
      stopSeriesSumPolling();
      state.seriesSum.running = true;
      state.seriesSum.cancelling = false;
      state.seriesSum.jobId = "";
      state.seriesSum.outputs = [];
      state.seriesSum.openTarget = "";
      setSeriesSumProgress(0, t("series.progress.submitting"));
      updateSeriesSumUi();
      const data = await fetchJSONWithInit(`${apiBase}/analysis/series-sum/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      state.seriesSum.jobId = String(data.job_id || "");
      state.seriesSum.running = true;
      state.seriesSum.cancelling = false;
      setSeriesSumProgress(0.01, t("series.progress.queued"));
      setStatus(t("status.series.started"));
      updateSeriesSumUi();
      pollSeriesSumStatus();
    } catch (err) {
      console.error(err);
      state.seriesSum.running = false;
      state.seriesSum.cancelling = false;
      setSeriesSumProgress(0, t("series.progress.start_failed"));
      updateSeriesSumUi();
      setStatus(t("status.series.start_failed"), { tone: "error" });
    }
  }

  async function openSeriesSumOutputTarget() {
    if (state.seriesSum.running || !state.seriesSum.openTarget) return;
    try {
      await ensureFileMode();
      const loaded = await loadAutoloadFile(state.seriesSum.openTarget);
      if (!loaded) {
        setStatus(t("status.series.output_open_failed"), { tone: "error" });
        return;
      }
      setStatus(t("status.series.output_opened"));
    } catch (err) {
      console.error(err);
      setStatus(t("status.series.output_open_failed"), { tone: "error" });
    }
  }

  return {
    syncSeriesSumOutputPath,
    setSeriesSumProgress,
    updateSeriesSumUi,
    startSeriesSumming,
    cancelSeriesSumming,
    openSeriesSumOutputTarget,
  };
}
