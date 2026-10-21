/**
 * The Combine Frames section of the Analysis tab: which frames (all or a
 * range, every Nth of them), how they combine (sum, mean, median) and into how
 * many images (one, or one per N frames), run as a background job.
 *
 * It sends the backend's mode "frames", which takes those two questions
 * separately; the older modes each answered one of them. A single TIFF result
 * can become the Flatfield section's flat directly ("Use as flat").
 */

import { t } from "./i18n.js";
import { canConvertFile, canStartSeriesOperation } from "./command_availability.js";
import { showConfirmDialog } from "./dialogs.js";

export function createSeriesSumController({
  apiBase,
  state,
  elements,
  callbacks,
}) {
  const {
    seriesSumFrames,
    seriesSumEvery,
    seriesSumOperation,
    seriesSumResult,
    seriesSumStepField,
    seriesSumStep,
    seriesSumRangeStartField,
    seriesSumRangeEndField,
    seriesSumRangeStart,
    seriesSumRangeEnd,
    seriesSumMedianEstimate,
    seriesSumOutput,
    seriesSumBrowse,
    seriesSumFormat,
    seriesSumMask,
    seriesSumStart,
    seriesSumCancel,
    seriesSumUseFlat,
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
    useAsFlat,
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

  /** The frames chosen and how they group, as the backend's mode "frames" reads them. */
  function readSelection() {
    const totalFrames = Math.max(1, parsePositiveInt(state.frameCount || 1, 1));
    const isRange = (seriesSumFrames?.value || "all") === "range";
    const start = isRange ? Math.min(totalFrames, parsePositiveInt(seriesSumRangeStart?.value || 1, 1)) : 1;
    const end = isRange
      ? Math.min(totalFrames, parsePositiveInt(seriesSumRangeEnd?.value || totalFrames, totalFrames))
      : totalFrames;
    const every = parsePositiveInt(seriesSumEvery?.value || 1, 1);
    const groups = (seriesSumResult?.value || "one") === "groups";
    const groupSize = parsePositiveInt(seriesSumStep?.value || 1, 1);
    const selected = start > end ? 0 : Math.floor((end - start) / every) + 1;
    return { totalFrames, isRange, start, end, every, groups, groupSize, selected };
  }

  function buildMedianEstimate() {
    const operation = (seriesSumOperation?.value || "sum").toLowerCase();
    if (operation !== "median") return null;
    const dims = resolveImageShape();
    if (!dims) return null;
    const selection = readSelection();
    const groupFrames = selection.groups
      ? Math.min(selection.groupSize, selection.selected)
      : selection.selected;
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

  /** The one TIFF a finished job wrote, which the Flatfield section can take as its flat. */
  function flatCandidate() {
    if (state.seriesSum.running) return "";
    const outputs = Array.isArray(state.seriesSum.outputs) ? state.seriesSum.outputs : [];
    return outputs.length === 1 && isTiffPath(outputs[0]) ? String(outputs[0]) : "";
  }

  function updateSeriesSumProgressOpenState() {
    if (seriesSumUseFlat) {
      seriesSumUseFlat.classList.toggle("is-hidden", !flatCandidate() || typeof useAsFlat !== "function");
    }
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
    const isRange = (seriesSumFrames?.value || "all") === "range";
    const isGroups = (seriesSumResult?.value || "one") === "groups";
    if (seriesSumStepField) {
      seriesSumStepField.classList.toggle("is-hidden", !isGroups);
    }
    if (seriesSumRangeStartField) {
      seriesSumRangeStartField.classList.toggle("is-hidden", !isRange);
    }
    if (seriesSumRangeEndField) {
      seriesSumRangeEndField.classList.toggle("is-hidden", !isRange);
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
      // Until someone sets it, a range runs to the last frame, like "All
      // frames" does: switching to Range then only asks for what changes.
      if (seriesSumRangeEnd.dataset.edited !== "1") seriesSumRangeEnd.value = String(totalFrames);
      const nextEnd = Math.max(1, Math.min(totalFrames, Math.round(Number(seriesSumRangeEnd.value || totalFrames))));
      seriesSumRangeEnd.value = String(nextEnd);
    }
    const ready = canConvertFile(state, isHdfFile);
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
    for (const control of [seriesSumFrames, seriesSumEvery, seriesSumOperation, seriesSumResult]) {
      if (control) control.disabled = state.seriesSum.running || !ready;
    }
    if (seriesSumStep) {
      seriesSumStep.disabled = state.seriesSum.running || !isGroups || !ready;
    }
    if (seriesSumRangeStart) {
      seriesSumRangeStart.disabled = state.seriesSum.running || !isRange || !ready;
    }
    if (seriesSumRangeEnd) {
      seriesSumRangeEnd.disabled = state.seriesSum.running || !isRange || !ready;
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
    updateSeriesSumProgressOpenState();
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
    const operation = (seriesSumOperation?.value || "sum").toLowerCase();
    const selection = readSelection();

    let groupSize = null;
    if (selection.groups) {
      const parsedStep = validateSeriesStepInput(true);
      if (!Number.isFinite(parsedStep)) {
        setStatus(t("status.series.step_invalid"), { tone: "warning" });
        return;
      }
      groupSize = parsedStep;
    }
    if (selection.isRange && selection.start > selection.end) {
      setStatus(t("status.series.range_invalid"), { tone: "warning" });
      return;
    }
    if (seriesSumEvery) seriesSumEvery.value = String(selection.every);

    const payload = {
      file: state.file,
      dataset: state.dataset,
      mode: "frames",
      operation,
      range_start: selection.isRange ? selection.start : null,
      range_end: selection.isRange ? selection.end : null,
      frame_step: selection.every,
      group_size: groupSize,
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

  /**
   * Set the section up for making a flat: every frame, averaged into one TIFF,
   * which the Flatfield section can read. Range, output path and mask are left
   * as they are.
   */
  function presetForFlat() {
    if (state.seriesSum.running) return;
    if (seriesSumEvery) seriesSumEvery.value = "1";
    if (seriesSumOperation) seriesSumOperation.value = "mean";
    if (seriesSumResult) seriesSumResult.value = "one";
    if (seriesSumFormat) seriesSumFormat.value = "tiff";
    syncSeriesSumOutputPath(false);
    updateSeriesSumUi();
  }

  async function useOutputAsFlat() {
    const path = flatCandidate();
    if (!path || typeof useAsFlat !== "function") return;
    await useAsFlat(path);
  }

  return {
    presetForFlat,
    useOutputAsFlat,
    syncSeriesSumOutputPath,
    setSeriesSumProgress,
    updateSeriesSumUi,
    startSeriesSumming,
    cancelSeriesSumming,
    openSeriesSumOutputTarget,
  };
}
