/**
 * Series summing workflow and UI state.
 */

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
    seriesSumNormalizeEnable,
    seriesSumNormalizeFrameField,
    seriesSumNormalizeFrame,
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
  } = callbacks;

  let pollTimer = null;

  function defaultSeriesSumOutputPath(filePath) {
    if (!filePath) return "output/series_sum";
    const normalized = String(filePath).replace(/\\/g, "/");
    const lastSlash = normalized.lastIndexOf("/");
    const lastDot = normalized.lastIndexOf(".");
    const base = lastDot > lastSlash ? normalized.slice(0, lastDot) : normalized;
    return `${base}_series_sum`;
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
    seriesSumProgress.title = canOpen ? "Click to open output" : "";
  }

  function setSeriesSumProgress(progress, text) {
    const value = Number.isFinite(progress) ? Math.max(0, Math.min(1, progress)) : 0;
    state.seriesSum.progress = value;
    state.seriesSum.message = text || state.seriesSum.message || "Idle";
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
        seriesSumProgressText.textContent = `${pct}  ${state.seriesSum.message || "Running…"}`;
      } else {
        let message = state.seriesSum.message || "Idle";
        if (canOpen && !/click to open/i.test(message)) {
          message = `${message} — click to open`;
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
    const normalizeEnabled = Boolean(seriesSumNormalizeEnable?.checked);
    if (seriesSumStepField) {
      seriesSumStepField.classList.toggle("is-hidden", mode === "all");
    }
    if (seriesSumStepLabel) {
      seriesSumStepLabel.textContent = isNth ? "Nth interval (N)" : "Chunk size (N)";
    }
    if (seriesSumRangeStartField) {
      seriesSumRangeStartField.classList.toggle("is-hidden", !isRange);
    }
    if (seriesSumRangeEndField) {
      seriesSumRangeEndField.classList.toggle("is-hidden", !isRange);
    }
    if (seriesSumNormalizeFrameField) {
      seriesSumNormalizeFrameField.classList.toggle("is-hidden", !normalizeEnabled);
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
    const ready = Boolean(state.file && (isHdfFile(state.file) ? state.dataset : true));
    if (seriesSumStart) {
      seriesSumStart.disabled = !ready || state.seriesSum.running;
      seriesSumStart.textContent = state.seriesSum.cancelling ? "Cancelling…" : state.seriesSum.running ? "Summing…" : "Start";
    }
    if (seriesSumCancel) {
      seriesSumCancel.classList.toggle("is-hidden", !state.seriesSum.running);
      seriesSumCancel.disabled = !state.seriesSum.running || state.seriesSum.cancelling || !state.seriesSum.jobId;
      seriesSumCancel.textContent = state.seriesSum.cancelling ? "Cancelling…" : "Cancel";
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
    if (seriesSumNormalizeEnable) {
      seriesSumNormalizeEnable.disabled = state.seriesSum.running || !ready;
    }
    if (seriesSumNormalizeFrame) {
      seriesSumNormalizeFrame.disabled = state.seriesSum.running || !normalizeEnabled || !ready;
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
      const message = data.message || state.seriesSum.message || "Running…";
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
        setStatus(`Series summing done (${count} file${count === 1 ? "" : "s"})`);
      } else if (status === "cancelled") {
        setStatus("Series summing cancelled");
      } else if (status === "error") {
        setStatus("Series summing failed");
      }
    } catch (err) {
      console.error(err);
      state.seriesSum.running = false;
      state.seriesSum.cancelling = false;
      state.seriesSum.openTarget = "";
      setSeriesSumProgress(1, "Failed to query status");
      updateSeriesSumUi();
      setStatus("Series summing status failed");
    }
  }

  async function cancelSeriesSumming() {
    if (!state.seriesSum.running || !state.seriesSum.jobId || state.seriesSum.cancelling) return;
    state.seriesSum.cancelling = true;
    setSeriesSumProgress(state.seriesSum.progress, "Cancelling…");
    updateSeriesSumUi();
    try {
      const data = await fetchJSONWithInit(`${apiBase}/analysis/series-sum/cancel`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ job_id: state.seriesSum.jobId }),
      });
      if (data?.accepted) {
        setStatus("Series summing cancellation requested");
      } else {
        setStatus(`Series summing is already ${data?.status || "finished"}`);
        state.seriesSum.cancelling = false;
      }
      stopSeriesSumPolling();
      pollSeriesSumStatus();
    } catch (err) {
      console.error(err);
      state.seriesSum.cancelling = false;
      updateSeriesSumUi();
      setStatus("Failed to cancel series summing");
    }
  }

  async function startSeriesSumming() {
    if (!state.file || (isHdfFile(state.file) && !state.dataset) || state.seriesSum.running) return;
    const mode = (seriesSumMode?.value || "all").toLowerCase();
    const operation = (seriesSumOperation?.value || "sum").toLowerCase();
    const normalizeEnabled = Boolean(seriesSumNormalizeEnable?.checked);
    const totalFrames = Math.max(1, Math.round(Number(state.frameCount || 1)));

    const parsedStep = validateSeriesStepInput(true);
    if (!Number.isFinite(parsedStep) && mode !== "all") {
      setStatus("Step must be an integer greater than or equal to 1");
      return;
    }
    const step = Number.isFinite(parsedStep) ? parsedStep : 1;

    const rangeStart = Math.max(1, Math.round(Number(seriesSumRangeStart?.value || 1)));
    const rangeEnd = Math.max(1, Math.round(Number(seriesSumRangeEnd?.value || totalFrames)));
    if (mode === "range" && rangeStart > rangeEnd) {
      setStatus("Range start must be <= range end");
      return;
    }

    let normalizeFrame = null;
    if (normalizeEnabled) {
      normalizeFrame = Math.max(1, Math.min(totalFrames, Math.round(Number(seriesSumNormalizeFrame?.value || 1))));
      if (seriesSumNormalizeFrame) {
        seriesSumNormalizeFrame.value = String(normalizeFrame);
      }
    }

    const payload = {
      file: state.file,
      dataset: state.dataset,
      mode,
      step,
      operation,
      normalize_frame: normalizeFrame,
      range_start: mode === "range" ? rangeStart : null,
      range_end: mode === "range" ? rangeEnd : null,
      output_path: (seriesSumOutput?.value || "").trim(),
      format: (seriesSumFormat?.value || "hdf5").toLowerCase(),
      apply_mask: Boolean(seriesSumMask?.checked),
    };
    try {
      stopSeriesSumPolling();
      state.seriesSum.running = true;
      state.seriesSum.cancelling = false;
      state.seriesSum.jobId = "";
      state.seriesSum.outputs = [];
      state.seriesSum.openTarget = "";
      setSeriesSumProgress(0, "Submitting job…");
      updateSeriesSumUi();
      const data = await fetchJSONWithInit(`${apiBase}/analysis/series-sum/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      state.seriesSum.jobId = String(data.job_id || "");
      state.seriesSum.running = true;
      state.seriesSum.cancelling = false;
      setSeriesSumProgress(0.01, "Queued");
      setStatus("Series summing started");
      updateSeriesSumUi();
      pollSeriesSumStatus();
    } catch (err) {
      console.error(err);
      state.seriesSum.running = false;
      state.seriesSum.cancelling = false;
      setSeriesSumProgress(0, "Start failed");
      updateSeriesSumUi();
      setStatus("Failed to start series summing");
    }
  }

  async function openSeriesSumOutputTarget() {
    if (state.seriesSum.running || !state.seriesSum.openTarget) return;
    try {
      await ensureFileMode();
      await loadAutoloadFile(state.seriesSum.openTarget);
      setStatus("Opened series output in ALBIS");
    } catch (err) {
      console.error(err);
      setStatus("Failed to open series output");
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
