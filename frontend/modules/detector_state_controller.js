/**
 * Detector tab (beta): what the panel shows of the detector's state. The
 * pill and the line beside it, the buttons, the pre-flight check, progress
 * and result of a series, the sensors, the Acquisition and Data output
 * headers, and opening a finished series in ALBIS.
 */

import { t } from "./i18n.js";
import { el } from "./detector_control_dom.js";
import {
  COMPRESSION_FACTOR,
  displayValue,
  formatBytes,
  formatDuration,
  formatProgress,
  isOn,
  primaryAction,
  STATE_TEXTS,
  STATE_TONES,
  thresholdRows,
} from "./detector_control_utils.js";

export function createDetectorStateController({
  ctx,
  content,
  summary,
  statePill,
  stateText,
  progress,
  progressBar,
  progressLeft,
  progressRight,
  primaryBtn,
  stopBtn,
  sensors,
  lockNote,
  seriesSummary,
  outputSummary,
  exposureBox,
  exposureInput,
  quickGroup,
  snapBtn,
  continuousBtn,
  preflightHost,
  resultHost,
  quickBox,
  setMessage,
  request,
  log,
  openPath,
  onChange,
  detectorState,
  isBusy,
  renderRecovery,
}) {
  // ---------- pre-flight ----------
  // What would make the next series fail or useless, as plain sentences, and
  // how much it writes: images x pixels x bit depth x images per frame, before
  // compression, so an upper bound.
  function preflight() {
    const det = ctx.params.detector;
    const issues = [];
    const fwOn = ctx.params.filewriter?.mode?.value === "enabled";
    const stOn = ctx.params.stream?.mode?.value === "enabled";
    if ((ctx.params.filewriter?.mode || ctx.params.stream?.mode) && !fwOn && !stOn) issues.push(t("detector.output.nothing_saved"));
    let estimate = null;
    let method = "";
    const pixels = Number(det.x_pixels_in_detector?.value) * Number(det.y_pixels_in_detector?.value);
    if (fwOn && pixels > 0) {
      const images = Number(det.nimages?.value || 1) * Number(det.ntrigger?.value || 1);
      const bytes = (Number(det.bit_depth_image?.value) || 32) / 8;
      const { images: kinds } = thresholdRows(det);
      const channels = kinds.length ? Math.max(1, kinds.filter((kind) => isOn(det[kind.key])).length) : 1;
      estimate = images * pixels * bytes * channels;
      // Compressed by the file writer unless it is told not to.
      const compressing = ctx.params.filewriter?.compression_enabled ? isOn(ctx.params.filewriter.compression_enabled) : true;
      const kind = String(det.compression?.value || "");
      if (compressing && COMPRESSION_FACTOR[kind]) {
        estimate /= COMPRESSION_FACTOR[kind];
        method = kind;
      }
    }
    const free = ctx.status?.filewriter?.buffer_free;
    const qualifier = method ? t("detector.preflight.compressed", { method }) : t("detector.preflight.uncompressed");
    if (estimate !== null && free !== undefined && free !== null && estimate > Number(free)) {
      issues.push(t("detector.preflight.storage", { need: formatBytes(estimate), qualifier, free: formatBytes(free) }));
    }
    const pattern = String(ctx.params.filewriter?.name_pattern?.value ?? "");
    if (fwOn && ctx.params.filewriter?.name_pattern && !pattern.includes("$id")) issues.push(t("detector.preflight.overwrite"));
    const hv = ctx.status?.detector?.["high_voltage/state"];
    if (hv && String(hv).toUpperCase() !== "READY") issues.push(t("detector.preflight.high_voltage", { state: hv }));
    const energy = det.photon_energy;
    const threshold = det.threshold_energy ?? det["threshold/1/energy"];
    if (energy && threshold && Number(threshold.value) > Number(energy.value)) {
      issues.push(t("detector.preflight.threshold", { threshold: displayValue(threshold), energy: displayValue(energy) }));
    }
    return { issues, estimate, free, qualifier };
  }

  function renderPreflight(show) {
    // Held as it was while a quick action switches settings back and forth:
    // it would only report the action's own, deliberate temporary state.
    if (!preflightHost || ctx.quickRunning) return;
    preflightHost.replaceChildren();
    if (!show) return;
    // Only what would make the series fail, one amber line each; when all
    // is well the line beside the state says what the series takes.
    for (const issue of preflight().issues) preflightHost.append(el("div", "detector-issue", issue));
  }

  // ---------- result ----------
  function renderResult(show) {
    if (!resultHost) return;
    resultHost.replaceChildren();
    if (!show || !ctx.lastResult) return;
    const { series: id, count, seconds, prefix, stopped } = ctx.lastResult;
    const line = el("div", "detector-meta");
    // A stopped series took fewer images than it was set to, and how many
    // only its files tell: say when it stopped instead.
    const parts = stopped
      ? [t("detector.result.stopped", { series: id ?? "-", elapsed: formatDuration(seconds) })]
      : [t("detector.result.series", { series: id ?? "-", count })];
    if (seconds !== null && !stopped) parts.push(formatDuration(seconds));
    const written = prefix ? ctx.files.filter((file) => file.name === `${prefix}_master.h5` || file.name.startsWith(`${prefix}_data_`)) : [];
    if (written.length) {
      const size = written.reduce((sum, file) => sum + (Number(file.size) || 0), 0);
      parts.push(t("detector.result.files", { count: written.length, size: formatBytes(size) }));
    } else if (!prefix) {
      parts.push(t("detector.result.not_saved"));
    }
    line.append(`${stopped ? "" : "✓ "}${parts.join(" · ")}`);
    if (prefix && openPath) {
      const open = el("button", "linkish", t("detector.action.open_in_albis"));
      open.type = "button";
      open.addEventListener("click", () => void openSeries(prefix, id));
      line.append(" ", open);
    }
    resultHost.append(line);
  }

  // Copy the series from the detector into ALBIS's data folder, then open it
  // like any file: the full data, not the live preview.
  async function openSeries(prefix, id) {
    setMessage(t("detector.message.fetching", { series: id ?? "-" }), "busy");
    try {
      const fetched = await request("/detector/series/fetch", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url: ctx.url, version: ctx.version, prefix }),
      });
      setMessage("");
      await openPath?.(fetched.path);
      log(t("detector.log.opened", { series: id ?? "-" }));
    } catch (err) {
      setMessage(err.message || t("simplon.probe.request_failed"), "error");
    }
  }

  // ---------- rendering: state ----------
  function renderState() {
    const value = detectorState() || "na";
    const job = ctx.status?.command;
    const triggerMode = ctx.params.detector.trigger_mode?.value;
    if (statePill) {
      statePill.dataset.tone = STATE_TONES[value] || "busy";
      statePill.textContent = value in STATE_TONES ? t(`detector.state.${value}`) : value;
    }
    if (summary) summary.textContent = ctx.url ? statePill?.textContent || "" : t("detector.summary.not_connected");
    // Between a quick action's steps the detector is idle with the action's
    // temporary settings: the line keeps what it said.
    if (stateText && !(ctx.quickRunning && value === "idle")) {
      let text;
      if (value === "initialize" || (job?.running && job.command === "initialize")) {
        const elapsed = Math.max(0, (Date.now() / 1000) - (job?.started || Date.now() / 1000));
        text = t("detector.state_text.initialize", { elapsed: formatDuration(elapsed) });
      } else if (value === "ready" && triggerMode === "inte") {
        text = t("detector.state_text.ready_enable", { n: ctx.triggered + 1, total: Number(ctx.params.detector.ntrigger?.value || 1) });
      } else if (value === "ready") {
        text = t(String(triggerMode || "").startsWith("int") ? "detector.state_text.ready_internal" : "detector.state_text.ready_external");
      } else if (value === "acquire" && String(triggerMode || "").startsWith("ext") && !(job?.running && job.command === "trigger")) {
        // An armed detector in an external mode waits in "acquire" for its signals.
        text = triggerMode === "exte"
          ? t("detector.state_text.waiting_enable", { total: Number(ctx.params.detector.ntrigger?.value || 1) })
          : t("detector.state_text.ready_external");
      } else {
        // Idle needs no words: the pill says it, and the Acquisition and
        // Data output headers say what the series takes and where it goes.
        text = STATE_TEXTS.has(value) ? t(`detector.state_text.${value}`) : "";
      }
      stateText.textContent = text;
    }
    const action = job?.running && job.command !== "trigger" ? null : primaryAction(value, triggerMode);
    if (exposureBox) {
      const show = action === "trigger" && triggerMode === "inte";
      if (show && exposureBox.hidden && !exposureInput.value) exposureInput.value = displayValue(ctx.params.detector.count_time);
      exposureBox.hidden = !show;
      exposureInput.disabled = Boolean(job?.running);
    }
    if (primaryBtn) {
      primaryBtn.disabled = !action || ctx.quickRunning || Boolean(job?.running && job.command !== "trigger");
      // While a quick action runs, the big button names it, from its first
      // temporary setting to its last restore.
      if (ctx.quickRunning) primaryBtn.textContent = t(ctx.continuous ? "detector.action.continuous" : "detector.action.snap");
      else primaryBtn.textContent = action ? t(`detector.action.${action}`) : statePill?.textContent || "";
      primaryBtn.dataset.action = ctx.quickRunning ? "" : action || "";
    }
    const running = ctx.quickRunning || value === "acquire" || value === "ready" || value === "configure" || Boolean(job?.running && job.command === "trigger");
    if (stopBtn) stopBtn.hidden = !running;
    quickGroup?.classList.toggle("is-running", Boolean(stopBtn) && running);
    const idle = action === "acquire" && !ctx.acquiring && !ctx.quickRunning && !job?.running;
    // Shown, disabled, whenever they cannot run (before Initialize too): the
    // row keeps its shape.
    if (snapBtn) snapBtn.disabled = !idle;
    if (continuousBtn) continuousBtn.disabled = !idle || !ctx.params.detector.nimages;
    renderPreflight(idle);
    renderResult(idle || value === "ready");
    renderProgress(value);
    renderSensors();
    renderRecovery(value);
    const locked = isBusy() || ctx.acquiring || ctx.quickRunning;
    if (lockNote) lockNote.hidden = !locked;
    // The Pixel Mask section on this tab sends only while nothing runs.
    onChange?.();
    quickBox?.querySelectorAll("input[id^='detector-quick-']").forEach((input) => {
      input.disabled = ctx.quickRunning;
    });
    renderSeriesSummary(locked);
    content?.querySelectorAll("[data-param-input]").forEach((input) => {
      input.disabled = locked || input.dataset.readonly === "true";
    });
  }

  // What the next series will be, in the Acquisition header: visible with
  // the section closed, and it ties the numbers together. The duration only
  // where this panel's settings decide it, an internally triggered series.
  // "20 images · 20 s · ≈ 22.3 MB": the size where the file writer writes,
  // estimated, with how in the tooltip.
  function renderSeriesSummary(locked) {
    if (!seriesSummary) return;
    const parts = locked ? [] : [seriesText()].filter(Boolean);
    const { estimate, qualifier } = preflight();
    if (parts.length && estimate !== null) {
      parts.push(`≈ ${formatBytes(estimate)}`);
      seriesSummary.title = t("detector.summary.size_hint", { size: formatBytes(estimate), qualifier });
    } else {
      seriesSummary.removeAttribute("title");
    }
    seriesSummary.textContent = parts.join(" · ");
    seriesSummary.hidden = !parts.length;
    renderOutputSummary();
  }

  // Where the series goes and the room there: "161.0 GB free".
  function renderOutputSummary() {
    if (!outputSummary) return;
    const free = ctx.status?.filewriter?.buffer_free;
    const known = ctx.params.filewriter?.mode && free !== undefined && free !== null;
    outputSummary.textContent = known ? t("detector.output.free", { free: formatBytes(free) }) : "";
    outputSummary.hidden = !known;
  }

  // "20 images · 20 s", or "" when the detector does not say.
  function seriesText() {
    const det = ctx.params.detector;
    const perTrigger = Number(det.nimages?.value);
    const triggers = Number(det.ntrigger?.value ?? 1);
    const count = perTrigger * (triggers > 0 ? triggers : 1);
    if (!det.nimages || !Number.isFinite(count) || count <= 0) return "";
    const frame = Number(det.frame_time?.value);
    const mode = String(det.trigger_mode?.value || "");
    if (mode === "inte") return t("detector.summary.series_by_trigger", { count });
    if (mode === "exte") return t("detector.summary.series_by_signal", { count });
    if (mode === "ints" && frame > 0) return t("detector.summary.series", { count, duration: formatDuration(count * frame) });
    return t("detector.summary.series_images", { count });
  }

  function renderProgress(value) {
    if (!progress) return;
    const running = ctx.seriesStarted && (value === "acquire" || ctx.acquiring || ctx.status?.command?.running);
    progress.hidden = !running;
    if (!running) return;
    const det = ctx.params.detector;
    if (ctx.continuous) {
      const elapsed = (Date.now() - ctx.seriesStarted) / 1000;
      if (progressBar) progressBar.style.width = "100%";
      if (progressLeft) progressLeft.textContent = t("detector.action.continuous");
      if (progressRight) progressRight.textContent = t("detector.progress.continuous", { elapsed: formatProgress(elapsed, elapsed).elapsed });
      return;
    }
    const total = det.trigger_mode?.value === "inte"
      ? ctx.triggerExposure
      : Number(det.nimages?.value || 1) * Number(det.ntrigger?.value || 1) * Number(det.frame_time?.value || 0);
    const elapsed = (Date.now() - ctx.seriesStarted) / 1000;
    const fraction = total > 0 ? Math.min(1, elapsed / total) : 0;
    if (progressBar) progressBar.style.width = `${(fraction * 100).toFixed(1)}%`;
    if (progressLeft) progressLeft.textContent = ctx.series !== null ? t("detector.progress.series", { series: ctx.series }) : "";
    if (progressRight) progressRight.textContent = t("detector.progress.elapsed", formatProgress(elapsed, total));
  }

  function renderSensors() {
    if (!sensors) return;
    sensors.replaceChildren();
    const det = ctx.status?.detector || {};
    const items = [
      ["temperature", det.temperature, (v) => `${Number(v).toFixed(1)} °C`],
      ["humidity", det.humidity, (v) => `${Number(v).toFixed(1)} %`],
      // SIMPLON 1.8 reports a state (READY); 1.6 a module's voltage.
      ["high_voltage", det["high_voltage/state"] ?? det.high_voltage, (v) => (typeof v === "number" ? `${Math.round(v)} V` : String(v))],
    ];
    for (const [key, value, fmt] of items) {
      if (value === null || value === undefined || value === "") continue;
      const box = el("div", "detector-sensor");
      const strong = el("strong", "", fmt(value));
      if (key === "high_voltage") {
        const critical = Array.isArray(det.critical) && det.critical.includes("high_voltage");
        const ready = typeof value === "number" ? !critical : String(value).toUpperCase() === "READY";
        strong.dataset.tone = ready ? "ok" : "warn";
      }
      box.append(el("span", "", t(`detector.sensor.${key}`)), strong);
      sensors.append(box);
    }
  }

  return {
    preflight,
    renderState,
    renderProgress,
  };
}
