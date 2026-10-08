/**
 * Detector tab (beta): control a DECTRIS detector over the SIMPLON API.
 *
 * Every field is built from what the detector says about the parameter --
 * value, type, unit, limits, allowed values, access -- so each detector model
 * gets its own correct form. The backend (/api/detector/*) proxies the
 * detector; it answers 404 unless Settings -> Viewer -> Beta: detector control
 * is on, which is also what shows this tab.
 *
 * SIMPLON reports no image counter while a series runs, so progress is the
 * elapsed time against images x triggers x frame time, and is labelled as an
 * estimate. Anything the detector sends (its description, file names) is put
 * on the page as text, never as markup.
 */

import { t } from "./i18n.js";
import { describeSimplonFailure } from "./simplon_diagnostics.js";
import { normalizeSimplonBaseUrl } from "./simplon_url_utils.js";

const STORAGE_KEY = "albis.detectorControl.url";
const POLL_ACTIVE_MS = 1000;
const POLL_IDLE_MS = 3000;
const LOG_LIMIT = 12;
// Below this much free detector storage, offer to clear it even if SIMPLON
// does not (yet) flag buffer_free as critical.
const LOW_STORAGE_BYTES = 1024 ** 3;

// The main view: what an acquisition needs, in this order, if the detector has
// it. A tuple lists alternatives, the first present one wins: X-ray detectors
// call the energy photon_energy, electron-microscopy ones incident_energy.
const CORE_GROUPS = [
  {
    id: "energy",
    keys: [
      ["photon_energy", "incident_energy"],
      // threshold_energy is threshold/1/energy under another name (SIMPLON
      // reference); each threshold's mode decides whether its images are
      // taken at all, so it sits next to the energy, not in Advanced.
      "threshold_energy",
      "threshold/1/mode",
      "threshold/2/energy",
      "threshold/2/mode",
      "threshold/3/energy",
      "threshold/3/mode",
      "threshold/4/energy",
      "threshold/4/mode",
      "threshold/difference/mode",
    ],
  },
  { id: "timing", keys: ["count_time", "frame_time"] },
  { id: "series", keys: ["nimages", "ntrigger", "trigger_mode"] },
];
const OUTPUT_MAIN = {
  filewriter: ["name_pattern", "nimages_per_file"],
  stream: ["header_detail"],
  monitor: [],
};
const BUSY_STATES = new Set(["initialize", "configure", "acquire", "test"]);
const STATE_TONES = {
  na: "busy",
  initialize: "busy",
  configure: "busy",
  test: "busy",
  idle: "idle",
  ready: "ready",
  acquire: "acquire",
  error: "error",
};
const TRIGGER_MODE_KEYS = new Set(["ints", "inte", "exts", "exte"]);
// Parameters with a translated label; every other one is shown by its SIMPLON
// key, which is also what the API documentation calls it.
const LABELLED_PARAMS = new Set([
  "photon_energy",
  "incident_energy",
  "threshold_energy",
  "count_time",
  "frame_time",
  "nimages",
  "ntrigger",
  "trigger_mode",
  "name_pattern",
  "nimages_per_file",
  "header_detail",
]);
const STATE_TEXTS = new Set(["na", "configure", "idle", "acquire", "test", "error"]);
const OUTPUT_STATES = new Set(["disabled", "ready", "acquire", "error", "normal", "overflow"]);

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined && text !== null) node.textContent = String(text);
  return node;
}

function readStored() {
  try {
    return window.localStorage.getItem(STORAGE_KEY) || "";
  } catch {
    return "";
  }
}

function writeStored(value) {
  try {
    window.localStorage.setItem(STORAGE_KEY, value);
  } catch {
    // Private windows: the address is simply not remembered.
  }
}

/** A duration for people: 0.0099999 s -> "10 ms", 125 s -> "2:05 min". */
export function formatDuration(seconds) {
  const s = Number(seconds);
  if (!Number.isFinite(s) || s < 0) return "-";
  if (s < 1e-3) return `${+(s * 1e6).toPrecision(3)} µs`;
  if (s < 1) return `${+(s * 1e3).toPrecision(3)} ms`;
  if (s < 120) return `${+s.toPrecision(3)} s`;
  const m = Math.floor(s / 60);
  return `${m}:${String(Math.round(s % 60)).padStart(2, "0")} min`;
}

export function formatBytes(bytes) {
  const b = Number(bytes);
  if (!Number.isFinite(b) || b < 0) return "";
  if (b >= 1e12) return `${(b / 1e12).toFixed(1)} TB`;
  if (b >= 1e9) return `${(b / 1e9).toFixed(1)} GB`;
  if (b >= 1e6) return `${(b / 1e6).toFixed(1)} MB`;
  return `${Math.max(1, Math.round(b / 1e3))} kB`;
}

/**
 * The value as the field shows it. Whole numbers exactly; energies to 0.1 eV
 * (8047.7798 -> 8047.8); other decimals to six significant digits. Only the
 * display is rounded: nothing is written unless the user edits the field.
 */
export function displayValue(descriptor) {
  const v = descriptor?.value;
  if (v === null || v === undefined) return "";
  if (typeof v !== "number") return String(v);
  const type = String(descriptor?.value_type || "");
  if (type === "uint" || type === "int" || Number.isInteger(v)) return String(v);
  if (descriptor?.unit === "eV") return String(+v.toFixed(1));
  return String(+v.toPrecision(6));
}

const UNIT_SYMBOLS = { angstrom: "Å", degree: "°", degrees: "°", deg: "°", micrometer: "µm", um: "µm", percent: "%" };

/** The short form of a SIMPLON unit, so it fits beside the value ("angstrom" -> "Å"). */
export function unitSymbol(unit) {
  const text = String(unit || "");
  return UNIT_SYMBOLS[text.toLowerCase()] || text;
}

/**
 * Parse and check what was typed against the detector's own description.
 * Returns { ok, value } or { ok: false, error }.
 */
export function validateInput(descriptor, raw) {
  const type = String(descriptor?.value_type || "");
  if (type === "bool") return { ok: true, value: raw === true || raw === "true" };
  if (Array.isArray(descriptor?.allowed_values) && descriptor.allowed_values.length) {
    return descriptor.allowed_values.includes(raw)
      ? { ok: true, value: raw }
      : { ok: false, error: t("detector.error.choose") };
  }
  if (type === "string") return { ok: true, value: String(raw) };
  const value = Number(String(raw).trim().replace(",", "."));
  if (String(raw).trim() === "" || !Number.isFinite(value)) {
    return { ok: false, error: t("detector.error.number") };
  }
  if ((type === "uint" || type === "int") && !Number.isInteger(value)) {
    return { ok: false, error: t("detector.error.whole") };
  }
  const { min, max } = descriptor || {};
  if ((typeof min === "number" && value < min) || (typeof max === "number" && value > max)) {
    return { ok: false, error: t("detector.error.range", { range: rangeText(descriptor) }) };
  }
  return { ok: true, value };
}

function fmtLimit(value, unit) {
  if (unit === "s") return formatDuration(value);
  const n = Math.abs(value) >= 1e5 || (Math.abs(value) < 1e-3 && value !== 0)
    ? Number(value).toExponential(2)
    : String(+Number(value).toPrecision(6));
  return unit ? `${n} ${unit}` : n;
}

export function rangeText(descriptor) {
  const { min, max, unit } = descriptor || {};
  if (Array.isArray(descriptor?.allowed_values) && descriptor.allowed_values.length) {
    return t("detector.range.options", { count: descriptor.allowed_values.length });
  }
  if (typeof min === "number" && typeof max === "number") {
    if (max >= 1e6 && unit !== "s") return t("detector.range.at_least", { min: fmtLimit(min, unit) });
    return t("detector.range.between", { min: fmtLimit(min, unit), max: fmtLimit(max, unit) });
  }
  return "";
}

/** What the one primary button does in a detector state. */
export function primaryAction(stateValue, triggerMode) {
  if (stateValue === "na" || stateValue === "error") return "initialize";
  if (stateValue === "idle") return "acquire";
  if (stateValue === "ready" && String(triggerMode || "").startsWith("int")) return "trigger";
  return null;
}

export function paramLabel(key) {
  const threshold = /^threshold\/(\d+)\/energy$/.exec(key);
  if (threshold) return t("detector.param.threshold_n", { n: threshold[1] });
  const thresholdMode = /^threshold\/(\d+)\/mode$/.exec(key);
  if (thresholdMode) return t("detector.param.threshold_n_mode", { n: thresholdMode[1] });
  if (key === "threshold/difference/mode") return t("detector.param.difference_mode");
  return LABELLED_PARAMS.has(key) ? t(`detector.param.${key}`) : key;
}

function optionLabel(key, value) {
  if (key === "trigger_mode" && TRIGGER_MODE_KEYS.has(value)) return t(`detector.trigger_mode.${value}`);
  if (value === "enabled") return t("detector.value.on");
  if (value === "disabled") return t("detector.value.off");
  return String(value);
}

export function createDetectorControlController({ apiBase, elements, callbacks = {} }) {
  const {
    tab,
    content,
    urlInput,
    connectBtn,
    message,
    summary,
    live,
    model,
    serial,
    statePill,
    stateText,
    progress,
    progressBar,
    progressLeft,
    progressRight,
    confirm,
    confirmText,
    confirmYes,
    confirmNo,
    primaryBtn,
    stopBtn,
    sensors,
    notice,
    noticeText,
    noticeDismiss,
    sections,
    paramsHost,
    lockNote,
    outputsHost,
    filesHost,
    logHost,
    advancedHost,
    commandsHost,
  } = elements;
  const { getPanelTab, setPanelTab, watchLive } = callbacks;

  let enabled = false;
  let url = "";
  let version = "1.8.0";
  let params = { detector: {}, monitor: {}, filewriter: {}, stream: {} };
  let status = null;
  let lastModes = null;
  let pollTimer = null;
  let series = null;
  let seriesStarted = 0;
  let acquiring = false;
  let confirmAction = null;
  let files = [];
  let filesError = "";
  // Recovery hints sit right under the sensors, in the detector card.
  const recoverHost = sensors ? el("div", "detector-recover") : null;
  sensors?.after?.(recoverHost);

  // ---------- small pieces ----------
  function setMessage(text, variant = "") {
    if (!message) return;
    message.textContent = text || "";
    message.classList.toggle("is-hidden", !text);
    message.classList.toggle("is-busy", variant === "busy");
    message.classList.toggle("is-ok", variant === "ok");
    message.classList.toggle("is-error", variant === "error");
  }

  function log(text, kind = "") {
    if (!logHost) return;
    const li = el("li", kind ? `is-${kind}` : "");
    li.append(el("time", "", new Date().toLocaleTimeString([], { hour12: false })), el("span", "", text));
    logHost.prepend(li);
    while (logHost.children.length > LOG_LIMIT) logHost.lastChild.remove();
  }

  function ask(text, yesLabel, noLabel, onYes) {
    if (!confirm) return;
    confirmText.textContent = text;
    confirmYes.textContent = yesLabel;
    confirmNo.textContent = noLabel;
    confirmAction = onYes;
    confirm.hidden = false;
    confirmYes.focus?.();
  }

  function reason(detail) {
    if (detail && typeof detail === "object") {
      return detail.code ? describeSimplonFailure({ api_version: version, ...detail }) : detail.summary || "";
    }
    return String(detail || "");
  }

  async function request(path, init) {
    const res = await fetch(`${apiBase}${path}`, init);
    let body = null;
    try {
      body = await res.json();
    } catch {
      body = null;
    }
    if (!res.ok) {
      const error = new Error(reason(body?.detail) || `HTTP ${res.status}`);
      error.status = res.status;
      throw error;
    }
    return body;
  }

  const query = () => `url=${encodeURIComponent(url)}&version=${encodeURIComponent(version)}`;

  function detectorState() {
    return String(status?.detector?.state || "").toLowerCase();
  }

  function isBusy() {
    return BUSY_STATES.has(detectorState()) || Boolean(status?.command?.running);
  }

  // ---------- connecting ----------
  async function connect() {
    const normalized = normalizeSimplonBaseUrl(urlInput?.value || "");
    if (!normalized) {
      setMessage(t("simplon.probe.enter_address"), "error");
      urlInput?.focus?.();
      return;
    }
    if (urlInput) urlInput.value = normalized;
    url = normalized;
    writeStored(url);
    stopPolling();
    setMessage(t("detector.message.connecting", { url }), "busy");
    if (connectBtn) connectBtn.disabled = true;
    try {
      // Not the monitor's connection test: that reads a monitor setting, and
      // before initialize a detector answers only its state ("Before
      // initializing the detector only the detector state is available!") --
      // a freshly switched-on detector would look like no detector at all.
      await refreshDescription();
      await poll();
      const name = displayValue(params.detector.description) || url;
      setMessage(t("detector.message.connected", { detector: name }), "ok");
      log(t("detector.log.connected", { detector: name }));
      if (live) live.hidden = false;
      sections?.forEach((section) => { section.hidden = false; });
      await refreshFiles();
    } catch (err) {
      setMessage(err.message || t("simplon.probe.request_failed"), "error");
    } finally {
      if (connectBtn) connectBtn.disabled = false;
    }
  }

  async function refreshDescription() {
    const payload = await request(`/detector/describe?${query()}`);
    params = { detector: {}, monitor: {}, filewriter: {}, stream: {}, ...(payload?.params || {}) };
    if (model) model.textContent = displayValue(params.detector.description) || t("detector.section.detector");
    if (serial) serial.textContent = displayValue(params.detector.detector_number);
    renderParams();
    renderOutputs();
    renderAdvanced();
  }

  // ---------- polling ----------
  function stopPolling() {
    if (pollTimer) window.clearTimeout(pollTimer);
    pollTimer = null;
  }

  function schedulePoll() {
    stopPolling();
    if (!enabled || !url) return;
    pollTimer = window.setTimeout(() => {
      void poll();
    }, isBusy() || acquiring ? POLL_ACTIVE_MS : POLL_IDLE_MS);
  }

  async function poll() {
    if (!enabled || !url) return;
    try {
      const next = await request(`/detector/status?${query()}`);
      const before = detectorState();
      status = next;
      noticeExternalChanges();
      // Leaving "na" means initialize finished, possibly started elsewhere:
      // only now are the parameters readable.
      if (before === "na" && detectorState() !== "na") await refreshDescription();
      renderState();
      renderOutputs();
    } catch (err) {
      if (stateText) stateText.textContent = err.message;
      if (statePill) {
        statePill.dataset.tone = "error";
        statePill.textContent = t("detector.state.unreachable");
      }
    } finally {
      schedulePoll();
    }
  }

  function noticeExternalChanges() {
    const modes = {
      filewriter: status?.filewriter?.mode,
      stream: status?.stream?.mode,
      monitor: status?.monitor?.mode,
    };
    const job = status?.command;
    // A stream reset switches the stream off and on again: not "elsewhere".
    if (job?.running && job.subsystem === "stream") return;
    if (lastModes) {
      for (const [name, mode] of Object.entries(modes)) {
        if (mode && lastModes[name] && mode !== lastModes[name]) {
          if (params[name]?.mode) params[name].mode.value = mode;
          log(t("detector.log.elsewhere", { what: t(`detector.output.${name}`) }), "elsewhere");
          if (notice) {
            noticeText.textContent = t("detector.notice.elsewhere");
            notice.hidden = false;
          }
        }
      }
    }
    lastModes = modes;
  }

  async function waitForCommand(command, timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      await new Promise((resolve) => window.setTimeout(resolve, 400));
      const next = await request(`/detector/status?${query()}`);
      status = next;
      renderState();
      const job = next?.command;
      if (job && job.command === command && !job.running) return job;
    }
    return null;
  }

  // ---------- commands ----------
  // "initialize" alone would read as the detector's; name other subsystems.
  const commandName = (subsystem, command) => (subsystem === "detector" ? command : `${subsystem} ${command}`);

  async function sendCommand(subsystem, command, { quiet = false } = {}) {
    if (!quiet) log(t("detector.log.sent", { command: commandName(subsystem, command) }));
    try {
      return await request("/detector/command", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url, version, subsystem, command }),
      });
    } catch (err) {
      log(t("detector.log.failed", { command: commandName(subsystem, command), reason: err.message }), "error");
      throw err;
    }
  }

  async function runCommand(subsystem, command, timeoutMs = 60000) {
    await sendCommand(subsystem, command);
    schedulePoll();
    const job = await waitForCommand(command, timeoutMs);
    if (!job) return null;
    const name = commandName(subsystem, command);
    if (job.ok) log(t("detector.log.done", { command: name }));
    else log(t("detector.log.failed", { command: name, reason: reason(job.error) }), "error");
    return job;
  }

  async function initialize() {
    renderState();
    const job = await runCommand("detector", "initialize", 260000);
    if (job?.ok) await refreshDescription();
    await poll();
  }

  // ---------- recovery ----------
  // Initialize, stream reset and deleting the files are recovery steps, not
  // everyday ones: each is offered next to what it fixes when the detector
  // reports that problem, always behind a confirmation that says what it does,
  // and all three together under Advanced -> Troubleshooting.
  function askInitialize() {
    ask(t("detector.confirm.initialize"), t("detector.action.initialize"), t("detector.action.cancel"), initialize);
  }

  function askResetStream() {
    ask(t("detector.confirm.reset_stream"), t("detector.confirm.reset"), t("detector.action.cancel"), async () => {
      const job = await runCommand("stream", "initialize", 60000);
      if (job?.ok) {
        log(t("detector.log.stream_reset"));
        const mode = job.result?.mode;
        if (mode && params.stream?.mode) params.stream.mode.value = mode;
        lastModes = { ...(lastModes || {}), stream: mode || lastModes?.stream };
      }
      await poll();
    });
  }

  function askDeleteFiles() {
    const size = files.reduce((sum, file) => sum + (Number(file.size) || 0), 0);
    const text = files.length
      ? t("detector.confirm.delete_files", { count: files.length, size: formatBytes(size) })
      : t("detector.confirm.delete_unlisted");
    ask(text, t("detector.confirm.delete"), t("detector.action.cancel"), async () => {
      const job = await runCommand("filewriter", "clear", 60000);
      if (job?.ok) log(t("detector.log.files_deleted"));
      await refreshFiles();
      await poll();
    });
  }

  function storageLow() {
    const fw = status?.filewriter || {};
    if (Array.isArray(fw.critical) && fw.critical.includes("buffer_free")) return true;
    return fw.buffer_free !== null && fw.buffer_free !== undefined && Number(fw.buffer_free) < LOW_STORAGE_BYTES;
  }

  function recoveryButton(label, onClick) {
    const button = el("button", "linkish", label);
    button.type = "button";
    button.addEventListener("click", onClick);
    return button;
  }

  // Under the sensors: a quiet "Re-initialize…" while all is well, a short
  // explanation with it when high voltage is not ready or a command failed.
  // Not while busy, and not in "na" or "error", where the main button already
  // says Initialize.
  function renderRecovery(value) {
    if (!recoverHost) return;
    recoverHost.replaceChildren();
    if (!url || !status || value === "na" || value === "error" || isBusy() || acquiring) return;
    const job = status.command;
    const hv = String(status.detector?.["high_voltage/state"] || "");
    let text = "";
    if (job && job.ok === false && job.subsystem === "detector") {
      text = t("detector.recover.command_failed", { command: job.command });
    } else if (hv && hv.toUpperCase() !== "READY") {
      text = t("detector.recover.high_voltage", { state: hv });
    }
    const button = recoveryButton(t("detector.action.reinitialize"), askInitialize);
    if (text) {
      const note = el("div", "detector-warning is-caution", text);
      note.append(" ", button);
      recoverHost.append(note);
    } else {
      recoverHost.append(button);
    }
  }

  function startAcquire() {
    const fw = params.filewriter?.mode?.value;
    const st = params.stream?.mode?.value;
    if (fw !== "enabled" && st !== "enabled") {
      ask(t("detector.confirm.nothing_saved"), t("detector.confirm.acquire_anyway"), t("detector.action.cancel"), acquire);
      return;
    }
    void acquire();
  }

  async function acquire() {
    acquiring = true;
    try {
      const arm = await runCommand("detector", "arm", 130000);
      if (!arm?.ok) return;
      series = arm.result?.["sequence id"] ?? arm.result?.sequence_id ?? null;
      const mode = String(params.detector.trigger_mode?.value || "");
      if (mode.startsWith("int")) await trigger();
      else renderState();
    } finally {
      acquiring = false;
      schedulePoll();
    }
  }

  async function trigger() {
    seriesStarted = Date.now();
    acquiring = true;
    const job = await runCommand("detector", "trigger", 24 * 3600 * 1000);
    acquiring = false;
    seriesStarted = 0;
    if (job?.ok) log(t("detector.log.series_done", { series: series ?? "-" }));
    await poll();
    await refreshFiles();
  }

  function stop() {
    ask(t("detector.confirm.abort"), t("detector.action.abort"), t("detector.action.keep_running"), async () => {
      try {
        await sendCommand("detector", "abort");
      } finally {
        await poll();
      }
    });
  }

  // ---------- rendering: state ----------
  function renderState() {
    const value = detectorState() || "na";
    const job = status?.command;
    const triggerMode = params.detector.trigger_mode?.value;
    if (statePill) {
      statePill.dataset.tone = STATE_TONES[value] || "busy";
      statePill.textContent = value in STATE_TONES ? t(`detector.state.${value}`) : value;
    }
    if (summary) summary.textContent = url ? statePill?.textContent || "" : t("detector.summary.not_connected");
    if (stateText) {
      let text;
      if (value === "initialize" || (job?.running && job.command === "initialize")) {
        const elapsed = Math.max(0, (Date.now() / 1000) - (job?.started || Date.now() / 1000));
        text = t("detector.state_text.initialize", { elapsed: formatDuration(elapsed) });
      } else if (value === "ready") {
        text = t(String(triggerMode || "").startsWith("int") ? "detector.state_text.ready_internal" : "detector.state_text.ready_external");
      } else {
        text = STATE_TEXTS.has(value) ? t(`detector.state_text.${value}`) : "";
      }
      stateText.textContent = text;
    }
    const action = job?.running && job.command !== "trigger" ? null : primaryAction(value, triggerMode);
    if (primaryBtn) {
      primaryBtn.disabled = !action || Boolean(job?.running && job.command !== "trigger");
      primaryBtn.textContent = action ? t(`detector.action.${action}`) : statePill?.textContent || "";
      primaryBtn.dataset.action = action || "";
    }
    if (stopBtn) stopBtn.hidden = !(value === "acquire" || value === "ready" || value === "configure" || (job?.running && job.command === "trigger"));
    renderProgress(value);
    renderSensors();
    renderRecovery(value);
    const locked = isBusy() || acquiring;
    if (lockNote) lockNote.hidden = !locked;
    content?.querySelectorAll("[data-param-input]").forEach((input) => {
      input.disabled = locked || input.dataset.readonly === "true";
    });
  }

  function renderProgress(value) {
    if (!progress) return;
    const running = seriesStarted && (value === "acquire" || status?.command?.running);
    progress.hidden = !running;
    if (!running) return;
    const det = params.detector;
    const total = Number(det.nimages?.value || 1) * Number(det.ntrigger?.value || 1) * Number(det.frame_time?.value || 0);
    const elapsed = (Date.now() - seriesStarted) / 1000;
    const fraction = total > 0 ? Math.min(1, elapsed / total) : 0;
    if (progressBar) progressBar.style.width = `${(fraction * 100).toFixed(1)}%`;
    if (progressLeft) progressLeft.textContent = series !== null ? t("detector.progress.series", { series }) : "";
    if (progressRight) {
      progressRight.textContent = t("detector.progress.elapsed", {
        elapsed: formatDuration(elapsed),
        total: formatDuration(total),
      });
    }
  }

  function renderSensors() {
    if (!sensors) return;
    sensors.replaceChildren();
    const det = status?.detector || {};
    const items = [
      ["temperature", det.temperature, (v) => `${Number(v).toFixed(1)} °C`],
      ["humidity", det.humidity, (v) => `${Number(v).toFixed(1)} %`],
      ["high_voltage", det["high_voltage/state"], (v) => String(v)],
    ];
    for (const [key, value, fmt] of items) {
      if (value === null || value === undefined || value === "") continue;
      const box = el("div", "detector-sensor");
      const strong = el("strong", "", fmt(value));
      if (key === "high_voltage") strong.dataset.tone = String(value).toUpperCase() === "READY" ? "ok" : "warn";
      box.append(el("span", "", t(`detector.sensor.${key}`)), strong);
      sensors.append(box);
    }
  }

  // ---------- rendering: parameters ----------
  function labelFor(subsystem, key) {
    // With several thresholds, the first one is "Threshold 1", not "Threshold".
    if (subsystem === "detector" && key === "threshold_energy" && params.detector["threshold/2/energy"]) {
      return t("detector.param.threshold_n", { n: 1 });
    }
    return paramLabel(key);
  }

  function paramRow(subsystem, key, descriptor) {
    const row = el("div", "detector-param");
    row.dataset.key = `${subsystem}:${key}`;
    const id = `detector-p-${subsystem}-${key.replace(/[^a-z0-9]/gi, "_")}`;
    const name = el("div", "detector-param-name");
    const labelText = labelFor(subsystem, key);
    const label = el("label", "", labelText);
    label.htmlFor = id;
    name.append(label);
    // The SIMPLON key under a translated name; once is enough when they match.
    if (labelText !== key) name.append(el("code", "", key));
    row.dataset.search = `${labelText} ${key}`.toLowerCase();
    row.append(name);
    const readonly = !String(descriptor.access_mode || "rw").includes("w");
    if (readonly) {
      const unit = descriptor.unit ? ` ${unitSymbol(descriptor.unit)}` : "";
      const value = el("div", "detector-readonly", `${displayValue(descriptor)}${unit}`);
      value.id = id;
      row.append(value);
      return row;
    }
    const field = el("div", "detector-field");
    let input;
    const allowed = Array.isArray(descriptor.allowed_values) ? descriptor.allowed_values : null;
    if (descriptor.value_type === "bool") {
      input = el("select");
      for (const [value, text] of [["true", t("detector.value.on")], ["false", t("detector.value.off")]]) {
        const option = el("option", "", text);
        option.value = value;
        input.append(option);
      }
      input.value = String(Boolean(descriptor.value));
    } else if (allowed) {
      input = el("select");
      for (const value of allowed) {
        const option = el("option", "", optionLabel(key, value));
        option.value = String(value);
        input.append(option);
      }
      input.value = String(descriptor.value);
    } else {
      input = el("input");
      input.type = "text";
      input.autocomplete = "off";
      input.inputMode = descriptor.value_type === "string" ? "text" : "decimal";
      input.value = displayValue(descriptor);
      const unit = unitSymbol(descriptor.unit);
      if (unit) {
        field.append(el("span", "detector-unit", unit));
        input.style.paddingRight = `${16 + unit.length * 7}px`;
      }
      if (descriptor.value_type === "string") {
        // Free text (a name pattern, a sample name) needs the whole row.
        row.classList.add("is-wide");
      } else {
        input.classList.add("is-number");
      }
    }
    input.id = id;
    input.dataset.paramInput = "";
    field.prepend(input);
    row.append(field);
    // A range says something about a number; a dropdown already shows its options.
    const numeric = !allowed && descriptor.value_type !== "bool" && descriptor.value_type !== "string";
    const hint = el("div", "detector-hint", numeric ? rangeText(descriptor) : "");
    hint.dataset.range = hint.textContent;
    row.append(hint);
    const commit = () => void writeParam(subsystem, key, input, hint, row);
    input.addEventListener("change", commit);
    if (input.tagName === "INPUT") {
      input.addEventListener("keydown", (event) => {
        if (event.key === "Enter") input.blur();
      });
    }
    return row;
  }

  async function writeParam(subsystem, key, input, hint, row) {
    const descriptor = params[subsystem]?.[key];
    if (!descriptor) return;
    const checked = validateInput(descriptor, input.value);
    input.setAttribute("aria-invalid", String(!checked.ok));
    if (!checked.ok) {
      hint.textContent = checked.error;
      hint.className = "detector-hint is-error";
      return;
    }
    if (checked.value === descriptor.value) {
      hint.textContent = rangeText(descriptor);
      hint.className = "detector-hint";
      return;
    }
    if (subsystem === "filewriter" && key === "name_pattern" && !String(checked.value).includes("$id")) {
      ask(t("detector.confirm.no_id"), t("detector.confirm.use_anyway"), t("detector.action.cancel"), () => sendParam(subsystem, key, checked.value, input, hint, row));
      input.value = displayValue(descriptor);
      return;
    }
    await sendParam(subsystem, key, checked.value, input, hint, row);
  }

  async function sendParam(subsystem, key, value, input, hint, row) {
    row.classList.add("is-pending");
    try {
      const result = await request("/detector/config", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url, version, subsystem, key, value }),
      });
      // SIMPLON lists what changed "or may have been changed"; flag only the
      // values that really moved.
      const before = {};
      for (const name of Object.keys(result?.params || {})) before[name] = params[subsystem][name]?.value;
      for (const [name, descriptor] of Object.entries(result?.params || {})) {
        params[subsystem][name] = descriptor;
      }
      const moved = Object.keys(result?.params || {}).filter(
        (name) => name !== key && JSON.stringify(before[name]) !== JSON.stringify(params[subsystem][name]?.value),
      );
      // An output's mode reads as "File writer: On", not "mode set to enabled".
      const isMode = key === "mode" && subsystem !== "detector";
      const label = isMode ? t(`detector.output.${subsystem}`) : paramLabel(key);
      const shown = isMode
        ? t(value === "enabled" ? "detector.value.on" : "detector.value.off")
        : optionLabel(key, displayValue(params[subsystem][key]));
      log(t("detector.log.set", { label, value: shown }));
      if (isMode && status?.[subsystem]) {
        // The status poll would say so only on its next round; until then the
        // row must not contradict the switch just flipped.
        status[subsystem].mode = value;
        status[subsystem].state =
          value !== "enabled" ? "disabled" : subsystem === "monitor" ? "normal" : "ready";
      }
      for (const name of moved) {
        log(t("detector.log.adjusted", { label: labelFor(subsystem, name), value: displayValue(params[subsystem][name]) }));
      }
      rerenderAfterWrite(subsystem, key, moved);
    } catch (err) {
      hint.textContent = err.message;
      hint.className = "detector-hint is-error";
      input.value = displayValue(params[subsystem]?.[key]);
    } finally {
      row.classList.remove("is-pending");
    }
  }

  function rerenderAfterWrite(subsystem, key, changed) {
    if (subsystem === "detector") renderParams();
    renderOutputs();
    renderAdvanced();
    for (const name of changed) {
      if (name === key) continue;
      const selector = `[data-key="${`${subsystem}:${name}`.replace(/["\\]/g, "\\$&")}"]`;
      const row = content?.querySelector(selector);
      if (!row) continue;
      row.classList.add("is-flash");
      const hint = row.querySelector(".detector-hint");
      if (hint) {
        hint.textContent = t("detector.hint.adjusted");
        hint.className = "detector-hint is-changed";
        window.setTimeout(() => {
          if (!hint.classList.contains("is-changed")) return;
          hint.textContent = hint.dataset.range || "";
          hint.className = "detector-hint";
        }, 3500);
      }
      window.setTimeout(() => row.classList.remove("is-flash"), 1600);
    }
  }

  function coreKeys() {
    const out = [];
    for (const group of CORE_GROUPS) {
      const keys = [];
      for (const entry of group.keys) {
        const options = Array.isArray(entry) ? entry : [entry];
        const found = options.find((key) => params.detector[key]);
        if (found) keys.push(found);
      }
      if (keys.length) out.push({ id: group.id, keys });
    }
    return out;
  }

  function renderParams() {
    if (!paramsHost) return;
    paramsHost.replaceChildren();
    for (const group of coreKeys()) {
      paramsHost.append(el("div", "detector-group-label", t(`detector.group.${group.id}`)));
      for (const key of group.keys) paramsHost.append(paramRow("detector", key, params.detector[key]));
    }
    renderState();
  }

  // ---------- rendering: data output ----------
  function outputState(name) {
    const st = status?.[name] || {};
    const mode = st.mode ?? params[name]?.mode?.value;
    if (mode !== "enabled") return { tone: "", text: t("detector.output.state.disabled") };
    const value = String(st.state || "ready");
    const text = OUTPUT_STATES.has(value) ? t(`detector.output.state.${value}`) : value;
    const tone = value === "acquire" ? "busy" : value === "error" || value === "overflow" ? "error" : "ok";
    return { tone, text };
  }

  function renderOutputs() {
    if (!outputsHost) return;
    outputsHost.replaceChildren();
    const fw = params.filewriter?.mode?.value;
    const st = params.stream?.mode?.value;
    if (params.filewriter?.mode || params.stream?.mode) {
      if (fw !== "enabled" && st !== "enabled") {
        outputsHost.append(el("p", "detector-warning", t("detector.output.nothing_saved")));
      }
    }
    for (const name of ["filewriter", "stream", "monitor"]) {
      const mode = params[name]?.mode;
      if (!mode) continue;
      const block = el("div", "detector-output");
      const head = el("div", "detector-output-head");
      const switchLabel = el("label", "detector-switch");
      const toggle = el("input");
      toggle.type = "checkbox";
      toggle.checked = mode.value === "enabled";
      toggle.dataset.paramInput = "";
      toggle.setAttribute("aria-label", t(`detector.output.${name}`));
      switchLabel.append(toggle, el("span"));
      const title = el("div", "detector-param-name");
      title.append(el("strong", "", t(`detector.output.${name}`)), el("code", "", name));
      const state = outputState(name);
      const dot = el("span", "detector-dot", state.text);
      dot.dataset.tone = state.tone;
      head.append(switchLabel, title, dot);
      block.append(head);
      toggle.addEventListener("change", () => {
        const next = toggle.checked ? "enabled" : "disabled";
        if (name === "stream" && next === "disabled") {
          toggle.checked = true;
          ask(t("detector.confirm.stream_off"), t("detector.confirm.turn_off"), t("detector.confirm.keep_on"), () => setMode(name, next));
          return;
        }
        void setMode(name, next);
      });
      if (mode.value === "enabled") {
        for (const key of OUTPUT_MAIN[name]) {
          if (params[name][key]) block.append(paramRow(name, key, params[name][key]));
        }
        if (name === "filewriter" && params.filewriter.name_pattern) {
          const pattern = String(params.filewriter.name_pattern.value || "");
          if (!pattern.includes("$id")) {
            block.append(el("div", "detector-warning", t("detector.output.no_id")));
          } else if (series !== null && Number.isFinite(Number(series))) {
            // Known only once a series was armed here; SIMPLON numbers them in turn.
            const next = pattern.replace("$id", String(Number(series) + 1));
            block.append(el("div", "detector-note", t("detector.output.next_file", { name: `${next}_master.h5` })));
          } else {
            block.append(el("div", "detector-note", t("detector.output.next_file_pattern", { name: `${pattern}_master.h5` })));
          }
        }
        if (name === "filewriter" && status?.filewriter?.buffer_free !== undefined && status?.filewriter?.buffer_free !== null) {
          block.append(el("div", "detector-note", t("detector.output.storage_free", { free: formatBytes(status.filewriter.buffer_free) })));
        }
        if (name === "filewriter" && storageLow()) {
          const warning = el("div", "detector-warning", t("detector.output.storage_low"));
          warning.append(" ", recoveryButton(t("detector.action.delete_files"), askDeleteFiles));
          block.append(warning);
        }
        if (name === "filewriter") {
          // The DCU serves the files it wrote at /data/ (SIMPLON reference),
          // which is also where its own web interface lists them.
          const page = el("a", "linkish detector-external", t("detector.output.data_page"));
          page.href = `${url.replace(/\/+$/, "")}/data/`;
          page.target = "_blank";
          page.rel = "noopener";
          block.append(page);
        }
        if (name === "stream") {
          block.append(el("div", "detector-note", t("detector.output.stream_note")));
          // SIMPLON counts images nobody picked up and resets the count at
          // each arm: with no receiver, a non-zero count is expected, not a fault.
          if (status?.stream?.dropped) block.append(el("div", "detector-note", t("detector.output.dropped", { count: status.stream.dropped })));
          if (String(status?.stream?.state || "") === "error") {
            const warning = el("div", "detector-warning", t("detector.output.stream_error"));
            warning.append(" ", recoveryButton(t("detector.action.reset_stream"), askResetStream));
            block.append(warning);
          }
        }
        if (name === "monitor") {
          const watch = el("button", "linkish", t("detector.action.watch_live"));
          watch.type = "button";
          watch.addEventListener("click", () => watchLive?.(url, version));
          block.append(watch);
        }
      }
      outputsHost.append(block);
    }
    renderFiles();
    renderState();
  }

  async function setMode(name, value) {
    const descriptor = params[name]?.mode;
    if (!descriptor) return;
    const row = el("div");
    const hint = el("div");
    await sendParam(name, "mode", value, { value: "" }, hint, row);
    if (hint.className.includes("is-error")) log(hint.textContent, "error");
    lastModes = { ...(lastModes || {}), [name]: value };
  }

  // ---------- files ----------
  async function refreshFiles() {
    if (!params.filewriter?.mode) return;
    try {
      const payload = await request(`/detector/files?${query()}`);
      files = Array.isArray(payload?.files) ? payload.files : [];
      filesError = "";
    } catch (err) {
      files = [];
      filesError = err.message || t("simplon.probe.request_failed");
    }
    renderFiles();
  }

  function renderFiles() {
    if (!filesHost) return;
    filesHost.replaceChildren();
    if (!params.filewriter?.mode) return;
    const head = el("div", "detector-group-label", t("detector.files.title"));
    const refresh = el("button", "linkish", t("detector.action.refresh_files"));
    refresh.type = "button";
    refresh.addEventListener("click", () => void refreshFiles());
    head.append(" ", refresh);
    filesHost.append(head);
    if (filesError) {
      filesHost.append(el("p", "detector-warning", t("detector.files.error", { reason: filesError })));
      return;
    }
    if (!files.length) {
      filesHost.append(el("p", "detector-note", t("detector.files.none")));
      return;
    }
    const list = el("ul", "detector-files");
    for (const file of files.slice(-12).reverse()) {
      const li = el("li");
      const link = el("a", "", file.name);
      link.href = `${apiBase}/detector/files/download?url=${encodeURIComponent(url)}&name=${encodeURIComponent(file.name)}`;
      link.download = file.name.split("/").pop();
      link.title = t("detector.action.download");
      li.append(link, el("span", "", formatBytes(file.size)));
      list.append(li);
    }
    filesHost.append(list);
    if (files.length > 12) filesHost.append(el("p", "detector-note", t("detector.files.more", { count: files.length - 12 })));
    const clear = recoveryButton(t("detector.action.delete_files"), askDeleteFiles);
    clear.classList.add("is-danger");
    filesHost.append(clear);
  }

  // ---------- advanced ----------
  // Everything else the detector documents, grouped so it can be found:
  // corrections, readout, geometry and metadata, test images, the rest; then
  // the data interfaces by name, then read-only information. A filter narrows
  // the list as you type.
  const ADVANCED_GROUPS = [
    ["corrections", /correction|mask|auto_sum|virtual_pixel/],
    ["readout", /bit_depth|roi_|binning|compression|pixel_format|counting_mode|extg|nexpi|ntriggers_skipped|trigger_start_delay|sensor_movement|threshold\//],
    ["geometry", /beam_center|distance|orientation|_start$|_increment$|sample_name|source_name|instrument_name|element|flux|wavelength|energy/],
    ["test", /^test_image/],
  ];
  let advancedFilter = "";

  function renderAdvanced() {
    if (!advancedHost) return;
    advancedHost.replaceChildren();
    const filter = el("input", "detector-filter");
    filter.type = "search";
    filter.placeholder = t("detector.advanced.filter");
    filter.setAttribute("aria-label", t("detector.advanced.filter"));
    filter.value = advancedFilter;
    advancedHost.append(filter);

    const shown = new Set(coreKeys().flatMap((group) => group.keys));
    if (shown.has("threshold_energy")) shown.add("threshold/1/energy");
    const buckets = new Map([...ADVANCED_GROUPS.map(([id]) => [id, []]), ["other", []]]);
    const info = [];
    for (const [key, descriptor] of Object.entries(params.detector)) {
      if (shown.has(key)) continue;
      if (!String(descriptor.access_mode || "rw").includes("w")) {
        info.push(key);
        continue;
      }
      const group = ADVANCED_GROUPS.find(([, pattern]) => pattern.test(key));
      buckets.get(group ? group[0] : "other").push(key);
    }
    const groups = [];
    const addGroup = (title, rows) => {
      if (!rows.length) return;
      const box = el("div", "detector-adv-group");
      box.append(el("div", "detector-group-label", title), ...rows);
      advancedHost.append(box);
      groups.push(box);
    };
    for (const [id, keys] of buckets) {
      addGroup(t(`detector.advanced.group.${id}`), keys.sort().map((key) => paramRow("detector", key, params.detector[key])));
    }
    for (const name of ["filewriter", "stream", "monitor"]) {
      const rows = Object.entries(params[name] || {})
        .filter(([key]) => key !== "mode" && !OUTPUT_MAIN[name].includes(key))
        .map(([key, descriptor]) => paramRow(name, key, descriptor));
      addGroup(t(`detector.output.${name}`), rows);
    }
    addGroup(t("detector.advanced.info"), info.sort().map((key) => paramRow("detector", key, params.detector[key])));
    const empty = el("p", "detector-note", t("detector.advanced.no_match"));
    advancedHost.append(empty);
    advancedHost.append(troubleshooting());

    const applyFilter = () => {
      const needle = advancedFilter.trim().toLowerCase();
      let any = false;
      for (const box of groups) {
        let visible = 0;
        box.querySelectorAll(".detector-param").forEach((row) => {
          const match = !needle || (row.dataset.search || "").includes(needle);
          row.hidden = !match;
          if (match) visible += 1;
        });
        box.hidden = visible === 0;
        any = any || visible > 0;
      }
      empty.hidden = any;
    };
    filter.addEventListener("input", () => {
      advancedFilter = filter.value;
      applyFilter();
    });
    applyFilter();
    renderState();
  }

  // The three recovery steps in one place, each with what it is for.
  function troubleshooting() {
    const box = el("div", "detector-adv-group detector-fixes");
    box.append(el("div", "detector-group-label", t("detector.advanced.troubleshooting")));
    const items = [[t("detector.action.reinitialize"), t("detector.fix.initialize"), askInitialize]];
    if (params.stream?.mode) items.push([t("detector.action.reset_stream"), t("detector.fix.stream"), askResetStream]);
    if (params.filewriter?.mode) items.push([t("detector.action.delete_files"), t("detector.fix.files"), askDeleteFiles]);
    for (const [label, text, onClick] of items) {
      const row = el("div", "detector-fix");
      const button = el("button", "btn btn-secondary", label);
      button.type = "button";
      button.addEventListener("click", onClick);
      row.append(button, el("p", "detector-note", text));
      box.append(row);
    }
    return box;
  }

  function bindCommands() {
    commandsHost?.querySelectorAll("[data-detector-command]").forEach((button) => {
      button.addEventListener("click", () => {
        const command = button.dataset.detectorCommand;
        if (command === "refresh") {
          void refreshDescription().then(() => log(t("detector.log.reread")));
        } else if (command === "trigger") {
          void trigger();
        } else {
          void runCommand("detector", command, 130000).then(() => poll());
        }
      });
    });
  }

  // ---------- wiring ----------
  primaryBtn?.addEventListener("click", () => {
    const action = primaryBtn.dataset.action;
    if (action === "initialize") void initialize();
    else if (action === "acquire") startAcquire();
    else if (action === "trigger") void trigger();
  });
  stopBtn?.addEventListener("click", stop);
  connectBtn?.addEventListener("click", () => void connect());
  urlInput?.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      void connect();
    }
  });
  confirmNo?.addEventListener("click", () => {
    confirm.hidden = true;
    confirmAction = null;
  });
  confirmYes?.addEventListener("click", () => {
    confirm.hidden = true;
    const action = confirmAction;
    confirmAction = null;
    void action?.();
  });
  noticeDismiss?.addEventListener("click", () => {
    notice.hidden = true;
  });
  bindCommands();
  if (urlInput && !urlInput.value) urlInput.value = readStored();

  /** Show or hide the whole feature; a hidden tab cannot stay open. */
  function setEnabled(next) {
    enabled = Boolean(next);
    if (tab) tab.hidden = !enabled;
    if (!enabled) {
      stopPolling();
      if (getPanelTab?.() === "detector") setPanelTab?.("view");
    } else if (url) {
      schedulePoll();
    }
  }

  return {
    setEnabled,
    connect,
    poll,
    refreshDescription,
    get params() {
      return params;
    },
    get status() {
      return status;
    },
    _setConnection(nextUrl, nextVersion = "1.8.0") {
      url = nextUrl;
      version = nextVersion;
    },
    _renderAll() {
      renderParams();
      renderOutputs();
      renderAdvanced();
    },
    _setStatus(next) {
      status = next;
      renderState();
      renderOutputs();
    },
    _setParams(next) {
      params = { detector: {}, monitor: {}, filewriter: {}, stream: {}, ...next };
    },
  };
}
