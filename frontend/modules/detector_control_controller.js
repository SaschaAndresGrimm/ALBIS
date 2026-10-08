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

// The main view: what an acquisition needs, in this order, if the detector has
// it. A tuple lists alternatives, the first present one wins: X-ray detectors
// call the energy photon_energy, electron-microscopy ones incident_energy.
const CORE_GROUPS = [
  { id: "energy", keys: [["photon_energy", "incident_energy"], "threshold_energy", "threshold/2/energy", "threshold/3/energy", "threshold/4/energy"] },
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

/** The value as the field shows it: numbers to a sensible precision. */
export function displayValue(descriptor) {
  const v = descriptor?.value;
  if (typeof v === "number") return String(+v.toPrecision(8));
  if (v === null || v === undefined) return "";
  return String(v);
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
  return LABELLED_PARAMS.has(key) ? t(`detector.param.${key}`) : key;
}

function optionLabel(key, value) {
  if (key === "trigger_mode" && TRIGGER_MODE_KEYS.has(value)) return t(`detector.trigger_mode.${value}`);
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
  async function sendCommand(subsystem, command, { quiet = false } = {}) {
    if (!quiet) log(t("detector.log.sent", { command }));
    try {
      return await request("/detector/command", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url, version, subsystem, command }),
      });
    } catch (err) {
      log(t("detector.log.failed", { command, reason: err.message }), "error");
      throw err;
    }
  }

  async function runCommand(subsystem, command, timeoutMs = 60000) {
    await sendCommand(subsystem, command);
    schedulePoll();
    const job = await waitForCommand(command, timeoutMs);
    if (!job) return null;
    if (job.ok) log(t("detector.log.done", { command }));
    else log(t("detector.log.failed", { command, reason: reason(job.error) }), "error");
    return job;
  }

  async function initialize() {
    renderState();
    const job = await runCommand("detector", "initialize", 260000);
    if (job?.ok) await refreshDescription();
    await poll();
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
  function paramRow(subsystem, key, descriptor) {
    const row = el("div", "detector-param");
    row.dataset.key = `${subsystem}:${key}`;
    const id = `detector-p-${subsystem}-${key.replace(/[^a-z0-9]/gi, "_")}`;
    const name = el("div", "detector-param-name");
    const label = el("label", "", paramLabel(key));
    label.htmlFor = id;
    name.append(label, el("code", "", key));
    row.append(name);
    const readonly = !String(descriptor.access_mode || "rw").includes("w");
    if (readonly) {
      const unit = descriptor.unit ? ` ${descriptor.unit}` : "";
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
      if (descriptor.unit) field.append(el("span", "detector-unit", descriptor.unit));
      if (descriptor.value_type !== "string") input.classList.add("is-number");
    }
    input.id = id;
    input.dataset.paramInput = "";
    field.prepend(input);
    row.append(field);
    const hint = el("div", "detector-hint", rangeText(descriptor));
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
      for (const [name, descriptor] of Object.entries(result?.params || {})) {
        params[subsystem][name] = descriptor;
      }
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
      for (const name of Object.keys(result?.params || {})) {
        if (name === key) continue;
        log(t("detector.log.adjusted", { label: paramLabel(name), value: displayValue(params[subsystem][name]) }));
      }
      rerenderAfterWrite(subsystem, key, Object.keys(result?.params || {}));
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
          const next = String(params.filewriter.name_pattern.value || "").replace("$id", String((Number(series) || 0) + 1));
          block.append(el("div", "detector-note", t("detector.output.next_file", { name: `${next}_master.h5` })));
        }
        if (name === "filewriter" && status?.filewriter?.buffer_free !== undefined && status?.filewriter?.buffer_free !== null) {
          block.append(el("div", "detector-note", t("detector.output.storage_free", { free: formatBytes(status.filewriter.buffer_free) })));
        }
        if (name === "stream") {
          block.append(el("div", "detector-note", t("detector.output.stream_note")));
          if (status?.stream?.dropped) block.append(el("div", "detector-note", t("detector.output.dropped", { count: status.stream.dropped })));
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
    } catch {
      files = [];
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
    const clear = el("button", "linkish is-danger", t("detector.action.delete_files"));
    clear.type = "button";
    clear.addEventListener("click", () => {
      ask(t("detector.confirm.delete_files", { count: files.length }), t("detector.confirm.delete"), t("detector.action.cancel"), async () => {
        const job = await runCommand("filewriter", "clear", 60000);
        if (job?.ok) log(t("detector.log.files_deleted"));
        await refreshFiles();
      });
    });
    filesHost.append(clear);
  }

  // ---------- advanced ----------
  function renderAdvanced() {
    if (!advancedHost) return;
    advancedHost.replaceChildren();
    const shown = new Set(coreKeys().flatMap((group) => group.keys));
    const writable = [];
    const info = [];
    for (const [key, descriptor] of Object.entries(params.detector)) {
      if (shown.has(key)) continue;
      (String(descriptor.access_mode || "rw").includes("w") ? writable : info).push(key);
    }
    const section = (title, rows) => {
      if (!rows.length) return;
      advancedHost.append(el("div", "detector-group-label", title));
      rows.forEach((row) => advancedHost.append(row));
    };
    section(t("detector.advanced.detector_settings"), writable.sort().map((key) => paramRow("detector", key, params.detector[key])));
    const outputRows = [];
    for (const name of ["filewriter", "stream", "monitor"]) {
      for (const [key, descriptor] of Object.entries(params[name] || {})) {
        if (key === "mode" || OUTPUT_MAIN[name].includes(key)) continue;
        outputRows.push(paramRow(name, key, descriptor));
      }
    }
    section(t("detector.advanced.output_settings"), outputRows);
    section(t("detector.advanced.info"), info.sort().map((key) => paramRow("detector", key, params.detector[key])));
    renderState();
  }

  function bindCommands() {
    commandsHost?.querySelectorAll("[data-detector-command]").forEach((button) => {
      button.addEventListener("click", () => {
        const command = button.dataset.detectorCommand;
        if (command === "initialize") {
          ask(t("detector.confirm.initialize"), t("detector.action.initialize"), t("detector.action.cancel"), initialize);
        } else if (command === "refresh") {
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
