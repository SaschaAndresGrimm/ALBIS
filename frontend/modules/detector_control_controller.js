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
const FOLLOW_KEY = "albis.detectorControl.followLive";
const POLL_ACTIVE_MS = 1000;
const POLL_IDLE_MS = 3000;
const LOG_LIMIT = 12;
// Below this much free detector storage, offer to clear it even if SIMPLON
// does not (yet) flag buffer_free as critical.
const LOW_STORAGE_BYTES = 1024 ** 3;

// The main view: what an acquisition needs, if the detector has it, ordered
// by how often it changes -- the series and its timing from one measurement to
// the next, the energy and thresholds once per experiment. On a wide panel the
// groups sit side by side, each a compact column. A row is a key; `oneOf` takes
// the first key the detector has (X-ray detectors call the energy
// photon_energy, electron-microscopy ones incident_energy); `thresholds` is one
// row per threshold energy, one to four, then the images the detector delivers.
const CORE_GROUPS = [
  { id: "series", rows: ["nimages", "ntrigger", "trigger_mode"] },
  // Frame time first: it sets the rate, and the count time fits inside it.
  { id: "timing", rows: ["frame_time", "count_time"] },
  { id: "energy", rows: [{ oneOf: ["photon_energy", "incident_energy"] }, { thresholds: true }] },
];
// The progress bar runs on the clock, not on the status polls: a poll to a
// real detector takes a variable time, and the bar jumped with it.
const PROGRESS_TICK_MS = 100;
// Snap: one image of its own exposure, whatever the series is set to (a 10 s
// count time made a 10 s snap). Continuous: an alignment view at its own rate
// for a bounded time, inside the one-week limit a detector puts on a series
// ("trigger sequence duration exceeds maximum allowed duration"). Exposure
// and rate are set under the buttons (Options), remembered per browser; the
// longest run is fixed: long enough for any alignment, well inside the week.
const QUICK_DEFAULTS = { snapExposure: 1, continuousRate: 10 };
const CONTINUOUS_HOURS = 10;
const QUICK_LIMITS = {
  snapExposure: { min: 1e-6, max: 3600, unit: "s" },
  continuousRate: { min: 0.01, max: 100000, unit: "Hz" },
};
const QUICK_KEY = "albis.detectorControl.quick";

function readQuick() {
  try {
    const stored = JSON.parse(window.localStorage?.getItem(QUICK_KEY) || "{}");
    const out = { ...QUICK_DEFAULTS };
    for (const [key, { min, max }] of Object.entries(QUICK_LIMITS)) {
      const value = Number(stored?.[key]);
      if (Number.isFinite(value) && value >= min && value <= max) out[key] = value;
    }
    return out;
  } catch {
    return { ...QUICK_DEFAULTS };
  }
}

function writeQuick(values) {
  try {
    window.localStorage?.setItem(QUICK_KEY, JSON.stringify(values));
  } catch {
    // A remembered preference only.
  }
}

// How much smaller compressed images come out: a guesstimate, for the
// pre-flight's storage estimate. bslz4 on detector data: about 4.
const COMPRESSION_FACTOR = { bslz4: 4, lz4: 2 };
// What the main view's settings mean, written from the SIMPLON reference: the
// detector describes a value's type, unit and limits, not what it is for.
// Each "?" also names the SIMPLON key, the bridge to scripting the detector.
const PARAM_HELP = {
  "detector:nimages": "nimages",
  "detector:ntrigger": "ntrigger",
  "detector:trigger_mode": "trigger_mode",
  "detector:frame_time": "frame_time",
  "detector:count_time": "count_time",
  "detector:photon_energy": "photon_energy",
  "detector:incident_energy": "incident_energy",
  "detector:threshold_energy": "threshold_energy",
  "detector:threshold/difference/mode": "difference_mode",
  "filewriter:name_pattern": "name_pattern",
  "filewriter:nimages_per_file": "nimages_per_file",
  "stream:header_detail": "header_detail",
};

/**
 * The thresholds a detector has, one to four: each one's energy, and the images
 * it can deliver. The energies are always in use -- they decide what is
 * counted, and the difference image is calculated from thresholds 1 and 2 --
 * while each mode only decides whether that threshold's images are delivered.
 * With a single threshold there is no choice to offer (switching it off would
 * switch off the images), so its mode stays in Advanced. threshold_energy is
 * threshold/1/energy under another name (SIMPLON reference), preferred for the
 * first.
 */
export function thresholdRows(detectorParams) {
  const energies = [];
  for (let n = 1; n <= 4; n += 1) {
    const energy = n === 1 && detectorParams.threshold_energy ? "threshold_energy" : `threshold/${n}/energy`;
    if (detectorParams[energy]) energies.push({ n, energy });
  }
  const images = [];
  if (energies.length > 1) {
    for (const { n } of energies) {
      if (detectorParams[`threshold/${n}/mode`]) images.push({ key: `threshold/${n}/mode`, n });
    }
    if (detectorParams["threshold/difference/mode"]) images.push({ key: "threshold/difference/mode", n: null });
  }
  return { energies, images };
}

// An on/off setting, whichever way the detector types it.
function isBinary(descriptor) {
  if (descriptor?.value_type === "bool") return true;
  const allowed = descriptor?.allowed_values;
  return Array.isArray(allowed) && allowed.length === 2 && allowed.includes("enabled") && allowed.includes("disabled");
}

function isOn(descriptor) {
  return descriptor?.value_type === "bool" ? Boolean(descriptor.value) : descriptor?.value === "enabled";
}

export function helpKey(subsystem, key) {
  if (subsystem === "detector" && /^threshold\/\d\/energy$/.test(key)) return "detector.help.threshold_energy";
  if (subsystem === "detector" && /^threshold\/\d\/mode$/.test(key)) return "detector.help.threshold_mode";
  const id = PARAM_HELP[`${subsystem}:${key}`];
  return id ? `detector.help.${id}` : "";
}

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
// Enable modes take one image per trigger, exposed for as long as the trigger
// says: the value sent with Trigger (inte) or the length of the external
// signal (exte). The detector refuses them unless images per trigger is 1
// ("number_of_images must be 1 for trigger mode inte"), and refuses raising
// it while one is set; an armed exte detector goes straight to "acquire".
const ENABLE_MODES = new Set(["inte", "exte"]);

// Equal for the detector's purposes: a float may come back rounded
// (0.1 as 0.099999999).
export function sameValue(a, b) {
  if (typeof a === "number" && typeof b === "number") return Math.abs(a - b) <= Math.max(1e-9, Math.abs(b) * 1e-6);
  return a === b;
}

export function isEnableMode(mode) {
  return ENABLE_MODES.has(String(mode || ""));
}
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
const STATE_TEXTS = new Set(["na", "configure", "acquire", "test", "error"]);
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

function readFollow() {
  try {
    return window.localStorage?.getItem(FOLLOW_KEY) !== "0";
  } catch {
    return true;
  }
}

function writeFollow(on) {
  try {
    window.localStorage?.setItem(FOLLOW_KEY, on ? "1" : "0");
  } catch {
    // A remembered preference only.
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

/**
 * Elapsed and total time of a running series, in one fixed format chosen by
 * the total, so the text does not change width as the seconds tick: two
 * decimals under a second, one under a minute, minutes and seconds above.
 */
export function formatProgress(elapsed, total) {
  const e = Math.max(0, Number(elapsed) || 0);
  const t = Math.max(0, Number(total) || 0);
  if (t >= 60) {
    const clock = (v) => `${Math.floor(v / 60)}:${String(Math.floor(v % 60)).padStart(2, "0")}`;
    return { elapsed: clock(e), total: `${clock(t)} min` };
  }
  const digits = t < 1 ? 2 : 1;
  return { elapsed: `${e.toFixed(digits)} s`, total: `${t.toFixed(digits)} s` };
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

/**
 * A page on the detector control unit, `http(s)://<dcu>/<path>`, or "" for an
 * address that is not http(s): the address is typed text, and only an http(s)
 * one may become a link, never `javascript:` and the like.
 */
export function detectorPage(base, path = "") {
  let parsed;
  try {
    parsed = new URL(`${String(base || "").replace(/\/+$/, "")}/${path}`);
  } catch {
    return "";
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return "";
  return /^https?:\/\//i.test(parsed.href) ? parsed.href : "";
}

/** The detector's data page, `http(s)://<dcu>/data/`, or "" for any other address. */
export function detectorDataPage(base) {
  return detectorPage(base, "data/");
}

/** A read-only value with its unit; a length in metres in µm or mm. */
export function readableValue(descriptor) {
  const unit = String(descriptor?.unit || "").toLowerCase();
  const v = descriptor?.value;
  if (typeof v === "number" && ["m", "meter", "meters", "metre", "metres"].includes(unit) && v !== 0 && Math.abs(v) < 1) {
    const mm = Math.abs(v) >= 1e-3;
    return `${+(v * (mm ? 1e3 : 1e6)).toPrecision(6)} ${mm ? "mm" : "µm"}`;
  }
  const symbol = descriptor?.unit ? ` ${unitSymbol(descriptor.unit)}` : "";
  return `${displayValue(descriptor)}${symbol}`;
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
    addressHost,
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
    followToggle,
    sensors,
    notice,
    noticeText,
    noticeDismiss,
    sections,
    paramsHost,
    lockNote,
    seriesSummary,
    outputSummary,
    outputsHost,
    filesHost,
    logHost,
    advancedHost,
    troubleshootingHost,
    commandsHost,
  } = elements;
  const { getPanelTab, setPanelTab, watchLive, refreshInfoTips, openPath } = callbacks;

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
  let followedBefore = false;
  // Internal enable: images triggered since arm, and the exposure being taken.
  let triggered = 0;
  let triggerExposure = 0;
  // The series last taken here, for the result line; Continuous while it runs.
  let lastResult = null;
  let armedPrefix = "";
  // Stop was confirmed for the running series: its result says so, rather
  // than the image count it was set to.
  let stopRequested = false;
  let continuous = false;
  // A quick action from its first temporary change to its last restore. Its
  // own writes pass through states the panel would otherwise react to -- the
  // file writer and stream briefly off, the detector idle between steps -- so
  // the buttons and the pre-flight line hold still until it is done.
  let quickRunning = false;
  // Stop pressed while a quick action was still setting up or arming: it
  // must not go on to take its series.
  let quickCancelled = false;
  let quick = readQuick();
  let progressTimer = null;
  // Internal enable: each Trigger sends its own exposure, chosen here, next to
  // the button, while the detector is armed.
  const exposureBox = primaryBtn ? el("label", "detector-exposure") : null;
  const exposureInput = exposureBox ? el("input") : null;
  const exposureHint = exposureBox ? el("span", "detector-hint is-error") : null;
  if (exposureBox) {
    exposureInput.type = "text";
    exposureInput.inputMode = "decimal";
    exposureInput.autocomplete = "off";
    exposureInput.classList.add("is-number");
    const field = el("span", "detector-field");
    field.append(exposureInput, el("span", "detector-unit", "s"));
    exposureBox.append(el("span", "", t("detector.exposure.label")), field, exposureHint);
    exposureBox.hidden = true;
    primaryBtn.before?.(exposureBox);
    exposureInput.addEventListener("keydown", (event) => {
      if (event.key === "Enter" && primaryBtn.dataset.action === "trigger") {
        event.preventDefault();
        void trigger();
      }
    });
  }

  // Beside Acquire: Snap (one image now) and Continuous (until Stop), one
  // block of equal widths; while anything runs, Stop takes exactly their
  // place, so Acquire never changes size or position. Under the buttons, the
  // pre-flight check before a series and the result after.
  const actionsRow = primaryBtn?.parentElement || null;
  const quickGroup = actionsRow ? el("div", "detector-side") : null;
  const snapBtn = actionsRow ? el("button", "btn btn-secondary detector-quick", t("detector.action.snap")) : null;
  const continuousBtn = actionsRow ? el("button", "btn btn-secondary detector-quick", t("detector.action.continuous")) : null;
  const slot = actionsRow ? el("div", "detector-slot") : null;
  const preflightHost = actionsRow ? el("div", "detector-preflight") : null;
  const resultHost = actionsRow ? el("div", "detector-result") : null;
  if (actionsRow) {
    snapBtn.type = "button";
    continuousBtn.type = "button";
    labelQuickButtons();
    snapBtn.disabled = true;
    continuousBtn.disabled = true;
    quickGroup.append(snapBtn, continuousBtn);
    if (stopBtn) quickGroup.append(stopBtn);
    primaryBtn.after(quickGroup);
    // One slot under the buttons, at least a line high even while a series
    // is armed and nothing shows: a question or notice, the check before a
    // series, its progress while it runs, the result after. Nothing above
    // the buttons comes and goes, so they stay where they are.
    if (confirm) slot.append(confirm);
    if (notice) slot.append(notice);
    slot.append(preflightHost, resultHost);
    if (progress) slot.append(progress);
    actionsRow.after(slot);
    snapBtn.addEventListener("click", () => void snap());
    continuousBtn.addEventListener("click", () => void runContinuous());
  }
  const quickBox = actionsRow ? quickSettings() : null;
  if (quickBox) {
    const followRow = followToggle?.closest?.(".detector-follow");
    if (followRow) quickBox.prepend(followRow);
    slot.after(quickBox);
  }

  // Plain names on the buttons, to keep the row narrow; the timings are in
  // their tooltips and in the settings under the buttons.
  function labelQuickButtons() {
    if (!snapBtn) return;
    const exposure = formatDuration(quick.snapExposure);
    const rate = `${+quick.continuousRate.toPrecision(3)} Hz`;
    snapBtn.textContent = t("detector.action.snap");
    continuousBtn.textContent = t("detector.action.continuous");
    snapBtn.title = t("detector.snap.hint", { exposure });
    continuousBtn.title = t("detector.continuous.hint", { rate, hours: CONTINUOUS_HOURS });
  }

  // Once connected, the address shrinks to a link beside the serial number,
  // with Change to bring the field back: the name already says it connected.
  // "D029661 · 192.168.20.191 ↗ · Change", on the name's line.
  const whereLine = model ? el("span", "detector-where") : null;
  if (whereLine && serial?.before) {
    const side = el("span", "detector-ident-side");
    serial.before(side);
    side.append(serial, whereLine);
  } else if (whereLine) {
    model.parentElement?.after?.(whereLine);
  }

  function showAddress(show) {
    if (addressHost) addressHost.hidden = !show;
    if (whereLine) whereLine.hidden = show;
  }

  function renderWhere() {
    if (!whereLine) return;
    whereLine.replaceChildren();
    const change = el("button", "linkish", t("detector.action.change"));
    change.type = "button";
    change.addEventListener("click", () => {
      showAddress(true);
      urlInput?.focus?.();
    });
    // The address opens the control unit's own web interface, served at its
    // root by every DCU (an EIGER1's: "EIGER Detector Control Unit").
    const shown = url.replace(/^https?:\/\//, "").replace(/\/+$/, "");
    const webUi = detectorPage(url);
    let address;
    if (webUi) {
      address = el("a", "linkish detector-external", `${shown} ↗`);
      address.href = webUi;
      address.target = "_blank";
      address.rel = "noopener";
      address.title = t("detector.where.web_ui");
    } else {
      address = el("span", "", shown);
    }
    whereLine.append(address, " · ", change);
  }

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
    // The question stands in for the check: "Acquire anyway?" repeats it.
    if (preflightHost) preflightHost.hidden = true;
    confirmYes.focus?.();
    // Asked from Data output or Troubleshooting, further down the tab.
    confirm.scrollIntoView?.({ block: "nearest", behavior: "smooth" });
  }

  function closeConfirm() {
    confirm.hidden = true;
    if (preflightHost) preflightHost.hidden = false;
    const action = confirmAction;
    confirmAction = null;
    return action;
  }

  function reason(detail) {
    if (detail && typeof detail === "object") {
      // The detector's own explanation says more than the status code.
      if (detail.detector_message) return t("detector.failure.detector_says", { message: detail.detector_message });
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
      setMessage("");
      renderWhere();
      showAddress(false);
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
    // The detector says which SIMPLON it speaks (an EIGER1: 1.6.0); every
    // request from here on uses that.
    if (payload?.api_version) version = String(payload.api_version);
    params = { detector: {}, monitor: {}, filewriter: {}, stream: {}, ...(payload?.params || {}) };
    if (model) model.textContent = displayValue(params.detector.description) || t("detector.section.detector");
    if (serial) serial.textContent = displayValue(params.detector.detector_number);
    renderParams();
    renderOutputs();
    renderAdvanced();
    renderTroubleshooting();
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

  async function sendCommand(subsystem, command, { quiet = false, value = undefined } = {}) {
    if (!quiet) log(t("detector.log.sent", { command: commandName(subsystem, command) }));
    try {
      return await request("/detector/command", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(value === undefined ? { url, version, subsystem, command } : { url, version, subsystem, command, value }),
      });
    } catch (err) {
      log(t("detector.log.failed", { command: commandName(subsystem, command), reason: err.message }), "error");
      throw err;
    }
  }

  async function runCommand(subsystem, command, timeoutMs = 60000, value = undefined) {
    await sendCommand(subsystem, command, { value });
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

  // Under the sensors, only when something is wrong: high voltage not ready
  // or a failed command, with Re-initialize… next to the explanation. Not
  // while busy, and not in "na" or "error", where the main button already
  // says Initialize. Otherwise it lives under Troubleshooting.
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
    if (!text) return;
    const button = recoveryButton(t("detector.action.reinitialize"), askInitialize);
    const note = el("div", "detector-warning is-caution", text);
    note.append(" ", button);
    recoverHost.append(note);
  }

  function startAcquire() {
    const { issues } = preflight();
    if (issues.length) {
      ask(`${issues.join(" ")} ${t("detector.preflight.ask")}`, t("detector.confirm.acquire_anyway"), t("detector.action.cancel"), () => acquire());
      return;
    }
    void acquire();
  }

  // ---------- quick actions ----------
  // Snap and Continuous change the series settings only for themselves and
  // put them back afterwards, also after Stop or an error. Applied in order,
  // put back in reverse: that keeps the detector's rule that an enable mode
  // needs images per trigger at 1 satisfied at every step.
  async function withTemporarySettings(changes, run) {
    // Recorded before the first change: writing one time can make the
    // detector adjust another (a frame time pulls the count time along), so
    // what to put back is what was there at the start, compared with a
    // tolerance for the detector's own rounding.
    const before = changes.map(([subsystem, key]) => [subsystem, key, params[subsystem]?.[key]?.value]);
    const setOne = async (subsystem, key, value) => {
      await sendParam(subsystem, key, value, { value: "" }, el("div"), el("div"), { quiet: true });
      // Our own switch, not "changed by another program".
      if (key === "mode") lastModes = { ...(lastModes || {}), [subsystem]: value };
      return sameValue(params[subsystem]?.[key]?.value, value);
    };
    try {
      for (const [subsystem, key, value] of changes) {
        if (quickCancelled) break;
        if (!params[subsystem]?.[key] || sameValue(params[subsystem][key].value, value)) continue;
        if (!(await setOne(subsystem, key, value))) {
          log(t("detector.log.failed", { command: paramLabel(key), reason: "" }), "error");
          return;
        }
      }
      if (!quickCancelled) await run();
    } finally {
      let changed = false;
      for (const [subsystem, key, value] of before.reverse()) {
        if (value === undefined || sameValue(params[subsystem]?.[key]?.value, value)) continue;
        changed = true;
        if (!(await setOne(subsystem, key, value))) {
          log(t("detector.log.restore_failed", { label: subsystem === "detector" ? paramLabel(key) : t(`detector.output.${subsystem}`) }), "error");
        }
      }
      if (changed) log(t("detector.log.restored"));
    }
  }

  // The timing for a quick action: frame time first, which the detector
  // follows with the count time, then the count time itself.
  const timingFor = (exposure) => [
    ["detector", "frame_time", exposure],
    ["detector", "count_time", exposure],
  ];

  // One image of SNAP_EXPOSURE_S now, shown live.
  async function snap() {
    quickRunning = true;
    quickCancelled = false;
    renderState();
    log(t("detector.log.snap"));
    try {
      await withTemporarySettings(
        [["detector", "trigger_mode", "ints"], ...timingFor(quick.snapExposure), ["detector", "nimages", 1], ["detector", "ntrigger", 1]],
        () => acquire({ forceFollow: true }),
      );
    } finally {
      quickRunning = false;
      renderState();
      await poll();
    }
  }

  // Images one after another, shown live, until Stop: an alignment view. It
  // saves nothing: the file writer and the stream are off while it runs.
  async function runContinuous() {
    const wanted = Math.max(1, Math.round(quick.continuousRate * CONTINUOUS_HOURS * 3600));
    const most = Math.min(wanted, Number(params.detector.nimages?.max) || wanted);
    continuous = true;
    quickRunning = true;
    quickCancelled = false;
    renderState();
    log(t("detector.log.continuous"));
    try {
      await withTemporarySettings(
        [
          ["detector", "trigger_mode", "ints"],
          // Times before the image count: a long series at the old frame
          // time could pass the detector's one-week limit in between.
          ...timingFor(1 / quick.continuousRate),
          ["detector", "nimages", most],
          ["detector", "ntrigger", 1],
          ["filewriter", "mode", "disabled"],
          ["stream", "mode", "disabled"],
        ],
        () => acquire({ forceFollow: true }),
      );
    } finally {
      continuous = false;
      quickRunning = false;
      renderState();
      await poll();
    }
  }

  // ---------- pre-flight ----------
  // What would make the next series fail or useless, as plain sentences, and
  // how much it writes: images x pixels x bit depth x images per frame, before
  // compression, so an upper bound.
  function preflight() {
    const det = params.detector;
    const issues = [];
    const fwOn = params.filewriter?.mode?.value === "enabled";
    const stOn = params.stream?.mode?.value === "enabled";
    if ((params.filewriter?.mode || params.stream?.mode) && !fwOn && !stOn) issues.push(t("detector.output.nothing_saved"));
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
      const compressing = params.filewriter?.compression_enabled ? isOn(params.filewriter.compression_enabled) : true;
      const kind = String(det.compression?.value || "");
      if (compressing && COMPRESSION_FACTOR[kind]) {
        estimate /= COMPRESSION_FACTOR[kind];
        method = kind;
      }
    }
    const free = status?.filewriter?.buffer_free;
    const qualifier = method ? t("detector.preflight.compressed", { method }) : t("detector.preflight.uncompressed");
    if (estimate !== null && free !== undefined && free !== null && estimate > Number(free)) {
      issues.push(t("detector.preflight.storage", { need: formatBytes(estimate), qualifier, free: formatBytes(free) }));
    }
    const pattern = String(params.filewriter?.name_pattern?.value ?? "");
    if (fwOn && params.filewriter?.name_pattern && !pattern.includes("$id")) issues.push(t("detector.preflight.overwrite"));
    const hv = status?.detector?.["high_voltage/state"];
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
    if (!preflightHost || quickRunning) return;
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
    if (!show || !lastResult) return;
    const { series: id, count, seconds, prefix, stopped } = lastResult;
    const line = el("div", "detector-meta");
    // A stopped series took fewer images than it was set to, and how many
    // only its files tell: say when it stopped instead.
    const parts = stopped
      ? [t("detector.result.stopped", { series: id ?? "-", elapsed: formatDuration(seconds) })]
      : [t("detector.result.series", { series: id ?? "-", count })];
    if (seconds !== null && !stopped) parts.push(formatDuration(seconds));
    const written = prefix ? files.filter((file) => file.name === `${prefix}_master.h5` || file.name.startsWith(`${prefix}_data_`)) : [];
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
        body: JSON.stringify({ url, version, prefix }),
      });
      setMessage("");
      await openPath?.(fetched.path);
      log(t("detector.log.opened", { series: id ?? "-" }));
    } catch (err) {
      setMessage(err.message || t("simplon.probe.request_failed"), "error");
    }
  }

  async function acquire({ forceFollow = false } = {}) {
    acquiring = true;
    lastResult = null;
    try {
      const arm = await runCommand("detector", "arm", 130000);
      if (!arm?.ok) return;
      // Stopped while arming: disarm rather than start the series.
      if (quickRunning && quickCancelled) {
        await sendCommand("detector", "abort");
        return;
      }
      series = arm.result?.["sequence id"] ?? arm.result?.sequence_id ?? null;
      triggered = 0;
      // The files this series writes: the name pattern with $id as its number.
      armedPrefix = params.filewriter?.mode?.value === "enabled"
        ? String(params.filewriter.name_pattern?.value ?? "").replace("$id", String(series ?? ""))
        : "";
      if (forceFollow || followToggle?.checked) await follow();
      const mode = String(params.detector.trigger_mode?.value || "");
      // Internal series starts at once; internal enable waits for the first
      // Trigger, so its exposure can be chosen too.
      if (mode === "ints") await trigger();
      else {
        await poll();
        if (mode === "inte") exposureInput?.focus?.();
      }
    } finally {
      acquiring = false;
      schedulePoll();
    }
  }

  // Show the series as it is taken: switch the viewer to this detector's
  // monitor, switching the monitor on first if needed. Only after a
  // successful arm, so a refused series leaves the open image alone.
  async function follow() {
    if (!params.monitor?.mode) return;
    if (params.monitor.mode.value !== "enabled") await setMode("monitor", "enabled");
    if (params.monitor.mode.value !== "enabled") return;
    watchLive?.(url, version);
    if (!followedBefore) {
      followedBefore = true;
      log(t("detector.log.following"));
    }
  }

  async function trigger() {
    const mode = String(params.detector.trigger_mode?.value || "");
    let value;
    if (mode === "inte" && exposureInput) {
      // The exposure goes with the trigger, checked against the count time's limits.
      const checked = validateInput(params.detector.count_time || { value_type: "float" }, exposureInput.value);
      exposureInput.setAttribute("aria-invalid", String(!checked.ok));
      exposureHint.textContent = checked.ok ? "" : checked.error;
      if (!checked.ok) return;
      value = checked.value;
      triggerExposure = value;
    }
    seriesStarted = Date.now();
    const startedAt = seriesStarted;
    acquiring = true;
    stopRequested = false;
    progressTimer = window.setInterval(() => renderProgress(detectorState()), PROGRESS_TICK_MS);
    let job;
    try {
      job = await runCommand("detector", "trigger", 24 * 3600 * 1000, value);
    } finally {
      window.clearInterval(progressTimer);
      progressTimer = null;
      acquiring = false;
      seriesStarted = 0;
    }
    await poll();
    if (job?.ok && mode === "inte") {
      triggered += 1;
      const total = Number(params.detector.ntrigger?.value || 1);
      log(t("detector.log.image_done", { n: triggered, total, exposure: formatDuration(value) }));
      // The detector ends the series by itself after the last trigger.
      if (detectorState() !== "idle") {
        renderState();
        exposureInput?.focus?.();
        return;
      }
    }
    const seconds = (Date.now() - startedAt) / 1000;
    const stopped = stopRequested;
    stopRequested = false;
    if (job?.ok && !stopped) log(t("detector.log.series_done", { series: series ?? "-" }));
    await refreshFiles();
    if ((job?.ok || stopped) && !continuous) {
      const det = params.detector;
      lastResult = {
        series,
        count: Number(det.nimages?.value || 1) * Number(det.ntrigger?.value || 1),
        seconds: mode === "ints" || stopped ? seconds : null,
        prefix: armedPrefix,
        stopped,
      };
      renderState();
    }
  }

  function stop() {
    // A quick action saves nothing Stop could lose: no question. Before it
    // arms, the flag alone stops it; once armed, the detector is told.
    if (quickRunning) {
      quickCancelled = true;
      if (acquiring) void sendCommand("detector", "abort").finally(() => poll());
      return;
    }
    ask(t("detector.confirm.abort"), t("detector.action.abort"), t("detector.action.keep_running"), async () => {
      stopRequested = Boolean(seriesStarted);
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
    // Between a quick action's steps the detector is idle with the action's
    // temporary settings: the line keeps what it said.
    if (stateText && !(quickRunning && value === "idle")) {
      let text;
      if (value === "initialize" || (job?.running && job.command === "initialize")) {
        const elapsed = Math.max(0, (Date.now() / 1000) - (job?.started || Date.now() / 1000));
        text = t("detector.state_text.initialize", { elapsed: formatDuration(elapsed) });
      } else if (value === "ready" && triggerMode === "inte") {
        text = t("detector.state_text.ready_enable", { n: triggered + 1, total: Number(params.detector.ntrigger?.value || 1) });
      } else if (value === "ready") {
        text = t(String(triggerMode || "").startsWith("int") ? "detector.state_text.ready_internal" : "detector.state_text.ready_external");
      } else if (value === "acquire" && String(triggerMode || "").startsWith("ext") && !(job?.running && job.command === "trigger")) {
        // An armed detector in an external mode waits in "acquire" for its signals.
        text = triggerMode === "exte"
          ? t("detector.state_text.waiting_enable", { total: Number(params.detector.ntrigger?.value || 1) })
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
      if (show && exposureBox.hidden && !exposureInput.value) exposureInput.value = displayValue(params.detector.count_time);
      exposureBox.hidden = !show;
      exposureInput.disabled = Boolean(job?.running);
    }
    if (primaryBtn) {
      primaryBtn.disabled = !action || quickRunning || Boolean(job?.running && job.command !== "trigger");
      // While a quick action runs, the big button names it, from its first
      // temporary setting to its last restore.
      if (quickRunning) primaryBtn.textContent = t(continuous ? "detector.action.continuous" : "detector.action.snap");
      else primaryBtn.textContent = action ? t(`detector.action.${action}`) : statePill?.textContent || "";
      primaryBtn.dataset.action = quickRunning ? "" : action || "";
    }
    const running = quickRunning || value === "acquire" || value === "ready" || value === "configure" || Boolean(job?.running && job.command === "trigger");
    if (stopBtn) stopBtn.hidden = !running;
    quickGroup?.classList.toggle("is-running", Boolean(stopBtn) && running);
    const idle = action === "acquire" && !acquiring && !quickRunning && !job?.running;
    // Shown, disabled, whenever they cannot run (before Initialize too): the
    // row keeps its shape.
    if (snapBtn) snapBtn.disabled = !idle;
    if (continuousBtn) continuousBtn.disabled = !idle || !params.detector.nimages;
    renderPreflight(idle);
    renderResult(idle || value === "ready");
    renderProgress(value);
    renderSensors();
    renderRecovery(value);
    const locked = isBusy() || acquiring || quickRunning;
    if (lockNote) lockNote.hidden = !locked;
    quickBox?.querySelectorAll("input[id^='detector-quick-']").forEach((input) => {
      input.disabled = quickRunning;
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
    const free = status?.filewriter?.buffer_free;
    const known = params.filewriter?.mode && free !== undefined && free !== null;
    outputSummary.textContent = known ? t("detector.output.free", { free: formatBytes(free) }) : "";
    outputSummary.hidden = !known;
  }

  // "20 images · 20 s", or "" when the detector does not say.
  function seriesText() {
    const det = params.detector;
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
    const running = seriesStarted && (value === "acquire" || acquiring || status?.command?.running);
    progress.hidden = !running;
    if (!running) return;
    const det = params.detector;
    if (continuous) {
      const elapsed = (Date.now() - seriesStarted) / 1000;
      if (progressBar) progressBar.style.width = "100%";
      if (progressLeft) progressLeft.textContent = t("detector.action.continuous");
      if (progressRight) progressRight.textContent = t("detector.progress.continuous", { elapsed: formatProgress(elapsed, elapsed).elapsed });
      return;
    }
    const total = det.trigger_mode?.value === "inte"
      ? triggerExposure
      : Number(det.nimages?.value || 1) * Number(det.ntrigger?.value || 1) * Number(det.frame_time?.value || 0);
    const elapsed = (Date.now() - seriesStarted) / 1000;
    const fraction = total > 0 ? Math.min(1, elapsed / total) : 0;
    if (progressBar) progressBar.style.width = `${(fraction * 100).toFixed(1)}%`;
    if (progressLeft) progressLeft.textContent = series !== null ? t("detector.progress.series", { series }) : "";
    if (progressRight) progressRight.textContent = t("detector.progress.elapsed", formatProgress(elapsed, total));
  }

  function renderSensors() {
    if (!sensors) return;
    sensors.replaceChildren();
    const det = status?.detector || {};
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

  // ---------- rendering: parameters ----------
  function labelFor(subsystem, key) {
    // With several thresholds, the first one is "Threshold 1", not "Threshold".
    if (subsystem === "detector" && key === "threshold_energy" && params.detector["threshold/2/energy"]) {
      return t("detector.param.threshold_n", { n: 1 });
    }
    // ...and a lone threshold is "Threshold", whichever name it goes by.
    if (subsystem === "detector" && key === "threshold/1/energy" && !params.detector["threshold/2/energy"]) {
      return t("detector.param.threshold_energy");
    }
    return paramLabel(key);
  }

  function infoTip(infoKey, simplonPaths, extra = "") {
    const button = el("button", "info-tip");
    button.type = "button";
    button.dataset.infoKey = infoKey;
    const lines = [].concat(simplonPaths).map((path) => `SIMPLON: ${path}`);
    if (extra) lines.push(extra);
    button.dataset.infoDetail = lines.join("\n");
    button.setAttribute("aria-label", t("info.button_label"));
    return button;
  }

  // A setting with an explanation shows its name and a "?" that explains it
  // and names its SIMPLON key; the others, all in Advanced and mostly without
  // a translated name, show the key itself.
  function paramRow(subsystem, key, descriptor, options = {}) {
    const row = buildParamRow(subsystem, key, descriptor, options);
    return options.inline ? row : tipRight(row);
  }

  // In the main view the "?" has a column of its own right of the field: one
  // aligned column down the section, whatever the length of the names. Rows
  // without one keep the column, empty, so the fields line up all the same.
  // Advanced keeps it beside the name: its rows show SIMPLON keys, mostly
  // without a "?".
  function tipRight(row) {
    const value = row.querySelector(":scope > .detector-field, :scope > .detector-readonly");
    if (!value) return row;
    const cell = el("div", "detector-tip");
    const tip = row.querySelector(".detector-param-name .info-tip");
    if (tip) cell.append(tip);
    value.after(cell);
    row.classList.add("has-tip");
    return row;
  }

  function buildParamRow(subsystem, key, descriptor, { help: helpOverride = "", alsoKeys = [], inline = false } = {}) {
    const row = el("div", "detector-param");
    row.dataset.key = `${subsystem}:${key}`;
    const id = `detector-p-${subsystem}-${key.replace(/[^a-z0-9]/gi, "_")}`;
    const name = el("div", "detector-param-name");
    const labelText = labelFor(subsystem, key);
    const label = el("label", "", labelText);
    label.htmlFor = id;
    const help = helpOverride || helpKey(subsystem, key);
    if (help) {
      const line = el("span", "detector-param-label");
      // The range is in the "?" too, since the row shows it only while editing.
      const range = rangeText(descriptor);
      const paths = [key, ...alsoKeys].map((name) => `${subsystem}/config/${name}`);
      line.append(label, infoTip(help, paths, range ? t("detector.help.range", { range }) : ""));
      name.append(line);
    } else {
      name.append(label);
      // The SIMPLON key under a translated name; once is enough when they match.
      if (labelText !== key) name.append(el("code", "", key));
    }
    row.dataset.search = `${labelText} ${key}`.toLowerCase();
    row.append(name);
    const readonly = !String(descriptor.access_mode || "rw").includes("w");
    if (readonly) {
      const value = el("div", "detector-readonly", readableValue(descriptor));
      value.id = id;
      row.append(value);
      return row;
    }
    const field = el("div", "detector-field");
    if (isBinary(descriptor)) return switchRow(subsystem, key, descriptor, row, field, id);
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
      // A value outside the list (an empty test image mode) is still shown,
      // rather than a blank dropdown.
      const values = allowed.map(String).includes(String(descriptor.value)) ? allowed : [descriptor.value, ...allowed];
      for (const value of values) {
        const option = el("option", "", value === "" || value === null ? t("detector.value.none") : optionLabel(key, value));
        option.value = String(value ?? "");
        input.append(option);
      }
      input.value = String(descriptor.value ?? "");
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
      if (descriptor.value_type === "string" && !inline) {
        // Free text (a name pattern) needs the whole row; in Advanced it
        // shares the line, as the other settings there do.
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

  // On/off as a switch: one click, and the same control as the data outputs.
  function switchRow(subsystem, key, descriptor, row, field, id) {
    const toggle = el("label", "detector-switch");
    const box = el("input");
    box.type = "checkbox";
    box.id = id;
    box.checked = isOn(descriptor);
    box.dataset.paramInput = "";
    toggle.append(box, el("span"));
    field.classList.add("is-switch");
    field.append(toggle);
    const hint = el("div", "detector-hint", "");
    hint.dataset.range = "";
    row.append(field, hint);
    box.addEventListener("change", async () => {
      const value = descriptor.value_type === "bool" ? box.checked : box.checked ? "enabled" : "disabled";
      await sendParam(subsystem, key, value, { value: "" }, hint, row);
      // Refused: the switch shows what the detector has.
      box.checked = isOn(params[subsystem]?.[key]);
    });
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
    if (subsystem === "detector" && key === "trigger_mode" && isEnableMode(checked.value) && params.detector.nimages && Number(params.detector.nimages.value) !== 1) {
      // The detector refuses an enable mode otherwise.
      await sendParam("detector", "nimages", 1, { value: "" }, hint, row, { quiet: true });
      if (Number(params.detector.nimages?.value) !== 1) return;
      log(t("detector.log.enable_one_image"));
    }
    if (subsystem === "filewriter" && key === "name_pattern" && !String(checked.value).includes("$id")) {
      ask(t("detector.confirm.no_id"), t("detector.confirm.use_anyway"), t("detector.action.cancel"), () => sendParam(subsystem, key, checked.value, input, hint, row));
      input.value = displayValue(descriptor);
      return;
    }
    await sendParam(subsystem, key, checked.value, input, hint, row);
  }

  async function sendParam(subsystem, key, value, input, hint, row, { quiet = false } = {}) {
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
      const shown = isMode || isBinary(params[subsystem][key])
        ? t(isOn(params[subsystem][key]) || value === "enabled" || value === true ? "detector.value.on" : "detector.value.off")
        : optionLabel(key, displayValue(params[subsystem][key]));
      if (!quiet) log(t("detector.log.set", { label, value: shown }));
      if (isMode && status?.[subsystem]) {
        // The status poll would say so only on its next round; until then the
        // row must not contradict the switch just flipped.
        status[subsystem].mode = value;
        status[subsystem].state =
          value !== "enabled" ? "disabled" : subsystem === "monitor" ? "normal" : "ready";
      }
      // A quick action's temporary writes are its own business, not news.
      if (!quiet) {
        for (const name of moved) {
          log(t("detector.log.adjusted", { label: labelFor(subsystem, name), value: displayValue(params[subsystem][name]) }));
        }
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

  // The main view's rows for this detector: the groups, each with its rows
  // (a key, a threshold energy, or the images row), and every key they show.
  function coreLayout() {
    const det = params.detector;
    const groups = [];
    const keys = [];
    // In an enable mode the trigger sets the exposure: internal enable keeps
    // the count time as the default exposure, external enable needs neither
    // time. What does not apply moves to Advanced, still within reach.
    const mode = String(det.trigger_mode?.value || "");
    const unused = mode === "inte" ? new Set(["frame_time"]) : mode === "exte" ? new Set(["frame_time", "count_time"]) : new Set();
    const notes = { inte: "detector.enable.count_time_note", exte: "detector.enable.signal_note" };
    for (const group of CORE_GROUPS) {
      const rows = [];
      let groupImages = null;
      for (const entry of group.rows) {
        if (typeof entry === "string") {
          if (det[entry] && !unused.has(entry)) rows.push({ keys: [entry] });
        } else if (entry.oneOf) {
          const found = entry.oneOf.find((key) => det[key]);
          if (found) rows.push({ keys: [found] });
        } else if (entry.thresholds) {
          // Each threshold's energy, with the switch for its images beside it
          // when there is a choice; then the difference image on its own row.
          const { energies, images } = thresholdRows(det);
          const switchFor = (n) => images.find((image) => image.n === n)?.key || null;
          for (const { n, energy } of energies) {
            const mode = switchFor(n);
            rows.push({ keys: mode ? [energy, mode] : [energy], imageSwitch: mode });
          }
          const difference = images.find((image) => image.n === null);
          if (difference) rows.push({ keys: [difference.key], imageSwitch: difference.key, switchOnly: true });
          if (images.length) groupImages = images;
        }
      }
      const note = group.id === "timing" ? notes[mode] : "";
      if (rows.length || note) groups.push({ id: group.id, rows, note, images: groupImages });
      for (const row of rows) keys.push(...row.keys);
    }
    return { groups, keys };
  }

  function coreKeys() {
    return coreLayout().keys;
  }

  // The switch deciding whether a threshold's images (or the difference
  // image) are delivered. The energies stay in use either way: they decide
  // what is counted, and the difference image is calculated from them.
  function imageSwitch(key, row) {
    const descriptor = params.detector[key];
    const cell = el("div", "detector-image-switch");
    const toggle = el("label", "detector-switch");
    const box = el("input");
    box.type = "checkbox";
    box.checked = isOn(descriptor);
    box.dataset.paramInput = "";
    box.dataset.imageKey = key;
    if (!String(descriptor.access_mode || "rw").includes("w")) box.dataset.readonly = "true";
    box.setAttribute("aria-label", paramLabel(key));
    // No column heading: what the switch does is said on hover, and by the
    // tab's help.
    toggle.title = t("detector.help.images");
    toggle.append(box, el("span"));
    cell.append(toggle);
    box.addEventListener("change", async () => {
      const hint = row.querySelector(".detector-hint") || el("div");
      await sendParam("detector", key, box.checked ? "enabled" : "disabled", { value: "" }, hint, row);
      // Refused: the switch shows what the detector has.
      box.checked = isOn(params.detector[key]);
    });
    return cell;
  }

  // A row of the energy group: name, images switch, energy. The switch has a
  // column of its own left of the field, so every field in the tab keeps the
  // same width and the switches line up, the difference image's included.
  function switchedRow(row) {
    const key = row.keys[0];
    let line;
    if (row.switchOnly) {
      // The difference image: a name and a switch, no value of its own here.
      // "Difference", short enough for one line beside the switch when the
      // groups sit side by side; the switch and the log say "Difference image".
      line = el("div", "detector-param");
      line.dataset.key = `detector:${key}`;
      const name = el("div", "detector-param-name");
      const label = el("span", "detector-param-label");
      label.append(el("label", "", t("detector.param.difference_short")), infoTip("detector.help.difference_mode", `detector/config/${key}`));
      name.append(label);
      line.append(name, el("div", "detector-field is-empty"), el("div", "detector-hint"));
      tipRight(line);
    } else {
      line = paramRow("detector", key, params.detector[key], { alsoKeys: row.keys.slice(1) });
    }
    const field = line.querySelector(".detector-field");
    if (row.imageSwitch && field) {
      line.classList.add("has-switch");
      field.before(imageSwitch(row.imageSwitch, line));
    }
    return line;
  }

  // How fast a series runs, from frame and count time: "100 Hz · 0.1 µs
  // between images". Makes plain how the two times relate.
  function timingSummary(det) {
    const frame = Number(det.frame_time?.value);
    if (!(frame > 0)) return "";
    const hz = 1 / frame;
    const rate = hz >= 1 ? `${+hz.toPrecision(3)} Hz` : t("detector.timing.every", { interval: formatDuration(frame) });
    const gap = frame - Number(det.count_time?.value);
    return det.count_time && gap > 0 ? t("detector.timing.summary", { rate, gap: formatDuration(gap) }) : rate;
  }

  // Images per trigger in an enable mode: one, fixed by the mode.
  function oneImageRow() {
    const row = paramRow("detector", "nimages", { ...params.detector.nimages, access_mode: "r" });
    const value = row.querySelector(".detector-readonly");
    if (value) value.textContent = t("detector.enable.one_per_trigger");
    return row;
  }

  function renderParams() {
    if (!paramsHost) return;
    paramsHost.replaceChildren();
    const columns = el("div", "detector-groups");
    for (const group of coreLayout().groups) {
      const column = el("div", "detector-group");
      column.dataset.group = group.id;
      const heading = el("div", "detector-group-label", t(`detector.group.${group.id}`));
      column.append(heading);
      if (group.images) column.classList.add("has-image-switches");
      for (const row of group.rows) {
        if (group.images) column.append(switchedRow(row));
        else if (row.keys[0] === "nimages" && isEnableMode(params.detector.trigger_mode?.value)) column.append(oneImageRow());
        else column.append(paramRow("detector", row.keys[0], params.detector[row.keys[0]]));
      }
      if (group.images && !group.images.some((image) => isOn(params.detector[image.key]))) {
        column.append(el("div", "detector-warning", t("detector.images.none")));
      }
      if (group.note) column.append(el("p", "detector-note detector-group-note", t(group.note)));
      if (group.id === "timing" && !group.note) {
        const rate = timingSummary(params.detector);
        if (rate) column.append(el("p", "detector-note detector-group-note", rate));
      }
      columns.append(column);
    }
    paramsHost.append(columns);
    refreshInfoTips?.();
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
      const titleLine = el("span", "detector-param-label");
      titleLine.append(el("strong", "", t(`detector.output.${name}`)), infoTip(`detector.help.output.${name}`, `${name}/config/mode`));
      title.append(titleLine);
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
        if (name === "filewriter") block.append(...fileWriterNotes());
        if (name === "stream") {
          // SIMPLON counts images nobody picked up and resets the count at
          // each arm: with no receiver, a non-zero count is expected, not a
          // fault -- the short line says what, its "?" why.
          if (status?.stream?.dropped) {
            const dropped = el("div", "detector-meta", t("detector.output.dropped", { count: status.stream.dropped }));
            dropped.append(" ", infoTip("detector.help.dropped", "stream/status/dropped"));
            block.append(dropped);
          }
          if (String(status?.stream?.state || "") === "error") {
            const warning = el("div", "detector-warning", t("detector.output.stream_error"));
            warning.append(" ", recoveryButton(t("detector.action.reset_stream"), askResetStream));
            block.append(warning);
          }
        }
      }
      if (name === "monitor" && mode.value === "enabled") {
        // In the header line: the monitor has no settings of its own here.
        const watch = el("button", "linkish", t("detector.action.watch_live"));
        watch.type = "button";
        watch.addEventListener("click", () => watchLive?.(url, version));
        dot.before(watch);
        head.classList.add("has-action");
      }
      outputsHost.append(block);
    }
    renderFiles();
    refreshInfoTips?.();
    renderState();
  }

  // The file writer's facts on one line -- the next file, the free storage,
  // the detector's data page -- and a warning of its own when one is needed.
  function fileWriterNotes() {
    const out = [];
    const parts = [];
    const pattern = String(params.filewriter.name_pattern?.value ?? "");
    if (params.filewriter.name_pattern && !pattern.includes("$id")) {
      out.push(el("div", "detector-warning", t("detector.output.no_id")));
    } else if (params.filewriter.name_pattern) {
      // The number is known only once a series was armed here; SIMPLON
      // numbers them in turn. Until then $id stands in, as the "?" explains.
      const known = series !== null && Number.isFinite(Number(series));
      const next = known ? pattern.replace("$id", String(Number(series) + 1)) : pattern;
      parts.push(el("span", "", t("detector.output.next_file", { name: `${next}_master.h5` })));
    }
    const free = status?.filewriter?.buffer_free;
    if (free !== undefined && free !== null) parts.push(el("span", "", t("detector.output.free", { free: formatBytes(free) })));
    // The DCU serves the files it wrote at /data/ (SIMPLON reference), which
    // is also where its own web interface lists them. The address is typed
    // text: only an http(s) one becomes a link, never `javascript:` and the like.
    const dataPage = detectorDataPage(url);
    if (dataPage) {
      const page = el("a", "linkish detector-external", t("detector.output.data_page_short"));
      page.href = dataPage;
      page.target = "_blank";
      page.rel = "noopener";
      page.title = t("detector.output.data_page");
      parts.push(page);
    }
    const line = el("div", "detector-meta");
    parts.forEach((part, index) => line.append(...(index ? [" · ", part] : [part])));
    out.unshift(line);
    if (storageLow()) {
      const warning = el("div", "detector-warning", t("detector.output.storage_low"));
      warning.append(" ", recoveryButton(t("detector.action.delete_files"), askDeleteFiles));
      out.push(warning);
    }
    return out;
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
    const refresh = el("button", "linkish", t("detector.action.refresh_files"));
    refresh.type = "button";
    refresh.addEventListener("click", () => void refreshFiles());
    if (!filesError && !files.length) {
      // Nothing to list: heading and "none" on one line.
      const line = el("div", "detector-meta detector-files-line", t("detector.files.none_inline"));
      line.append(" · ", refresh);
      filesHost.append(line);
      return;
    }
    const head = el("div", "detector-group-label", t("detector.files.title"));
    head.append(" ", refresh);
    filesHost.append(head);
    if (filesError) {
      filesHost.append(el("p", "detector-warning", t("detector.files.error", { reason: filesError })));
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

    const shown = new Set(coreKeys());
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
      return box;
    };
    for (const [id, keys] of buckets) {
      addGroup(t(`detector.advanced.group.${id}`), keys.sort().map((key) => paramRow("detector", key, params.detector[key], { inline: true })));
    }
    for (const name of ["filewriter", "stream", "monitor"]) {
      const rows = Object.entries(params[name] || {})
        .filter(([key]) => key !== "mode" && !OUTPUT_MAIN[name].includes(key))
        .map(([key, descriptor]) => paramRow(name, key, descriptor, { inline: true }));
      addGroup(t(`detector.output.${name}`), rows);
    }
    // Reference, not settings: a dense table, two columns when there is room.
    addGroup(t("detector.advanced.info"), info.sort().map((key) => paramRow("detector", key, params.detector[key], { inline: true })))?.classList.add("is-info");
    const empty = el("p", "detector-note", t("detector.advanced.no_match"));
    advancedHost.append(empty);

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
    refreshInfoTips?.();
    renderState();
  }

  // One line under the buttons: Live view, and the timings of Snap and
  // Continuous right beside it, labelled with the buttons' own names. ALBIS
  // settings, not the detector's, remembered per browser.
  function quickSettings() {
    const box = el("div", "detector-quick-settings");
    // The two timings wrap together, under Live view, when the panel is narrow.
    const fields = el("div", "detector-quick-fields");
    box.append(fields);
    for (const key of Object.keys(QUICK_DEFAULTS)) {
      const { min, max, unit } = QUICK_LIMITS[key];
      const id = `detector-quick-${key}`;
      const row = el("div", "detector-quick-field");
      const label = el("label", "", t(key === "snapExposure" ? "detector.action.snap" : "detector.action.continuous"));
      label.htmlFor = id;
      const field = el("div", "detector-field");
      const input = el("input", "is-number");
      input.type = "text";
      input.inputMode = "decimal";
      input.id = id;
      input.value = String(+quick[key].toPrecision(6));
      // "Snap exposure", "Continuous rate", and the range on hover.
      input.setAttribute("aria-label", t(`detector.quick.${key}`));
      input.title = `${t(`detector.quick.${key}`)}: ${rangeText({ value_type: "float", min, max, unit: unit === "s" ? "s" : "" })}`;
      field.append(input, el("span", "detector-unit", unit));
      row.append(label, field);
      input.addEventListener("change", () => {
        const value = Number(String(input.value).replace(",", "."));
        const ok = Number.isFinite(value) && value >= min && value <= max;
        input.setAttribute("aria-invalid", String(!ok));
        if (!ok) return;
        quick = { ...quick, [key]: value };
        writeQuick(quick);
        labelQuickButtons();
      });
      fields.append(row);
    }
    return box;
  }

  // The three recovery steps in their own section, each with what it is for.
  function renderTroubleshooting() {
    if (!troubleshootingHost) return;
    troubleshootingHost.replaceChildren();
    const box = el("div", "detector-fixes");
    const items = [[t("detector.action.reinitialize"), t("detector.fix.initialize"), askInitialize]];
    if (params.stream?.mode) items.push([t("detector.action.reset_stream"), t("detector.fix.stream"), askResetStream]);
    if (params.filewriter?.mode) items.push([t("detector.action.delete_files"), t("detector.fix.files"), askDeleteFiles]);
    for (const [label, text, onClick] of items) {
      const row = el("div", "detector-fix");
      const button = el("button", "btn btn-secondary", label);
      button.type = "button";
      if (onClick === askDeleteFiles) button.classList.add("is-danger");
      button.addEventListener("click", onClick);
      row.append(button, el("p", "detector-note", text));
      box.append(row);
    }
    troubleshootingHost.append(box);
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
  if (followToggle) {
    followToggle.checked = readFollow();
    followToggle.addEventListener("change", () => writeFollow(followToggle.checked));
  }
  connectBtn?.addEventListener("click", () => void connect());
  urlInput?.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      void connect();
    }
  });
  confirmNo?.addEventListener("click", () => {
    closeConfirm();
  });
  confirmYes?.addEventListener("click", () => {
    const action = closeConfirm();
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
      renderTroubleshooting();
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
