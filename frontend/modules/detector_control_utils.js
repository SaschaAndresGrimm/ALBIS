/**
 * Detector tab (beta): the pure pieces -- what a SIMPLON value means, how it
 * is shown and checked, the main view's help keys and the quick actions'
 * limits. No DOM, storage or network; detector_control_controller.js and its
 * parts build the panel from them.
 */

import { t } from "./i18n.js";

// Snap: one image of its own exposure, whatever the series is set to (a 10 s
// count time made a 10 s snap). Continuous: an alignment view at its own rate
// for a bounded time, inside the one-week limit a detector puts on a series
// ("trigger sequence duration exceeds maximum allowed duration"). Exposure
// and rate are set under the buttons (Options), remembered per browser; the
// longest run is fixed: long enough for any alignment, well inside the week.
export const QUICK_DEFAULTS = { snapExposure: 1, continuousRate: 10 };
export const CONTINUOUS_HOURS = 10;
export const QUICK_LIMITS = {
  snapExposure: { min: 1e-6, max: 3600, unit: "s" },
  continuousRate: { min: 0.01, max: 100000, unit: "Hz" },
};

// How much smaller compressed images come out: a guesstimate, for the
// pre-flight's storage estimate. bslz4 on detector data: about 4.
export const COMPRESSION_FACTOR = { bslz4: 4, lz4: 2 };
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
  // In Data output's More settings.
  "detector:compression": "compression",
  "filewriter:compression_enabled": "compression_enabled",
  "stream:format": "stream_format",
  "stream:header_appendix": "header_appendix",
  "stream:image_appendix": "image_appendix",
  "monitor:buffer_size": "buffer_size",
  "monitor:discard_new": "discard_new",
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
export function isBinary(descriptor) {
  if (descriptor?.value_type === "bool") return true;
  const allowed = descriptor?.allowed_values;
  return Array.isArray(allowed) && allowed.length === 2 && allowed.includes("enabled") && allowed.includes("disabled");
}

export function isOn(descriptor) {
  return descriptor?.value_type === "bool" ? Boolean(descriptor.value) : descriptor?.value === "enabled";
}

export function helpKey(subsystem, key) {
  if (subsystem === "detector" && /^threshold\/\d\/energy$/.test(key)) return "detector.help.threshold_energy";
  if (subsystem === "detector" && /^threshold\/\d\/mode$/.test(key)) return "detector.help.threshold_mode";
  const id = PARAM_HELP[`${subsystem}:${key}`];
  if (id) return `detector.help.${id}`;
  if (subsystem === "detector") {
    const known = ADVANCED_HELP.find(([pattern]) => pattern.test(key));
    if (known) return `detector.help.adv.${known[1]}`;
  }
  return "";
}

// Advanced's settings, from the SIMPLON reference, where what they do can be
// said in a sentence. The rest get a "?" with their SIMPLON path and range
// (detector.help.adv.generic) rather than a guess.
const ADVANCED_HELP = [
  [/^auto_summation$/, "auto_summation"],
  [/^countrate_correction_applied$/, "countrate_correction"],
  [/^flatfield_correction_applied$/, "flatfield"],
  [/^mask_to_zero$/, "mask_to_zero"],
  [/^pixel_mask_applied$/, "pixel_mask"],
  [/^virtual_pixel_correction_applied$/, "virtual_pixel"],
  [/^counting_mode$/, "counting_mode"],
  [/^nexpi$/, "nexpi"],
  [/^roi_mode$/, "roi_mode"],
  [/^trigger_start_delay$/, "trigger_start_delay"],
  [/^test_image_mode$/, "test_image_mode"],
  [/^test_image_value$/, "test_image_value"],
  [/^beam_center_[xy]$/, "beam_center"],
  [/^detector_distance$/, "detector_distance"],
  [/^wavelength$/, "wavelength"],
  [/^element$/, "element"],
  [/^(chi|kappa|omega|phi|two_theta)_start$/, "goniometer_start"],
  [/^(chi|kappa|omega|phi|two_theta)_increment$/, "goniometer_increment"],
  [/^(chi|kappa|omega|phi|two_theta)_axis$/, "goniometer_axis"],
  [/^(sample_name|source_name|instrument_name)$/, "metadata_name"],
  [/^(detector_orientation|detector_orientation_angle|detector_orientation_axis|detector_translation|transformation_order)$/, "geometry"],
  [/^flux_(type|value)$/, "flux"],
];

export const OUTPUT_MAIN = {
  filewriter: ["name_pattern", "nimages_per_file"],
  stream: ["header_detail"],
  monitor: [],
};
export const BUSY_STATES = new Set(["initialize", "configure", "acquire", "test"]);
export const STATE_TONES = {
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

// The detector's dead time between two images, where it says.
export function readoutTime(det) {
  const value = Number(det?.detector_readout_time?.value);
  return det?.detector_readout_time && Number.isFinite(value) && value >= 0 ? value : null;
}

// Full duty cycle: the count time fills the frame time but for the readout
// (600 s and 599.9999999 s with 0.1 µs readout).
export function atFullDutyCycle(det) {
  const readout = readoutTime(det);
  const frame = Number(det?.frame_time?.value);
  const count = Number(det?.count_time?.value);
  if (readout === null || !(frame > 0) || !(count > 0)) return false;
  return Math.abs(frame - count - readout) <= Math.max(1e-9, readout * 0.01);
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
  "compression",
  "compression_enabled",
  "format",
  "header_appendix",
  "image_appendix",
  "buffer_size",
  "discard_new",
]);
export const STATE_TEXTS = new Set(["na", "configure", "acquire", "test", "error"]);
export const OUTPUT_STATES = new Set(["disabled", "ready", "acquire", "error", "normal", "overflow"]);

/** A duration for people: 0.0099999 s -> "10 ms", 125 s -> "2:05 min". */
export function formatDuration(seconds) {
  const s = Number(seconds);
  if (!Number.isFinite(s) || s < 0) return "-";
  if (s < 1e-3) return `${+(s * 1e6).toPrecision(3)} µs`;
  if (s < 1) return `${+(s * 1e3).toPrecision(3)} ms`;
  if (s < 120) return `${+s.toPrecision(3)} s`;
  // Rounded as a whole first: 179.7 s is 3:00 min, not 2:60.
  const whole = Math.round(s);
  if (whole < 3600) return `${Math.floor(whole / 60)}:${String(whole % 60).padStart(2, "0")} min`;
  // From an hour on, hours and minutes: 10 h, not 600:00 min.
  const minutes = Math.round(s / 60);
  const h = Math.floor(minutes / 60);
  const m = minutes % 60;
  return m ? `${h}:${String(m).padStart(2, "0")} h` : `${h} h`;
}

/**
 * Elapsed and total time of a running series, in one fixed format chosen by
 * the total, so the text does not change width as the seconds tick: two
 * decimals under a second, one under a minute, minutes and seconds under an
 * hour, hours, minutes and seconds above.
 */
export function formatProgress(elapsed, total) {
  const e = Math.max(0, Number(elapsed) || 0);
  const t = Math.max(0, Number(total) || 0);
  const pad = (v) => String(Math.floor(v)).padStart(2, "0");
  if (t >= 3600) {
    const clock = (v) => `${Math.floor(v / 3600)}:${pad((v % 3600) / 60)}:${pad(v % 60)}`;
    return { elapsed: clock(e), total: `${clock(t)} h` };
  }
  if (t >= 60) {
    const clock = (v) => `${Math.floor(v / 60)}:${pad(v % 60)}`;
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

// Units the SIMPLON reference gives but a detector may not report (a
// PILATUS4 sends none for these): without them a distance or an angle is a
// bare number. Only filled in where the detector says nothing.
const KNOWN_UNITS = {
  beam_center_x: "px",
  beam_center_y: "px",
  detector_distance: "m",
  trigger_start_delay: "s",
  x_pixel_size: "m",
  y_pixel_size: "m",
  sensor_thickness: "m",
  detector_readout_time: "s",
  frame_count_time: "s",
};

export function knownUnit(key, descriptor) {
  if (!descriptor || typeof descriptor !== "object" || descriptor.unit) return descriptor;
  const unit = KNOWN_UNITS[key] || (/^(chi|kappa|omega|phi|two_theta)_(start|increment)$/.test(key) ? "deg" : "");
  return unit ? { ...descriptor, unit } : descriptor;
}

export function withKnownUnits(all) {
  for (const subsystem of Object.keys(all)) {
    for (const [key, descriptor] of Object.entries(all[subsystem] || {})) all[subsystem][key] = knownUnit(key, descriptor);
  }
  return all;
}

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
  // A time as one: 0.1 µs, not 1e-7 s.
  if (typeof v === "number" && unit === "s" && v > 0) return formatDuration(v);
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

export function optionLabel(key, value) {
  if (key === "trigger_mode" && TRIGGER_MODE_KEYS.has(value)) return t(`detector.trigger_mode.${value}`);
  if (value === "enabled") return t("detector.value.on");
  if (value === "disabled") return t("detector.value.off");
  return String(value);
}
