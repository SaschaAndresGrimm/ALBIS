/**
 * Detector tab (beta): what the panel remembers per browser -- the
 * address, Live view, the quick actions' timings, which closed sections are
 * open, and whether frame and count time are linked.
 */

import { QUICK_DEFAULTS, QUICK_LIMITS } from "./detector_control_utils.js";

const STORAGE_KEY = "albis.detectorControl.url";
const FOLLOW_KEY = "albis.detectorControl.followLive";
const QUICK_KEY = "albis.detectorControl.quick";

export function readQuick() {
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

// Which of Data output's closed sections are open: the files and each
// output's other settings. Remembered per browser.
const OPEN_KEY = "albis.detectorControl.open";

export function readOpen(id) {
  try {
    return Boolean(JSON.parse(window.localStorage?.getItem(OPEN_KEY) || "{}")?.[id]);
  } catch {
    return false;
  }
}

export function writeOpen(id, open) {
  try {
    const all = JSON.parse(window.localStorage?.getItem(OPEN_KEY) || "{}") || {};
    all[id] = open;
    window.localStorage?.setItem(OPEN_KEY, JSON.stringify(all));
  } catch {
    // A remembered preference only.
  }
}

// Frame and count time linked: one sets both, the count time the frame time
// less the readout, for full duty cycle. On unless switched off; remembered
// per browser.
const LINK_KEY = "albis.detectorControl.linkTiming";

export function readLinkTiming() {
  try {
    return window.localStorage?.getItem(LINK_KEY) !== "0";
  } catch {
    return true;
  }
}

export function writeLinkTiming(on) {
  try {
    window.localStorage?.setItem(LINK_KEY, on ? "1" : "0");
  } catch {
    // A remembered preference only.
  }
}

export function writeQuick(values) {
  try {
    window.localStorage?.setItem(QUICK_KEY, JSON.stringify(values));
  } catch {
    // A remembered preference only.
  }
}

export function readStored() {
  try {
    return window.localStorage.getItem(STORAGE_KEY) || "";
  } catch {
    return "";
  }
}

export function readFollow() {
  try {
    return window.localStorage?.getItem(FOLLOW_KEY) !== "0";
  } catch {
    return true;
  }
}

export function writeFollow(on) {
  try {
    window.localStorage?.setItem(FOLLOW_KEY, on ? "1" : "0");
  } catch {
    // A remembered preference only.
  }
}

export function writeStored(value) {
  try {
    window.localStorage.setItem(STORAGE_KEY, value);
  } catch {
    // Private windows: the address is simply not remembered.
  }
}
