/**
 * Duplicating a viewer window: what travels, and how.
 *
 * "Open this image in a new window with the same settings" is a snapshot
 * problem with one hard rule -- copy only what a fresh page load cannot
 * reconstruct for itself, and re-derive everything else. Two kinds of field
 * must NOT travel:
 *
 *   Decoded data (`dataRaw`, `dataFloat`, `maskRaw`, `histogram`, `stats`,
 *   `peaks`) is megabytes, and the clone fetches its own copy anyway.
 *
 *   Anything sized to the window (`renderOffsetX/Y`, `width`, `height`,
 *   `pixelAspect`, `frameCount`) is derived from the viewport or from the file.
 *   A clone is a different size, so a copied render offset would place the
 *   image somewhere it does not belong.
 *
 * That is why the viewport travels as an image-space CENTRE plus a zoom rather
 * than as scroll pixels: a centre means the same thing in a window of any size,
 * and it is what viewer_sync_controller already exchanges between linked
 * windows for exactly this reason.
 *
 * The payload itself goes through `localStorage` under a single-use key, with
 * only the key's nonce in the opened URL. Handing it over in the URL would put
 * a filesystem path in the address bar, in history, and in anything that logs
 * `location.href`; a nonce is opaque and the slot is deleted the moment it is
 * claimed.
 */

const SLOT_PREFIX = "albis.windowClone.";
/** A slot nobody claimed -- a popup the browser blocked -- is swept after this. */
const SLOT_TTL_MS = 60_000;
const PAYLOAD_VERSION = 1;

function numberOrNull(value) {
  // null must survive as null. Number(null) is 0, and for these fields the two
  // mean entirely different things: a null detector distance is "not known and
  // so no d-spacing is reported", a zero is "calibrated to zero millimetres".
  // Copying the first as the second would hand the clone a geometry the
  // original never had.
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "boolean") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function boolOf(value) {
  return Boolean(value);
}

/** A finite point, or null. Used for the ROI's corners. */
function pointOrNull(point) {
  if (!point) return null;
  const x = Number(point.x);
  const y = Number(point.y);
  if (!Number.isFinite(x) || !Number.isFinite(y)) return null;
  return { x, y };
}

/**
 * Everything a clone needs, and deliberately nothing else.
 *
 * `viewport` comes from viewer_sync_controller's own reader so the two
 * mechanisms cannot disagree about what "the current view" means.
 */
export function captureWindowState({ state, analysisState, roiState, viewport }) {
  if (!state?.file) return null;

  const manual = {};
  // Only the overrides actually in force. The KEY these are scoped to is
  // deliberately absent: it is re-stamped on the far side once the clone's own
  // geometry has loaded, because it is compared against that geometry's key
  // and a copied one would never match.
  if (analysisState?.geometryDistanceManual) manual.distance = true;
  if (analysisState?.geometryCenterXManual) manual.centerX = true;
  if (analysisState?.geometryCenterYManual) manual.centerY = true;

  return {
    v: PAYLOAD_VERSION,
    source: {
      file: String(state.file),
      dataset: String(state.dataset || ""),
      frameIndex: Math.max(0, Math.round(Number(state.frameIndex) || 0)),
      thresholdIndex: Math.max(0, Math.round(Number(state.thresholdIndex) || 0)),
    },
    viewport: viewport
      ? {
          zoom: numberOrNull(viewport.zoom),
          centerX: numberOrNull(viewport.centerX),
          centerY: numberOrNull(viewport.centerY),
        }
      : null,
    contrast: {
      autoScale: boolOf(state.autoScale),
      min: numberOrNull(state.min),
      max: numberOrNull(state.max),
      colormap: String(state.colormap || ""),
      invert: boolOf(state.invert),
    },
    display: {
      histLogX: boolOf(state.histLogX),
      histLogY: boolOf(state.histLogY),
      pixelLabels: boolOf(state.pixelLabels),
    },
    mask: {
      enabled: boolOf(state.maskEnabled),
      auto: boolOf(state.maskAuto),
      saturated: boolOf(state.maskSaturatedEnabled),
      file: String(state.maskFile || ""),
      path: String(state.maskPath || ""),
    },
    analysis: {
      ringsEnabled: boolOf(analysisState?.ringsEnabled),
      ringMode: String(analysisState?.ringMode || "planar"),
      rings: Array.isArray(analysisState?.rings)
        ? analysisState.rings.map(numberOrNull).filter((v) => v !== null)
        : [],
      ringCount: Math.max(0, Math.round(Number(analysisState?.ringCount) || 0)),
      distanceMm: numberOrNull(analysisState?.distanceMm),
      pixelSizeUm: numberOrNull(analysisState?.pixelSizeUm),
      energyEv: numberOrNull(analysisState?.energyEv),
      centerX: numberOrNull(analysisState?.centerX),
      centerY: numberOrNull(analysisState?.centerY),
      geometryOverridePath: String(analysisState?.geometryOverridePath || ""),
      geometryLocked: boolOf(analysisState?.geometryLocked),
      manual,
      peaksEnabled: boolOf(analysisState?.peaksEnabled),
      peakCount: Math.max(1, Math.round(Number(analysisState?.peakCount) || 25)),
      peakMinSnr: Math.max(0, Number(analysisState?.peakMinSnr) || 0),
    },
    roi: {
      enabled: boolOf(roiState?.enabled),
      active: boolOf(roiState?.active),
      mode: String(roiState?.mode || "line"),
      start: pointOrNull(roiState?.start),
      end: pointOrNull(roiState?.end),
      innerRadius: Math.max(0, Number(roiState?.innerRadius) || 0),
      outerRadius: Math.max(0, Number(roiState?.outerRadius) || 0),
      histogramEnabled: boolOf(roiState?.histogramEnabled),
    },
  };
}

/**
 * Validate a stored payload, or null.
 *
 * Anything unparseable is nothing rather than a half-applied view, the same
 * posture recent_files.js takes with its own stored list. A payload from a
 * different version is refused outright: a partially-understood snapshot could
 * leave the clone claiming settings it did not actually apply.
 */
export function parseWindowClonePayload(raw) {
  if (!raw) return null;
  let parsed = null;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== "object") return null;
  if (parsed.v !== PAYLOAD_VERSION) return null;
  if (!parsed.source || typeof parsed.source.file !== "string" || !parsed.source.file) {
    return null;
  }
  return parsed;
}

function storage() {
  try {
    return window.localStorage;
  } catch {
    // Private windows and locked-down browsers throw on access itself.
    return null;
  }
}

function nonce() {
  try {
    if (window.crypto?.randomUUID) return window.crypto.randomUUID();
  } catch {
    // fall through
  }
  // Not security-critical: the slot is same-origin, single-use and short-lived.
  // This only has to avoid colliding with another window opened in the same
  // moment, which a timestamp plus a random tail does.
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

/** Store a payload and return the nonce naming it, or null if storage refused. */
export function stashClonePayload(payload) {
  const store = storage();
  if (!store || !payload) return null;
  const token = nonce();
  try {
    store.setItem(SLOT_PREFIX + token, JSON.stringify({ at: Date.now(), payload }));
  } catch {
    return null;
  }
  return token;
}

/** Take a payload out of storage. Removed before parsing, so it is single-use. */
export function claimClonePayload(token) {
  const store = storage();
  if (!store || !token) return null;
  const key = SLOT_PREFIX + token;
  let raw = null;
  try {
    raw = store.getItem(key);
    store.removeItem(key);
  } catch {
    return null;
  }
  if (!raw) return null;
  let envelope = null;
  try {
    envelope = JSON.parse(raw);
  } catch {
    return null;
  }
  return parseWindowClonePayload(JSON.stringify(envelope?.payload ?? null));
}

/** Drop a slot nobody claimed, so a blocked popup cannot leak one per attempt. */
export function discardClonePayload(token) {
  const store = storage();
  if (!store || !token) return;
  try {
    store.removeItem(SLOT_PREFIX + token);
  } catch {
    // nothing to do
  }
}

/** Remove slots older than the TTL, including any left by a blocked popup. */
export function sweepStaleClonePayloads(now = Date.now()) {
  const store = storage();
  if (!store) return 0;
  let removed = 0;
  try {
    const stale = [];
    for (let i = 0; i < store.length; i += 1) {
      const key = store.key(i);
      if (!key || !key.startsWith(SLOT_PREFIX)) continue;
      let at = 0;
      try {
        at = Number(JSON.parse(store.getItem(key) || "{}")?.at) || 0;
      } catch {
        at = 0;
      }
      // A slot with no readable timestamp is already unusable.
      if (!at || now - at > SLOT_TTL_MS) stale.push(key);
    }
    for (const key of stale) {
      store.removeItem(key);
      removed += 1;
    }
  } catch {
    return removed;
  }
  return removed;
}

/** The nonce in `#albis-clone=<token>`, or "". */
export function readCloneTokenFromHash(hash) {
  const text = String(hash || "").replace(/^#/, "");
  if (!text) return "";
  const match = /(?:^|&)albis-clone=([A-Za-z0-9-]+)/.exec(text);
  return match ? match[1] : "";
}

export const CLONE_HASH_PREFIX = "albis-clone=";
export const CLONE_SLOT_PREFIX = SLOT_PREFIX;
export const CLONE_PAYLOAD_VERSION = PAYLOAD_VERSION;
