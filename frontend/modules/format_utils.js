/**
 * Text the interface shows for values: statistics, plot ticks, energies, the
 * inspector's values, long paths shortened in the middle, a path's folder.
 *
 * Pure: no DOM, no state; eslint holds *_utils.js to that.
 */

/**
 * How many characters of the file and frame fit in the header: measured from
 * the room the label has (`roomPx`), not from the label itself. The label is
 * only as wide as its text, so measuring it shrank the name to its 160px
 * floor -- about 21 characters, "7118_E1_...0007.cbf" in a header with room
 * for the whole name.
 */
export function toolbarCharBudget(roomPx) {
  const width = Math.max(160, Number(roomPx) || 0);
  return Math.max(24, Math.floor(width / 7.4));
}

export function middleTruncate(text, maxChars) {
  const value = String(text || "");
  if (!value) return "";
  const limit = Math.max(6, Math.floor(Number(maxChars) || 0));
  if (value.length <= limit) return value;
  const side = Math.max(2, Math.floor((limit - 1) / 2));
  return `${value.slice(0, side)}…${value.slice(-side)}`;
}

export function formatStat(value) {
  if (!Number.isFinite(value)) return "-";
  const abs = Math.abs(value);
  if (abs >= 1e6 || (abs > 0 && abs < 1e-3)) {
    return value.toExponential(3);
  }
  if (Number.isInteger(value)) {
    return value.toString();
  }
  return value.toFixed(3);
}

/**
 * A BG or FG level as the histogram labels and the level fields show it: three
 * decimals for float data, a whole number otherwise. `dtype` is the frame's,
 * as in state.dtype ("<f4", "uint32", ...). Empty for no value.
 */
export function formatLevelValue(value, dtype) {
  if (!Number.isFinite(value)) return "";
  if (String(dtype || "").includes("f")) return value.toFixed(3);
  return Math.round(value).toString();
}

export function formatRoiTick(value) {
  if (!Number.isFinite(value)) return "-";
  const abs = Math.abs(value);
  if (abs >= 1e5 || (abs > 0 && abs < 1e-3)) {
    return value.toExponential(2);
  }
  if (abs >= 1000) return value.toFixed(0);
  if (abs >= 100) return value.toFixed(1);
  if (abs >= 10) return value.toFixed(2);
  return value.toFixed(3).replace(/0+$/, "").replace(/\.$/, "");
}

export function formatEnergy(value) {
  if (!Number.isFinite(value)) return "";
  const rounded = Math.round(value);
  if (Math.abs(value - rounded) < 0.05) {
    return String(rounded);
  }
  return value.toFixed(1);
}

export function formatInspectorValue(value) {
  if (value === null || value === undefined) return "-";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

export function dirnameFromPath(path) {
  if (!path) return "";
  const normalized = path.replace(/\\/g, "/");
  const idx = normalized.lastIndexOf("/");
  if (idx <= 0) {
    return normalized.startsWith("/") ? "/" : "";
  }
  return normalized.slice(0, idx);
}

/** A poll or frame time as the status line shows it, in the browser's locale. */
export function formatTimeStamp(ts) {
  if (!ts) return "";
  return new Date(ts).toLocaleTimeString();
}
