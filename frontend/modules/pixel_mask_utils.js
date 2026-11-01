/**
 * The pixel mask arithmetic and naming behind Data -> Pixel Mask
 * (pixel_mask_controller.js): counting a mask by reason, what replacing one
 * mask with another changes, and the names masks are saved under. No DOM, no
 * state: inputs in, outputs out, so all of it is tested without a page.
 */

export const GAP_BIT = 0x1;
const DEFECTIVE_BITS = 0x1e;

/** How many pixels a mask masks, and for which reasons. */
export function summarizeMask(data) {
  const out = { masked: 0, gap: 0, defective: 0, other: 0 };
  if (!data) return out;
  for (let i = 0; i < data.length; i += 1) {
    const value = data[i];
    if (!value) continue;
    out.masked += 1;
    if (value & GAP_BIT) out.gap += 1;
    else if (value & DEFECTIVE_BITS) out.defective += 1;
    else out.other += 1;
  }
  return out;
}

/** What replacing `before` with `after` changes, pixel by pixel. */
export function diffMasks(before, after) {
  const out = { added: 0, removed: 0, changed: 0 };
  const n = Math.min(before?.length || 0, after?.length || 0);
  for (let i = 0; i < n; i += 1) {
    const a = before[i];
    const b = after[i];
    if (a === b) continue;
    if (!a) out.added += 1;
    else if (!b) out.removed += 1;
    else out.changed += 1;
  }
  return out;
}

/** A name part kept to characters every file system takes, without repeats. */
export function safeNamePart(text) {
  return String(text || "")
    .trim()
    .replace(/[^A-Za-z0-9._-]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^[._]+|[._]+$/g, "");
}

/**
 * The file name a mask is saved under, without extension:
 * `<source>_pixel_mask[_edited]_<date>-<time>`. The time keeps successive
 * saves of a changing mask apart; a source that is itself a saved mask loses
 * its old suffix, so names do not grow with each round.
 */
export function maskFileName(source, { edited = false, now = new Date() } = {}) {
  const base =
    safeNamePart(source).replace(/_pixel_mask(_edited)?(_\d{8}-\d{4,6})?$/i, "").slice(0, 60) || "detector";
  const pad = (n) => String(n).padStart(2, "0");
  const stamp = `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}-${pad(now.getHours())}${pad(now.getMinutes())}`;
  return `${base}_pixel_mask${edited ? "_edited" : ""}_${stamp}`;
}

/** The host of a detector address typed with or without a scheme. */
export function hostOf(address) {
  const text = String(address || "").trim();
  if (!text) return "";
  try {
    return new URL(/^[a-z][a-z0-9+.-]*:\/\//i.test(text) ? text : `http://${text}`).hostname;
  } catch {
    return "";
  }
}

export function maskFileStem(path) {
  const base = String(path || "").replace(/\\/g, "/").split("/").pop() || "";
  return base.replace(/(_master)?\.(h5|hdf5|tiff?|cbf(\.gz)?|edf)$/i, "") || "pixel_mask";
}

export function shapeText(shape) {
  return Array.isArray(shape) && shape.length === 2 ? `${shape[1]} × ${shape[0]}` : "—";
}

export function filenameFrom(disposition, fallback) {
  const match = /filename="?([^";]+)"?/i.exec(String(disposition || ""));
  return match ? match[1] : fallback;
}
