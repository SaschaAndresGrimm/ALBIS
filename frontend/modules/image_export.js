/**
 * Still-image (PNG) export: output sizes, and the resolution stamp.
 *
 * ALBIS renders an image at one detector pixel per output pixel. That is exact,
 * but a Pollux frame is 1544 x 96 pixels, and every program that shows it
 * larger -- a slide, a PDF, Preview on a Retina screen -- smooths the pixels
 * into each other. Enlarging here, by whole factors and without smoothing,
 * keeps every detector pixel a sharp square, so there is nothing left for that
 * program to blur.
 */

export const EXPORT_SCALES = Object.freeze([1, 2, 4, 8]);
// Browsers refuse or silently blank canvases past these: Safari and Chrome
// both stop at 16384 px per side on common setups, and the RGBA buffer of a
// 100-megapixel image is already 400 MB.
export const MAX_EXPORT_SIDE_PX = 16384;
export const MAX_EXPORT_PIXELS = 100_000_000;
// Wide enough for a full-width slide without the program enlarging it again.
export const TARGET_EXPORT_WIDTH_PX = 2000;
export const EXPORT_DPI_CHOICES = Object.freeze([0, 150, 300, 600]);
export const DEFAULT_EXPORT_DPI = 300;

/** Output size for a region at a scale, with the pixel aspect (Y/X) applied. */
export function exportSize(region, scale, pixelAspect = 1) {
  const aspect = Number(pixelAspect) > 0 ? Number(pixelAspect) : 1;
  return {
    width: Math.max(1, Math.round(region.width * scale)),
    height: Math.max(1, Math.round(region.height * aspect * scale)),
  };
}

/** Each scale with its output size and whether a browser can produce it. */
export function exportScaleOptions(region, pixelAspect = 1) {
  return EXPORT_SCALES.map((scale) => {
    const { width, height } = exportSize(region, scale, pixelAspect);
    const allowed = width <= MAX_EXPORT_SIDE_PX && height <= MAX_EXPORT_SIDE_PX && width * height <= MAX_EXPORT_PIXELS;
    return { scale, width, height, allowed };
  });
}

/** The smallest allowed scale reaching the target width, else the largest allowed. */
export function defaultExportScale(region, pixelAspect = 1) {
  const allowed = exportScaleOptions(region, pixelAspect).filter((option) => option.allowed);
  if (!allowed.length) return 1;
  const wide = allowed.find((option) => option.width >= TARGET_EXPORT_WIDTH_PX);
  return (wide || allowed[allowed.length - 1]).scale;
}

/** Printed size in centimetres at a resolution, or null when none is set. */
export function printSizeCm(width, height, dpi) {
  if (!(Number(dpi) > 0)) return null;
  return { width: (width / dpi) * 2.54, height: (height / dpi) * 2.54 };
}

const CRC_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n += 1) {
    let c = n;
    for (let k = 0; k < 8; k += 1) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[n] = c >>> 0;
  }
  return table;
})();

function crc32(bytes) {
  let crc = 0xffffffff;
  for (let i = 0; i < bytes.length; i += 1) crc = CRC_TABLE[(crc ^ bytes[i]) & 0xff] ^ (crc >>> 8);
  return (crc ^ 0xffffffff) >>> 0;
}

const PNG_SIGNATURE = [137, 80, 78, 71, 13, 10, 26, 10];

function chunkType(bytes, offset) {
  return String.fromCharCode(bytes[offset + 4], bytes[offset + 5], bytes[offset + 6], bytes[offset + 7]);
}

/**
 * The PNG with its resolution set to `dpi`, in a pHYs chunk right after IHDR.
 *
 * A browser's canvas encoder writes no resolution, so a journal's layout
 * program assumes 72 dpi and prints a 2000-pixel figure 70 cm wide. PNG states
 * resolution in pixels per metre. Any pHYs already present is replaced; a dpi
 * of 0 or less removes it. Bytes that are not a PNG come back unchanged.
 */
export function setPngDpi(png, dpi) {
  const bytes = png instanceof Uint8Array ? png : new Uint8Array(png);
  if (bytes.length < 33 || PNG_SIGNATURE.some((value, i) => bytes[i] !== value)) return bytes;
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const chunks = [];
  let offset = 8;
  while (offset + 12 <= bytes.length) {
    const length = view.getUint32(offset);
    const end = offset + 12 + length;
    if (end > bytes.length) return bytes;
    chunks.push({ type: chunkType(bytes, offset), start: offset, end });
    offset = end;
  }
  if (!chunks.length || chunks[0].type !== "IHDR") return bytes;

  let phys = new Uint8Array(0);
  if (Number(dpi) > 0) {
    const perMetre = Math.round(Number(dpi) / 0.0254);
    phys = new Uint8Array(21);
    const pv = new DataView(phys.buffer);
    pv.setUint32(0, 9);
    phys.set([0x70, 0x48, 0x59, 0x73], 4); // "pHYs"
    pv.setUint32(8, perMetre);
    pv.setUint32(12, perMetre);
    phys[16] = 1; // unit: metre
    pv.setUint32(17, crc32(phys.subarray(4, 17)));
  }

  const kept = chunks.filter((chunk) => chunk.type !== "pHYs");
  const parts = [bytes.subarray(0, 8), bytes.subarray(kept[0].start, kept[0].end), phys];
  kept.slice(1).forEach((chunk) => parts.push(bytes.subarray(chunk.start, chunk.end)));
  const out = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  let at = 0;
  parts.forEach((part) => {
    out.set(part, at);
    at += part.length;
  });
  return out;
}

/** The resolution a PNG states, in dpi, or null. */
export function readPngDpi(png) {
  const bytes = png instanceof Uint8Array ? png : new Uint8Array(png);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let offset = 8;
  while (offset + 12 <= bytes.length) {
    const length = view.getUint32(offset);
    if (chunkType(bytes, offset) === "pHYs" && length === 9 && bytes[offset + 16] === 1) {
      return Math.round(view.getUint32(offset + 8) * 0.0254);
    }
    offset += 12 + length;
  }
  return null;
}
