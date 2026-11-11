/**
 * Painting single image pixels onto an overlay, as a tint over each pixel's
 * area on screen: what Edit on image shows of the pixels changed by hand, and
 * what Find bad pixels shows when it found too many to ring one by one.
 *
 * Pure: it writes into an RGBA buffer the caller hands it (an ImageData's
 * `data`) and leaves the canvas to the caller.
 */

/**
 * Most rings Find bad pixels draws. Past this the rings merge into one blob
 * and cost a path segment each, so the pixels are painted instead -- all of
 * them: a limit on rings drew only the first rows of the image and left the
 * rest looking clean.
 */
export const MARKER_RING_LIMIT = 20000;

/** Whether markers this many are drawn as rings (true) or painted (false). */
export function markersAsRings(groups) {
  let count = 0;
  for (const group of groups || []) count += group.indices.length;
  return count <= MARKER_RING_LIMIT;
}

/**
 * A painter for single pixels of an image `width` wide into `buf`, an RGBA
 * buffer `w` x `h` device pixels. `view` maps image to CSS pixels
 * ({ scaleX, scaleY, offsetX, offsetY }), `dpr` CSS to device pixels.
 * `paint(index, [r, g, b, a])` tints the pixel's area, at least one device
 * pixel so it shows at any zoom, and returns whether it was in view.
 */
export function createPixelPainter(buf, w, h, { width, view, dpr = 1 }) {
  return function paint(index, [r, g, b, a]) {
    const x = index % width;
    const y = (index - x) / width;
    const sx0 = Math.floor((x * view.scaleX + view.offsetX) * dpr);
    const sy0 = Math.floor((y * view.scaleY + view.offsetY) * dpr);
    const sx1 = Math.max(sx0 + 1, Math.floor(((x + 1) * view.scaleX + view.offsetX) * dpr));
    const sy1 = Math.max(sy0 + 1, Math.floor(((y + 1) * view.scaleY + view.offsetY) * dpr));
    if (sx1 <= 0 || sy1 <= 0 || sx0 >= w || sy0 >= h) return false;
    const px0 = Math.max(0, sx0);
    const px1 = Math.min(w, sx1);
    for (let py = Math.max(0, sy0); py < Math.min(h, sy1); py += 1) {
      let o = (py * w + px0) * 4;
      for (let px = px0; px < px1; px += 1) {
        buf[o] = r;
        buf[o + 1] = g;
        buf[o + 2] = b;
        buf[o + 3] = a;
        o += 4;
      }
    }
    return true;
  };
}

/** Tint each pixel of `indices`, coloured by `colorOf(index)`; returns how many were in view. */
export function paintPixels(buf, w, h, { indices, width, view, dpr = 1, colorOf }) {
  const paint = createPixelPainter(buf, w, h, { width, view, dpr });
  let shown = 0;
  for (const index of indices) if (paint(index, colorOf(index))) shown += 1;
  return shown;
}

/**
 * The image pixels a canvas `cssWidth` x `cssHeight` shows through `view`, as
 * an inclusive box clamped to the image, or null when none are in view.
 */
export function visibleImageArea(view, cssWidth, cssHeight, imageWidth, imageHeight) {
  if (!(view.scaleX > 0) || !(view.scaleY > 0)) return null;
  const x0 = Math.max(0, Math.floor(-view.offsetX / view.scaleX));
  const y0 = Math.max(0, Math.floor(-view.offsetY / view.scaleY));
  const x1 = Math.min(imageWidth - 1, Math.floor((cssWidth - view.offsetX) / view.scaleX));
  const y1 = Math.min(imageHeight - 1, Math.floor((cssHeight - view.offsetY) / view.scaleY));
  return x0 > x1 || y0 > y1 ? null : { x0, y0, x1, y1 };
}
