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
 * Tint each pixel of `indices` (row-major in an image `width` wide) in `buf`,
 * an RGBA buffer `w` x `h` device pixels. `view` maps image to CSS pixels
 * ({ scaleX, scaleY, offsetX, offsetY }), `dpr` CSS to device pixels.
 * `colorOf(index)` gives [r, g, b, a]. A pixel smaller than a device pixel
 * still takes one, so it shows at any zoom. Returns how many were in view.
 */
export function paintPixels(buf, w, h, { indices, width, view, dpr = 1, colorOf }) {
  let shown = 0;
  for (const index of indices) {
    const x = index % width;
    const y = (index - x) / width;
    const sx0 = Math.floor((x * view.scaleX + view.offsetX) * dpr);
    const sy0 = Math.floor((y * view.scaleY + view.offsetY) * dpr);
    const sx1 = Math.max(sx0 + 1, Math.floor(((x + 1) * view.scaleX + view.offsetX) * dpr));
    const sy1 = Math.max(sy0 + 1, Math.floor(((y + 1) * view.scaleY + view.offsetY) * dpr));
    if (sx1 <= 0 || sy1 <= 0 || sx0 >= w || sy0 >= h) continue;
    shown += 1;
    const [r, g, b, a] = colorOf(index);
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
  }
  return shown;
}
