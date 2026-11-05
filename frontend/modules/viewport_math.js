/**
 * The arithmetic of the main view, one axis at a time.
 *
 * The view is an image-space centre plus a zoom. On each axis the image point
 * `center` sits at the middle of the visible area, so image coordinate `u`
 * lands on screen at `mid + (u - center) * scale`. Every way of changing the
 * view (wheel, slider, minimap, resize, a linked window) goes through these
 * functions, so they all agree on where the view is.
 *
 * An axis is `{ start, end, edgeEnd }` in screen pixels: the visible span, and
 * where the image's far edge may stop (before a control floating over the
 * image, such as the footer). `edgeEnd` defaults to `end`.
 */

function finiteOr(value, fallback) {
  return Number.isFinite(value) ? value : fallback;
}

export function axisMid(axis) {
  return (axis.start + axis.end) / 2;
}

/** Screen pixels of image that must stay in view: enough to grab it back. */
export const MIN_VISIBLE_PX = 48;

/**
 * The nearest allowed centre. The image may be moved anywhere, as long as a
 * strip of it (MIN_VISIBLE_PX, or all of it when it is smaller) stays inside
 * the visible span, between `start` and `edgeEnd`.
 *
 * The limit is deliberately loose. Anything that pins image edges to the
 * sides of the view -- "a large image fills it", "a small one stays inside"
 * -- fights zooming about the pointer: with the pointer on an image corner in
 * the middle of the screen, the corner has to leave the pointer for the edges
 * to meet the sides, and it does so in a jump as the image crosses the size
 * of the view. With this limit, zooming about a pixel on the image never
 * moves that pixel.
 */
export function clampAxisCenter(center, { size, scale, axis }) {
  if (!(size > 0) || !(scale > 0)) return finiteOr(center, 0);
  const mid = axisMid(axis);
  const edgeEnd = Math.min(axis.end, finiteOr(axis.edgeEnd, axis.end));
  const span = size * scale;
  const keep = Math.max(0, Math.min(span, MIN_VISIBLE_PX, edgeEnd - axis.start));
  // Near edge no further in than `edgeEnd - keep`, far edge no further out than `start + keep`.
  const min = (mid - edgeEnd + keep) / scale;
  const max = size - (axis.start + keep - mid) / scale;
  if (!(min <= max)) return size / 2;
  return Math.max(min, Math.min(max, finiteOr(center, size / 2)));
}

/**
 * The centre that keeps the image point under screen position `point` where
 * it is while the scale changes from `fromScale` to `toScale`. Not clamped:
 * the point may be off the image, and clamping is the caller's next step.
 */
export function zoomAxisAbout(center, { point, fromScale, toScale, axis }) {
  const offset = point - axisMid(axis);
  const imagePoint = center + offset / fromScale;
  return imagePoint - offset / toScale;
}

/** Screen pixels from the canvas origin to image coordinate 0, negated. */
export function axisScroll(center, { scale, axis }) {
  return center * scale - axisMid(axis);
}

/** The inverse of axisScroll. */
export function centerFromScroll(scroll, { scale, axis }) {
  return (scroll + axisMid(axis)) / scale;
}

/** The part of the image inside the visible span, as `{ from, length }`. */
export function visibleAxisSpan(center, { size, scale, axis }) {
  const mid = axisMid(axis);
  const from = Math.max(0, center - (mid - axis.start) / scale);
  const to = Math.min(size, center + (axis.end - mid) / scale);
  return { from, length: Math.max(0, to - from) };
}

/** The zoom at which the whole image fits a `width` by `height` area. */
export function fitZoom({ imageWidth, imageHeight, pixelAspect = 1, width, height }) {
  const zoom = Math.min(width / imageWidth, height / (imageHeight * (pixelAspect || 1)));
  return Number.isFinite(zoom) && zoom > 0 ? zoom : null;
}
