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

/**
 * The nearest allowed centre. An image that fits the visible span is centred
 * in it; a larger one cannot be panned past its edges, so no empty band opens
 * between an image edge and the side of the view.
 */
export function clampAxisCenter(center, { size, scale, axis }) {
  if (!(size > 0) || !(scale > 0)) return finiteOr(center, 0);
  const span = size * scale;
  const visible = axis.end - axis.start;
  if (!(span > visible)) return size / 2;
  const mid = axisMid(axis);
  const edgeEnd = Math.min(axis.end, finiteOr(axis.edgeEnd, axis.end));
  const min = (mid - axis.start) / scale;
  const max = size - (edgeEnd - mid) / scale;
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
