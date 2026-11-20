/**
 * Zoom as the user sees it: screen (device) pixels per image pixel.
 *
 * The viewport keeps `state.zoom` in CSS pixels, which the canvas transform,
 * the overlays and the hit tests all work in. Everything shown to the user --
 * the slider, the zoom field, the footer, "As on screen" in the export
 * dialogs -- counts screen pixels instead, so 1x is one image pixel on one
 * screen pixel on any display. On a Retina screen (devicePixelRatio 2) that
 * is a CSS zoom of 0.5; on Windows at 150 % it is 0.667.
 */

function safeRatio(dpr) {
  const ratio = Number(dpr);
  return Number.isFinite(ratio) && ratio > 0 ? ratio : 1;
}

/** The zoom in screen pixels per image pixel, from the CSS zoom. */
export function toScreenZoom(cssZoom, dpr) {
  return cssZoom * safeRatio(dpr);
}

/** The CSS zoom that shows `screenZoom` screen pixels per image pixel. */
export function fromScreenZoom(screenZoom, dpr) {
  return screenZoom / safeRatio(dpr);
}

/**
 * `value`, a CSS length, moved to the nearest whole screen pixel. At a whole
 * screen zoom this keeps every image pixel the same number of screen pixels
 * wide; half a screen pixel off, `image-rendering: pixelated` makes some
 * columns one pixel wider than the rest.
 */
export function snapToScreenPixel(value, dpr) {
  const ratio = safeRatio(dpr);
  return Math.round(value * ratio) / ratio;
}
