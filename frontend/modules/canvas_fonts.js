/**
 * The font stack canvas text uses.
 *
 * A 2D context takes a CSS font *shorthand string*, not a custom property, so
 * `--font-ui` cannot be referenced from `ctx.font`. Six modules each carried
 * their own literal instead, and they had drifted: five said "Avenir Next",
 * the resolution-ring labels said "Avenir" (a different face that also ships
 * on macOS), and the pixel-value labels said "Lucida Grande". Canvas text sits
 * directly beside DOM text on screen -- an axis label next to a panel label --
 * so they have to name the same family, and there has to be one place to
 * change it.
 */

export const CANVAS_FONT_STACK =
  '"Inter", system-ui, -apple-system, "Segoe UI", "Helvetica Neue", Arial, sans-serif';

/**
 * The `[<weight> ]<size>px <family>` shorthand `ctx.font` expects.
 *
 * The weight is omitted at 400 because the shorthand already defaults to it,
 * and because a leading `400 ` breaks the common trick of reading the size
 * back out with `parseFloat(ctx.font)`.
 */
export function canvasFont(sizePx, weight = 400) {
  const prefix = Number(weight) === 400 ? "" : `${weight} `;
  return `${prefix}${sizePx}px ${CANVAS_FONT_STACK}`;
}

/**
 * Resolve once the bundled face is usable, so canvas text is not painted in
 * the fallback and left there.
 *
 * DOM text re-flows by itself when a `font-display: swap` face arrives; canvas
 * text does not -- whatever was rasterised stays until something redraws it.
 * The font is served from the same local origin as the page, so this settles
 * in about a millisecond; the timeout is only so a browser without the Font
 * Loading API, or a missing file, cannot leave overlays unpainted forever.
 */
export function whenCanvasFontReady(timeoutMs = 3000) {
  const fonts = typeof document !== "undefined" ? document.fonts : null;
  if (!fonts?.load) return Promise.resolve();
  const settled = Promise.all([
    fonts.load('400 12px "Inter"'),
    fonts.load('600 12px "Inter"'),
  ]).catch(() => {});
  return Promise.race([
    settled,
    new Promise((resolve) => setTimeout(resolve, timeoutMs)),
  ]).then(() => undefined);
}
