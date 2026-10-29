/**
 * The zoom slider's scale: logarithmic, the slider holding log2 of the zoom,
 * so every doubling takes the same length of track and 1x sits where the
 * zoomed-out and zoomed-in halves meet. A linear one from 0.02x to 50x put
 * everything up to 10x, where most viewing happens, into its first fifth.
 */

export function zoomToSlider(zoom) {
  return Math.log2(zoom);
}

export function sliderToZoom(value) {
  return 2 ** Number(value);
}
