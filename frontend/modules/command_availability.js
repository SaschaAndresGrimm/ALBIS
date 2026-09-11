/**
 * Whether the file commands can run right now.
 *
 * The File menu, the command palette and the keyboard shortcuts are three doors
 * onto the same commands, so each rule lives here once instead of once per
 * door: the palette leaves out what cannot run, the menu greys it out and names
 * the reason on hover, and the command's own handler keeps the last guard for
 * the shortcut path, which passes both.
 */

// Writing out the rendered image needs a frame on screen, not a file — a live
// stream qualifies. `hasFrame` rather than `dataRaw` alone, because dataRaw
// outlives a failed frame load and an HDF5 dataset rescan and would otherwise
// save the previous frame's pixels over the splash.
export function canSaveImage(state) {
  return Boolean(state.hasFrame && state.dataRaw);
}

/**
 * Duplicating a window means reopening the same source in a second one, so the
 * source has to be something a path can name.
 *
 * A live source deliberately puts a human-readable label in `state.file` (see
 * file_session_controller's applyExternalFrame), not a path -- there is no file
 * for a second window to open, and the frames are arriving over a stream only
 * this window is subscribed to. `canSaveImage` is true in that situation,
 * because saving a PNG of the frame on screen is perfectly possible; that is
 * why this is its own rule rather than an alias of it.
 */
export function canDuplicateWindow(state, isHdfFile) {
  if (!state.hasFrame || !state.file) return false;
  if (state.autoload?.running && state.autoload.mode !== "file") return false;
  return Boolean(!isHdfFile(state.file) || state.dataset);
}

// A single frame cannot be animated, and the frames have to come from
// somewhere: a file series, or a multi-frame dataset.
export function canExportAnimation(state) {
  const total = Math.round(Number(state.frameCount) || 1);
  if (total <= 1) return false;
  const hasSeries = Array.isArray(state.seriesFiles) && state.seriesFiles.length > 0;
  return Boolean(state.file && (hasSeries || state.dataset));
}

// Converting reads frames from the source, so an HDF5 file needs its dataset
// chosen first; any other format is a single image and needs nothing.
export function canExportData(state, isHdfFile) {
  return Boolean(state.file && (!isHdfFile(state.file) || state.dataset));
}

// Series operations read the same frames a conversion does, and only one job
// runs at a time.
export function canStartSeriesOperation(state, isHdfFile) {
  return canExportData(state, isHdfFile) && !state.seriesSum.running;
}
