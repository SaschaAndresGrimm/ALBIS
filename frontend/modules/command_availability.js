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
