/**
 * Shared file-type classification helpers used across frontend controllers.
 */

const HDF_EXTS = [".h5", ".hdf5"];
const SERIES_IMAGE_EXTS = [".cbf", ".cbf.gz", ".edf", ".tif", ".tiff"];

function normalizePath(path) {
  return typeof path === "string" ? path.toLowerCase() : "";
}

export function isHdfFile(path) {
  const lower = normalizePath(path);
  return HDF_EXTS.some((ext) => lower.endsWith(ext));
}

export function isHdf5File(path) {
  return isHdfFile(path);
}

export function isHeaderCapableFile(path) {
  const lower = normalizePath(path);
  return SERIES_IMAGE_EXTS.some((ext) => lower.endsWith(ext));
}

export function isSeriesCapableFile(path) {
  const lower = normalizePath(path);
  if (!lower || isHdfFile(lower)) return false;
  return SERIES_IMAGE_EXTS.some((ext) => lower.endsWith(ext));
}
