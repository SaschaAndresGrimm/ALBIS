/**
 * Shared file-type classification helpers used across frontend controllers.
 */

export const HDF_EXTS = [".h5", ".hdf5"];
export const TIFF_EXTS = [".tif", ".tiff"];
export const CBF_EXTS = [".cbf", ".cbf.gz"];
export const EDF_EXTS = [".edf"];
// MYTHEN(2) strip-detector acquisitions are opened via their .cfg descriptor.
// They expose metadata (like CBF/TIFF) but are not numbered image series.
export const MYTHEN_EXTS = [".cfg"];
export const SERIES_IMAGE_EXTS = [...CBF_EXTS, ...EDF_EXTS, ...TIFF_EXTS];
export const HEADER_CAPABLE_EXTS = [...SERIES_IMAGE_EXTS, ...MYTHEN_EXTS];

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
  return HEADER_CAPABLE_EXTS.some((ext) => lower.endsWith(ext));
}

export function isSeriesCapableFile(path) {
  const lower = normalizePath(path);
  if (!lower || isHdfFile(lower)) return false;
  return SERIES_IMAGE_EXTS.some((ext) => lower.endsWith(ext));
}
