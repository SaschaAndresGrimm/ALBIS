/**
 * Helpers for loading detector geometry per file or series.
 *
 * Which geometry file the user chose, and whether it applies, is the geometry
 * override's business (geometry_params.js): it is kept across files rather than
 * scoped to one, so it is not decided here.
 */

export function isExptPath(path) {
  const text = String(path || "").trim().toLowerCase();
  return text.endsWith(".expt");
}

/** One key per series, so stepping through its frames does not refetch geometry. */
export function getGeometryScopeKey(state, fallbackFile = "") {
  const seriesFiles = Array.isArray(state?.seriesFiles)
    ? state.seriesFiles.map((item) => String(item || "").trim()).filter(Boolean)
    : [];
  if (seriesFiles.length > 1) {
    return `series:${seriesFiles[0]}:${seriesFiles.length}`;
  }
  const file = String(state?.file || fallbackFile || "").trim();
  return file ? `file:${file}` : "";
}

export function buildGeometryRequestKey(scopeKey, overridePath = "") {
  const baseKey = String(scopeKey || "").trim();
  const override = String(overridePath || "").trim();
  if (!override) {
    return baseKey;
  }
  return `${baseKey}|geometry:${override}`;
}
