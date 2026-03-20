/**
 * Shared helpers for series-scoped manual geometry overrides.
 */

export function isExptPath(path) {
  const text = String(path || "").trim().toLowerCase();
  return text.endsWith(".expt");
}

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

export function getActiveGeometryOverridePath(analysisState, scopeKey) {
  const activeScopeKey = String(scopeKey || "").trim();
  const overridePath = String(analysisState?.geometryOverridePath || "").trim();
  const overrideScopeKey = String(analysisState?.geometryOverrideScopeKey || "").trim();
  if (!activeScopeKey || !overridePath || overrideScopeKey !== activeScopeKey) {
    return "";
  }
  return overridePath;
}

export function buildGeometryRequestKey(scopeKey, overridePath = "") {
  const baseKey = String(scopeKey || "").trim();
  const override = String(overridePath || "").trim();
  if (!override) {
    return baseKey;
  }
  return `${baseKey}|geometry:${override}`;
}
