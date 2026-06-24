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

/**
 * Stable identity for the data source currently feeding geometry metadata.
 *
 * For a running live source the key is derived from the source endpoint so it
 * survives across frames but changes when the user switches source. Otherwise
 * it falls back to the file/series scope. Used to scope the manual geometry
 * lock so corrections persist for one source but reset on a source/file switch.
 */
export function getActiveSourceScopeKey(state) {
  const auto = state?.autoload || {};
  if (auto.running) {
    const mode = String(auto.mode || "");
    if (mode === "simplon") return `simplon:${String(auto.simplonUrl || "").trim()}`;
    if (mode === "remote") return `remote:${String(auto.remoteSourceId || "").trim()}`;
    if (mode === "jungfraujoch") {
      return `jfjoch:${String(auto.jfjochEndpoint || "").trim()}:${String(auto.jfjochSourceId || "").trim()}`;
    }
    if (mode === "file") return `filewatch:${String(auto.dir || "").trim()}`;
  }
  return getGeometryScopeKey(state, state?.file || "");
}

/**
 * True when a manual geometry lock is engaged AND still scoped to the active
 * source — i.e. incoming frame metadata should be ignored.
 */
export function isGeometryLockActive(analysisState, state) {
  if (!analysisState?.geometryLocked) return false;
  const lockKey = String(analysisState.geometryLockKey || "");
  const activeKey = String(getActiveSourceScopeKey(state) || "");
  return Boolean(lockKey && activeKey && lockKey === activeKey);
}

export function buildGeometryRequestKey(scopeKey, overridePath = "") {
  const baseKey = String(scopeKey || "").trim();
  const override = String(overridePath || "").trim();
  if (!override) {
    return baseKey;
  }
  return `${baseKey}|geometry:${override}`;
}
