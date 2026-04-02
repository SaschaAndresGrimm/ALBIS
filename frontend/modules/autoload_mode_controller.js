/**
 * File-watch and SIMPLON autoload runtime.
 */

import { t } from "./i18n.js";

export function createAutoloadModeController({
  apiBase,
  state,
  callbacks,
}) {
  const {
    setAutoloadStatus,
    setAutoloadLatest,
    updateAutoloadMeta,
    loadAutoloadFile,
    fetchSimplonMask,
    parseDtype,
    parseShape,
    typedArrayFrom,
    hashBufferSample,
    parseSimplonMeta,
    createLiveSourceSnapshot,
    appendLiveFrame,
    logClient,
    formatSimplonTimestamp,
    updateLiveBadge,
  } = callbacks;

  let _watchTickInFlight = false;
  let _simplonTickInFlight = false;

  async function autoloadWatchTick() {
    if (_watchTickInFlight) return;
    _watchTickInFlight = true;
    try {
      await _autoloadWatchTickImpl();
    } finally {
      _watchTickInFlight = false;
    }
  }

  async function _autoloadWatchTickImpl() {
    const folder = state.autoload.dir || "";
    const exts = [];
    if (state.autoload.types.hdf5) exts.push("h5", "hdf5");
    if (state.autoload.types.tiff) exts.push("tif", "tiff");
    if (state.autoload.types.cbf) exts.push("cbf", "cbf.gz");
    if (state.autoload.types.edf) exts.push("edf");
    if (exts.length === 0) {
      setAutoloadStatus(t("autoload.status.watch.no_types_selected"));
      return;
    }
    const pattern = state.autoload.pattern || "";
    const url = `${apiBase}/autoload/latest?folder=${encodeURIComponent(folder)}&exts=${encodeURIComponent(
      exts.join(",")
    )}&pattern=${encodeURIComponent(pattern)}`;
    const res = await fetch(url);
    if (res.status === 204) {
      setAutoloadStatus(t("autoload.status.watch.no_files"));
      setAutoloadLatest("-");
      return;
    }
    if (!res.ok) {
      setAutoloadStatus(t("autoload.status.watch.error"));
      return;
    }
    const payload = await res.json();
    if (!payload?.file) {
      setAutoloadStatus(t("autoload.status.watch.no_files"));
      return;
    }
    const mtime = Number(payload.mtime || 0);
    if (payload.file === state.autoload.lastFile && mtime <= state.autoload.lastMtime) {
      return;
    }
    const previousFile = state.autoload.lastFile;
    const previousMtime = state.autoload.lastMtime;
    let loaded = false;
    try {
      loaded = await loadAutoloadFile(payload.file);
    } catch (err) {
      console.error(err);
      loaded = false;
    }
    if (!loaded) {
      setAutoloadStatus(t("autoload.status.watch.error"));
      return;
    }
    state.autoload.lastFile = payload.file;
    state.autoload.lastMtime = mtime;
    const changed = payload.file !== previousFile || mtime > previousMtime;
    if (changed) {
      state.autoload.lastUpdate = Date.now();
      updateAutoloadMeta();
    }
    setAutoloadStatus(
      payload.file === previousFile ? t("autoload.status.watch.updated") : t("autoload.status.watch.loaded"),
    );
  }

  async function autoloadSimplonTick() {
    if (_simplonTickInFlight) return;
    _simplonTickInFlight = true;
    try {
      await _autoloadSimplonTickImpl();
    } finally {
      _simplonTickInFlight = false;
    }
  }

  async function _autoloadSimplonTickImpl() {
    const baseUrl = state.autoload.simplonUrl || "";
    if (!baseUrl) {
      setAutoloadStatus(t("autoload.status.simplon.set_base_url"));
      return;
    }
    if (!state.maskAvailable) {
      const now = Date.now();
      const lastAttempt = state.autoload.lastMaskAttempt || 0;
      if (now - lastAttempt > 5000) {
        state.autoload.lastMaskAttempt = now;
        await fetchSimplonMask();
      }
    }
    const version = state.autoload.simplonVersion || "1.8.0";
    const timeout = state.autoload.simplonTimeout || 500;
    const enable = state.autoload.simplonEnable ? "1" : "0";
    const url = `${apiBase}/simplon/monitor?url=${encodeURIComponent(baseUrl)}&version=${encodeURIComponent(
      version
    )}&timeout=${encodeURIComponent(timeout)}&enable=${enable}`;
    const res = await fetch(url);
    if (res.status === 204) {
      setAutoloadStatus(t("autoload.status.simplon.no_frame"));
      updateLiveBadge();
      return;
    }
    if (!res.ok) {
      setAutoloadStatus(t("autoload.status.simplon.error"));
      updateLiveBadge();
      return;
    }
    const buffer = await res.arrayBuffer();
    const dtype = parseDtype(res.headers.get("X-Dtype"));
    const shape = parseShape(res.headers.get("X-Shape"));
    const data = typedArrayFrom(buffer, dtype);
    const sig = hashBufferSample(buffer);
    const changed = Boolean(sig) && sig !== state.autoload.lastMonitorSig;
    const parsed = parseSimplonMeta(res.headers);
    const simplonMeta = parsed.meta || {};
    if (!state.autoload.loggedSimplonHeaders) {
      state.autoload.loggedSimplonHeaders = true;
      logClient("info", "SIMPLON response headers", {
        headers: Object.fromEntries(res.headers.entries()),
      });
    }
    let label = t("source.label.simplon_monitor");
    let hostLabel = "";
    try {
      hostLabel = new URL(baseUrl).host;
    } catch {
      hostLabel = baseUrl || "";
    }
    if (hostLabel) {
      label = `${label} (${hostLabel})`;
    }
    const detailParts = [];
    if (simplonMeta.series !== "" && simplonMeta.series != null) detailParts.push(`S${simplonMeta.series}`);
    if (simplonMeta.image !== "" && simplonMeta.image != null) detailParts.push(`Img${simplonMeta.image}`);
    const timeLabel = formatSimplonTimestamp(simplonMeta.date);
    if (timeLabel) detailParts.push(timeLabel);
    if (detailParts.length) {
      label = `${label} ${detailParts.join(" ")}`;
    }
    if (state.autoload.livePaused) {
      updateLiveBadge();
      return;
    }
    if (!changed) {
      updateLiveBadge();
      return;
    }
    state.autoload.lastMonitorSig = sig;
    state.autoload.lastUpdate = Date.now();
    updateAutoloadMeta();
    const result = appendLiveFrame({
      sourceKey: `simplon:${baseUrl}`,
      sourceKind: "simplon",
      dedupeKey: sig,
      label,
      data,
      shape,
      dtype,
      snapshot: createLiveSourceSnapshot({
        sourceKind: "simplon",
        analysis: parsed.analysis,
        simplonMeta,
      }),
    });
    if (result.rendered) {
      setAutoloadStatus(t("autoload.status.simplon.updated"));
    }
    updateLiveBadge();
  }

  return {
    autoloadWatchTick,
    autoloadSimplonTick,
  };
}
