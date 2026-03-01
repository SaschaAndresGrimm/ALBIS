/**
 * File-watch and SIMPLON autoload runtime.
 */

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
    applySimplonMeta,
    logClient,
    formatSimplonTimestamp,
    applyExternalFrame,
    updateLiveBadge,
  } = callbacks;

  async function autoloadWatchTick() {
    const folder = state.autoload.dir || "";
    const exts = [];
    if (state.autoload.types.hdf5) exts.push("h5", "hdf5");
    if (state.autoload.types.tiff) exts.push("tif", "tiff");
    if (state.autoload.types.cbf) exts.push("cbf", "cbf.gz");
    if (state.autoload.types.edf) exts.push("edf");
    if (exts.length === 0) {
      setAutoloadStatus("Watch: no types selected");
      return;
    }
    const pattern = state.autoload.pattern || "";
    const url = `${apiBase}/autoload/latest?folder=${encodeURIComponent(folder)}&exts=${encodeURIComponent(
      exts.join(",")
    )}&pattern=${encodeURIComponent(pattern)}`;
    const res = await fetch(url);
    if (res.status === 204) {
      setAutoloadStatus("Watch: no files");
      setAutoloadLatest("-");
      return;
    }
    if (!res.ok) {
      setAutoloadStatus("Watch: error");
      return;
    }
    const payload = await res.json();
    if (!payload?.file) {
      setAutoloadStatus("Watch: no files");
      return;
    }
    const mtime = Number(payload.mtime || 0);
    if (payload.file === state.autoload.lastFile && mtime <= state.autoload.lastMtime) {
      return;
    }
    const previousFile = state.autoload.lastFile;
    state.autoload.lastFile = payload.file;
    const previousMtime = state.autoload.lastMtime;
    state.autoload.lastMtime = mtime;
    await loadAutoloadFile(payload.file);
    const changed = payload.file !== previousFile || mtime > previousMtime;
    if (changed) {
      state.autoload.lastUpdate = Date.now();
      updateAutoloadMeta();
    }
    setAutoloadStatus(payload.file === previousFile ? "Watch: updated" : "Watch: loaded");
  }

  async function autoloadSimplonTick() {
    const baseUrl = state.autoload.simplonUrl || "";
    if (!baseUrl) {
      setAutoloadStatus("SIMPLON: set base URL");
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
      setAutoloadStatus("SIMPLON: no frame");
      updateLiveBadge();
      return;
    }
    if (!res.ok) {
      setAutoloadStatus("SIMPLON: error");
      updateLiveBadge();
      return;
    }
    const buffer = await res.arrayBuffer();
    const dtype = parseDtype(res.headers.get("X-Dtype"));
    const shape = parseShape(res.headers.get("X-Shape"));
    const data = typedArrayFrom(buffer, dtype);
    const sig = hashBufferSample(buffer);
    const changed = sig && sig !== state.autoload.lastMonitorSig;
    if (changed) {
      state.autoload.lastMonitorSig = sig;
      state.autoload.lastUpdate = Date.now();
      updateAutoloadMeta();
    }
    const simplonMeta = applySimplonMeta(res.headers);
    if (!state.autoload.loggedSimplonHeaders) {
      state.autoload.loggedSimplonHeaders = true;
      logClient("info", "SIMPLON response headers", {
        headers: Object.fromEntries(res.headers.entries()),
      });
    }
    let label = "SIMPLON monitor";
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
    applyExternalFrame(data, shape, dtype, label, false, true);
    setAutoloadStatus("SIMPLON: updated");
    updateLiveBadge();
  }

  return {
    autoloadWatchTick,
    autoloadSimplonTick,
  };
}
