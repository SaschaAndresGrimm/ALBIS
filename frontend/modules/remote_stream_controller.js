/**
 * Remote and JUNGFRAUJOCH stream autoload runtime.
 */

import { t } from "./i18n.js";
import { fetchResponse } from "./http.js";

export function createRemoteStreamController({
  apiBase,
  state,
  callbacks,
  core,
}) {
  const {
    updateAutoloadMeta,
    parseDtype,
    parseShape,
    typedArrayFrom,
    parseRemoteMeta,
    createLiveSourceSnapshot,
    applyLiveSourceSnapshot,
    appendLiveFrame,
    updateLiveHistoryEntry,
    resetLiveHistory,
  } = callbacks;

  function normalizePeakSets(payload) {
    const peakSets = Array.isArray(payload?.peak_sets) ? payload.peak_sets : [];
    const normalized = [];
    peakSets.forEach((set, idx) => {
      if (!set || typeof set !== "object") return;
      const color = typeof set.color === "string" && set.color ? set.color : "#4aa3ff";
      const name = typeof set.name === "string" && set.name ? set.name : `Set ${idx + 1}`;
      const style = typeof set.style === "string" && set.style ? set.style : "";
      const points = Array.isArray(set.points) ? set.points : [];
      const list = [];
      for (let i = 0; i < points.length; i += 1) {
        const point = points[i];
        if (!Array.isArray(point) || point.length < 2) continue;
        const x = Number(point[0]);
        const y = Number(point[1]);
        const intensity = point.length > 2 ? Number(point[2]) : null;
        if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
        list.push({
          x,
          y,
          intensity: Number.isFinite(intensity) ? intensity : null,
        });
      }
      if (list.length) {
        normalized.push({ name, color, style, points: list });
      }
    });
    return normalized;
  }

  async function fetchRemoteMeta(sourceId, seq) {
    if (!sourceId) return null;
    const params = new URLSearchParams({ source_id: sourceId });
    if (Number.isFinite(seq) && seq > 0) {
      params.set("seq", String(Math.round(seq)));
    }
    try {
      const res = await fetchResponse(`${apiBase}/remote/v1/meta?${params.toString()}`, { cache: "no-store" });
      if (res.status === 204) return null;
      const payload = await res.json();
      if (!payload || typeof payload !== "object") return null;
      const normalized = normalizePeakSets(payload);
      return { payload, normalized };
    } catch (err) {
      // An HTTP answer (409: the frame is gone already) is routine while
      // polling; only a failure to ask is worth a warning.
      if (!(err?.status >= 400)) console.warn(err);
      return null;
    }
  }

  async function autoloadJfjochTick() {
    const endpoint = (state.autoload.jfjochEndpoint || "").trim();
    if (!endpoint) {
      core.autoloadStatus?.setAutoloadStatus(t("autoload.status.jfjoch.set_preview_endpoint"));
      core.backendStatus.updateLiveBadge();
      return;
    }
    const sourceId = (state.autoload.jfjochSourceId || "jungfraujoch").trim() || "jungfraujoch";
    const status = state.autoload.jfjochStatus || {};
    const statusMismatch =
      String(status.endpoint || endpoint) !== endpoint ||
      String(status.source_id || sourceId) !== sourceId;
    if (!status.running || statusMismatch) {
      const started = await core.jfjochBridge.startJfjochPreviewBridge();
      if (!started) {
        core.backendStatus.updateLiveBadge();
        return;
      }
    }

    const params = new URLSearchParams({ source_id: sourceId });
    if (state.autoload.lastJfjochSeq > 0) {
      params.set("after_seq", String(state.autoload.lastJfjochSeq));
    }
    let res;
    try {
      res = await fetchResponse(`${apiBase}/remote/v1/latest?${params.toString()}`, { cache: "no-store" });
    } catch (err) {
      if (!(err?.status >= 400)) throw err;
      await core.jfjochBridge.fetchJfjochPreviewStatus();
      core.autoloadStatus?.setAutoloadStatus(t("autoload.status.jfjoch.error"));
      core.backendStatus.updateLiveBadge();
      return;
    }
    if (res.status === 204) {
      await core.jfjochBridge.fetchJfjochPreviewStatus();
      core.autoloadStatus?.setAutoloadStatus(t("autoload.status.jfjoch.waiting"));
      core.backendStatus.updateLiveBadge();
      return;
    }

    const buffer = await res.arrayBuffer();
    const dtype = parseDtype(res.headers.get("X-Dtype"));
    const shape = parseShape(res.headers.get("X-Shape"));
    const data = typedArrayFrom(buffer, dtype);
    const parsed = parseRemoteMeta(res.headers);
    const remoteMeta = parsed.meta || {};
    const seq = Number(remoteMeta.seq || 0);
    const seqChanged = Number.isFinite(seq) && seq > 0 && seq !== state.autoload.lastJfjochSeq;
    const label =
      remoteMeta.displayName ||
      t("source.label.jfjoch_preview_with_seq", {
        sourceId,
        seqSuffix: Number.isFinite(seq) && seq > 0 ? ` #${seq}` : "",
      });
    if (state.autoload.livePaused) {
      core.backendStatus.updateLiveBadge();
      return;
    }
    const initialJfjochMeta = {
      source: sourceId,
      seq,
      series: remoteMeta.series,
      image: remoteMeta.image,
      date: remoteMeta.date || "",
      reflections: 0,
      channel: "",
    };
    if (seqChanged) {
      state.autoload.lastJfjochSeq = seq;
      state.autoload.lastUpdate = Date.now();
      updateAutoloadMeta();
      const result = appendLiveFrame({
        sourceKey: `jungfraujoch:${sourceId}`,
        sourceKind: "jungfraujoch",
        dedupeKey: String(seq),
        label,
        data,
        shape,
        dtype,
        snapshot: createLiveSourceSnapshot({
          sourceKind: "jungfraujoch",
          analysis: parsed.analysis,
          jfjochMeta: initialJfjochMeta,
          jfjochStatus: state.autoload.jfjochStatus || {},
          externalPeakSets: [],
        }),
      });
      const metaResult = await fetchRemoteMeta(sourceId, seq);
      const payload = metaResult?.payload;
      const normalized = Array.isArray(metaResult?.normalized) ? metaResult.normalized : [];
      const totalPoints = normalized.reduce((sum, set) => sum + (set.points?.length || 0), 0);
      const extra = payload?.extra && typeof payload.extra === "object" ? payload.extra : {};
      const latestStatus = await core.jfjochBridge.fetchJfjochPreviewStatus();
      const finalMeta = {
        source: sourceId,
        seq,
        series: payload?.series_number ?? remoteMeta.series,
        image: payload?.image_number ?? remoteMeta.image,
        date: payload?.image_datetime || remoteMeta.date || "",
        reflections: totalPoints,
        channel: typeof extra.channel === "string" ? extra.channel : "",
      };
      updateLiveHistoryEntry(String(seq), {
        snapshot: createLiveSourceSnapshot({
          sourceKind: "jungfraujoch",
          analysis: parsed.analysis,
          jfjochMeta: finalMeta,
          jfjochStatus: latestStatus || state.autoload.jfjochStatus || {},
          externalPeakSets: normalized,
        }),
      });
      if (result.rendered) {
        core.autoloadStatus?.setAutoloadStatus(t("autoload.status.jfjoch.updated"));
      }
    } else {
      if (!(Number.isFinite(seq) && seq > 0)) {
        resetLiveHistory?.();
        const latestStatus = await core.jfjochBridge.fetchJfjochPreviewStatus();
        applyLiveSourceSnapshot(
          createLiveSourceSnapshot({
            sourceKind: "jungfraujoch",
            analysis: parsed.analysis,
            jfjochMeta: initialJfjochMeta,
            jfjochStatus: latestStatus || state.autoload.jfjochStatus || {},
            externalPeakSets: [],
          }),
        );
        core.fileSession.applyExternalFrame(data, shape, dtype, label, false, false, { autoMask: false });
        state.autoload.lastUpdate = Date.now();
        updateAutoloadMeta();
        core.autoloadStatus?.setAutoloadStatus(t("autoload.status.jfjoch.updated"));
      }
    }
    core.backendStatus.updateLiveBadge();
  }

  async function autoloadRemoteTick() {
    const sourceId = (state.autoload.remoteSourceId || "default").trim() || "default";
    const params = new URLSearchParams({ source_id: sourceId });
    if (state.autoload.lastRemoteSeq > 0) {
      params.set("after_seq", String(state.autoload.lastRemoteSeq));
    }
    let res;
    try {
      res = await fetchResponse(`${apiBase}/remote/v1/latest?${params.toString()}`, { cache: "no-store" });
    } catch (err) {
      if (!(err?.status >= 400)) throw err;
      core.autoloadStatus?.setAutoloadStatus(t("autoload.status.remote.error"));
      core.backendStatus.updateLiveBadge();
      return;
    }
    if (res.status === 204) {
      core.autoloadStatus?.setAutoloadStatus(t("autoload.status.remote.waiting"));
      core.backendStatus.updateLiveBadge();
      return;
    }
    const buffer = await res.arrayBuffer();
    const dtype = parseDtype(res.headers.get("X-Dtype"));
    const shape = parseShape(res.headers.get("X-Shape"));
    const data = typedArrayFrom(buffer, dtype);
    const parsed = parseRemoteMeta(res.headers);
    const remoteMeta = parsed.meta || {};
    const seq = Number(remoteMeta.seq || 0);
    const seqChanged = Number.isFinite(seq) && seq > 0 && seq !== state.autoload.lastRemoteSeq;
    const label =
      remoteMeta.displayName ||
      t("source.label.remote_stream_with_seq", {
        sourceId,
        seqSuffix: Number.isFinite(seq) && seq > 0 ? ` #${seq}` : "",
      });
    if (state.autoload.livePaused) {
      core.backendStatus.updateLiveBadge();
      return;
    }
    if (seqChanged) {
      state.autoload.lastRemoteSeq = seq;
      state.autoload.lastUpdate = Date.now();
      updateAutoloadMeta();
      const result = appendLiveFrame({
        sourceKey: `remote:${sourceId}`,
        sourceKind: "remote",
        dedupeKey: String(seq),
        label,
        data,
        shape,
        dtype,
        snapshot: createLiveSourceSnapshot({
          sourceKind: "remote",
          analysis: parsed.analysis,
          remoteMeta: {
            ...remoteMeta,
            peakSets: 0,
          },
          externalPeakSets: [],
        }),
      });
      const metaResult = await fetchRemoteMeta(sourceId, seq);
      const normalized = Array.isArray(metaResult?.normalized) ? metaResult.normalized : [];
      updateLiveHistoryEntry(String(seq), {
        snapshot: createLiveSourceSnapshot({
          sourceKind: "remote",
          analysis: parsed.analysis,
          remoteMeta: {
            ...remoteMeta,
            peakSets: normalized.length,
          },
          externalPeakSets: normalized,
        }),
      });
      if (result.rendered) {
        core.autoloadStatus?.setAutoloadStatus(t("autoload.status.remote.updated"));
      }
    } else {
      if (!(Number.isFinite(seq) && seq > 0)) {
        resetLiveHistory?.();
        applyLiveSourceSnapshot(
          createLiveSourceSnapshot({
            sourceKind: "remote",
            analysis: parsed.analysis,
            remoteMeta,
            externalPeakSets: [],
          }),
        );
        core.fileSession.applyExternalFrame(data, shape, dtype, label, false, false, { autoMask: false });
        state.autoload.lastUpdate = Date.now();
        updateAutoloadMeta();
        core.autoloadStatus?.setAutoloadStatus(t("autoload.status.remote.updated"));
      }
    }
    core.backendStatus.updateLiveBadge();
  }

  return {
    autoloadJfjochTick,
    autoloadRemoteTick,
  };
}
