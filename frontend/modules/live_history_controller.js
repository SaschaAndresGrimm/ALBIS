/**
 * Frontend-only live-history ring buffer for live sources.
 */

import { t } from "./i18n.js";

const LIVE_SOURCE_MODES = new Set(["simplon", "remote", "jungfraujoch"]);

function isTypedArray(value) {
  return ArrayBuffer.isView(value) && !(value instanceof DataView);
}

function cloneObject(value) {
  return value && typeof value === "object" ? { ...value } : {};
}

function clonePeakSets(peakSets) {
  if (!Array.isArray(peakSets)) return [];
  return peakSets.map((set) => ({
    ...cloneObject(set),
    points: Array.isArray(set?.points)
      ? set.points.map((point) => ({
          ...cloneObject(point),
        }))
      : [],
  }));
}

function cloneAnalysis(analysis) {
  const source = analysis && typeof analysis === "object" ? analysis : {};
  return {
    distanceMm: Number.isFinite(Number(source.distanceMm)) ? Number(source.distanceMm) : null,
    pixelSizeUm: Number.isFinite(Number(source.pixelSizeUm)) ? Number(source.pixelSizeUm) : null,
    energyEv: Number.isFinite(Number(source.energyEv)) ? Number(source.energyEv) : null,
    centerX: Number.isFinite(Number(source.centerX)) ? Number(source.centerX) : null,
    centerY: Number.isFinite(Number(source.centerY)) ? Number(source.centerY) : null,
  };
}

function cloneSnapshot(snapshot) {
  const source = snapshot && typeof snapshot === "object" ? snapshot : {};
  return {
    sourceKind: String(source.sourceKind || ""),
    analysis: cloneAnalysis(source.analysis),
    simplonMeta: cloneObject(source.simplonMeta),
    remoteMeta: cloneObject(source.remoteMeta),
    jfjochMeta: cloneObject(source.jfjochMeta),
    jfjochStatus: cloneObject(source.jfjochStatus),
    externalPeakSets: clonePeakSets(source.externalPeakSets),
  };
}

function mergeSnapshot(baseSnapshot, patchSnapshot) {
  const base = cloneSnapshot(baseSnapshot);
  const patch = patchSnapshot && typeof patchSnapshot === "object" ? patchSnapshot : {};
  const merged = {
    sourceKind: String(patch.sourceKind || base.sourceKind || ""),
    analysis: cloneAnalysis(patch.analysis ?? base.analysis),
    simplonMeta: cloneObject(
      patch.simplonMeta === undefined ? base.simplonMeta : patch.simplonMeta,
    ),
    remoteMeta: cloneObject(
      patch.remoteMeta === undefined ? base.remoteMeta : patch.remoteMeta,
    ),
    jfjochMeta: cloneObject(
      patch.jfjochMeta === undefined ? base.jfjochMeta : patch.jfjochMeta,
    ),
    jfjochStatus: cloneObject(
      patch.jfjochStatus === undefined ? base.jfjochStatus : patch.jfjochStatus,
    ),
    externalPeakSets: clonePeakSets(
      patch.externalPeakSets === undefined ? base.externalPeakSets : patch.externalPeakSets,
    ),
  };
  return merged;
}

export function createLiveHistoryController({
  state,
  callbacks,
}) {
  const {
    applyExternalFrame,
    applyLiveSourceSnapshot,
    updateFrameControls,
    updatePlayButtons,
    updateToolbar,
    updateAutoloadUI,
    setStatus,
  } = callbacks;

  let historySourceKey = "";

  function getEntries() {
    return Array.isArray(state.autoload.historyEntries) ? state.autoload.historyEntries : [];
  }

  function isLiveMode(mode = state.autoload.mode) {
    return LIVE_SOURCE_MODES.has(String(mode || "").toLowerCase());
  }

  function hasLiveHistory() {
    return getEntries().length > 0;
  }

  function isLiveHistoryActive() {
    return isLiveMode() && hasLiveHistory();
  }

  function syncPendingNewFrames() {
    const entries = getEntries();
    if (!entries.length || state.autoload.followingLatest || state.autoload.livePaused) {
      state.autoload.pendingNewFrames = 0;
      return;
    }
    const lastIndex = entries.length - 1;
    const cursor = Math.max(0, Math.min(lastIndex, Number(state.autoload.historyCursor) || 0));
    state.autoload.pendingNewFrames = Math.max(0, lastIndex - cursor);
  }

  function syncNavigationState() {
    if (!isLiveMode()) {
      updateFrameControls();
      updatePlayButtons();
      updateToolbar();
      return;
    }
    const entries = getEntries();
    state.frameCount = Math.max(1, entries.length || 1);
    const lastIndex = Math.max(0, state.frameCount - 1);
    const cursor = Math.max(0, Math.min(lastIndex, Number(state.autoload.historyCursor) || 0));
    state.autoload.historyCursor = cursor;
    state.frameIndex = cursor;
    syncPendingNewFrames();
    updateFrameControls();
    updatePlayButtons();
    updateAutoloadUI?.();
    updateToolbar();
  }

  function renderEntryAt(
    index,
    {
      followingLatest = state.autoload.followingLatest,
      livePaused = state.autoload.livePaused,
      updateStatus = false,
    } = {},
  ) {
    const entries = getEntries();
    if (!entries.length) return false;
    const clamped = Math.max(0, Math.min(entries.length - 1, Math.round(Number(index) || 0)));
    const entry = entries[clamped];
    if (!entry) return false;
    state.autoload.historyCursor = clamped;
    state.autoload.followingLatest = Boolean(followingLatest);
    state.autoload.livePaused = Boolean(livePaused);
    syncPendingNewFrames();
    applyLiveSourceSnapshot(entry.snapshot);
    applyExternalFrame(
      entry.data,
      entry.shape,
      entry.dtype,
      entry.label,
      false,
      entry.sourceKind === "simplon",
      { autoMask: false },
    );
    syncNavigationState();
    if (updateStatus) {
      if (state.autoload.followingLatest) {
        setStatus(t("status.frame.position", { current: clamped + 1, total: entries.length }));
      } else if (state.autoload.pendingNewFrames > 0) {
        setStatus(t("status.live_history.paused_new_frames", { count: state.autoload.pendingNewFrames }));
      } else {
        setStatus(t("status.live_history.paused"));
      }
    }
    return true;
  }

  function resetLiveHistory() {
    const hadHistory = getEntries().length > 0;
    historySourceKey = "";
    state.autoload.historyEntries = [];
    state.autoload.historyCursor = 0;
    state.autoload.followingLatest = true;
    state.autoload.livePaused = false;
    state.autoload.pendingNewFrames = 0;
    if (hadHistory || isLiveMode()) {
      state.frameCount = 1;
      state.frameIndex = 0;
    }
    syncNavigationState();
  }

  function appendLiveFrame(entry) {
    const shape = Array.isArray(entry?.shape) ? entry.shape.slice() : [];
    if (shape.length < 2 || !isTypedArray(entry?.data)) {
      return { appended: false, rendered: false };
    }
    const sourceKey = String(entry.sourceKey || "");
    if (sourceKey && historySourceKey && sourceKey !== historySourceKey) {
      resetLiveHistory();
    }
    if (sourceKey) {
      historySourceKey = sourceKey;
    }
    const entries = getEntries();
    const dedupeKey = String(entry.dedupeKey || "");
    if (entries.length && dedupeKey && String(entries[entries.length - 1]?.dedupeKey || "") === dedupeKey) {
      return { appended: false, rendered: false };
    }
    const normalizedEntry = {
      sourceKey,
      sourceKind: String(entry.sourceKind || ""),
      dedupeKey,
      label: String(entry.label || ""),
      data: entry.data,
      shape,
      dtype: String(entry.dtype || ""),
      snapshot: cloneSnapshot(entry.snapshot),
    };
    const nextEntries = [...entries, normalizedEntry];
    const capacity = Math.max(1, Number(state.autoload.historyCapacity) || 8);
    let dropped = 0;
    while (nextEntries.length > capacity) {
      nextEntries.shift();
      dropped += 1;
    }
    state.autoload.historyEntries = nextEntries;
    if (state.autoload.followingLatest || nextEntries.length === 1) {
      state.autoload.historyCursor = nextEntries.length - 1;
      state.autoload.followingLatest = true;
      state.autoload.livePaused = false;
      const rendered = renderEntryAt(state.autoload.historyCursor, {
        followingLatest: true,
        livePaused: false,
        updateStatus: false,
      });
      return { appended: true, rendered };
    }
    const previousCursor = Math.max(0, Number(state.autoload.historyCursor) || 0);
    state.autoload.historyCursor = Math.max(
      0,
      Math.min(nextEntries.length - 1, previousCursor - dropped),
    );
    if (dropped > 0 && previousCursor < dropped) {
      renderEntryAt(state.autoload.historyCursor, {
        followingLatest: false,
        livePaused: true,
        updateStatus: true,
      });
      return { appended: true, rendered: false };
    }
    syncNavigationState();
    if (state.autoload.pendingNewFrames > 0) {
      setStatus(t("status.live_history.paused_new_frames", { count: state.autoload.pendingNewFrames }));
    } else {
      setStatus(t("status.live_history.paused"));
    }
    return { appended: true, rendered: false };
  }

  function showLiveHistoryFrame(index) {
    if (!isLiveHistoryActive()) return false;
    return renderEntryAt(index, {
      followingLatest: false,
      livePaused: true,
      updateStatus: true,
    });
  }

  function goLive() {
    if (!isLiveHistoryActive()) return false;
    return renderEntryAt(getEntries().length - 1, {
      followingLatest: true,
      livePaused: false,
      updateStatus: true,
    });
  }

  function pauseLive() {
    if (!isLiveHistoryActive()) return false;
    return renderEntryAt(state.frameIndex, {
      followingLatest: false,
      livePaused: true,
      updateStatus: true,
    });
  }

  function updateLiveHistoryEntry(dedupeKey, patch = {}) {
    const key = String(dedupeKey || "");
    if (!key) return false;
    const entries = getEntries();
    const index = entries.findIndex((entry) => String(entry?.dedupeKey || "") === key);
    if (index < 0) return false;
    const current = entries[index];
    entries[index] = {
      ...current,
      label: patch.label === undefined ? current.label : String(patch.label || current.label),
      snapshot: patch.snapshot === undefined ? current.snapshot : mergeSnapshot(current.snapshot, patch.snapshot),
    };
    state.autoload.historyEntries = [...entries];
    if (index === state.autoload.historyCursor) {
      applyLiveSourceSnapshot(entries[index].snapshot);
      if (patch.label !== undefined) {
        state.file = entries[index].label;
        updateToolbar();
      }
      updatePlayButtons();
    }
    return true;
  }

  return {
    resetLiveHistory,
    appendLiveFrame,
    showLiveHistoryFrame,
    goLive,
    pauseLive,
    hasLiveHistory,
    isLiveHistoryActive,
    updateLiveHistoryEntry,
  };
}
