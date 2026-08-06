/**
 * Autoload runtime orchestration: start/stop/tick state machine.
 */

import { t } from "./i18n.js";
import { normalizeJfjochEndpointInput } from "./jfjoch_endpoint_utils.js";
import { recordSimplonHost, renderSimplonHostOptions } from "./simplon_host_history.js";
import { normalizeSimplonUrlInput } from "./simplon_url_utils.js";

export function createAutoloadOrchestrationController({
  state,
  analysisState,
  elements,
  callbacks,
}) {
  const {
    autoloadMode,
    autoloadWatchEnabled,
    autoloadDir,
    autoloadInterval,
    autoloadTypeHdf5,
    autoloadTypeTiff,
    autoloadTypeCbf,
    autoloadPattern,
    simplonUrl,
    simplonUrlList,
    simplonVersion,
    simplonTimeout,
    simplonEnable,
    remoteSourceInput,
    remoteIntervalInput,
    jfjochEndpointInput,
    jfjochSourceInput,
    jfjochTopicInput,
    jfjochChannelInput,
    jfjochIntervalInput,
  } = elements;

  const {
    updateAutoloadUI,
    updateAutoloadMeta,
    setAutoloadStatus,
    setStatus,
    persistAutoloadSettings,
    resetLiveHistory,
    setSimplonMode,
    fetchSimplonMask,
    updateLiveBadge,
    stopJfjochPreviewBridge,
    updateRemoteMetaUI,
    updateJfjochMetaUI,
    schedulePeakOverlay,
    autoloadWatchTick,
    autoloadSimplonTick,
    autoloadJfjochTick,
    autoloadRemoteTick,
  } = callbacks;

  async function stopAutoload({ keepMode = true, disableMonitor = true } = {}) {
    const previousMode = state.autoload.mode;
    if (state.autoload.timer) {
      window.clearInterval(state.autoload.timer);
      state.autoload.timer = null;
    }
    if (state.autoload.running && state.autoload.mode === "simplon" && disableMonitor) {
      await setSimplonMode(false);
    }
    if (state.autoload.running && state.autoload.mode === "jungfraujoch") {
      await stopJfjochPreviewBridge();
    }
    state.autoload.running = false;
    state.autoload.busy = false;
    state.autoload.autoStart = keepMode ? state.autoload.autoStart : false;
    state.autoload.livePaused = false;
    if (!keepMode) {
      state.autoload.mode = "file";
    }
    resetLiveHistory?.();
    if (previousMode === "remote") {
      state.autoload.remoteMeta = {};
      state.autoload.lastRemoteSeq = 0;
      state.autoload.remoteSeq = 0;
      analysisState.externalPeakSets = [];
      updateRemoteMetaUI({});
      schedulePeakOverlay();
    } else if (previousMode === "jungfraujoch") {
      state.autoload.jfjochMeta = {};
      state.autoload.jfjochStatus = {};
      state.autoload.lastJfjochSeq = 0;
      analysisState.externalPeakSets = [];
      updateJfjochMetaUI({}, {});
      schedulePeakOverlay();
    }
    updateAutoloadUI();
    setAutoloadStatus(state.autoload.mode === "file" ? t("autoload.status.idle") : t("autoload.status.stopped"));
    updateLiveBadge();
    persistAutoloadSettings();
  }

  async function startAutoload() {
    state.autoload.mode = autoloadMode?.value || state.autoload.mode;
    state.autoload.watchEnabled = autoloadWatchEnabled?.checked ?? state.autoload.watchEnabled;
    state.autoload.dir = autoloadDir?.value?.trim() || "";
    state.autoload.interval = Math.max(200, Number(autoloadInterval?.value || 1000));
    state.autoload.types = {
      hdf5: autoloadTypeHdf5?.checked ?? true,
      tiff: autoloadTypeTiff?.checked ?? true,
      cbf: autoloadTypeCbf?.checked ?? true,
    };
    state.autoload.pattern = autoloadPattern?.value?.trim() || "";
    state.autoload.simplonUrl = normalizeSimplonUrlInput(simplonUrl);
    state.autoload.simplonVersion = simplonVersion?.value?.trim() || "1.8.0";
    state.autoload.simplonTimeout = Math.max(100, Number(simplonTimeout?.value || 500));
    state.autoload.simplonEnable = simplonEnable?.checked ?? true;
    state.autoload.remoteSourceId =
      (remoteSourceInput?.value || state.autoload.remoteSourceId || "default").trim() || "default";
    state.autoload.jfjochEndpoint =
      normalizeJfjochEndpointInput(jfjochEndpointInput) || state.autoload.jfjochEndpoint || "";
    state.autoload.jfjochSourceId =
      (jfjochSourceInput?.value || state.autoload.jfjochSourceId || "jungfraujoch").trim() || "jungfraujoch";
    state.autoload.jfjochTopic = (jfjochTopicInput?.value || state.autoload.jfjochTopic || "").trim();
    state.autoload.jfjochChannel = (jfjochChannelInput?.value || state.autoload.jfjochChannel || "").trim();
    state.autoload.jfjochInterval = Math.max(
      100,
      Number(jfjochIntervalInput?.value || state.autoload.jfjochInterval || 250),
    );
    if (state.autoload.mode === "remote") {
      state.autoload.interval = Math.max(100, Number(remoteIntervalInput?.value || state.autoload.interval || 1000));
    } else if (state.autoload.mode === "jungfraujoch") {
      state.autoload.interval = Math.max(100, Number(state.autoload.jfjochInterval || 250));
    }

    if (state.autoload.mode === "file" && !state.autoload.watchEnabled) {
      await stopAutoload({ keepMode: false });
      return;
    }

    await stopAutoload({ keepMode: true, disableMonitor: false });
    state.autoload.running = true;
    state.autoload.autoStart = true;
    state.autoload.lastFile = "";
    state.autoload.lastMtime = 0;
    state.autoload.lastUpdate = 0;
    state.autoload.lastPoll = 0;
    state.autoload.lastMonitorSig = "";
    state.autoload.lastRemoteSeq = 0;
    state.autoload.lastJfjochSeq = 0;
    state.autoload.remoteSeq = 0;
    state.autoload.remoteMeta = {};
    state.autoload.jfjochMeta = {};
    state.autoload.jfjochStatus = {};
    state.autoload.livePaused = false;
    analysisState.externalPeakSets = [];
    resetLiveHistory?.();
    updateAutoloadUI();
    updateAutoloadMeta();
    setAutoloadStatus(
      `${t("autoload.running")} (${
        state.autoload.mode === "file" && state.autoload.watchEnabled
          ? t("autoload.mode.watch_folder")
          : state.autoload.mode === "simplon"
            ? t("autoload.mode.simplon_monitor")
            : state.autoload.mode === "jungfraujoch"
              ? t("autoload.mode.jfjoch_preview")
              : t("autoload.mode.remote_stream")
      })`
    );
    persistAutoloadSettings();

    if (state.autoload.mode === "simplon" && state.autoload.simplonEnable) {
      if (recordSimplonHost(state, state.autoload.simplonUrl)) {
        renderSimplonHostOptions(simplonUrlList, state.autoload.simplonRecentHosts);
        persistAutoloadSettings();
      }
      setStatus(t("status.autoload.simplon_monitor"));
      await setSimplonMode(true);
      state.autoload.lastMaskAttempt = Date.now();
      await fetchSimplonMask();
    } else if (state.autoload.mode === "jungfraujoch") {
      setStatus(t("status.autoload.jfjoch_preview"));
    }

    updateLiveBadge();
    autoloadTick();
    state.autoload.timer = window.setInterval(autoloadTick, state.autoload.interval);
  }

  async function ensureFileMode() {
    if (state.autoload.watchEnabled) {
      state.autoload.watchEnabled = false;
    }
    if (state.autoload.running || state.autoload.mode !== "file") {
      await stopAutoload({ keepMode: false, disableMonitor: true });
    } else {
      updateAutoloadUI();
      setAutoloadStatus(t("autoload.status.idle"));
      persistAutoloadSettings();
    }
    if (analysisState.externalPeakSets.length) {
      analysisState.externalPeakSets = [];
      schedulePeakOverlay();
    }
  }

  async function autoloadTick() {
    if (!state.autoload.running || state.autoload.busy) return;
    if (state.autoload.livePaused) return;
    if (state.isLoading) return;
    state.autoload.busy = true;
    state.autoload.lastPoll = Date.now();
    updateAutoloadMeta();
    try {
      if (state.autoload.mode === "file" && state.autoload.watchEnabled) {
        await autoloadWatchTick();
      } else if (state.autoload.mode === "simplon") {
        await autoloadSimplonTick();
      } else if (state.autoload.mode === "jungfraujoch") {
        await autoloadJfjochTick();
      } else if (state.autoload.mode === "remote") {
        await autoloadRemoteTick();
      }
    } finally {
      state.autoload.busy = false;
    }
  }

  return {
    startAutoload,
    stopAutoload,
    ensureFileMode,
    autoloadTick,
  };
}
