/**
 * Autoload settings persistence and UI synchronization.
 */

import { t } from "./i18n.js";
import { normalizeJfjochEndpoint } from "./jfjoch_endpoint_utils.js";
import { renderSimplonHostOptions, sanitizeRecentSimplonHosts } from "./simplon_host_history.js";
import { normalizeSimplonBaseUrl } from "./simplon_url_utils.js";

export function createAutoloadSettingsController({
  state,
  elements,
  callbacks,
}) {
  const {
    autoloadMode,
    autoloadFolder,
    autoloadWatch,
    autoloadWatchEnabled,
    autoloadWatchOptions,
    autoloadTypesRow,
    autoloadSimplon,
    autoloadSimplonAdvanced,
    autoloadStatusBlock,
    autoloadStatusPrimarySlot,
    autoloadStatusAdvancedSlot,
    autoloadRemote,
    autoloadJfjoch,
    filesystemField,
    fileField,
    datasetField,
    thresholdField,
    toolbarFrameWrap,
    toolbarFrameIndexWrap,
    toolbarStepWrap,
    toolbarFpsWrap,
    toolbarPlaybackWrap,
    toolbarMoreStepField,
    toolbarMoreFpsField,
    autoloadStatus,
    simplonMetaPanel,
    remoteMetaPanel,
    jfjochMetaPanel,
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
    closeToolbarPlaybackPopover,
    updateSimplonMetaUI,
    updateRemoteMetaUI,
    updateJfjochMetaUI,
    updateAutoloadMeta,
    updateLiveBadge,
    updateThresholdOptions,
    updateDataSourceSummary,
    setDataSourceSectionState,
    setAutoloadStatus,
    setAutoloadLatest,
    updatePlayButtons,
    startAutoload,
    isBackendLocal = () => false,
  } = callbacks;

  function placeAutoloadStatusBlock() {
    if (!autoloadStatusBlock) return;
    const target = state.autoload.mode === "simplon" ? autoloadStatusPrimarySlot : autoloadStatusAdvancedSlot;
    if (target && autoloadStatusBlock.parentElement !== target) {
      target.appendChild(autoloadStatusBlock);
    }
  }

  function persistAutoloadSettings() {
    try {
      const payload = {
        mode: state.autoload.mode,
        watchEnabled: state.autoload.watchEnabled,
        dir: state.autoload.dir,
        interval: state.autoload.interval,
        types: state.autoload.types,
        pattern: state.autoload.pattern,
        simplonUrl: state.autoload.simplonUrl,
        simplonRecentHosts: state.autoload.simplonRecentHosts,
        simplonVersion: state.autoload.simplonVersion,
        simplonTimeout: state.autoload.simplonTimeout,
        simplonEnable: state.autoload.simplonEnable,
        remoteSourceId: state.autoload.remoteSourceId,
        jfjochEndpoint: state.autoload.jfjochEndpoint,
        jfjochSourceId: state.autoload.jfjochSourceId,
        jfjochTopic: state.autoload.jfjochTopic,
        jfjochChannel: state.autoload.jfjochChannel,
        jfjochInterval: state.autoload.jfjochInterval,
        autoStart: state.autoload.autoStart,
      };
      localStorage.setItem("albis.autoload", JSON.stringify(payload));
    } catch {
      // ignore storage errors
    }
  }

  function updateAutoloadUI() {
    placeAutoloadStatusBlock();
    if (autoloadMode) autoloadMode.value = state.autoload.mode;
    if (autoloadFolder) {
      autoloadFolder.classList.toggle(
        "is-hidden",
        state.autoload.mode === "simplon" ||
          state.autoload.mode === "remote" ||
          state.autoload.mode === "jungfraujoch",
      );
    }
    if (autoloadWatch) autoloadWatch.classList.toggle("is-hidden", state.autoload.mode !== "file");
    if (autoloadWatchEnabled) autoloadWatchEnabled.checked = Boolean(state.autoload.watchEnabled);
    if (autoloadWatchOptions) autoloadWatchOptions.classList.toggle("is-hidden", !state.autoload.watchEnabled);
    if (autoloadTypesRow) autoloadTypesRow.classList.toggle("is-hidden", !state.autoload.watchEnabled);
    if (autoloadSimplon) autoloadSimplon.classList.toggle("is-hidden", state.autoload.mode !== "simplon");
    if (autoloadSimplonAdvanced) {
      autoloadSimplonAdvanced.classList.toggle("is-hidden", state.autoload.mode !== "simplon");
    }
    if (autoloadRemote) autoloadRemote.classList.toggle("is-hidden", state.autoload.mode !== "remote");
    if (autoloadJfjoch) autoloadJfjoch.classList.toggle("is-hidden", state.autoload.mode !== "jungfraujoch");
    if (filesystemField) {
      filesystemField.classList.toggle("is-hidden", Boolean(isBackendLocal()) || state.autoload.mode !== "file");
    }
    const hideDatasetUi =
      state.autoload.mode === "simplon" ||
      state.autoload.mode === "remote" ||
      state.autoload.mode === "jungfraujoch";
    if (fileField) fileField.classList.toggle("is-hidden", hideDatasetUi);
    if (datasetField) datasetField.classList.toggle("is-hidden", hideDatasetUi);
    if (thresholdField) thresholdField.classList.toggle("is-hidden", hideDatasetUi);
    const liveHistoryLength = Array.isArray(state.autoload.historyEntries) ? state.autoload.historyEntries.length : 0;
    const showFrameControls = state.autoload.mode === "file" || liveHistoryLength > 1;
    const showPlaybackControls = state.autoload.mode === "file";
    if (toolbarFrameWrap) toolbarFrameWrap.classList.toggle("is-hidden", !showFrameControls);
    if (toolbarFrameIndexWrap) toolbarFrameIndexWrap.classList.toggle("is-hidden", !showFrameControls);
    if (toolbarStepWrap) toolbarStepWrap.classList.toggle("is-hidden", !showPlaybackControls);
    if (toolbarFpsWrap) toolbarFpsWrap.classList.toggle("is-hidden", !showPlaybackControls);
    if (toolbarPlaybackWrap) {
      toolbarPlaybackWrap.classList.toggle("is-hidden", !showPlaybackControls);
      if (!showPlaybackControls) {
        closeToolbarPlaybackPopover();
      }
    }
    if (toolbarMoreStepField) toolbarMoreStepField.classList.toggle("is-hidden", !showPlaybackControls);
    if (toolbarMoreFpsField) toolbarMoreFpsField.classList.toggle("is-hidden", !showPlaybackControls);
    const showAutoloadStatusBlock = state.autoload.mode !== "file" || Boolean(state.autoload.watchEnabled);
    if (autoloadStatusBlock) autoloadStatusBlock.classList.toggle("is-hidden", !showAutoloadStatusBlock);
    if (autoloadStatus) {
      const meta = autoloadStatus.closest(".autoload-meta");
      if (meta) meta.classList.toggle("is-hidden", !showAutoloadStatusBlock);
    }
    if (simplonMetaPanel) {
      simplonMetaPanel.classList.toggle("is-hidden", state.autoload.mode !== "simplon");
      if (state.autoload.mode === "simplon") {
        updateSimplonMetaUI(state.autoload.simplonMeta || {});
      }
    }
    if (remoteMetaPanel) {
      remoteMetaPanel.classList.toggle("is-hidden", state.autoload.mode !== "remote");
      if (state.autoload.mode === "remote") {
        updateRemoteMetaUI(state.autoload.remoteMeta || {});
      }
    }
    if (jfjochMetaPanel) {
      jfjochMetaPanel.classList.toggle("is-hidden", state.autoload.mode !== "jungfraujoch");
      if (state.autoload.mode === "jungfraujoch") {
        updateJfjochMetaUI(state.autoload.jfjochMeta || {}, state.autoload.jfjochStatus || {});
      }
    }
    updateAutoloadMeta();
    updateLiveBadge();
    updateThresholdOptions();
    updatePlayButtons();
    updateDataSourceSummary();
    if (state.autoload.mode === "file") {
      setDataSourceSectionState(
        state.file ? "active" : "empty",
        state.file ? t("data_source.state.file_mode_ready") : t("data_source.state.select_file_to_begin"),
      );
    } else if (state.autoload.running) {
      const label =
        state.autoload.mode === "simplon"
          ? t("data_source.state.simplon_active")
          : state.autoload.mode === "jungfraujoch"
            ? t("data_source.state.jfjoch_active")
            : t("data_source.state.remote_active");
      setDataSourceSectionState("active", label);
    } else {
      const label =
        state.autoload.mode === "simplon"
          ? t("data_source.state.configure_simplon")
          : state.autoload.mode === "jungfraujoch"
            ? t("data_source.state.configure_jfjoch")
            : t("data_source.state.configure_remote");
      setDataSourceSectionState("empty", label);
    }
  }

  function loadAutoloadSettings() {
    try {
      const raw = localStorage.getItem("albis.autoload");
      if (raw) {
        const stored = JSON.parse(raw);
        if (stored && typeof stored === "object") {
          const storedMode = stored.mode || state.autoload.mode;
          state.autoload.mode = storedMode === "off" || storedMode === "watch" ? "file" : storedMode;
          state.autoload.watchEnabled =
            stored.watchEnabled !== undefined ? Boolean(stored.watchEnabled) : storedMode === "watch";
          state.autoload.dir = stored.dir || "";
          state.autoload.interval = Number(stored.interval || state.autoload.interval);
          if (stored.types && typeof stored.types === "object") {
            state.autoload.types = {
              hdf5: stored.types.hdf5 !== false,
              tiff: stored.types.tiff !== false,
              cbf: stored.types.cbf !== false,
            };
          }
          state.autoload.pattern = stored.pattern || "";
          // Settings persisted before URL normalization may hold a bare host.
          state.autoload.simplonUrl = normalizeSimplonBaseUrl(stored.simplonUrl);
          state.autoload.simplonRecentHosts = sanitizeRecentSimplonHosts(stored.simplonRecentHosts);
          state.autoload.simplonVersion = stored.simplonVersion || state.autoload.simplonVersion;
          state.autoload.simplonTimeout = Number(stored.simplonTimeout || state.autoload.simplonTimeout);
          state.autoload.simplonEnable =
            stored.simplonEnable !== undefined ? Boolean(stored.simplonEnable) : state.autoload.simplonEnable;
          state.autoload.remoteSourceId = String(stored.remoteSourceId || state.autoload.remoteSourceId || "default");
          state.autoload.jfjochEndpoint = normalizeJfjochEndpoint(
            stored.jfjochEndpoint || state.autoload.jfjochEndpoint || "",
          );
          state.autoload.jfjochSourceId = String(stored.jfjochSourceId || state.autoload.jfjochSourceId || "jungfraujoch");
          state.autoload.jfjochTopic = String(stored.jfjochTopic || state.autoload.jfjochTopic || "");
          state.autoload.jfjochChannel = String(stored.jfjochChannel || state.autoload.jfjochChannel || "");
          state.autoload.jfjochInterval = Math.max(
            100,
            Number(stored.jfjochInterval || state.autoload.jfjochInterval || 250),
          );
          state.autoload.autoStart = Boolean(stored.autoStart);
        }
      }
    } catch {
      // ignore storage errors
    }
    if (autoloadMode) autoloadMode.value = state.autoload.mode;
    if (autoloadWatchEnabled) autoloadWatchEnabled.checked = Boolean(state.autoload.watchEnabled);
    if (autoloadDir) autoloadDir.value = state.autoload.dir;
    if (autoloadInterval) autoloadInterval.value = String(state.autoload.interval || 1000);
    if (autoloadTypeHdf5) autoloadTypeHdf5.checked = state.autoload.types.hdf5;
    if (autoloadTypeTiff) autoloadTypeTiff.checked = state.autoload.types.tiff;
    if (autoloadTypeCbf) autoloadTypeCbf.checked = state.autoload.types.cbf;
    if (autoloadPattern) autoloadPattern.value = state.autoload.pattern;
    if (simplonUrl) simplonUrl.value = state.autoload.simplonUrl;
    renderSimplonHostOptions(simplonUrlList, state.autoload.simplonRecentHosts);
    if (simplonVersion) simplonVersion.value = state.autoload.simplonVersion;
    if (simplonTimeout) simplonTimeout.value = String(state.autoload.simplonTimeout || 500);
    if (simplonEnable) simplonEnable.checked = Boolean(state.autoload.simplonEnable);
    if (remoteSourceInput) remoteSourceInput.value = state.autoload.remoteSourceId || "default";
    if (remoteIntervalInput) remoteIntervalInput.value = String(state.autoload.interval || 1000);
    if (jfjochEndpointInput) jfjochEndpointInput.value = state.autoload.jfjochEndpoint || "";
    if (jfjochSourceInput) jfjochSourceInput.value = state.autoload.jfjochSourceId || "jungfraujoch";
    if (jfjochTopicInput) jfjochTopicInput.value = state.autoload.jfjochTopic || "";
    if (jfjochChannelInput) jfjochChannelInput.value = state.autoload.jfjochChannel || "";
    if (jfjochIntervalInput) jfjochIntervalInput.value = String(state.autoload.jfjochInterval || 250);
    updateAutoloadUI();
    setAutoloadStatus(t("autoload.status.idle"));
    setAutoloadLatest("-");
    if (state.autoload.mode !== "file" || state.autoload.watchEnabled) {
      startAutoload();
    }
  }

  return {
    persistAutoloadSettings,
    updateAutoloadUI,
    loadAutoloadSettings,
  };
}
