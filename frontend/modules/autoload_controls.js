/**
 * Bind autoload + live-source control listeners.
 *
 * This module keeps event wiring separate from app-level orchestration logic.
 */

import { attachDetectorPicker } from "./detector_picker.js";
import { createJfjochProbeController } from "./jfjoch_probe_controller.js";
import { normalizeJfjochEndpointInput } from "./jfjoch_endpoint_utils.js";
import { createSimplonProbeController } from "./simplon_probe_controller.js";
import { normalizeSimplonUrlInput } from "./simplon_url_utils.js";
import { chooseNativePath } from "./native_picker.js";

export function bindAutoloadControls({
  apiBase,
  state,
  analysisState,
  backendIsLocal,
  elements,
  callbacks,
  core,
}) {
  const {
    autoloadMode,
    autoloadWatchEnabled,
    autoloadDir,
    autoloadInterval,
    remoteIntervalInput,
    jfjochIntervalInput,
    remoteSourceInput,
    jfjochSourceInput,
    jfjochEndpointInput,
    jfjochTest,
    jfjochProbeMessage,
    jfjochTopicInput,
    jfjochChannelInput,
    autoloadTypeHdf5,
    autoloadTypeTiff,
    autoloadTypeCbf,
    autoloadPattern,
    autoloadBrowse,
    autoloadSelectFile,
    filesystemMode,
    simplonUrl,
    simplonTest,
    simplonProbeMessage,
    simplonVersion,
    simplonTimeout,
    simplonEnable,
  } = elements;

  const {
    updateAutoloadUI,
    persistAutoloadSettings,
    updateRemoteMetaUI,
    schedulePeakOverlay,
    openFileBrowser,
    openFileModal,
    handleLocalFileSelection,
    logClient,
  } = callbacks;

  autoloadMode?.addEventListener("change", async () => {
    const nextMode = autoloadMode.value;
    if (state.autoload.running) {
      await core.autoloadOrchestration.stopAutoload();
    }
    state.autoload.mode = nextMode;
    updateAutoloadUI();
    persistAutoloadSettings();
    if (state.autoload.mode === "file") {
      core.frameMetadata?.loadFiles().catch((err) => console.error(err));
      if (state.autoload.watchEnabled) {
        core.autoloadOrchestration.startAutoload().catch((err) => console.error(err));
      }
    } else {
      core.autoloadOrchestration.startAutoload().catch((err) => console.error(err));
    }
  });

  autoloadWatchEnabled?.addEventListener("change", () => {
    state.autoload.watchEnabled = autoloadWatchEnabled.checked;
    updateAutoloadUI();
    persistAutoloadSettings();
    if (state.autoload.mode !== "file") return;
    if (state.autoload.watchEnabled) {
      core.autoloadOrchestration.startAutoload();
    } else if (state.autoload.running) {
      core.autoloadOrchestration.stopAutoload({ keepMode: true, disableMonitor: false });
    }
  });

  autoloadDir?.addEventListener("change", () => {
    state.autoload.dir = autoloadDir.value.trim();
    persistAutoloadSettings();
    if (state.autoload.mode === "file") {
      core.frameMetadata?.loadFiles().catch((err) => console.error(err));
    }
    if (state.autoload.running && state.autoload.mode === "file" && state.autoload.watchEnabled) {
      core.autoloadOrchestration.autoloadTick();
    }
  });

  autoloadInterval?.addEventListener("change", () => {
    state.autoload.interval = Math.max(200, Number(autoloadInterval.value || 1000));
    persistAutoloadSettings();
    if (state.autoload.running && state.autoload.mode === "file" && state.autoload.watchEnabled) {
      core.autoloadOrchestration.startAutoload();
    }
  });

  remoteIntervalInput?.addEventListener("change", () => {
    state.autoload.interval = Math.max(100, Number(remoteIntervalInput.value || 1000));
    persistAutoloadSettings();
    if (state.autoload.running && state.autoload.mode === "remote") {
      core.autoloadOrchestration.startAutoload();
    }
  });

  jfjochIntervalInput?.addEventListener("change", () => {
    state.autoload.jfjochInterval = Math.max(100, Number(jfjochIntervalInput.value || 250));
    persistAutoloadSettings();
    if (state.autoload.running && state.autoload.mode === "jungfraujoch") {
      core.autoloadOrchestration.startAutoload();
    }
  });

  remoteSourceInput?.addEventListener("change", () => {
    state.autoload.remoteSourceId = (remoteSourceInput.value || "default").trim() || "default";
    persistAutoloadSettings();
    if (state.autoload.running && state.autoload.mode === "remote") {
      state.autoload.lastRemoteSeq = 0;
      state.autoload.remoteSeq = 0;
      state.autoload.remoteMeta = {};
      analysisState.externalPeakSets = [];
      updateRemoteMetaUI({});
      schedulePeakOverlay();
      core.autoloadOrchestration.autoloadTick();
    }
  });

  jfjochSourceInput?.addEventListener("change", () => {
    state.autoload.jfjochSourceId = (jfjochSourceInput.value || "jungfraujoch").trim() || "jungfraujoch";
    persistAutoloadSettings();
    if (state.autoload.running && state.autoload.mode === "jungfraujoch") {
      state.autoload.lastJfjochSeq = 0;
      state.autoload.jfjochMeta = {};
      analysisState.externalPeakSets = [];
      core.sourceMetadata?.updateJfjochMetaUI(state.autoload.jfjochMeta || {}, state.autoload.jfjochStatus || {});
      schedulePeakOverlay();
      core.autoloadOrchestration.autoloadTick();
    }
  });

  const jfjochProbeController = createJfjochProbeController({
    apiBase,
    elements: { jfjochEndpointInput, jfjochTest, jfjochProbeMessage },
    callbacks: { persistAutoloadSettings, logClient },
  });

  jfjochTest?.addEventListener("click", () => {
    void jfjochProbeController.probeJfjochEndpoint();
  });

  jfjochEndpointInput?.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      void jfjochProbeController.probeJfjochEndpoint();
    }
  });

  jfjochEndpointInput?.addEventListener("change", () => {
    // Canonical form is written back so the user sees the transport and port
    // that will actually be connected.
    state.autoload.jfjochEndpoint = normalizeJfjochEndpointInput(jfjochEndpointInput);
    jfjochProbeController.clearJfjochProbeMessage();
    persistAutoloadSettings();
    if (state.autoload.running && state.autoload.mode === "jungfraujoch") {
      core.autoloadOrchestration.startAutoload();
    }
  });

  jfjochTopicInput?.addEventListener("change", () => {
    state.autoload.jfjochTopic = (jfjochTopicInput.value || "").trim();
    persistAutoloadSettings();
    if (state.autoload.running && state.autoload.mode === "jungfraujoch") {
      core.autoloadOrchestration.startAutoload();
    }
  });

  jfjochChannelInput?.addEventListener("change", () => {
    state.autoload.jfjochChannel = (jfjochChannelInput.value || "").trim();
    persistAutoloadSettings();
    if (state.autoload.running && state.autoload.mode === "jungfraujoch") {
      core.autoloadOrchestration.startAutoload();
    }
  });

  [autoloadTypeHdf5, autoloadTypeTiff, autoloadTypeCbf].forEach((input) => {
    input?.addEventListener("change", () => {
      state.autoload.types = {
        hdf5: autoloadTypeHdf5?.checked ?? true,
        tiff: autoloadTypeTiff?.checked ?? true,
        cbf: autoloadTypeCbf?.checked ?? true,
      };
      persistAutoloadSettings();
      if (state.autoload.running && state.autoload.mode === "file" && state.autoload.watchEnabled) {
        core.autoloadOrchestration.autoloadTick();
      }
    });
  });

  autoloadPattern?.addEventListener("change", () => {
    state.autoload.pattern = autoloadPattern.value.trim();
    persistAutoloadSettings();
    if (state.autoload.running && state.autoload.mode === "file" && state.autoload.watchEnabled) {
      core.autoloadOrchestration.autoloadTick();
    }
  });

  autoloadBrowse?.addEventListener("click", async () => {
    if (backendIsLocal) {
      try {
        const path = await chooseNativePath(apiBase, { kind: "folder", purpose: "autoload" });
        if (path && autoloadDir) {
          autoloadDir.value = path;
          state.autoload.dir = path;
          persistAutoloadSettings();
          if (state.autoload.mode === "file") {
            core.frameMetadata?.loadFiles().catch((err) => console.error(err));
          }
          if (state.autoload.running && state.autoload.mode === "file" && state.autoload.watchEnabled) {
            core.autoloadOrchestration.autoloadTick();
          }
        }
        return;
      } catch (err) {
        console.error(err);
      }
      openFileBrowser("autoload", autoloadDir);
      return;
    } else if (filesystemMode?.value === "local") {
      handleLocalFileSelection("autoload");
    } else {
      openFileBrowser("autoload", autoloadDir);
    }
  });

  autoloadSelectFile?.addEventListener("click", () => {
    void openFileModal();
  });

  const simplonProbeController = createSimplonProbeController({
    apiBase,
    state,
    elements: { simplonUrl, simplonVersion, simplonTest, simplonProbeMessage },
    callbacks: { persistAutoloadSettings, logClient },
  });

  // The detectors used before, under the address: picking one tests it.
  attachDetectorPicker({
    input: simplonUrl,
    onPick: () => {
      state.autoload.simplonUrl = normalizeSimplonUrlInput(simplonUrl);
      persistAutoloadSettings();
      void simplonProbeController.probeSimplonConnection();
    },
  });

  simplonUrl?.addEventListener("change", () => {
    // Canonical form is written back into the field so the user sees the
    // scheme/port that will actually be used.
    state.autoload.simplonUrl = normalizeSimplonUrlInput(simplonUrl);
    // A previous test result no longer describes the address on screen.
    simplonProbeController.clearSimplonProbeMessage();
    persistAutoloadSettings();
  });

  simplonTest?.addEventListener("click", () => {
    void simplonProbeController.probeSimplonConnection();
  });

  simplonUrl?.addEventListener("keydown", (event) => {
    // Enter on a highlighted known detector was taken by the list.
    if (event.key === "Enter" && !event.defaultPrevented) {
      event.preventDefault();
      state.autoload.simplonUrl = normalizeSimplonUrlInput(simplonUrl);
      persistAutoloadSettings();
      void simplonProbeController.probeSimplonConnection();
    }
  });

  simplonVersion?.addEventListener("change", () => {
    state.autoload.simplonVersion = simplonVersion.value.trim() || "1.8.0";
    simplonProbeController.clearSimplonProbeMessage();
    persistAutoloadSettings();
  });

  simplonTimeout?.addEventListener("change", () => {
    state.autoload.simplonTimeout = Math.max(100, Number(simplonTimeout.value || 500));
    persistAutoloadSettings();
  });

  simplonEnable?.addEventListener("change", async () => {
    state.autoload.simplonEnable = simplonEnable.checked;
    persistAutoloadSettings();
    if (state.autoload.running && state.autoload.mode === "simplon") {
      await core.autoloadStatus?.setSimplonMode(state.autoload.simplonEnable);
    }
  });
}
