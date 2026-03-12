/**
 * Bind autoload + live-source control listeners.
 *
 * This module keeps event wiring separate from app-level orchestration logic.
 */

export function bindAutoloadControls({
  apiBase,
  state,
  analysisState,
  backendIsLocal,
  elements,
  callbacks,
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
    simplonVersion,
    simplonTimeout,
    simplonEnable,
  } = elements;

  const {
    stopAutoload,
    startAutoload,
    updateAutoloadUI,
    persistAutoloadSettings,
    loadFiles,
    autoloadTick,
    updateRemoteMetaUI,
    updateJfjochMetaUI,
    schedulePeakOverlay,
    setSimplonMode,
    openFileBrowser,
    openFileModal,
    handleLocalFileSelection,
  } = callbacks;

  autoloadMode?.addEventListener("change", async () => {
    const nextMode = autoloadMode.value;
    if (state.autoload.running) {
      await stopAutoload();
    }
    state.autoload.mode = nextMode;
    updateAutoloadUI();
    persistAutoloadSettings();
    if (state.autoload.mode === "file") {
      loadFiles().catch((err) => console.error(err));
      if (state.autoload.watchEnabled) {
        startAutoload().catch((err) => console.error(err));
      }
    } else {
      startAutoload().catch((err) => console.error(err));
    }
  });

  autoloadWatchEnabled?.addEventListener("change", () => {
    state.autoload.watchEnabled = autoloadWatchEnabled.checked;
    updateAutoloadUI();
    persistAutoloadSettings();
    if (state.autoload.mode !== "file") return;
    if (state.autoload.watchEnabled) {
      startAutoload();
    } else if (state.autoload.running) {
      stopAutoload({ keepMode: true, disableMonitor: false });
    }
  });

  autoloadDir?.addEventListener("change", () => {
    state.autoload.dir = autoloadDir.value.trim();
    persistAutoloadSettings();
    if (state.autoload.mode === "file") {
      loadFiles().catch((err) => console.error(err));
    }
    if (state.autoload.running && state.autoload.mode === "file" && state.autoload.watchEnabled) {
      autoloadTick();
    }
  });

  autoloadInterval?.addEventListener("change", () => {
    state.autoload.interval = Math.max(200, Number(autoloadInterval.value || 1000));
    persistAutoloadSettings();
    if (state.autoload.running && state.autoload.mode === "file" && state.autoload.watchEnabled) {
      startAutoload();
    }
  });

  remoteIntervalInput?.addEventListener("change", () => {
    state.autoload.interval = Math.max(100, Number(remoteIntervalInput.value || 1000));
    persistAutoloadSettings();
    if (state.autoload.running && state.autoload.mode === "remote") {
      startAutoload();
    }
  });

  jfjochIntervalInput?.addEventListener("change", () => {
    state.autoload.jfjochInterval = Math.max(100, Number(jfjochIntervalInput.value || 250));
    persistAutoloadSettings();
    if (state.autoload.running && state.autoload.mode === "jungfraujoch") {
      startAutoload();
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
      autoloadTick();
    }
  });

  jfjochSourceInput?.addEventListener("change", () => {
    state.autoload.jfjochSourceId = (jfjochSourceInput.value || "jungfraujoch").trim() || "jungfraujoch";
    persistAutoloadSettings();
    if (state.autoload.running && state.autoload.mode === "jungfraujoch") {
      state.autoload.lastJfjochSeq = 0;
      state.autoload.jfjochMeta = {};
      analysisState.externalPeakSets = [];
      updateJfjochMetaUI(state.autoload.jfjochMeta || {}, state.autoload.jfjochStatus || {});
      schedulePeakOverlay();
      autoloadTick();
    }
  });

  jfjochEndpointInput?.addEventListener("change", () => {
    state.autoload.jfjochEndpoint = (jfjochEndpointInput.value || "").trim();
    persistAutoloadSettings();
    if (state.autoload.running && state.autoload.mode === "jungfraujoch") {
      startAutoload();
    }
  });

  jfjochTopicInput?.addEventListener("change", () => {
    state.autoload.jfjochTopic = (jfjochTopicInput.value || "").trim();
    persistAutoloadSettings();
    if (state.autoload.running && state.autoload.mode === "jungfraujoch") {
      startAutoload();
    }
  });

  jfjochChannelInput?.addEventListener("change", () => {
    state.autoload.jfjochChannel = (jfjochChannelInput.value || "").trim();
    persistAutoloadSettings();
    if (state.autoload.running && state.autoload.mode === "jungfraujoch") {
      startAutoload();
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
        autoloadTick();
      }
    });
  });

  autoloadPattern?.addEventListener("change", () => {
    state.autoload.pattern = autoloadPattern.value.trim();
    persistAutoloadSettings();
    if (state.autoload.running && state.autoload.mode === "file" && state.autoload.watchEnabled) {
      autoloadTick();
    }
  });

  autoloadBrowse?.addEventListener("click", async () => {
    if (backendIsLocal) {
      try {
        const res = await fetch(`${apiBase}/choose-folder`);
        if (res.status === 204) {
          return;
        }
        if (res.ok) {
          const data = await res.json();
          if (data?.path && autoloadDir) {
            autoloadDir.value = data.path;
            state.autoload.dir = data.path;
            persistAutoloadSettings();
            if (state.autoload.mode === "file") {
              loadFiles().catch((err) => console.error(err));
            }
            if (state.autoload.running && state.autoload.mode === "file" && state.autoload.watchEnabled) {
              autoloadTick();
            }
          }
          return;
        }
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

  simplonUrl?.addEventListener("change", () => {
    state.autoload.simplonUrl = simplonUrl.value.trim();
    persistAutoloadSettings();
  });

  simplonVersion?.addEventListener("change", () => {
    state.autoload.simplonVersion = simplonVersion.value.trim() || "1.8.0";
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
      await setSimplonMode(state.autoload.simplonEnable);
    }
  });
}
