/**
 * Command palette command list construction.
 */

export function buildCommandPaletteCommands({
  state,
  panelTabState,
  platformShortcutLabel,
  isHdfFile,
  getThresholdIndexAtOffset,
  actions,
}) {
  const {
    openFileModal,
    closeCurrentFile,
    openSettingsModal,
    stopPlayback,
    startPlayback,
    requestFrame,
    setThresholdIndex,
    fitImageToView,
    exportFullImage,
    exportVisibleArea,
    exportViewerWindow,
    startSeriesSumming,
    openSeriesSumOutputTarget,
    cancelSeriesSumming,
    toggleFullscreen,
    togglePanel,
    setPanelTab,
    handleMenuAction,
  } = actions;

  const hasFile = Boolean(state.file);
  const hasSeries = Array.isArray(state.seriesFiles) && state.seriesFiles.length > 0;
  const hasDataset = Boolean(state.dataset);
  const hasFrame = Boolean(state.hasFrame);
  const hasNavigableFrames = hasFrame && state.frameCount > 1 && (hasDataset || hasSeries);
  const hasThresholds = hasFile && state.autoload.mode === "file" && state.thresholdCount > 1;
  const canStartSeriesOps = hasFile && (!isHdfFile(state.file) || hasDataset) && !state.seriesSum.running;
  const canCancelSeriesOps = state.seriesSum.running && Boolean(state.seriesSum.jobId);
  const canOpenSeriesOutput = !state.seriesSum.running && Boolean(state.seriesSum.openTarget);
  const togglePlaybackLabel = state.playing ? "Playback: Pause" : "Playback: Play";

  const commands = [
    {
      id: "open-file",
      label: "File: Open…",
      shortcut: platformShortcutLabel("open"),
      search: "file open load",
      run: () => openFileModal(),
    },
    {
      id: "close-file",
      label: "File: Close",
      shortcut: platformShortcutLabel("close-file"),
      search: "file close",
      when: hasFile,
      run: () => closeCurrentFile(),
    },
    {
      id: "new-window",
      label: "File: New Window",
      shortcut: platformShortcutLabel("new-window"),
      search: "file window",
      run: () => window.open(window.location.href, "_blank"),
    },
    {
      id: "preferences",
      label: "Settings: Preferences…",
      shortcut: platformShortcutLabel("settings-open"),
      search: "settings preferences options",
      run: () => openSettingsModal(),
    },
    {
      id: "toggle-playback",
      label: togglePlaybackLabel,
      shortcut: "Tab",
      search: "playback play pause",
      when: hasNavigableFrames,
      run: () => {
        if (state.playing) {
          stopPlayback();
        } else {
          startPlayback();
        }
      },
    },
    {
      id: "frame-prev",
      label: "Frame: Previous",
      shortcut: "Left Arrow",
      search: "frame previous left",
      when: hasNavigableFrames,
      run: () => {
        stopPlayback();
        requestFrame(state.frameIndex - 1);
      },
    },
    {
      id: "frame-next",
      label: "Frame: Next",
      shortcut: "Right Arrow",
      search: "frame next right",
      when: hasNavigableFrames,
      run: () => {
        stopPlayback();
        requestFrame(state.frameIndex + 1);
      },
    },
    {
      id: "threshold-prev",
      label: "Threshold: Previous",
      shortcut: "Up Arrow",
      search: "threshold detector previous",
      when: hasThresholds,
      run: async () => {
        stopPlayback();
        await setThresholdIndex(getThresholdIndexAtOffset(-1));
      },
    },
    {
      id: "threshold-next",
      label: "Threshold: Next",
      shortcut: "Down Arrow",
      search: "threshold detector next",
      when: hasThresholds,
      run: async () => {
        stopPlayback();
        await setThresholdIndex(getThresholdIndexAtOffset(1));
      },
    },
    {
      id: "fit-view",
      label: "View: Fit Image",
      shortcut: "",
      search: "fit reset zoom view",
      when: hasFrame,
      run: () => fitImageToView(),
    },
    {
      id: "export-full",
      label: "Export: Full Image",
      shortcut: platformShortcutLabel("export-full"),
      search: "export save full image png",
      when: hasFrame,
      run: () => exportFullImage(),
    },
    {
      id: "export-visible",
      label: "Export: Visible Area",
      shortcut: platformShortcutLabel("export-visible"),
      search: "export save visible area png",
      when: hasFrame,
      run: () => exportVisibleArea(),
    },
    {
      id: "export-window",
      label: "Export: Viewer Window",
      shortcut: platformShortcutLabel("export-window"),
      search: "export save viewer window screenshot",
      when: hasFrame,
      run: () => exportViewerWindow(),
    },
    {
      id: "series-start",
      label: "Series: Start Operation",
      shortcut: "",
      search: "series sum mean median start analysis",
      when: canStartSeriesOps,
      run: () => startSeriesSumming(),
    },
    {
      id: "series-open-output",
      label: "Series: Open Latest Output",
      shortcut: "",
      search: "series sum output open result",
      when: canOpenSeriesOutput,
      run: () => openSeriesSumOutputTarget(),
    },
    {
      id: "series-cancel",
      label: "Series: Cancel Operation",
      shortcut: "",
      search: "series cancel stop abort",
      when: canCancelSeriesOps,
      run: () => cancelSeriesSumming(),
    },
    {
      id: "toggle-fullscreen",
      label: document.fullscreenElement ? "View: Exit Full Screen" : "View: Enter Full Screen",
      shortcut: "F",
      search: "fullscreen immersive",
      run: () => toggleFullscreen(),
    },
    {
      id: "panel-toggle",
      label: state.panelCollapsed ? "Panel: Open Side Panel" : "Panel: Collapse Side Panel",
      shortcut: "",
      search: "panel side toggle collapse",
      run: () => togglePanel(),
    },
    {
      id: "tab-view",
      label: "Panel: View Tab",
      shortcut: "",
      search: "panel tab view",
      when: panelTabState !== "view",
      run: () => setPanelTab("view"),
    },
    {
      id: "tab-data",
      label: "Panel: Data Tab",
      shortcut: "",
      search: "panel tab data",
      when: panelTabState !== "data",
      run: () => setPanelTab("data"),
    },
    {
      id: "tab-overlay",
      label: "Panel: Overlay Tab",
      shortcut: "",
      search: "panel tab overlay analysis",
      when: panelTabState !== "analysis",
      run: () => setPanelTab("analysis"),
    },
    {
      id: "help-docs",
      label: "Help: Documentation",
      shortcut: "F1",
      search: "help docs documentation",
      run: () => handleMenuAction("help-docs"),
    },
  ];

  return commands.filter((command) => command.when !== false);
}
