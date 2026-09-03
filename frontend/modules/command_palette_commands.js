/**
 * Command palette command list construction.
 */

import { t } from "./i18n.js";
import {
  canExportAnimation,
  canExportData,
  canSaveImage,
  canStartSeriesOperation,
} from "./command_availability.js";

export function buildCommandPaletteCommands({
  state,
  panelTabState,
  backendIsLocal = false,
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
    openDataExportDialog,
    openAnimationExportDialog,
    startSeriesSumming,
    openSeriesSumOutputTarget,
    cancelSeriesSumming,
    toggleFullscreen,
    togglePanel,
    setPanelTab,
    handleMenuAction,
    openDebugFilePicker,
    openDebugFolderPicker,
    openDebugGeometryPicker,
  } = actions;

  const hasFile = Boolean(state.file);
  const hasSeries = Array.isArray(state.seriesFiles) && state.seriesFiles.length > 0;
  const hasDataset = Boolean(state.dataset);
  const hasFrame = Boolean(state.hasFrame);
  const hasNavigableFrames = hasFrame && state.frameCount > 1 && (hasDataset || hasSeries);
  const hasThresholds = hasFile && state.autoload.mode === "file" && state.thresholdCount > 1;
  const canStartSeriesOps = canStartSeriesOperation(state, isHdfFile);
  const canSaveCurrentImage = canSaveImage(state);
  const canExportGif = canExportAnimation(state);
  const canConvertDataset = canExportData(state, isHdfFile);
  const canCancelSeriesOps = state.seriesSum.running && Boolean(state.seriesSum.jobId);
  const canOpenSeriesOutput = !state.seriesSum.running && Boolean(state.seriesSum.openTarget);
  const togglePlaybackLabel = state.playing ? t("command.label.playback_pause") : t("command.label.playback_play");

  const commands = [
    {
      id: "open-file",
      label: t("command.label.file_open"),
      shortcut: platformShortcutLabel("open"),
      search: "file open load",
      run: () => openFileModal(),
    },
    {
      id: "close-file",
      label: t("command.label.file_close"),
      shortcut: platformShortcutLabel("close-file"),
      search: "file close",
      when: hasFile,
      run: () => closeCurrentFile(),
    },
    {
      id: "new-window",
      label: t("command.label.file_new_window"),
      shortcut: platformShortcutLabel("new-window"),
      search: "file window",
      run: () => window.open(window.location.href, "_blank"),
    },
    {
      id: "preferences",
      label: t("command.label.settings_preferences"),
      shortcut: platformShortcutLabel("settings-open"),
      search: "settings preferences options",
      run: () => openSettingsModal(),
    },
    {
      id: "debug-picker-file",
      label: t("command.label.debug_picker_file"),
      shortcut: "",
      search: "debug picker albis web file local",
      when: backendIsLocal,
      run: () => openDebugFilePicker(),
    },
    {
      id: "debug-picker-folder",
      label: t("command.label.debug_picker_folder"),
      shortcut: "",
      search: "debug picker albis web folder local",
      when: backendIsLocal,
      run: () => openDebugFolderPicker(),
    },
    {
      id: "debug-picker-geometry",
      label: t("command.label.debug_picker_geometry"),
      shortcut: "",
      search: "debug picker albis web geometry expt local",
      when: backendIsLocal,
      run: () => openDebugGeometryPicker(),
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
      label: t("command.label.frame_previous"),
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
      label: t("command.label.frame_next"),
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
      label: t("command.label.threshold_previous"),
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
      label: t("command.label.threshold_next"),
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
      label: t("command.label.view_fit"),
      shortcut: "",
      search: "fit reset zoom view",
      when: hasFrame,
      run: () => fitImageToView(),
    },
    {
      id: "export-full",
      label: t("command.label.export_full"),
      shortcut: platformShortcutLabel("save-full"),
      search: "export save full image png",
      when: canSaveCurrentImage,
      run: () => exportFullImage({ saveAs: true }),
    },
    {
      id: "export-visible",
      label: t("command.label.export_visible"),
      shortcut: platformShortcutLabel("save-visible"),
      search: "export save visible area png",
      when: canSaveCurrentImage,
      run: () => exportVisibleArea({ saveAs: true }),
    },
    {
      id: "export-window",
      label: t("command.label.export_window"),
      shortcut: platformShortcutLabel("save-window"),
      search: "export save viewer window screenshot",
      when: canSaveCurrentImage,
      run: () => exportViewerWindow({ saveAs: true }),
    },
    {
      id: "export-animation",
      label: t("command.label.export_animation"),
      shortcut: platformShortcutLabel("export-animation"),
      search: "export animation gif animated movie frames",
      when: canExportGif,
      run: () => openAnimationExportDialog(),
    },
    {
      id: "export-data",
      label: t("command.label.export_data"),
      shortcut: platformShortcutLabel("export-data"),
      search: "export convert dataset tiff cbf image frames",
      when: canConvertDataset,
      run: () => openDataExportDialog(),
    },
    {
      id: "series-start",
      label: t("command.label.series_start"),
      shortcut: "",
      search: "series sum mean median start analysis",
      when: canStartSeriesOps,
      run: () => startSeriesSumming(),
    },
    {
      id: "series-open-output",
      label: t("command.label.series_open_output"),
      shortcut: "",
      search: "series sum output open result",
      when: canOpenSeriesOutput,
      run: () => openSeriesSumOutputTarget(),
    },
    {
      id: "series-cancel",
      label: t("command.label.series_cancel"),
      shortcut: "",
      search: "series cancel stop abort",
      when: canCancelSeriesOps,
      run: () => cancelSeriesSumming(),
    },
    {
      id: "toggle-fullscreen",
      label: document.fullscreenElement ? t("command.label.view_exit_fullscreen") : t("command.label.view_enter_fullscreen"),
      shortcut: "F",
      search: "fullscreen immersive",
      run: () => toggleFullscreen(),
    },
    {
      id: "panel-toggle",
      label: state.panelCollapsed ? t("command.label.panel_open") : t("command.label.panel_collapse"),
      shortcut: "",
      search: "panel side toggle collapse",
      run: () => togglePanel(),
    },
    {
      id: "tab-view",
      label: t("command.label.tab_view"),
      shortcut: "",
      search: "panel tab view",
      when: panelTabState !== "view",
      run: () => setPanelTab("view"),
    },
    {
      id: "tab-data",
      label: t("command.label.tab_data"),
      shortcut: "",
      search: "panel tab data",
      when: panelTabState !== "data",
      run: () => setPanelTab("data"),
    },
    {
      id: "tab-overlay",
      label: t("command.label.tab_overlay"),
      shortcut: "",
      search: "panel tab overlay analysis",
      when: panelTabState !== "analysis",
      run: () => setPanelTab("analysis"),
    },
    {
      id: "help-docs",
      label: t("command.label.help_docs"),
      shortcut: "F1",
      search: "help docs documentation",
      run: () => handleMenuAction("help-docs"),
    },
    {
      id: "help-log",
      label: t("command.label.help_log"),
      shortcut: "",
      search: "help backend log diagnostics",
      run: () => handleMenuAction("help-log"),
    },
  ];

  return commands.filter((command) => command.when !== false);
}
