import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

function jsonResponse(payload) {
  return {
    ok: true,
    json: async () => payload,
  };
}

describe("command_palette_commands", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete globalThis.fetch;
  });

  it("adds local-only debug picker commands and dispatches them to the ALBIS picker actions", async () => {
    vi.resetModules();
    globalThis.fetch = vi.fn(async (url) => {
      if (String(url).includes("locales/")) {
        return jsonResponse({
          "command.label.file_open": "File: Open…",
          "command.label.file_close": "File: Close",
          "command.label.file_new_window": "File: New Window",
          "command.label.settings_preferences": "Settings: Preferences…",
          "command.label.debug_picker_file": "Debug: Open ALBIS File Picker",
          "command.label.debug_picker_folder": "Debug: Open ALBIS Folder Picker",
          "command.label.debug_picker_geometry": "Debug: Open ALBIS Geometry Picker",
          "command.label.frame_previous": "Frame: Previous",
          "command.label.frame_next": "Frame: Next",
          "command.label.threshold_previous": "Threshold: Previous",
          "command.label.threshold_next": "Threshold: Next",
          "command.label.view_fit": "View: Fit Image",
          "command.label.export_full": "Export: Full Image",
          "command.label.export_visible": "Export: Visible Area",
          "command.label.export_window": "Export: Viewer Window",
          "command.label.export_data": "Export: Convert Dataset",
          "command.label.series_start": "Series: Start Operation",
          "command.label.series_open_output": "Series: Open Latest Output",
          "command.label.series_cancel": "Series: Cancel Operation",
          "command.label.playback_pause": "Playback: Pause",
          "command.label.playback_play": "Playback: Play",
          "command.label.view_enter_fullscreen": "View: Enter Full Screen",
          "command.label.view_exit_fullscreen": "View: Exit Full Screen",
          "command.label.panel_open": "Panel: Open Side Panel",
          "command.label.panel_collapse": "Panel: Collapse Side Panel",
          "command.label.tab_view": "Panel: View Tab",
          "command.label.tab_data": "Panel: Data Tab",
          "command.label.tab_overlay": "Panel: Overlay Tab",
          "command.label.help_docs": "Help: Documentation",
          "command.label.help_log": "Help: Backend Log",
        });
      }
      return jsonResponse({});
    });

    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { buildCommandPaletteCommands } = await import("../modules/command_palette_commands.js");

    const openDebugFilePicker = vi.fn(async () => "");
    const openDebugFolderPicker = vi.fn();
    const openDebugGeometryPicker = vi.fn(async () => "");

    const actions = {
      openFileModal: vi.fn(),
      closeCurrentFile: vi.fn(),
      openSettingsModal: vi.fn(),
      stopPlayback: vi.fn(),
      startPlayback: vi.fn(),
      requestFrame: vi.fn(),
      setThresholdIndex: vi.fn(async () => {}),
      fitImageToView: vi.fn(),
      exportFullImage: vi.fn(),
      exportVisibleArea: vi.fn(),
      exportViewerWindow: vi.fn(),
      openDataExportDialog: vi.fn(),
      startSeriesSumming: vi.fn(),
      openSeriesSumOutputTarget: vi.fn(),
      cancelSeriesSumming: vi.fn(),
      toggleFullscreen: vi.fn(),
      togglePanel: vi.fn(),
      setPanelTab: vi.fn(),
      handleMenuAction: vi.fn(),
      openDebugFilePicker,
      openDebugFolderPicker,
      openDebugGeometryPicker,
    };

    const baseArgs = {
      state: {
        file: "",
        hasFrame: false,
        seriesFiles: [],
        dataset: "",
        autoload: { mode: "file" },
        thresholdCount: 1,
        seriesSum: { running: false, jobId: "", openTarget: "" },
        panelCollapsed: true,
        playing: false,
        frameCount: 1,
        frameIndex: 0,
      },
      panelTabState: "view",
      platformShortcutLabel: () => "",
      isHdfFile: () => false,
      getThresholdIndexAtOffset: () => 0,
      actions,
    };

    const localCommands = buildCommandPaletteCommands({
      ...baseArgs,
      backendIsLocal: true,
    });
    const remoteCommands = buildCommandPaletteCommands({
      ...baseArgs,
      backendIsLocal: false,
    });

    expect(localCommands.map((command) => command.id)).toEqual(
      expect.arrayContaining(["debug-picker-file", "debug-picker-folder", "debug-picker-geometry"]),
    );
    expect(remoteCommands.map((command) => command.id)).not.toEqual(
      expect.arrayContaining(["debug-picker-file", "debug-picker-folder", "debug-picker-geometry"]),
    );

    await localCommands.find((command) => command.id === "debug-picker-file")?.run();
    localCommands.find((command) => command.id === "debug-picker-folder")?.run();
    await localCommands.find((command) => command.id === "debug-picker-geometry")?.run();

    expect(openDebugFilePicker).toHaveBeenCalledTimes(1);
    expect(openDebugFolderPicker).toHaveBeenCalledTimes(1);
    expect(openDebugGeometryPicker).toHaveBeenCalledTimes(1);
  });
});
