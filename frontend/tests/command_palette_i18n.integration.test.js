import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

function buildFetchMock(dictionaries) {
  return vi.fn(async (url) => {
    const match = String(url).match(/locales\/([^/]+)\.json/);
    const language = match ? decodeURIComponent(match[1]) : "en";
    const payload = dictionaries[language] || {};
    return {
      ok: true,
      json: async () => payload,
    };
  });
}

describe("command palette re-localization", () => {
  beforeEach(() => {
    document.body.innerHTML = `
      <div id="command-modal" class="modal is-open"></div>
      <input id="command-input" />
      <div id="command-list"></div>
    `;
    localStorage.clear();
    if (!window.Element.prototype.scrollIntoView) {
      window.Element.prototype.scrollIntoView = () => {};
    }
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete globalThis.fetch;
  });

  it("rebuilds command labels when language changes", async () => {
    vi.resetModules();

    const dictionaries = {
      en: {
        "command.empty": "No commands found.",
        "command.label.file_open": "File: Open…",
        "command.label.file_close": "File: Close",
        "command.label.file_new_window": "File: New Window",
        "command.label.settings_preferences": "Settings: Preferences…",
        "command.label.debug_picker_file": "Debug: Open ALBIS File Picker",
        "command.label.debug_picker_folder": "Debug: Open ALBIS Folder Picker",
        "command.label.debug_picker_geometry": "Debug: Open ALBIS Geometry Picker",
        "command.label.playback_pause": "Playback: Pause",
        "command.label.playback_play": "Playback: Play",
        "command.label.frame_previous": "Frame: Previous",
        "command.label.frame_next": "Frame: Next",
        "command.label.threshold_previous": "Threshold: Previous",
        "command.label.threshold_next": "Threshold: Next",
        "command.label.view_fit": "View: Fit Image",
        "command.label.export_full": "Export: Full Image",
        "command.label.export_visible": "Export: Visible Area",
        "command.label.export_window": "Export: Viewer Window",
        "command.label.series_start": "Series: Start Operation",
        "command.label.series_open_output": "Series: Open Latest Output",
        "command.label.series_cancel": "Series: Cancel Operation",
        "command.label.view_exit_fullscreen": "View: Exit Full Screen",
        "command.label.view_enter_fullscreen": "View: Enter Full Screen",
        "command.label.panel_open": "Panel: Open Side Panel",
        "command.label.panel_collapse": "Panel: Collapse Side Panel",
        "command.label.tab_view": "Panel: View Tab",
        "command.label.tab_data": "Panel: Data Tab",
        "command.label.tab_overlay": "Panel: Overlay Tab",
        "command.label.help_docs": "Help: Documentation",
        "command.label.help_log": "Help: Backend Log",
      },
      "zh-CN": {
        "command.empty": "未找到命令。",
        "command.label.file_open": "文件：打开…",
        "command.label.file_close": "文件：关闭",
        "command.label.file_new_window": "文件：新建窗口",
        "command.label.settings_preferences": "设置：偏好设置…",
        "command.label.debug_picker_file": "调试：打开 ALBIS 文件选择器",
        "command.label.debug_picker_folder": "调试：打开 ALBIS 文件夹选择器",
        "command.label.debug_picker_geometry": "调试：打开 ALBIS 几何文件选择器",
        "command.label.playback_pause": "播放：暂停",
        "command.label.playback_play": "播放：开始",
        "command.label.frame_previous": "帧：上一帧",
        "command.label.frame_next": "帧：下一帧",
        "command.label.threshold_previous": "阈值：上一个",
        "command.label.threshold_next": "阈值：下一个",
        "command.label.view_fit": "视图：适应窗口",
        "command.label.export_full": "导出：完整图像",
        "command.label.export_visible": "导出：可见区域",
        "command.label.export_window": "导出：查看器窗口",
        "command.label.series_start": "序列：开始操作",
        "command.label.series_open_output": "序列：打开最新输出",
        "command.label.series_cancel": "序列：取消操作",
        "command.label.view_exit_fullscreen": "视图：退出全屏",
        "command.label.view_enter_fullscreen": "视图：进入全屏",
        "command.label.panel_open": "面板：打开侧边面板",
        "command.label.panel_collapse": "面板：折叠侧边面板",
        "command.label.tab_view": "面板：视图标签",
        "command.label.tab_data": "面板：数据标签",
        "command.label.tab_overlay": "面板：叠加标签",
        "command.label.help_docs": "帮助：文档",
        "command.label.help_log": "帮助：后端日志",
      },
      ja: {},
    };

    globalThis.fetch = buildFetchMock(dictionaries);

    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });

    const { buildCommandPaletteCommands } = await import("../modules/command_palette_commands.js");
    const { createCommandPaletteController } = await import("../modules/command_palette.js");

    const state = {
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
    };

    const noop = () => {};
    const actions = {
      openFileModal: noop,
      closeCurrentFile: noop,
      openSettingsModal: noop,
      stopPlayback: noop,
      startPlayback: noop,
      requestFrame: noop,
      setThresholdIndex: async () => {},
      fitImageToView: noop,
      exportFullImage: noop,
      exportVisibleArea: noop,
      exportViewerWindow: noop,
      startSeriesSumming: noop,
      openSeriesSumOutputTarget: noop,
      cancelSeriesSumming: noop,
      toggleFullscreen: noop,
      togglePanel: noop,
      setPanelTab: noop,
      handleMenuAction: noop,
    };

    const getCommands = () => buildCommandPaletteCommands({
      state,
      panelTabState: "view",
      backendIsLocal: true,
      platformShortcutLabel: () => "",
      isHdfFile: () => false,
      getThresholdIndexAtOffset: () => 0,
      actions,
    });

    const controller = createCommandPaletteController({
      elements: {
        commandModal: document.getElementById("command-modal"),
        commandInput: document.getElementById("command-input"),
        commandList: document.getElementById("command-list"),
      },
      callbacks: {
        getCommands,
        closeMenu: noop,
        closeToolbarPlaybackPopover: noop,
        closeToolbarMorePopover: noop,
        focusModal: noop,
        openModal: noop,
        closeModal: () => true,
      },
    });

    controller.render();
    let labels = Array.from(document.querySelectorAll(".command-label")).map((el) => el.textContent);
    expect(labels[0]).toBe("File: Open…");
    expect(labels).toContain("Debug: Open ALBIS File Picker");
    expect(labels).toContain("Help: Backend Log");

    i18n.setLanguage("zh-CN", { persist: false, applyDom: false });
    controller.render();

    labels = Array.from(document.querySelectorAll(".command-label")).map((el) => el.textContent);
    expect(labels[0]).toBe("文件：打开…");
    expect(labels).toContain("调试：打开 ALBIS 文件选择器");
    expect(labels).toContain("帮助：后端日志");
  });
});
