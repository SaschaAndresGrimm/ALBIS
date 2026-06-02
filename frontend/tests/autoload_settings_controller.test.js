import { describe, expect, it, vi } from "vitest";

describe("autoload_settings_controller", () => {
  function baseCallbacks() {
    return {
      closeToolbarPlaybackPopover: vi.fn(),
      updateSimplonMetaUI: vi.fn(),
      updateRemoteMetaUI: vi.fn(),
      updateJfjochMetaUI: vi.fn(),
      updateAutoloadMeta: vi.fn(),
      updateLiveBadge: vi.fn(),
      updateThresholdOptions: vi.fn(),
      updateDataSourceSummary: vi.fn(),
      setDataSourceSectionState: vi.fn(),
      setAutoloadStatus: vi.fn(),
      setAutoloadLatest: vi.fn(),
      updatePlayButtons: vi.fn(),
      startAutoload: vi.fn(),
    };
  }

  it("shows frame controls for live history but keeps playback settings hidden", async () => {
    const { createAutoloadSettingsController } = await import("../modules/autoload_settings_controller.js");

    const toolbarFrameWrap = document.createElement("div");
    const toolbarFrameIndexWrap = document.createElement("div");
    const toolbarStepWrap = document.createElement("div");
    const toolbarFpsWrap = document.createElement("div");
    const toolbarPlaybackWrap = document.createElement("div");
    const toolbarMoreStepField = document.createElement("div");
    const toolbarMoreFpsField = document.createElement("div");
    const autoloadStatus = document.createElement("div");
    const metaWrapper = document.createElement("div");
    metaWrapper.className = "autoload-meta";
    metaWrapper.appendChild(autoloadStatus);

    const state = {
      file: "",
      autoload: {
        mode: "remote",
        running: true,
        historyEntries: [{}, {}],
      },
    };

    const controller = createAutoloadSettingsController({
      state,
      elements: {
        autoloadMode: null,
        autoloadFolder: null,
        autoloadWatch: null,
        autoloadWatchEnabled: null,
        autoloadWatchOptions: null,
        autoloadTypesRow: null,
        autoloadSimplon: null,
        autoloadRemote: null,
        autoloadJfjoch: null,
        fileField: document.createElement("div"),
        datasetField: document.createElement("div"),
        thresholdField: document.createElement("div"),
        toolbarFrameWrap,
        toolbarFrameIndexWrap,
        toolbarStepWrap,
        toolbarFpsWrap,
        toolbarPlaybackWrap,
        toolbarMoreStepField,
        toolbarMoreFpsField,
        autoloadStatus,
        simplonMetaPanel: null,
        remoteMetaPanel: null,
        jfjochMetaPanel: null,
        autoloadDir: null,
        autoloadInterval: null,
        autoloadTypeHdf5: null,
        autoloadTypeTiff: null,
        autoloadTypeCbf: null,
        autoloadPattern: null,
        simplonUrl: null,
        simplonVersion: null,
        simplonTimeout: null,
        simplonEnable: null,
        remoteSourceInput: null,
        remoteIntervalInput: null,
        jfjochEndpointInput: null,
        jfjochSourceInput: null,
        jfjochTopicInput: null,
        jfjochChannelInput: null,
        jfjochIntervalInput: null,
      },
      callbacks: {
        ...baseCallbacks(),
      },
    });

    controller.updateAutoloadUI();

    expect(toolbarFrameWrap.classList.contains("is-hidden")).toBe(false);
    expect(toolbarFrameIndexWrap.classList.contains("is-hidden")).toBe(false);
    expect(toolbarStepWrap.classList.contains("is-hidden")).toBe(true);
    expect(toolbarFpsWrap.classList.contains("is-hidden")).toBe(true);
    expect(toolbarPlaybackWrap.classList.contains("is-hidden")).toBe(true);
    expect(toolbarMoreStepField.classList.contains("is-hidden")).toBe(true);
    expect(toolbarMoreFpsField.classList.contains("is-hidden")).toBe(true);
  });

  it("keeps SIMPLON primary controls and live info outside advanced settings", async () => {
    const { createAutoloadSettingsController } = await import("../modules/autoload_settings_controller.js");

    const autoloadSimplon = document.createElement("div");
    const autoloadSimplonAdvanced = document.createElement("div");
    const autoloadStatusBlock = document.createElement("div");
    const autoloadStatusPrimarySlot = document.createElement("div");
    const autoloadStatusAdvancedSlot = document.createElement("div");
    const autoloadStatus = document.createElement("div");
    const autoloadMeta = document.createElement("div");
    const simplonMetaPanel = document.createElement("div");
    const filesystemField = document.createElement("label");

    autoloadSimplon.className = "autoload-group is-hidden";
    autoloadSimplonAdvanced.className = "autoload-group is-hidden";
    autoloadStatusBlock.className = "autoload-status-block is-hidden";
    autoloadMeta.className = "autoload-meta";
    autoloadMeta.appendChild(autoloadStatus);
    autoloadStatusBlock.appendChild(autoloadMeta);
    autoloadStatusPrimarySlot.appendChild(autoloadStatusBlock);
    simplonMetaPanel.className = "simplon-meta is-hidden";

    const state = {
      file: "",
      autoload: {
        mode: "simplon",
        running: false,
        watchEnabled: false,
        historyEntries: [],
        simplonMeta: {},
      },
    };

    const controller = createAutoloadSettingsController({
      state,
      elements: {
        autoloadMode: null,
        autoloadFolder: null,
        autoloadWatch: null,
        autoloadWatchEnabled: null,
        autoloadWatchOptions: null,
        autoloadTypesRow: null,
        autoloadSimplon,
        autoloadSimplonAdvanced,
        autoloadStatusBlock,
        autoloadStatusPrimarySlot,
        autoloadStatusAdvancedSlot,
        autoloadRemote: null,
        autoloadJfjoch: null,
        filesystemField,
        fileField: document.createElement("div"),
        datasetField: document.createElement("div"),
        thresholdField: document.createElement("div"),
        toolbarFrameWrap: null,
        toolbarFrameIndexWrap: null,
        toolbarStepWrap: null,
        toolbarFpsWrap: null,
        toolbarPlaybackWrap: null,
        toolbarMoreStepField: null,
        toolbarMoreFpsField: null,
        autoloadStatus,
        simplonMetaPanel,
        remoteMetaPanel: null,
        jfjochMetaPanel: null,
        autoloadDir: null,
        autoloadInterval: null,
        autoloadTypeHdf5: null,
        autoloadTypeTiff: null,
        autoloadTypeCbf: null,
        autoloadPattern: null,
        simplonUrl: null,
        simplonVersion: null,
        simplonTimeout: null,
        simplonEnable: null,
        remoteSourceInput: null,
        remoteIntervalInput: null,
        jfjochEndpointInput: null,
        jfjochSourceInput: null,
        jfjochTopicInput: null,
        jfjochChannelInput: null,
        jfjochIntervalInput: null,
      },
      callbacks: {
        ...baseCallbacks(),
        isBackendLocal: () => true,
      },
    });

    controller.updateAutoloadUI();

    expect(autoloadSimplon.classList.contains("is-hidden")).toBe(false);
    expect(autoloadSimplonAdvanced.classList.contains("is-hidden")).toBe(false);
    expect(autoloadStatusBlock.parentElement).toBe(autoloadStatusPrimarySlot);
    expect(autoloadStatusBlock.classList.contains("is-hidden")).toBe(false);
    expect(autoloadMeta.classList.contains("is-hidden")).toBe(false);
    expect(simplonMetaPanel.classList.contains("is-hidden")).toBe(false);
    expect(filesystemField.classList.contains("is-hidden")).toBe(true);

    state.autoload.mode = "file";
    controller.updateAutoloadUI();

    expect(autoloadSimplon.classList.contains("is-hidden")).toBe(true);
    expect(autoloadSimplonAdvanced.classList.contains("is-hidden")).toBe(true);
    expect(autoloadStatusBlock.parentElement).toBe(autoloadStatusAdvancedSlot);
    expect(autoloadStatusBlock.classList.contains("is-hidden")).toBe(true);
  });
});
