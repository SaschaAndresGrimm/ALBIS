import { describe, expect, it, vi } from "vitest";

describe("autoload_orchestration_controller", () => {
  it("does not start a new autoload action while a frame load is active", async () => {
    const { createAutoloadOrchestrationController } = await import(
      "../modules/autoload_orchestration_controller.js"
    );

    const autoloadWatchTick = vi.fn(async () => {});
    const state = {
      isLoading: true,
      autoload: {
        running: true,
        busy: false,
        mode: "file",
        watchEnabled: true,
        livePaused: false,
        lastPoll: 0,
      },
    };
    const controller = createAutoloadOrchestrationController({
      state,
      analysisState: {
        externalPeakSets: [],
      },
      elements: {},
      callbacks: {
        updateAutoloadUI: vi.fn(),
        updateAutoloadMeta: vi.fn(),
        setStatus: vi.fn(),
        persistAutoloadSettings: vi.fn(),
        resetLiveHistory: vi.fn(),
        updateRemoteMetaUI: vi.fn(),
      },
      core: {
        overlayRender: { schedulePeakOverlay: vi.fn() },
        autoloadStatus: { setAutoloadStatus: vi.fn(), setSimplonMode: vi.fn(async () => {}) },
        maskCursor: { fetchSimplonMask: vi.fn(async () => {}) },
        backendStatus: { updateLiveBadge: vi.fn() },
        jfjochBridge: { stopJfjochPreviewBridge: vi.fn(async () => {}) },
        sourceMetadata: { updateJfjochMetaUI: vi.fn() },
        autoloadMode: { autoloadWatchTick, autoloadSimplonTick: vi.fn(async () => {}) },
        remoteStream: { autoloadJfjochTick: vi.fn(async () => {}), autoloadRemoteTick: vi.fn(async () => {}) },
      },
    });

    await controller.autoloadTick();

    expect(autoloadWatchTick).not.toHaveBeenCalled();
    expect(state.autoload.busy).toBe(false);
    expect(state.autoload.lastPoll).toBe(0);
  });

  it("does not poll live sources while live browsing is paused", async () => {
    const { createAutoloadOrchestrationController } = await import(
      "../modules/autoload_orchestration_controller.js"
    );

    const autoloadRemoteTick = vi.fn(async () => {});
    const state = {
      isLoading: false,
      autoload: {
        running: true,
        busy: false,
        mode: "remote",
        watchEnabled: false,
        livePaused: true,
        lastPoll: 0,
      },
    };
    const controller = createAutoloadOrchestrationController({
      state,
      analysisState: {
        externalPeakSets: [],
      },
      elements: {},
      callbacks: {
        updateAutoloadUI: vi.fn(),
        updateAutoloadMeta: vi.fn(),
        setStatus: vi.fn(),
        persistAutoloadSettings: vi.fn(),
        resetLiveHistory: vi.fn(),
        updateRemoteMetaUI: vi.fn(),
      },
      core: {
        overlayRender: { schedulePeakOverlay: vi.fn() },
        autoloadStatus: { setAutoloadStatus: vi.fn(), setSimplonMode: vi.fn(async () => {}) },
        maskCursor: { fetchSimplonMask: vi.fn(async () => {}) },
        backendStatus: { updateLiveBadge: vi.fn() },
        jfjochBridge: { stopJfjochPreviewBridge: vi.fn(async () => {}) },
        sourceMetadata: { updateJfjochMetaUI: vi.fn() },
        autoloadMode: { autoloadWatchTick: vi.fn(async () => {}), autoloadSimplonTick: vi.fn(async () => {}) },
        remoteStream: { autoloadJfjochTick: vi.fn(async () => {}), autoloadRemoteTick },
      },
    });

    await controller.autoloadTick();

    expect(autoloadRemoteTick).not.toHaveBeenCalled();
    expect(state.autoload.busy).toBe(false);
    expect(state.autoload.lastPoll).toBe(0);
  });

  it("disables watch folder when switching back to manual file mode", async () => {
    const { createAutoloadOrchestrationController } = await import(
      "../modules/autoload_orchestration_controller.js"
    );

    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    const updateAutoloadUI = vi.fn();
    const persistAutoloadSettings = vi.fn();
    const setAutoloadStatus = vi.fn();
    const state = {
      isLoading: false,
      autoload: {
        running: true,
        busy: false,
        mode: "file",
        watchEnabled: true,
        livePaused: false,
        lastPoll: 0,
      },
    };
    try {
      const controller = createAutoloadOrchestrationController({
        state,
        analysisState: {
          externalPeakSets: [],
        },
        elements: {},
        callbacks: {
          updateAutoloadUI,
          updateAutoloadMeta: vi.fn(),
          setStatus: vi.fn(),
          persistAutoloadSettings,
          resetLiveHistory: vi.fn(),
          updateRemoteMetaUI: vi.fn(),
        },
        core: {
          overlayRender: { schedulePeakOverlay: vi.fn() },
          autoloadStatus: { setAutoloadStatus, setSimplonMode: vi.fn(async () => {}) },
          maskCursor: { fetchSimplonMask: vi.fn(async () => {}) },
          backendStatus: { updateLiveBadge: vi.fn() },
          jfjochBridge: { stopJfjochPreviewBridge: vi.fn(async () => {}) },
          sourceMetadata: { updateJfjochMetaUI: vi.fn() },
          autoloadMode: { autoloadWatchTick: vi.fn(async () => {}), autoloadSimplonTick: vi.fn(async () => {}) },
          remoteStream: { autoloadJfjochTick: vi.fn(async () => {}), autoloadRemoteTick: vi.fn(async () => {}) },
        },
      });

      await controller.ensureFileMode();

      expect(state.autoload.watchEnabled).toBe(false);
      expect(state.autoload.running).toBe(false);
      expect(updateAutoloadUI).toHaveBeenCalledTimes(1);
      expect(setAutoloadStatus).toHaveBeenCalledWith("autoload.status.idle");
      expect(persistAutoloadSettings).toHaveBeenCalledTimes(1);
    } finally {
      warnSpy.mockRestore();
    }
  });

  it("leaves the detector's monitor on when the Detector tab opens a series", async () => {
    const { createAutoloadOrchestrationController } = await import(
      "../modules/autoload_orchestration_controller.js"
    );
    const setSimplonMode = vi.fn(async () => {});
    const make = () => {
      const state = { autoload: { running: true, timer: null, mode: "simplon", watchEnabled: false, livePaused: false, lastPoll: 0 } };
      const noop = vi.fn();
      const controller = createAutoloadOrchestrationController({
        state,
        analysisState: { externalPeakSets: [] },
        elements: {},
        callbacks: {
          updateAutoloadUI: noop, updateAutoloadMeta: noop, setStatus: noop,
          persistAutoloadSettings: noop, resetLiveHistory: noop, updateRemoteMetaUI: noop,
        },
        core: {
          autoloadStatus: { setAutoloadStatus: noop, setSimplonMode },
          overlayRender: { schedulePeakOverlay: noop },
          backendStatus: { updateLiveBadge: noop },
          maskCursor: { fetchSimplonMask: vi.fn(async () => {}) },
          jfjochBridge: { stopJfjochPreviewBridge: vi.fn(async () => {}) },
          sourceMetadata: { updateJfjochMetaUI: noop },
          autoloadMode: { autoloadWatchTick: vi.fn(async () => {}), autoloadSimplonTick: vi.fn(async () => {}) },
          remoteStream: { autoloadJfjochTick: vi.fn(async () => {}), autoloadRemoteTick: vi.fn(async () => {}) },
        },
      });
      return { state, controller };
    };
    // The Detector tab: to File, monitor left as it is.
    const kept = make();
    await kept.controller.ensureFileMode({ disableMonitor: false });
    expect(kept.state.autoload.mode).toBe("file");
    expect(kept.state.autoload.running).toBe(false);
    expect(setSimplonMode).not.toHaveBeenCalled();
    // File -> Open: as before, the monitor ALBIS switched on is switched off.
    const usual = make();
    await usual.controller.ensureFileMode();
    expect(usual.state.autoload.mode).toBe("file");
    expect(setSimplonMode).toHaveBeenCalledWith(false);
  });
});
