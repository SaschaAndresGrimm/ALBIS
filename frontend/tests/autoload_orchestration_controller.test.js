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
        setAutoloadStatus: vi.fn(),
        setStatus: vi.fn(),
        persistAutoloadSettings: vi.fn(),
        resetLiveHistory: vi.fn(),
        setSimplonMode: vi.fn(async () => {}),
        fetchSimplonMask: vi.fn(async () => {}),
        updateLiveBadge: vi.fn(),
        stopJfjochPreviewBridge: vi.fn(async () => {}),
        updateRemoteMetaUI: vi.fn(),
        updateJfjochMetaUI: vi.fn(),
        schedulePeakOverlay: vi.fn(),
        autoloadWatchTick,
        autoloadSimplonTick: vi.fn(async () => {}),
        autoloadJfjochTick: vi.fn(async () => {}),
        autoloadRemoteTick: vi.fn(async () => {}),
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
        setAutoloadStatus: vi.fn(),
        setStatus: vi.fn(),
        persistAutoloadSettings: vi.fn(),
        resetLiveHistory: vi.fn(),
        setSimplonMode: vi.fn(async () => {}),
        fetchSimplonMask: vi.fn(async () => {}),
        updateLiveBadge: vi.fn(),
        stopJfjochPreviewBridge: vi.fn(async () => {}),
        updateRemoteMetaUI: vi.fn(),
        updateJfjochMetaUI: vi.fn(),
        schedulePeakOverlay: vi.fn(),
        autoloadWatchTick: vi.fn(async () => {}),
        autoloadSimplonTick: vi.fn(async () => {}),
        autoloadJfjochTick: vi.fn(async () => {}),
        autoloadRemoteTick,
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
          setAutoloadStatus,
          setStatus: vi.fn(),
          persistAutoloadSettings,
          resetLiveHistory: vi.fn(),
          setSimplonMode: vi.fn(async () => {}),
          fetchSimplonMask: vi.fn(async () => {}),
          updateLiveBadge: vi.fn(),
          stopJfjochPreviewBridge: vi.fn(async () => {}),
          updateRemoteMetaUI: vi.fn(),
          updateJfjochMetaUI: vi.fn(),
          schedulePeakOverlay: vi.fn(),
          autoloadWatchTick: vi.fn(async () => {}),
          autoloadSimplonTick: vi.fn(async () => {}),
          autoloadJfjochTick: vi.fn(async () => {}),
          autoloadRemoteTick: vi.fn(async () => {}),
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
});
