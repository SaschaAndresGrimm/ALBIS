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
});
