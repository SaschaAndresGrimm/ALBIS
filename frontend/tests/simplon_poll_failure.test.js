import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const DICTIONARY = {
  "autoload.status.simplon.error": "SIMPLON: error",
  "autoload.status.simplon.error_reason": "SIMPLON: {{reason}}",
  "autoload.status.simplon.no_frame": "SIMPLON: no frame",
  "autoload.status.simplon.set_base_url": "SIMPLON: set base URL",
  "simplon.failure.refused_port": "Connection refused on port {{port}} — SIMPLON normally listens on port 80.",
  "simplon.failure.api_missing_version": "Reachable, but no SIMPLON API {{version}} here.",
  "source.label.simplon_monitor": "SIMPLON monitor",
};

async function runTick({ failureBody, ok = false, status = 502 }) {
  vi.resetModules();
  global.fetch = vi.fn(async (url) => {
    const text = String(url);
    if (text.match(/locales\/([^/]+)\.json/)) {
      return { ok: true, json: async () => DICTIONARY };
    }
    if (text.includes("/simplon/monitor")) {
      return {
        ok,
        status,
        json: async () => {
          if (failureBody === undefined) throw new Error("not json");
          return failureBody;
        },
      };
    }
    throw new Error(`Unexpected fetch: ${text}`);
  });
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createAutoloadModeController } = await import("../modules/autoload_mode_controller.js");

  const setAutoloadStatus = vi.fn();
  const logClient = vi.fn();
  const state = {
    maskAvailable: true,
    autoload: {
      simplonUrl: "http://det.local:5000",
      simplonVersion: "1.8.0",
      simplonTimeout: 500,
      simplonEnable: true,
      livePaused: false,
      lastMaskAttempt: 0,
      lastMonitorSig: "",
      lastUpdate: 0,
      lastSimplonFailure: "",
    },
  };
  const controller = createAutoloadModeController({
    apiBase: "/api",
    state,
    callbacks: {
      setAutoloadStatus,
      setAutoloadLatest: vi.fn(),
      updateAutoloadMeta: vi.fn(),
      loadAutoloadFile: vi.fn(),
      fetchSimplonMask: vi.fn(),
      parseDtype: (value) => value,
      parseShape: (value) => String(value).split(",").map(Number),
      typedArrayFrom: (buffer) => new Uint16Array(buffer),
      hashBufferSample: vi.fn(() => "hash"),
      parseSimplonMeta: vi.fn(() => ({ analysis: {}, meta: {} })),
      createLiveSourceSnapshot: vi.fn((value) => value),
      appendLiveFrame: vi.fn(() => ({ appended: true, rendered: true })),
      logClient,
      formatSimplonTimestamp: vi.fn(() => ""),
      updateLiveBadge: vi.fn(),
    },
  });

  return { controller, state, setAutoloadStatus, logClient };
}

describe("SIMPLON poll failure reporting", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("reports the refused port instead of a generic error", async () => {
    const { controller, setAutoloadStatus } = await runTick({
      failureBody: {
        detail: {
          summary: "Failed to fetch SIMPLON monitor image",
          code: "refused",
          port: 5000,
        },
      },
    });

    await controller.autoloadSimplonTick();

    expect(setAutoloadStatus).toHaveBeenCalledWith(
      "SIMPLON: Connection refused on port 5000 — SIMPLON normally listens on port 80.",
    );
  });

  it("uses the configured API version when the backend omits it", async () => {
    const { controller, setAutoloadStatus } = await runTick({
      failureBody: { detail: { code: "api_missing", http_status: 404 } },
    });

    await controller.autoloadSimplonTick();

    expect(setAutoloadStatus).toHaveBeenCalledWith(
      "SIMPLON: Reachable, but no SIMPLON API 1.8.0 here.",
    );
  });

  it("logs a repeating failure once per code", async () => {
    const { controller, logClient, state } = await runTick({
      failureBody: { detail: { code: "refused", port: 5000 } },
    });

    await controller.autoloadSimplonTick();
    await controller.autoloadSimplonTick();

    expect(logClient).toHaveBeenCalledTimes(1);
    expect(state.autoload.lastSimplonFailure).toBe("refused");
  });

  it("falls back to the generic status when the body carries no diagnosis", async () => {
    const { controller, setAutoloadStatus } = await runTick({ failureBody: undefined });

    await controller.autoloadSimplonTick();

    expect(setAutoloadStatus).toHaveBeenCalledWith("SIMPLON: error");
  });

  it("falls back to the generic status for a plain-string detail", async () => {
    const { controller, setAutoloadStatus } = await runTick({
      failureBody: { detail: "Failed to fetch SIMPLON monitor image" },
    });

    await controller.autoloadSimplonTick();

    expect(setAutoloadStatus).toHaveBeenCalledWith("SIMPLON: error");
  });
});
