import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

/**
 * When the server stops answering, every request fails, and each failure used
 * to raise its own toast blaming the request ("Failed to load frame"). One
 * notice naming the cause replaces them until the server is back.
 */

function healthy() {
  return { ok: true, json: async () => ({ status: "ok", version: "1.0.0", commit: "abc" }) };
}

function baseState() {
  return {
    backendAlive: false,
    serverStopped: false,
    backendVersion: "",
    backendCommit: "",
    buildStampAtLoad: null,
    serverBuildChanged: false,
    autoload: { mode: "file", running: false, lastUpdate: 0, interval: 1000 },
  };
}

async function mount(state) {
  const toast = await import("../modules/toast.js");
  const http = await import("../modules/http.js");
  const { createBackendStatusController } = await import("../modules/backend_status_controller.js");
  const controller = createBackendStatusController({
    apiBase: "/api",
    state,
    elements: { liveBadge: null, backendBadge: null, aboutVersion: null },
    callbacks: {
      updateFooterVersions: () => {},
      updateSplashCallToAction: () => {},
      setSplashStatus: () => {},
    },
  });
  return { controller, toast, http };
}

function toasts(type) {
  return [...document.querySelectorAll(`.toast-${type}:not(.is-leaving)`)];
}

describe("lost connection notice", () => {
  beforeEach(() => {
    vi.resetModules();
    document.body.innerHTML = "";
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("replaces the failure toasts with one notice, and says when the server is back", async () => {
    global.fetch = vi.fn(async () => healthy());
    const state = baseState();
    const { controller, toast } = await mount(state);
    await controller.checkBackendHealth();
    expect(toasts("warning")).toHaveLength(0);

    toast.notifyError("Failed to load frame");
    global.fetch = vi.fn(async () => {
      throw new TypeError("Failed to fetch");
    });
    await controller.checkBackendHealth();

    expect(toasts("error")).toHaveLength(0);
    expect(toasts("warning")).toHaveLength(1);
    expect(toasts("warning")[0].textContent).toContain("backend.connection.lost");

    // Held back while the server is gone: the notice already says why.
    toast.notifyError("Failed to load frame");
    expect(toasts("error")).toHaveLength(0);

    global.fetch = vi.fn(async () => healthy());
    await controller.checkBackendHealth();
    expect(toasts("warning")).toHaveLength(0);
    expect(toasts("success")).toHaveLength(1);
    expect(toasts("success")[0].textContent).toContain("backend.connection.restored");

    toast.notifyError("Failed to load frame");
    expect(toasts("error")).toHaveLength(1);
  });

  it("stays away while ALBIS starts, before the server ever answered", async () => {
    global.fetch = vi.fn(async () => {
      throw new TypeError("Failed to fetch");
    });
    const { controller } = await mount(baseState());
    await controller.checkBackendHealth();
    expect(toasts("warning")).toHaveLength(0);
  });

  it("stays away after File -> Quit ALBIS stopped the server", async () => {
    global.fetch = vi.fn(async () => healthy());
    const state = baseState();
    const { controller } = await mount(state);
    await controller.checkBackendHealth();

    state.serverStopped = true;
    global.fetch = vi.fn(async () => {
      throw new TypeError("Failed to fetch");
    });
    await controller.checkBackendHealth();
    expect(toasts("warning")).toHaveLength(0);
  });

  it("checks the server at once when a request cannot reach it", async () => {
    global.fetch = vi.fn(async () => healthy());
    const state = baseState();
    const { controller, http } = await mount(state);
    await controller.checkBackendHealth();
    // Past the minimum interval between checks.
    vi.spyOn(Date, "now").mockReturnValue(Date.now() + 5000);

    global.fetch = vi.fn(async () => {
      throw new TypeError("Failed to fetch");
    });
    await expect(http.fetchJSON("/api/frame")).rejects.toMatchObject({ status: 0 });
    await vi.waitFor(() => expect(state.backendAlive).toBe(false));
    expect(toasts("warning")).toHaveLength(1);
  });
});
