import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

/**
 * Detecting that this tab has fallen behind the server it talks to.
 *
 * The cache policy already rules out being *served* a stale frontend: entry
 * documents are `no-store` and modules revalidate against an ETag. What it
 * cannot rule out is a tab loaded before the server was upgraded and never
 * reloaded -- the code in memory then predates the server. A viewer left open
 * on a workstation across an upgrade is exactly that, and only a reload fixes
 * it, so noticing is the whole value.
 */

function healthResponse({ version, commit }) {
  return {
    ok: true,
    json: async () => ({ status: "ok", version, commit, compression_encodings: ["gzip"] }),
  };
}

function baseState() {
  return {
    backendAlive: false,
    backendVersion: "",
    backendCommit: "",
    buildStampAtLoad: null,
    serverBuildChanged: false,
    autoload: { mode: "file", running: false, lastUpdate: 0, interval: 1000 },
  };
}

async function mountController(state) {
  const { createBackendStatusController } = await import("../modules/backend_status_controller.js");
  return createBackendStatusController({
    apiBase: "/api",
    state,
    elements: { liveBadge: null, backendBadge: null, aboutVersion: null },
    callbacks: {
      updateFooterVersions: () => {},
      updateSplashCallToAction: () => {},
      setSplashStatus: () => {},
    },
  });
}

describe("server build change detection", () => {
  beforeEach(() => {
    vi.resetModules();
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("records the server build at load without reporting a change", async () => {
    global.fetch = vi.fn(async () => healthResponse({ version: "0.12.0", commit: "a1b2c3d" }));
    const state = baseState();
    const controller = await mountController(state);

    await controller.checkBackendHealth();

    expect(state.buildStampAtLoad).toBe("0.12.0@a1b2c3d");
    expect(state.serverBuildChanged).toBe(false);
    expect(state.backendCommit).toBe("a1b2c3d");
  });

  it("stays quiet while the server keeps reporting the same build", async () => {
    global.fetch = vi.fn(async () => healthResponse({ version: "0.12.0", commit: "a1b2c3d" }));
    const state = baseState();
    const controller = await mountController(state);

    await controller.checkBackendHealth();
    await controller.checkBackendHealth();
    await controller.checkBackendHealth();

    expect(state.serverBuildChanged).toBe(false);
  });

  it("notices a rebuild of the same release, which the version alone cannot", async () => {
    let commit = "a1b2c3d";
    global.fetch = vi.fn(async () => healthResponse({ version: "0.12.0", commit }));
    const state = baseState();
    const controller = await mountController(state);

    await controller.checkBackendHealth();
    // Same version, different build: the case a version comparison misses.
    commit = "9f2e1c4";
    await controller.checkBackendHealth();

    expect(state.serverBuildChanged).toBe(true);
  });

  it("notices an upgrade when the server reports no commit at all", async () => {
    let version = "0.12.0";
    global.fetch = vi.fn(async () => healthResponse({ version, commit: undefined }));
    const state = baseState();
    const controller = await mountController(state);

    await controller.checkBackendHealth();
    expect(state.buildStampAtLoad).toBe("0.12.0@");
    version = "0.13.0";
    await controller.checkBackendHealth();

    expect(state.serverBuildChanged).toBe(true);
  });

  it("does not treat the backend going away as a new build", async () => {
    let online = true;
    global.fetch = vi.fn(async () => {
      if (!online) throw new Error("offline");
      return healthResponse({ version: "0.12.0", commit: "a1b2c3d" });
    });
    const state = baseState();
    const controller = await mountController(state);

    await controller.checkBackendHealth();
    online = false;
    await controller.checkBackendHealth();

    // A restart of the same build is not a reason to ask anyone to reload.
    expect(state.serverBuildChanged).toBe(false);
    online = true;
    await controller.checkBackendHealth();
    expect(state.serverBuildChanged).toBe(false);
  });

  it("stays flagged once behind, because the tab does not catch up on its own", async () => {
    let commit = "a1b2c3d";
    global.fetch = vi.fn(async () => healthResponse({ version: "0.12.0", commit }));
    const state = baseState();
    const controller = await mountController(state);

    await controller.checkBackendHealth();
    commit = "9f2e1c4";
    await controller.checkBackendHealth();
    // Even if the server were rolled back, this page is still the old code.
    commit = "a1b2c3d";
    await controller.checkBackendHealth();

    expect(state.serverBuildChanged).toBe(true);
  });
});
