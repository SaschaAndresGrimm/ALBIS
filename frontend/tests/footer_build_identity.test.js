import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

/**
 * The footer used to claim two versions: a frontend build hardcoded to the
 * string "local" that no build step ever replaced, and a backend version that
 * duplicated Help -> About. Since both ship inside one artifact they could not
 * disagree, so the popover answered nothing. These tests hold the replacement
 * to being useful: which build is running, whether it is current, and whether
 * this tab has fallen behind the server.
 */

const DICT = {
  "toolbar.footer.build": "ALBIS v{{version}} · {{commit}}",
  "toolbar.footer.build.unstamped": "ALBIS v{{version}}",
  "toolbar.footer.build.title": "Build identity — include this when reporting a problem",
  "toolbar.footer.copy_build": "Copy build info",
  "toolbar.footer.copy_build.copied": "Copied",
  "toolbar.footer.stale": "This tab is older than the server, now v{{version}} · {{commit}}.",
  "toolbar.footer.stale.reload": "Reload",
  "toolbar.footer.update.up_to_date": "Up to date",
  "toolbar.footer.update.available": "Version {{version}} available",
  "toolbar.footer.update.unavailable": "Update check unavailable",
  "toolbar.footer.update.disabled": "Update check is off",
};

function buildFetchMock() {
  return vi.fn(async () => ({ ok: true, json: async () => DICT }));
}

function baseState(overrides = {}) {
  return {
    fps: 1,
    step: 1,
    thresholdIndex: 0,
    panelCollapsed: true,
    zoom: 1,
    file: "",
    dataset: "",
    frameCount: 0,
    frameIndex: 0,
    backendAlive: true,
    backendVersion: "0.12.0",
    backendCommit: "a1b2c3d",
    buildStampAtLoad: "0.12.0@a1b2c3d",
    serverBuildChanged: false,
    updateStatus: "",
    updateLatestVersion: "",
    autoload: { mode: "file", running: false, lastUpdate: 0, interval: 1000 },
    ...overrides,
  };
}

async function mountController(state, callbacks = {}) {
  const { createChromeToolbarController } = await import("../modules/chrome_toolbar_controller.js");
  return createChromeToolbarController({
    state,
    constants: { frameStepOptions: [1], chromeIdleDelayMs: 2000 },
    elements: {
      footerVersionBuildEl: document.getElementById("footer-version-build"),
      footerVersionUpdateEl: document.getElementById("footer-version-update"),
      footerVersionStaleEl: document.getElementById("footer-version-stale"),
      footerVersionStaleTextEl: document.getElementById("footer-version-stale-text"),
      footerVersionReloadEl: document.getElementById("footer-version-reload"),
      footerVersionCopyEl: document.getElementById("footer-version-copy"),
    },
    callbacks: {
      middleTruncate: (value) => String(value || ""),
      fileLabel: (value) => String(value || ""),
      formatTimeStamp: () => "",
      setSummaryChip: () => {},
      estimateToolbarChars: () => 72,
      updateSeriesSumUi: () => {},
      isPhonePanelLayout: () => false,
      isMenuOpen: () => false,
      ...callbacks,
    },
  });
}

describe("footer build identity", () => {
  beforeEach(async () => {
    document.body.innerHTML = `
      <div id="footer-version-build"></div>
      <button id="footer-version-update" type="button" hidden></button>
      <div id="footer-version-stale" hidden><span id="footer-version-stale-text"></span>
        <button id="footer-version-reload" type="button">Reload</button></div>
      <button id="footer-version-copy" type="button">Copy build info</button>
    `;
    localStorage.clear();
    vi.resetModules();
    global.fetch = buildFetchMock();
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("names the build, not just the release", async () => {
    const controller = await mountController(baseState());
    controller.updateFooterVersions();

    // The commit is the half that separates two builds of one release, which is
    // the distinction a bug report turns on.
    expect(document.getElementById("footer-version-build").textContent).toBe(
      "ALBIS v0.12.0 · a1b2c3d",
    );
  });

  it("shows the version alone when the build is unstamped", async () => {
    const controller = await mountController(baseState({ backendCommit: "" }));
    controller.updateFooterVersions();

    // An unstamped build must not invent a commit; a source run is the case.
    expect(document.getElementById("footer-version-build").textContent).toBe("ALBIS v0.12.0");
  });

  it("reports update state, and only offers a control when one is pending", async () => {
    const state = baseState();
    const controller = await mountController(state);
    const row = document.getElementById("footer-version-update");

    state.updateStatus = "up_to_date";
    controller.updateFooterVersions();
    expect(row.hidden).toBe(false);
    expect(row.textContent).toBe("Up to date");
    // Nothing to open, so the row must not pretend to be actionable.
    expect(row.disabled).toBe(true);

    state.updateStatus = "update_available";
    state.updateLatestVersion = "0.13.0";
    controller.updateFooterVersions();
    expect(row.textContent).toBe("Version 0.13.0 available");
    expect(row.disabled).toBe(false);

    state.updateStatus = "disabled";
    controller.updateFooterVersions();
    expect(row.textContent).toBe("Update check is off");
    expect(row.disabled).toBe(true);
  });

  it("hides the update row entirely before the check has answered", async () => {
    const controller = await mountController(baseState({ updateStatus: "" }));
    controller.updateFooterVersions();

    expect(document.getElementById("footer-version-update").hidden).toBe(true);
  });

  it("opens the existing update dialog rather than duplicating it", async () => {
    const onOpenUpdateCheck = vi.fn();
    const state = baseState({ updateStatus: "update_available", updateLatestVersion: "0.13.0" });
    const controller = await mountController(state, { onOpenUpdateCheck });
    controller.updateFooterVersions();

    document.getElementById("footer-version-update").click();
    expect(onOpenUpdateCheck).toHaveBeenCalledTimes(1);
  });

  it("warns only once the running server is a different build", async () => {
    const state = baseState();
    const controller = await mountController(state);
    const stale = document.getElementById("footer-version-stale");

    controller.updateFooterVersions();
    expect(stale.hidden).toBe(true);

    // The condition the popover exists for: a tab left open across an upgrade.
    state.serverBuildChanged = true;
    state.backendVersion = "0.13.0";
    state.backendCommit = "9f2e1c4";
    controller.updateFooterVersions();

    expect(stale.hidden).toBe(false);
    expect(document.getElementById("footer-version-stale-text").textContent).toBe(
      "This tab is older than the server, now v0.13.0 · 9f2e1c4.",
    );
  });

  it("copies build info, falling back when there is no clipboard API", async () => {
    // A plain-HTTP LAN session is not a secure context, which is a documented
    // way to run ALBIS -- so the fallback is the ordinary path, not an edge case.
    const originalClipboard = navigator.clipboard;
    Object.defineProperty(navigator, "clipboard", { value: undefined, configurable: true });
    const execCommand = vi.fn(() => true);
    document.execCommand = execCommand;

    const controller = await mountController(baseState());
    controller.updateFooterVersions();
    const copy = document.getElementById("footer-version-copy");
    copy.click();
    await vi.waitFor(() => expect(copy.textContent).toBe("Copied"));

    expect(execCommand).toHaveBeenCalledWith("copy");

    Object.defineProperty(navigator, "clipboard", {
      value: originalClipboard,
      configurable: true,
    });
    delete document.execCommand;
  });
});
