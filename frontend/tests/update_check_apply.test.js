import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const DOWNLOAD_URL =
  "https://github.com/SaschaAndresGrimm/ALBIS/releases/download/v1.0.0/ALBIS-1.0.0-x86_64.AppImage";
const ASSET_NAME = "ALBIS-1.0.0-x86_64.AppImage";
const SHA256 = "7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069";

const DICTIONARY = {
  "common.cancel": "Cancel",
  "update_check.loading": "Checking...",
  "update_check.status.update_available": "A newer version of ALBIS is available.",
  "update_check.status.up_to_date": "ALBIS is up to date.",
  "update_check.status.unavailable": "Could not check for updates right now.",
  "update_check.action.open_release_page": "Open Release Page",
  "update_check.action.view_releases": "View Releases",
  "update_check.action.download": "Download Update",
  "update_check.action.reveal": "Show in Folder",
  "update_check.action.apply": "Install Update",
  "update_check.action.release_notes": "Release Notes",
  "update_check.action.copy_command": "Copy",
  "update_check.instruction.appimage": "Replace your installed AppImage.",
  "update_check.verify.checksum_ok": "Checksum verified, SHA-256",
  "update_check.verify.checksum_unavailable": "This download could not be verified.",
  "update_check.verify.saved_to": "Saved to",
  "update_check.apply.note": "ALBIS will close to finish installing. Start it again afterwards.",
  "update_check.apply.applying": "Installing the update...",
  "update_check.apply.applied": "The update is installed. ALBIS is closing now.",
  "update_check.apply.failed": "The update could not be installed.",
  "update_check.apply.refusal.busy":
    "ALBIS will not install an update while a live watch, series sum or export is running.",
  "update_check.apply.refusal.unverified_download":
    "Only a download whose checksum was verified can be installed.",
  "update_check.instruction.macos_app": "Quit ALBIS, then drag it to Applications.",
  "update_check.action.handoff.macos_app": "Quit and Open Disk Image",
  "update_check.action.handoff.windows_portable": "Quit and Show in Explorer",
  "update_check.handoff.steps.macos_app": "ALBIS quits and the disk image opens.",
  "update_check.handoff.steps.windows_portable": "ALBIS quits and Explorer shows the archive.",
  "update_check.handoff.stopped.macos_app": "Drag ALBIS to Applications, then start it again.",
  "update_check.handoff.failed": "ALBIS could not show the update and stays open.",
};

function updatePayload(overrides = {}) {
  return {
    status: "update_available",
    current_version: "0.19.0",
    latest_version: "1.0.0",
    release_url: "https://example.invalid/releases/v1.0.0",
    message: "",
    install_kind: "appimage",
    download_url: DOWNLOAD_URL,
    download_name: ASSET_NAME,
    update_command: "",
    download_supported: true,
    apply_supported: true,
    reveal_supported: true,
    ...overrides,
  };
}

function readyDownload(overrides = {}) {
  return {
    status: "ready",
    name: ASSET_NAME,
    path: `/home/x/Downloads/${ASSET_NAME}`,
    bytes_downloaded: 1024,
    bytes_total: 1024,
    sha256: SHA256,
    checksum: "verified",
    signature: "unavailable",
    message: "",
    ...overrides,
  };
}

function applyStatus(overrides = {}) {
  return { status: "idle", message: "", refusal: "", ...overrides };
}

function renderShell() {
  document.body.innerHTML = `
    <div id="update-check-modal" class="modal" aria-hidden="true">
      <div class="modal-card">
        <button id="update-check-close-icon" type="button">x</button>
        <div id="update-check-message"></div>
        <div id="update-check-detail" hidden></div>
        <strong id="update-check-current-version"></strong>
        <div id="update-check-latest-row"><strong id="update-check-latest-version"></strong></div>
        <div id="update-check-instruction" hidden></div>
        <div id="update-check-download-row" hidden><code id="update-check-download-name"></code></div>
        <div id="update-check-command-row" hidden>
          <code id="update-check-command"></code>
          <button id="update-check-command-copy" type="button">Copy</button>
        </div>
        <div id="update-check-progress" hidden>
          <div id="update-check-progress-fill"></div>
          <div id="update-check-progress-text"></div>
        </div>
        <div id="update-check-verify" hidden></div>
        <div id="update-check-apply-note" hidden></div>
        <button id="update-check-cancel" type="button" hidden></button>
        <button id="update-check-reveal" type="button" hidden></button>
        <button id="update-check-release-notes" type="button" hidden></button>
        <button id="update-check-action" type="button" hidden></button>
        <button id="update-check-close" type="button">close</button>
      </div>
    </div>
  `;
}

async function initialize({
  update = updatePayload(),
  download = readyDownload(),
  apply = applyStatus(),
  applyStart,
  applyStartStatus,
  blockers = [],
  blockersThrow = false,
  core = {},
} = {}) {
  vi.resetModules();
  renderShell();
  localStorage.clear();

  const posts = [];
  let applyState = apply;

  global.fetch = vi.fn(async (url, init) => {
    const requestUrl = String(url);
    const method = String(init?.method || "GET").toUpperCase();
    if (requestUrl.includes("locales/")) return { ok: true, json: async () => DICTIONARY };
    if (requestUrl.endsWith("/api/update-check")) return { ok: true, json: async () => update };
    if (requestUrl.endsWith("/api/update-download/status")) {
      return { ok: true, json: async () => download };
    }
    if (requestUrl.endsWith("/api/update-apply/status")) {
      return { ok: true, json: async () => applyState };
    }
    if (requestUrl.endsWith("/api/update-apply/start") && method === "POST") {
      posts.push({ url: "apply", body: JSON.parse(String(init?.body || "{}")) });
      // A refusal is an HTTP status with a body, not a thrown error: the
      // reason code only survives the real http.js path that way.
      if (applyStartStatus) {
        return {
          ok: false,
          status: applyStartStatus.status,
          json: async () => ({ detail: applyStartStatus.detail }),
        };
      }
      applyState = applyStart ?? applyStatus({ status: "applied" });
      return { ok: true, json: async () => applyState };
    }
    if (requestUrl.endsWith("/api/update-download/reveal")) {
      posts.push({ url: "reveal" });
      return { ok: true, json: async () => ({ status: "ok", path: "/tmp/x", opened: true }) };
    }
    throw new Error(`Unexpected fetch URL: ${requestUrl}`);
  });

  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });

  const { createUpdateCheckController } = await import("../modules/update_check_controller.js");
  const byId = (id) => document.getElementById(id);
  const closeModal = vi.fn(() => true);
  const controller = createUpdateCheckController({
    core,
    apiBase: "/api",
    state: { backendVersion: "0.19.0" },
    elements: {
      updateCheckModal: byId("update-check-modal"),
      updateCheckCloseIcon: byId("update-check-close-icon"),
      updateCheckMessage: byId("update-check-message"),
      updateCheckDetail: byId("update-check-detail"),
      updateCheckCurrentVersionValue: byId("update-check-current-version"),
      updateCheckLatestRow: byId("update-check-latest-row"),
      updateCheckLatestVersionValue: byId("update-check-latest-version"),
      updateCheckInstruction: byId("update-check-instruction"),
      updateCheckDownloadRow: byId("update-check-download-row"),
      updateCheckDownloadName: byId("update-check-download-name"),
      updateCheckCommandRow: byId("update-check-command-row"),
      updateCheckCommand: byId("update-check-command"),
      updateCheckCommandCopy: byId("update-check-command-copy"),
      updateCheckProgress: byId("update-check-progress"),
      updateCheckProgressFill: byId("update-check-progress-fill"),
      updateCheckProgressText: byId("update-check-progress-text"),
      updateCheckVerify: byId("update-check-verify"),
      updateCheckApplyNote: byId("update-check-apply-note"),
      updateCheckCancel: byId("update-check-cancel"),
      updateCheckReveal: byId("update-check-reveal"),
      updateCheckReleaseNotes: byId("update-check-release-notes"),
      updateCheckAction: byId("update-check-action"),
      updateCheckClose: byId("update-check-close"),
    },
    callbacks: {
      openModal: vi.fn(() => true),
      closeModal,
      getApplyBlockers: () => {
        if (blockersThrow) throw new Error("state unavailable");
        return blockers;
      },
    },
  });

  return { controller, posts, closeModal };
}

const action = () => document.getElementById("update-check-action");
const note = () => document.getElementById("update-check-apply-note");
const reveal = () => document.getElementById("update-check-reveal");

describe("applying an update", () => {
  beforeEach(() => {
    window.open = vi.fn();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("offers the install on a verified download and warns that ALBIS will close", async () => {
    const { controller, posts } = await initialize();

    await controller.openAndCheck();

    expect(action()?.textContent).toBe("Install Update");
    // The manual "replace your AppImage" instruction would contradict the
    // button that does it for you.
    expect(document.getElementById("update-check-instruction")?.hidden).toBe(true);
    // The folder stays reachable, so the user can still do it by hand.
    expect(reveal()?.hidden).toBe(false);
    expect(reveal()?.textContent).toBe("Show in Folder");
    // Said before the click: the window disappearing is the part that needs
    // warning about.
    expect(note()?.hidden).toBe(false);
    expect(note()?.textContent).toBe(
      "ALBIS will close to finish installing. Start it again afterwards.",
    );
    expect(note()?.classList.contains("is-warning")).toBe(false);

    action()?.click();
    await vi.advanceTimersByTimeAsync(0);

    expect(posts[0]).toEqual({ url: "apply", body: { busy: [] } });
    await vi.advanceTimersByTimeAsync(600);
    expect(note()?.textContent).toBe("The update is installed. ALBIS is closing now.");
    // Nothing left to click but Close.
    expect(action()?.hidden).toBe(true);
    expect(reveal()?.hidden).toBe(true);
    expect(document.getElementById("update-check-cancel")?.hidden).toBe(true);
  });

  it("refuses to install over live work and says why", async () => {
    const { controller, posts } = await initialize({ blockers: ["live_watch"] });

    await controller.openAndCheck();

    expect(note()?.textContent).toContain("will not install an update while a live watch");
    expect(note()?.classList.contains("is-warning")).toBe(true);
    // The download is still there to use by hand, so the instruction for
    // doing that stays.
    expect(document.getElementById("update-check-instruction")?.hidden).toBe(false);
    expect(action()?.textContent).toBe("Show in Folder");

    action()?.click();
    await vi.advanceTimersByTimeAsync(0);
    expect(posts.map((entry) => entry.url)).toEqual(["reveal"]);
  });

  it("never offers to install a download it could not verify", async () => {
    const { controller } = await initialize({
      download: readyDownload({ checksum: "unavailable", message: "No checksum list." }),
      apply: applyStatus({ refusal: "unverified_download" }),
    });

    await controller.openAndCheck();

    expect(action()?.textContent).toBe("Show in Folder");
    expect(note()?.textContent).toBe(
      "Only a download whose checksum was verified can be installed.",
    );
    expect(note()?.classList.contains("is-warning")).toBe(true);
  });

  it("keeps the folder button when the build cannot apply updates", async () => {
    // `ui.allow_update_apply: false`, macOS, Docker or a source checkout.
    const { controller, posts } = await initialize({
      update: updatePayload({ apply_supported: false }),
    });

    await controller.openAndCheck();

    expect(action()?.textContent).toBe("Show in Folder");
    expect(reveal()?.hidden).toBe(true);
    expect(note()?.hidden).toBe(true);

    action()?.click();
    await vi.advanceTimersByTimeAsync(0);
    expect(posts.map((entry) => entry.url)).toEqual(["reveal"]);
  });

  it("shows the backend's refusal when it knows something the interface does not", async () => {
    // A series sum runs in the backend; the interface has no blockers of its
    // own, so the 409 detail is the only thing that can say no.
    const { controller } = await initialize({
      applyStartStatus: { status: 409, detail: "busy" },
    });

    await controller.openAndCheck();
    expect(action()?.textContent).toBe("Install Update");

    action()?.click();
    await vi.advanceTimersByTimeAsync(600);

    expect(note()?.textContent).toContain("will not install an update while a live watch");
    // Refused, not failed: the download is untouched and still usable.
    expect(action()?.textContent).toBe("Show in Folder");
  });

  it("reports a failed install without claiming it worked", async () => {
    const { controller } = await initialize({
      applyStart: applyStatus({ status: "failed", message: "Could not replace the AppImage." }),
    });

    await controller.openAndCheck();
    action()?.click();
    await vi.advanceTimersByTimeAsync(600);

    expect(note()?.textContent).toBe("Could not replace the AppImage.");
    expect(note()?.classList.contains("is-warning")).toBe(true);
    expect(action()?.textContent).toBe("Show in Folder");
  });

  it("sends the live work it knows about so the backend can refuse it too", async () => {
    // Belt and braces: the interface blocks the button, and if it is reached
    // anyway the backend gets told what is running.
    const { controller, posts } = await initialize({ blockers: [] });
    await controller.openAndCheck();
    action()?.click();
    await vi.advanceTimersByTimeAsync(0);

    expect(posts[0].body).toEqual({ busy: [] });
  });

  it("treats a caller that cannot report its state as busy", async () => {
    // The safe reading of "I cannot tell you what is running" is that
    // something might be, so the install is withheld rather than offered.
    const { controller } = await initialize({ blockersThrow: true });

    await controller.openAndCheck();

    expect(action()?.textContent).toBe("Show in Folder");
    expect(note()?.textContent).toContain("will not install an update while a live watch");
  });

  it("does not offer the install while the download is still running", async () => {
    const { controller } = await initialize({
      download: readyDownload({ status: "downloading", bytes_downloaded: 10, bytes_total: 100 }),
    });

    await controller.openAndCheck();

    expect(document.getElementById("update-check-progress")?.hidden).toBe(false);
    expect(note()?.hidden).toBe(true);
    expect(action()?.hidden).toBe(true);
  });
});

describe("quitting so the update can be installed", () => {
  const MAC_ASSET = "ALBIS-macos-arm64-v1.0.0-abc1234.dmg";
  const macUpdate = (overrides = {}) =>
    updatePayload({
      install_kind: "macos_app",
      download_url: `https://github.com/SaschaAndresGrimm/ALBIS/releases/download/v1.0.0/${MAC_ASSET}`,
      download_name: MAC_ASSET,
      apply_supported: false,
      handoff_supported: true,
      ...overrides,
    });
  const quitter = (result = true) => ({ quit: vi.fn(async () => result) });

  beforeEach(() => {
    window.open = vi.fn();
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("offers Quit and Open Disk Image on a Mac, with the steps said first", async () => {
    const quit = quitter();
    const { controller, closeModal } = await initialize({
      update: macUpdate(),
      download: readyDownload({ name: MAC_ASSET }),
      core: { quit },
    });

    await controller.openAndCheck();

    expect(action()?.textContent).toBe("Quit and Open Disk Image");
    expect(note()?.textContent).toBe("ALBIS quits and the disk image opens.");
    // One set of steps on screen, not the manual one as well.
    expect(document.getElementById("update-check-instruction")?.hidden).toBe(true);
    // Showing the folder without quitting stays possible.
    expect(reveal()?.hidden).toBe(false);

    action()?.click();
    await vi.advanceTimersByTimeAsync(0);

    // The quit controller asks about running work and leaves the stopped
    // page, with the same steps on it.
    expect(quit.quit).toHaveBeenCalledWith({
      endpoint: "update-download/open-and-quit",
      stoppedText: "Drag ALBIS to Applications, then start it again.",
      failedText: "ALBIS could not show the update and stays open.",
    });
    expect(closeModal).toHaveBeenCalled();
  });

  it("names Explorer for the Windows portable zip", async () => {
    const { controller } = await initialize({
      update: macUpdate({ install_kind: "windows_portable" }),
      download: readyDownload({ name: MAC_ASSET }),
      core: { quit: quitter() },
    });

    await controller.openAndCheck();

    expect(action()?.textContent).toBe("Quit and Show in Explorer");
    expect(note()?.textContent).toBe("ALBIS quits and Explorer shows the archive.");
  });

  it("keeps the dialog open when ALBIS did not stop", async () => {
    const quit = quitter(false);
    const { controller, closeModal } = await initialize({
      update: macUpdate(),
      download: readyDownload({ name: MAC_ASSET }),
      core: { quit },
    });

    await controller.openAndCheck();
    action()?.click();
    await vi.advanceTimersByTimeAsync(0);

    expect(quit.quit).toHaveBeenCalled();
    expect(closeModal).not.toHaveBeenCalled();
  });

  it("falls back to Show in Folder where ALBIS cannot stop itself", async () => {
    const { controller, posts } = await initialize({
      update: macUpdate({ handoff_supported: false }),
      download: readyDownload({ name: MAC_ASSET }),
      core: { quit: quitter() },
    });

    await controller.openAndCheck();

    expect(action()?.textContent).toBe("Show in Folder");
    expect(document.getElementById("update-check-instruction")?.textContent).toBe(
      "Quit ALBIS, then drag it to Applications.",
    );
    action()?.click();
    await vi.advanceTimersByTimeAsync(0);
    expect(posts.map((entry) => entry.url)).toEqual(["reveal"]);
  });

  it("is not offered before the download is ready", async () => {
    const { controller } = await initialize({
      update: macUpdate(),
      download: readyDownload({ name: MAC_ASSET, status: "downloading", bytes_downloaded: 1 }),
      core: { quit: quitter() },
    });

    await controller.openAndCheck();

    expect(action()?.hidden).toBe(true);
    expect(note()?.hidden).toBe(true);
  });
});
