import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const DOWNLOAD_URL =
  "https://github.com/SaschaAndresGrimm/ALBIS/releases/download/v1.0.0/ALBIS-macos-arm64-v1.0.0-abc1234.dmg";
const ASSET_NAME = "ALBIS-macos-arm64-v1.0.0-abc1234.dmg";
const SHA256 = "7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069";

const DICTIONARY = {
  "common.cancel": "Cancel",
  "update_check.loading": "Checking for updates...",
  "update_check.status.update_available": "A newer version of ALBIS is available.",
  "update_check.status.up_to_date": "ALBIS is up to date.",
  "update_check.status.unavailable": "Could not check for updates right now.",
  "update_check.action.open_release_page": "Open Release Page",
  "update_check.action.view_releases": "View Releases",
  "update_check.action.download": "Download Update",
  "update_check.action.reveal": "Show in Folder",
  "update_check.action.release_notes": "Release Notes",
  "update_check.action.copy_command": "Copy",
  "update_check.instruction.macos_app": "Drag ALBIS to Applications.",
  "update_check.progress.downloading": "Downloading {{done}} of {{total}} ({{percent}}%)",
  "update_check.progress.downloading_unsized": "Downloading {{done}}",
  "update_check.progress.verifying": "Verifying checksum...",
  "update_check.verify.checksum_ok": "Checksum verified, SHA-256",
  "update_check.verify.checksum_unavailable": "This download could not be verified.",
  "update_check.verify.signature_ok": "Release signature verified.",
  "update_check.verify.signature_invalid": "Not signed by the ALBIS release key.",
  "update_check.verify.saved_to": "Saved to",
  "update_check.download.failed": "The download did not complete.",
};

function updatePayload(overrides = {}) {
  return {
    status: "update_available",
    current_version: "0.19.0",
    latest_version: "1.0.0",
    release_url: "https://example.invalid/releases/v1.0.0",
    message: "",
    install_kind: "macos_app",
    download_url: DOWNLOAD_URL,
    download_name: ASSET_NAME,
    update_command: "",
    download_supported: true,
    ...overrides,
  };
}

function downloadPayload(overrides = {}) {
  return {
    status: "idle",
    name: "",
    path: "",
    bytes_downloaded: 0,
    bytes_total: 0,
    sha256: "",
    checksum: "pending",
    signature: "pending",
    message: "",
    ...overrides,
  };
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
        <button id="update-check-cancel" type="button" hidden></button>
        <button id="update-check-release-notes" type="button" hidden></button>
        <button id="update-check-action" type="button" hidden></button>
        <button id="update-check-close" type="button">close</button>
      </div>
    </div>
  `;
}

/**
 * A fetch stand-in for the four endpoints the dialog talks to.
 *
 * `statusQueue` is consumed one entry per poll, so a test can describe a
 * transfer as the sequence of states the backend would report.
 */
function buildFetchMock({ update, start, statusQueue = [], posts }) {
  return vi.fn(async (url, init) => {
    const requestUrl = String(url);
    const method = String(init?.method || "GET").toUpperCase();
    if (requestUrl.includes("locales/")) {
      return { ok: true, json: async () => DICTIONARY };
    }
    if (requestUrl.endsWith("/api/update-check")) {
      return { ok: true, json: async () => update };
    }
    if (requestUrl.endsWith("/api/update-download/start")) {
      posts.push({ url: "start", body: JSON.parse(String(init?.body || "{}")) });
      if (start instanceof Error) throw start;
      return { ok: true, json: async () => start };
    }
    if (requestUrl.endsWith("/api/update-download/status") && method === "GET") {
      const next = statusQueue.length > 1 ? statusQueue.shift() : statusQueue[0];
      return { ok: true, json: async () => next ?? downloadPayload() };
    }
    if (requestUrl.endsWith("/api/update-download/cancel")) {
      posts.push({ url: "cancel" });
      return { ok: true, json: async () => downloadPayload({ status: "cancelled", name: ASSET_NAME }) };
    }
    if (requestUrl.endsWith("/api/update-download/reveal")) {
      posts.push({ url: "reveal" });
      return { ok: true, json: async () => ({ status: "ok", path: "/tmp/x", opened: true }) };
    }
    throw new Error(`Unexpected fetch URL: ${requestUrl}`);
  });
}

/**
 * @param initialStatus what the backend reports when the dialog first opens.
 *   Defaults to idle, because the dialog reads the download state on open and
 *   would otherwise consume the first entry a test meant for its polling.
 */
async function initialize({
  update = updatePayload(),
  start,
  statusQueue = [],
  initialStatus = downloadPayload(),
} = {}) {
  vi.resetModules();
  renderShell();
  localStorage.clear();

  const posts = [];
  global.fetch = buildFetchMock({
    update,
    start: start ?? downloadPayload({ status: "downloading", name: ASSET_NAME }),
    statusQueue: [initialStatus, ...statusQueue],
    posts,
  });

  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });

  const { createUpdateCheckController } = await import("../modules/update_check_controller.js");
  const byId = (id) => document.getElementById(id);
  const controller = createUpdateCheckController({
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
      updateCheckCancel: byId("update-check-cancel"),
      updateCheckReleaseNotes: byId("update-check-release-notes"),
      updateCheckAction: byId("update-check-action"),
      updateCheckClose: byId("update-check-close"),
    },
    callbacks: { openModal: vi.fn(() => true), closeModal: vi.fn(() => true) },
  });

  return { controller, posts };
}

const action = () => document.getElementById("update-check-action");
const verify = () => document.getElementById("update-check-verify");
const progress = () => document.getElementById("update-check-progress");

/** Let the polling timer fire until the dialog settles out of a running state. */
async function settle(times = 6) {
  for (let i = 0; i < times; i += 1) {
    await vi.advanceTimersByTimeAsync(600);
  }
}

describe("in-app update download", () => {
  beforeEach(() => {
    window.open = vi.fn();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("downloads, verifies and then offers the folder rather than the installer", async () => {
    const { controller, posts } = await initialize({
      statusQueue: [
        downloadPayload({
          status: "downloading",
          name: ASSET_NAME,
          bytes_downloaded: 5 * 1024 * 1024,
          bytes_total: 10 * 1024 * 1024,
        }),
        downloadPayload({ status: "verifying", name: ASSET_NAME, bytes_total: 10 * 1024 * 1024 }),
        downloadPayload({
          status: "ready",
          name: ASSET_NAME,
          path: `/Users/x/Downloads/${ASSET_NAME}`,
          sha256: SHA256,
          checksum: "verified",
          signature: "verified",
        }),
      ],
    });

    await controller.openAndCheck();
    expect(action()?.textContent).toBe("Download Update");

    action()?.click();
    await vi.advanceTimersByTimeAsync(0);

    // Reported as running before the start request resolves, so a slow start
    // does not leave the button looking unclicked.
    expect(progress()?.hidden).toBe(false);
    expect(document.getElementById("update-check-cancel")?.hidden).toBe(false);
    expect(action()?.hidden).toBe(true);
    expect(posts[0]).toEqual({ url: "start", body: { url: DOWNLOAD_URL, name: ASSET_NAME } });

    await vi.advanceTimersByTimeAsync(600);
    expect(document.getElementById("update-check-progress-text")?.textContent).toBe(
      "Downloading 5.0 MB of 10.0 MB (50%)",
    );
    expect(document.getElementById("update-check-progress-fill")?.style.width).toBe("50%");

    await settle();

    expect(progress()?.hidden).toBe(true);
    expect(verify()?.hidden).toBe(false);
    expect(verify()?.textContent).toContain("Checksum verified");
    expect(verify()?.textContent).toContain(SHA256.slice(0, 16));
    expect(verify()?.textContent).toContain("Release signature verified.");
    expect(verify()?.textContent).toContain(`/Users/x/Downloads/${ASSET_NAME}`);
    // ALBIS never launches the installer; it shows the user where it is.
    expect(action()?.textContent).toBe("Show in Folder");
    expect(document.getElementById("update-check-cancel")?.hidden).toBe(true);

    action()?.click();
    await vi.advanceTimersByTimeAsync(0);
    expect(posts.at(-1)).toEqual({ url: "reveal" });
  });

  it("reports a checksum mismatch and falls back to the release page", async () => {
    const { controller } = await initialize({
      statusQueue: [
        downloadPayload({
          status: "failed",
          name: ASSET_NAME,
          checksum: "mismatch",
          message: "The download does not match the checksum published for it and was deleted.",
        }),
      ],
    });

    await controller.openAndCheck();
    action()?.click();
    await settle();

    expect(verify()?.textContent).toContain("does not match the checksum");
    expect(verify()?.querySelector(".is-warning")).not.toBeNull();
    // No download to reveal, so the way forward is the release page.
    expect(action()?.textContent).toBe("Open Release Page");
  });

  it("hands over an unverifiable download but says it is unverified", async () => {
    const { controller } = await initialize({
      statusQueue: [
        downloadPayload({
          status: "ready",
          name: ASSET_NAME,
          path: `/tmp/${ASSET_NAME}`,
          sha256: SHA256,
          checksum: "unavailable",
          signature: "unavailable",
          message: "This release publishes no checksum list.",
        }),
      ],
    });

    await controller.openAndCheck();
    action()?.click();
    await settle();

    expect(verify()?.textContent).toContain("no checksum list");
    expect(verify()?.querySelector(".is-warning")).not.toBeNull();
    expect(action()?.textContent).toBe("Show in Folder");
  });

  it("refuses a download whose checksum list is not signed by the release key", async () => {
    // The backend deletes it: a digest that matches a list it cannot trust is
    // not evidence. So this arrives as a failure, not as a ready file with a
    // warning beside it.
    const { controller } = await initialize({
      statusQueue: [
        downloadPayload({
          status: "failed",
          name: ASSET_NAME,
          sha256: SHA256,
          checksum: "verified",
          signature: "invalid",
          message:
            "The release checksum list is not signed by the ALBIS release key, so the download could not be trusted and was deleted.",
        }),
      ],
    });

    await controller.openAndCheck();
    action()?.click();
    await settle();

    expect(verify()?.textContent).toContain("not signed by the ALBIS release key");
    expect(verify()?.querySelector(".is-warning")).not.toBeNull();
    // Nothing to reveal, so the release page is the way forward.
    expect(action()?.textContent).toBe("Open Release Page");
  });

  it("opens the link in the browser when the backend will not download", async () => {
    // `ui.allow_update_download: false`, or a build that cannot fetch it. The
    // dialog keeps the Tier-0 behaviour rather than losing the button.
    const { controller, posts } = await initialize({
      update: updatePayload({ download_supported: false }),
    });

    await controller.openAndCheck();
    expect(action()?.textContent).toBe("Download Update");

    action()?.click();
    await vi.advanceTimersByTimeAsync(0);

    expect(window.open).toHaveBeenCalledWith(DOWNLOAD_URL, "_blank", "noopener");
    expect(posts).toEqual([]);
    expect(progress()?.hidden).toBe(true);
  });

  it("cancels a running download on request", async () => {
    const { controller, posts } = await initialize({
      statusQueue: [downloadPayload({ status: "downloading", name: ASSET_NAME })],
    });

    await controller.openAndCheck();
    action()?.click();
    await vi.advanceTimersByTimeAsync(0);

    document.getElementById("update-check-cancel")?.click();
    await vi.advanceTimersByTimeAsync(0);

    expect(posts.map((entry) => entry.url)).toEqual(["start", "cancel"]);
    expect(progress()?.hidden).toBe(true);
    expect(action()?.textContent).toBe("Download Update");
  });

  it("ignores a finished download of a different asset", async () => {
    // A download left over from an earlier release is on disk, but it is not
    // this release's file, so it must not be presented as one.
    const { controller } = await initialize({
      statusQueue: [
        downloadPayload({
          status: "ready",
          name: "ALBIS-macos-arm64-v0.19.0-old1234.dmg",
          path: "/tmp/ALBIS-macos-arm64-v0.19.0-old1234.dmg",
          sha256: SHA256,
          checksum: "verified",
        }),
      ],
    });

    await controller.openAndCheck();

    expect(verify()?.hidden).toBe(true);
    expect(action()?.textContent).toBe("Download Update");
  });

  it("resumes onto the progress bar when reopened mid-download", async () => {
    const { controller } = await initialize({
      initialStatus: downloadPayload({
        status: "downloading",
        name: ASSET_NAME,
        bytes_downloaded: 1024,
        bytes_total: 4 * 1024 * 1024,
      }),
    });

    await controller.openAndCheck();

    // No click: the dialog read the state the backend was already in.
    expect(progress()?.hidden).toBe(false);
    expect(document.getElementById("update-check-progress-text")?.textContent).toBe(
      "Downloading 1 KB of 4.0 MB (0%)",
    );
  });

  it("stops polling when the dialog is closed", async () => {
    const { controller } = await initialize({
      statusQueue: [downloadPayload({ status: "downloading", name: ASSET_NAME })],
    });

    await controller.openAndCheck();
    action()?.click();
    await vi.advanceTimersByTimeAsync(600);

    const before = global.fetch.mock.calls.length;
    controller.close();
    await vi.advanceTimersByTimeAsync(5000);

    // Closing the dialog stops the polling, not the transfer.
    expect(global.fetch.mock.calls.length).toBe(before);
  });

  it("reports a refused start without claiming a download is running", async () => {
    const { controller } = await initialize({ start: new Error("403") });

    await controller.openAndCheck();
    action()?.click();
    await settle(2);

    expect(progress()?.hidden).toBe(true);
    expect(verify()?.textContent).toContain("The download did not complete.");
    expect(action()?.textContent).toBe("Open Release Page");
  });
});
