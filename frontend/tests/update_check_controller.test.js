import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

function createDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function buildFetchMock({ dictionaries, updateJson }) {
  return vi.fn(async (url) => {
    const requestUrl = String(url);
    if (requestUrl.includes("locales/")) {
      const match = requestUrl.match(/locales\/([^/]+)\.json/);
      const language = match ? decodeURIComponent(match[1]) : "en";
      return {
        ok: true,
        json: async () => dictionaries[language] || {},
      };
    }
    if (requestUrl.endsWith("/api/update-check")) {
      return {
        ok: true,
        json: async () => updateJson,
      };
    }
    throw new Error(`Unexpected fetch URL: ${requestUrl}`);
  });
}

function renderModalShell() {
  document.body.innerHTML = `
    <div id="update-check-modal" class="modal" aria-hidden="true">
      <div class="modal-backdrop"></div>
      <div class="modal-card">
        <button id="update-check-close-icon" type="button">x</button>
        <div id="update-check-message"></div>
        <div id="update-check-detail" hidden></div>
        <div id="update-check-version-grid">
          <div id="update-check-current-row">
            <strong id="update-check-current-version"></strong>
          </div>
          <div id="update-check-latest-row">
            <strong id="update-check-latest-version"></strong>
          </div>
        </div>
        <button id="update-check-action" type="button" hidden></button>
        <button id="update-check-close" type="button">close</button>
      </div>
    </div>
  `;
}

async function initializeModules({ updateJson }) {
  vi.resetModules();
  renderModalShell();
  localStorage.clear();

  const dictionaries = {
    en: {
      "update_check.loading": "Checking for updates...",
      "update_check.status.update_available": "A newer version of ALBIS is available.",
      "update_check.status.up_to_date": "ALBIS is up to date.",
      "update_check.status.unavailable": "Could not check for updates right now.",
      "update_check.action.open_release_page": "Open Release Page",
      "update_check.action.view_releases": "View Releases",
    },
  };

  global.fetch = buildFetchMock({ dictionaries, updateJson });
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });

  const { createUpdateCheckController } = await import("../modules/update_check_controller.js");
  const { createMenuActionHandler } = await import("../modules/menu_actions.js");

  const openModal = vi.fn((modalEl) => {
    modalEl.classList.add("is-open");
    modalEl.setAttribute("aria-hidden", "false");
    return true;
  });
  const closeModal = vi.fn((modalEl) => {
    modalEl.classList.remove("is-open");
    modalEl.setAttribute("aria-hidden", "true");
    return true;
  });

  const controller = createUpdateCheckController({
    apiBase: "/api",
    state: { backendVersion: "0.9.2" },
    elements: {
      updateCheckModal: document.getElementById("update-check-modal"),
      updateCheckCloseIcon: document.getElementById("update-check-close-icon"),
      updateCheckMessage: document.getElementById("update-check-message"),
      updateCheckDetail: document.getElementById("update-check-detail"),
      updateCheckCurrentVersionValue: document.getElementById("update-check-current-version"),
      updateCheckLatestRow: document.getElementById("update-check-latest-row"),
      updateCheckLatestVersionValue: document.getElementById("update-check-latest-version"),
      updateCheckAction: document.getElementById("update-check-action"),
      updateCheckClose: document.getElementById("update-check-close"),
    },
    callbacks: {
      openModal,
      closeModal,
    },
  });

  const handler = createMenuActionHandler({
    apiBase: "/api",
    state: {},
    callbacks: {
      setStatus: vi.fn(),
      openSettingsModal: vi.fn(),
      checkForUpdates: () => controller.openAndCheck(),
      openCommandPalette: vi.fn(),
      toggleFullscreen: vi.fn(),
      openAboutModal: vi.fn(),
      openFileModal: vi.fn(),
      closeCurrentFile: vi.fn(),
      exportFullImage: vi.fn(),
      exportVisibleArea: vi.fn(),
      exportViewerWindow: vi.fn(),
    },
  });

  return { controller, handler, openModal, closeModal };
}

describe("update check controller", () => {
  beforeEach(() => {
    window.open = vi.fn();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("opens from the menu action, shows loading immediately, then renders update-available state", async () => {
    const deferred = createDeferred();
    const { handler, openModal } = await initializeModules({
      updateJson: deferred.promise,
    });

    const pending = handler("help-check-updates");
    await Promise.resolve();

    expect(openModal).toHaveBeenCalledOnce();
    expect(document.getElementById("update-check-modal")?.classList.contains("is-open")).toBe(true);
    expect(document.getElementById("update-check-message")?.textContent).toBe("Checking for updates...");
    expect(document.getElementById("update-check-action")?.hidden).toBe(true);
    expect(global.fetch.mock.calls.some(([url]) => String(url).endsWith("/api/update-check"))).toBe(true);

    deferred.resolve({
      status: "update_available",
      current_version: "0.9.2",
      latest_version: "0.9.3",
      release_url: "https://example.invalid/releases/v0.9.3",
      message: "",
    });
    await pending;

    expect(document.getElementById("update-check-message")?.textContent).toBe("A newer version of ALBIS is available.");
    expect(document.getElementById("update-check-current-version")?.textContent).toBe("0.9.2");
    expect(document.getElementById("update-check-latest-version")?.textContent).toBe("0.9.3");
    expect(document.getElementById("update-check-action")?.textContent).toBe("Open Release Page");
    expect(document.getElementById("update-check-action")?.hidden).toBe(false);
  });

  it("renders the up-to-date state with a releases action", async () => {
    const { controller } = await initializeModules({
      updateJson: {
        status: "up_to_date",
        current_version: "0.9.2",
        latest_version: "0.9.2",
        release_url: "https://example.invalid/releases/v0.9.2",
        message: "",
      },
    });

    await controller.openAndCheck();

    expect(document.getElementById("update-check-message")?.textContent).toBe("ALBIS is up to date.");
    expect(document.getElementById("update-check-action")?.textContent).toBe("View Releases");
    expect(document.getElementById("update-check-latest-row")?.hidden).toBe(false);
  });

  it("renders the unavailable state and shows the backend detail message", async () => {
    const { controller } = await initializeModules({
      updateJson: {
        status: "unavailable",
        current_version: "0.9.2",
        latest_version: "",
        release_url: "https://github.com/SaschaAndresGrimm/ALBIS/releases",
        message: "GitHub release metadata was unavailable.",
      },
    });

    await controller.openAndCheck();

    expect(document.getElementById("update-check-message")?.textContent).toBe("Could not check for updates right now.");
    expect(document.getElementById("update-check-detail")?.textContent).toBe("GitHub release metadata was unavailable.");
    expect(document.getElementById("update-check-detail")?.hidden).toBe(false);
    expect(document.getElementById("update-check-action")?.textContent).toBe("View Releases");
    expect(document.getElementById("update-check-latest-row")?.hidden).toBe(true);
  });

  it("startup check opens the modal only when an update is available", async () => {
    const { controller, openModal } = await initializeModules({
      updateJson: {
        status: "update_available",
        current_version: "0.9.2",
        latest_version: "0.9.3",
        release_url: "https://example.invalid/releases/v0.9.3",
        message: "",
      },
    });

    const payload = await controller.checkOnStartup();

    expect(payload?.status).toBe("update_available");
    expect(openModal).toHaveBeenCalledOnce();
    expect(document.getElementById("update-check-modal")?.classList.contains("is-open")).toBe(true);
    expect(document.getElementById("update-check-message")?.textContent).toBe("A newer version of ALBIS is available.");
    expect(document.getElementById("update-check-action")?.textContent).toBe("Open Release Page");
  });

  it("startup check stays quiet when ALBIS is up to date", async () => {
    const { controller, openModal } = await initializeModules({
      updateJson: {
        status: "up_to_date",
        current_version: "0.9.2",
        latest_version: "0.9.2",
        release_url: "https://example.invalid/releases/v0.9.2",
        message: "",
      },
    });

    const payload = await controller.checkOnStartup();

    expect(payload?.status).toBe("up_to_date");
    expect(openModal).not.toHaveBeenCalled();
    expect(document.getElementById("update-check-modal")?.classList.contains("is-open")).toBe(false);
    expect(document.getElementById("update-check-message")?.textContent).toBe("");
  });

  it("startup check can be disabled before making a request", async () => {
    const { controller, openModal } = await initializeModules({
      updateJson: {
        status: "update_available",
        current_version: "0.9.2",
        latest_version: "0.9.3",
        release_url: "https://example.invalid/releases/v0.9.3",
        message: "",
      },
    });

    const payload = await controller.checkOnStartup({ enabled: false });

    expect(payload).toBeNull();
    expect(openModal).not.toHaveBeenCalled();
    expect(global.fetch.mock.calls.filter(([url]) => String(url).endsWith("/api/update-check"))).toHaveLength(0);
  });

  it("opens the release page when the modal action is clicked", async () => {
    const { controller } = await initializeModules({
      updateJson: {
        status: "update_available",
        current_version: "0.9.2",
        latest_version: "0.9.3",
        release_url: "https://example.invalid/releases/v0.9.3",
        message: "",
      },
    });

    await controller.openAndCheck();
    document.getElementById("update-check-action")?.click();

    expect(window.open).toHaveBeenCalledWith(
      "https://example.invalid/releases/v0.9.3",
      "_blank",
      "noopener",
    );
  });
});
