import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

function buildFetchMock({ dictionaries, tailResponses = [], openLogResponse = null }) {
  let tailIndex = 0;
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
    if (requestUrl.includes("/api/log-tail")) {
      const response = tailResponses[Math.min(tailIndex, tailResponses.length - 1)];
      tailIndex += 1;
      if (response instanceof Error) {
        throw response;
      }
      return {
        ok: response?.ok !== false,
        status: response?.status || 200,
        json: async () => response?.json || {},
      };
    }
    if (requestUrl.endsWith("/api/open-log")) {
      return {
        ok: true,
        json: async () => openLogResponse || { status: "ok", path: "/tmp/albis.log", opened: true },
      };
    }
    throw new Error(`Unexpected fetch URL: ${requestUrl}`);
  });
}

function renderModalShell() {
  document.body.innerHTML = `
    <div id="log-viewer-modal" class="modal" aria-hidden="true">
      <div class="modal-backdrop"></div>
      <div class="modal-card">
        <button id="log-viewer-close-icon" type="button">x</button>
        <div id="log-viewer-message" hidden></div>
        <select id="log-viewer-line-count">
          <option value="200">200</option>
          <option value="500" selected>500</option>
          <option value="1000">1000</option>
        </select>
        <button id="log-viewer-refresh" type="button">refresh</button>
        <input id="log-viewer-follow" type="checkbox" checked />
        <code id="log-viewer-path-value">-</code>
        <strong id="log-viewer-updated-value">-</strong>
        <pre id="log-viewer-content"></pre>
        <button id="log-viewer-open-host" type="button" hidden>open host</button>
        <a id="log-viewer-download" href="/api/log-file">download</a>
        <button id="log-viewer-close" type="button">close</button>
      </div>
    </div>
  `;
}

async function flushAsyncWork() {
  await Promise.resolve();
  await Promise.resolve();
}

async function initializeModules({ tailResponses, backendIsLocal = false }) {
  vi.resetModules();
  renderModalShell();
  localStorage.clear();

  const dictionaries = {
    en: {
      "common.close": "Close",
      "log_viewer.loading": "Loading backend log...",
      "log_viewer.empty": "No log entries yet.",
      "log_viewer.error_unknown": "Unknown error",
      "log_viewer.load_failed": "Failed to load backend log: {{detail}}",
      "log_viewer.opening_host": "Opening backend log on server host...",
      "log_viewer.host_opened": "Opened backend log on server host",
      "log_viewer.host_open_failed": "Failed to open backend log on server host",
      "status.log.opened_file": "Opened backend log file",
      "menu.prompt.save_as_full": "Save As (Full Image)",
      "menu.prompt.save_as_visible": "Save As (Visible Area)",
      "menu.prompt.save_as_window": "Save As (Viewer Window)",
    },
  };

  globalThis.fetch = buildFetchMock({ dictionaries, tailResponses });
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });

  const { createBackendLogViewerController } = await import("../modules/backend_log_viewer_controller.js");
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
  const setStatus = vi.fn();

  const controller = createBackendLogViewerController({
    apiBase: "/api",
    backendIsLocal,
    elements: {
      logViewerModal: document.getElementById("log-viewer-modal"),
      logViewerCloseIcon: document.getElementById("log-viewer-close-icon"),
      logViewerPathValue: document.getElementById("log-viewer-path-value"),
      logViewerUpdatedValue: document.getElementById("log-viewer-updated-value"),
      logViewerMessage: document.getElementById("log-viewer-message"),
      logViewerLineCount: document.getElementById("log-viewer-line-count"),
      logViewerRefresh: document.getElementById("log-viewer-refresh"),
      logViewerFollow: document.getElementById("log-viewer-follow"),
      logViewerContent: document.getElementById("log-viewer-content"),
      logViewerOpenHost: document.getElementById("log-viewer-open-host"),
      logViewerDownload: document.getElementById("log-viewer-download"),
      logViewerClose: document.getElementById("log-viewer-close"),
    },
    callbacks: {
      openModal,
      closeModal,
      setStatus,
    },
  });

  const handler = createMenuActionHandler({
    state: { frameIndex: 0, file: "" },
    callbacks: {
      openBackendLogViewer: () => controller.open(),
      openSettingsModal: vi.fn(),
      checkForUpdates: vi.fn(),
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

  return { controller, handler, openModal, closeModal, setStatus };
}

describe("backend_log_viewer_controller", () => {
  beforeEach(() => {
    window.open = vi.fn();
    if (!HTMLElement.prototype.scrollTo) {
      HTMLElement.prototype.scrollTo = () => {};
    }
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    delete globalThis.fetch;
  });

  it("opens from the menu action, loads log content, and does not use window.open", async () => {
    const { handler, openModal } = await initializeModules({
      tailResponses: [
        {
          json: {
            path: "/tmp/albis.log",
            text: "line 1\nline 2\n",
            requested_lines: 500,
            returned_lines: 2,
            truncated: false,
            size_bytes: 14,
            modified_at: 1710000000,
          },
        },
      ],
    });

    await handler("help-log");
    await flushAsyncWork();

    expect(openModal).toHaveBeenCalledOnce();
    expect(document.getElementById("log-viewer-modal")?.classList.contains("is-open")).toBe(true);
    expect(document.getElementById("log-viewer-content")?.textContent).toBe("line 1\nline 2\n");
    expect(document.getElementById("log-viewer-path-value")?.textContent).toBe("/tmp/albis.log");
    expect(window.open).not.toHaveBeenCalled();
  });

  it("polls while follow mode is enabled and stops polling after close", async () => {
    vi.useFakeTimers();
    const { controller, closeModal } = await initializeModules({
      tailResponses: [
        {
          json: {
            path: "/tmp/albis.log",
            text: "first batch\n",
            requested_lines: 500,
            returned_lines: 1,
            truncated: false,
            size_bytes: 12,
            modified_at: 1710000000,
          },
        },
        {
          json: {
            path: "/tmp/albis.log",
            text: "second batch\n",
            requested_lines: 500,
            returned_lines: 1,
            truncated: false,
            size_bytes: 13,
            modified_at: 1710000001,
          },
        },
      ],
    });

    controller.open();
    await flushAsyncWork();
    expect(globalThis.fetch.mock.calls.filter(([url]) => String(url).includes("/api/log-tail")).length).toBe(1);

    await vi.advanceTimersByTimeAsync(3000);
    await flushAsyncWork();
    expect(globalThis.fetch.mock.calls.filter(([url]) => String(url).includes("/api/log-tail")).length).toBe(2);
    expect(document.getElementById("log-viewer-content")?.textContent).toBe("second batch\n");

    controller.close();
    expect(closeModal).toHaveBeenCalledOnce();

    await vi.advanceTimersByTimeAsync(6000);
    await flushAsyncWork();
    expect(globalThis.fetch.mock.calls.filter(([url]) => String(url).includes("/api/log-tail")).length).toBe(2);
  });

  it("keeps the modal open and shows an inline error when refresh fails", async () => {
    const { controller } = await initializeModules({
      tailResponses: [
        {
          json: {
            path: "/tmp/albis.log",
            text: "stable log\n",
            requested_lines: 500,
            returned_lines: 1,
            truncated: false,
            size_bytes: 11,
            modified_at: 1710000000,
          },
        },
        {
          ok: false,
          status: 500,
          json: {
            detail: "boom",
          },
        },
      ],
    });

    controller.open();
    await vi.waitFor(() => {
      expect(globalThis.fetch.mock.calls.filter(([url]) => String(url).includes("/api/log-tail")).length).toBe(1);
      expect(document.getElementById("log-viewer-refresh")?.disabled).toBe(false);
    });

    document.getElementById("log-viewer-refresh")?.click();
    await vi.waitFor(() => {
      expect(globalThis.fetch.mock.calls.filter(([url]) => String(url).includes("/api/log-tail")).length).toBe(2);
      expect(document.getElementById("log-viewer-message")?.textContent).toContain("Failed to load backend log");
      expect(document.getElementById("log-viewer-content")?.textContent).toBe("stable log\n");
      expect(document.getElementById("log-viewer-refresh")?.disabled).toBe(false);
    });

    expect(document.getElementById("log-viewer-modal")?.classList.contains("is-open")).toBe(true);
  });
});
