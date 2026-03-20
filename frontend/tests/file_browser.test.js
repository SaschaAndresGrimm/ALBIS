import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

function buildFetchMock() {
  return vi.fn(async (url) => {
    if (String(url).includes("locales/")) {
      return {
        ok: true,
        json: async () => ({
          "file_browser.root": "Root",
          "file_browser.loading": "Loading {{label}}",
          "file_browser.no_folders": "No folders",
          "file_browser.no_images": "No files",
          "status.file.select_image_first": "Select a file first",
          "status.file.no_selection": "No selection",
        }),
      };
    }
    return {
      ok: true,
      json: async () => ({
        folders: [],
        files: ["imported.expt"],
        currentPath: "",
        root: "/tmp",
        canGoUp: false,
        allowAbsolutePaths: true,
      }),
    };
  });
}

describe("file_browser", () => {
  beforeEach(() => {
    document.body.innerHTML = `
      <div id="browse-modal"></div>
      <div id="browse-breadcrumb"></div>
      <div id="browse-folders-list"></div>
      <div id="browse-files-list"></div>
      <input id="browse-path-input" />
      <div id="browse-status"></div>
      <button id="browse-select"></button>
      <button id="browse-cancel"></button>
      <button id="browse-close"></button>
      <select id="filesystem-mode"><option value="remote">remote</option></select>
    `;
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("passes requested extensions through the web file dialog", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock();
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createFileBrowserController } = await import("../modules/file_browser.js");

    const controller = createFileBrowserController({
      apiBase: "/api",
      browseModal: document.getElementById("browse-modal"),
      browseBreadcrumb: document.getElementById("browse-breadcrumb"),
      browseFoldersList: document.getElementById("browse-folders-list"),
      browseFilesList: document.getElementById("browse-files-list"),
      browsePathInput: document.getElementById("browse-path-input"),
      browseStatus: document.getElementById("browse-status"),
      browseSelectBtn: document.getElementById("browse-select"),
      browseCancelBtn: document.getElementById("browse-cancel"),
      browseCloseBtn: document.getElementById("browse-close"),
      filesystemModeEl: document.getElementById("filesystem-mode"),
      openModal: () => {},
      closeModal: () => {},
      setStatus: () => {},
    });

    const selectionPromise = controller.openFileDialog({ exts: ".expt" });
    await Promise.resolve();
    await Promise.resolve();
    await new Promise((resolve) => window.setTimeout(resolve, 0));

    expect(global.fetch).toHaveBeenCalledWith("/api/browse?exts=.expt");

    const fileButton = Array.from(document.querySelectorAll("#browse-files-list .browse-item"))
      .find((button) => button.textContent === "imported.expt");
    expect(fileButton).toBeTruthy();
    fileButton.click();
    document.getElementById("browse-select").click();

    await expect(selectionPromise).resolves.toBe("imported.expt");
  });
});
