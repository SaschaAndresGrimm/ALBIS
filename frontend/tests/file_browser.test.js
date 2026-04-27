import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

function jsonResponse(payload) {
  return {
    ok: true,
    json: async () => payload,
  };
}

function buildFetchMock(handler) {
  return vi.fn(async (url) => {
    const text = String(url);
    if (text.includes("locales/")) {
      return jsonResponse({
        "browse.details.header.modified": "Modified",
        "browse.details.header.name": "Name",
        "browse.details.header.size": "Size",
        "browse.details.header.type": "Type",
        "browse.action.up": "Up",
        "browse.filter.option.all": "All",
        "browse.filter.option.hdf5": "HDF5",
        "browse.filter.option.tiff": "TIFF",
        "browse.filter.option.cbf": "CBF",
        "browse.filter.option.edf": "EDF",
        "browse.filter.option.geometry": "Geometry (.expt)",
        "browse.search.clear": "Clear",
        "browse.search.label": "Search",
        "browse.search.placeholder": "Search files and folders",
        "browse.title.select_file": "Select File",
        "browse.title.select_folder": "Select Folder",
        "browse.view.label": "View",
        "browse.view.option.details": "Details",
        "browse.view.option.list": "List",
        "file_browser.failed_load": "Failed to load folder contents",
        "file_browser.loading": "Loading {{label}}",
        "file_browser.no_folders": "No folders",
        "file_browser.no_images": "No files",
        "file_browser.root": "Root",
        "file_browser.series_badge": "Series • {{count}}",
        "status.file.select_image_first": "Select a file first",
        "status.file.no_selection": "No selection",
      });
    }
    return handler(text);
  });
}

async function flushAsyncWork() {
  await Promise.resolve();
  await Promise.resolve();
  await new Promise((resolve) => window.setTimeout(resolve, 0));
}

function buildBrowseDom() {
  document.body.innerHTML = `
    <div id="browse-modal" class="modal">
      <div id="browse-title"></div>
      <div id="browse-breadcrumb"></div>
      <button id="browse-up"></button>
      <input id="browse-search-input" type="search" />
      <button id="browse-search-clear"></button>
      <label id="browse-format-field"><select id="browse-format"></select></label>
      <label id="browse-sort-field"><select id="browse-sort">
        <option value="name_asc">Name A-Z</option>
        <option value="name_desc">Name Z-A</option>
        <option value="mtime_desc">Newest first</option>
        <option value="mtime_asc">Oldest first</option>
        <option value="type_asc">Type</option>
      </select></label>
      <label id="browse-series-field"><select id="browse-series-mode">
        <option value="all">All files</option>
        <option value="first_only">First image only</option>
      </select></label>
      <label id="browse-view-field"><select id="browse-view-mode">
        <option value="list">List</option>
        <option value="details">Details</option>
      </select></label>
      <div id="browse-content" class="browse-content">
        <div class="browse-folders"><div id="browse-folders-list"></div></div>
        <div id="browse-splitter"></div>
        <div class="browse-files"><div id="browse-files-list"></div></div>
      </div>
      <input id="browse-path-input" />
      <div id="browse-status"></div>
      <button id="browse-select"></button>
      <button id="browse-cancel"></button>
      <button id="browse-close"></button>
      <select id="filesystem-mode"><option value="remote">remote</option></select>
    </div>
  `;
  const browseContent = document.getElementById("browse-content");
  browseContent.getBoundingClientRect = () => ({
    width: 920,
    height: 320,
    top: 0,
    right: 920,
    bottom: 320,
    left: 0,
    x: 0,
    y: 0,
    toJSON: () => ({}),
  });
}

async function createController({ fetchHandler, onPathSelected = vi.fn() } = {}) {
  vi.resetModules();
  buildBrowseDom();
  globalThis.fetch = buildFetchMock(fetchHandler || (() => jsonResponse({ folders: [], files: [], fileItems: [] })));
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createFileBrowserController } = await import("../modules/file_browser.js");
  return createFileBrowserController({
    apiBase: "/api",
    browseModal: document.getElementById("browse-modal"),
    browseTitle: document.getElementById("browse-title"),
    browseBreadcrumb: document.getElementById("browse-breadcrumb"),
    browseUpBtn: document.getElementById("browse-up"),
    browseSearchInput: document.getElementById("browse-search-input"),
    browseSearchClearBtn: document.getElementById("browse-search-clear"),
    browseFormatField: document.getElementById("browse-format-field"),
    browseFormatSelect: document.getElementById("browse-format"),
    browseSortSelect: document.getElementById("browse-sort"),
    browseSeriesModeSelect: document.getElementById("browse-series-mode"),
    browseViewModeSelect: document.getElementById("browse-view-mode"),
    browseContent: document.getElementById("browse-content"),
    browseSplitter: document.getElementById("browse-splitter"),
    browseFoldersList: document.getElementById("browse-folders-list"),
    browseFilesList: document.getElementById("browse-files-list"),
    browsePathInput: document.getElementById("browse-path-input"),
    browseStatus: document.getElementById("browse-status"),
    browseSelectBtn: document.getElementById("browse-select"),
    browseCancelBtn: document.getElementById("browse-cancel"),
    browseCloseBtn: document.getElementById("browse-close"),
    filesystemModeEl: document.getElementById("filesystem-mode"),
    openModal: (modal) => modal.classList.add("is-open"),
    closeModal: (modal) => modal.classList.remove("is-open"),
    setStatus: vi.fn(),
    onPathSelected,
  });
}

function browseRequests() {
  return globalThis.fetch.mock.calls
    .map(([url]) => String(url))
    .filter((url) => url.startsWith("/api/browse"));
}

describe("file_browser", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete globalThis.fetch;
  });

  it("passes requested extensions through the web file dialog and hides irrelevant controls for expt-only browsing", async () => {
    localStorage.setItem("albis.browseFormat", "tiff");
    localStorage.setItem("albis.browseSeriesMode", "first_only");
    localStorage.setItem("albis.browseViewMode", "details");
    const controller = await createController({
      fetchHandler: () =>
        jsonResponse({
          folders: [],
          files: ["imported.expt"],
          fileItems: [
            {
              name: "imported.expt",
              path: "imported.expt",
              ext: ".expt",
              mtime: 1,
              sizeBytes: 2,
              isSeriesLead: false,
              seriesCount: 1,
            },
          ],
          currentPath: "",
          parentPath: "",
          root: "/tmp",
          canGoUp: false,
          allowAbsolutePaths: true,
        }),
    });

    const selectionPromise = controller.openFileDialog({ exts: ".expt" });
    await flushAsyncWork();

    expect(browseRequests()).toEqual(["/api/browse?exts=.expt&sort=name_asc&series_mode=all"]);
    expect(document.getElementById("browse-title").textContent).toBe("Select File");
    expect(document.getElementById("browse-format-field").classList.contains("is-hidden")).toBe(true);
    expect(document.getElementById("browse-series-mode").disabled).toBe(true);
    expect(document.getElementById("browse-view-mode").value).toBe("details");

    const fileButton = document.querySelector("#browse-files-list .browse-item");
    fileButton.click();
    document.getElementById("browse-select").click();

    await expect(selectionPromise).resolves.toBe("imported.expt");
  });

  it("reloads browse results when format, sort, and series controls change and renders series badges", async () => {
    const controller = await createController({
      fetchHandler: (url) => {
        const parsed = new URL(url, "http://localhost");
        const exts = parsed.searchParams.get("exts") || "";
        const sort = parsed.searchParams.get("sort") || "";
        const seriesMode = parsed.searchParams.get("series_mode") || "";
        if (exts === ".tif,.tiff" && sort === "mtime_desc" && seriesMode === "first_only") {
          return jsonResponse({
            folders: [],
            files: ["series_0001.tiff"],
            fileItems: [
              {
                name: "series_0001.tiff",
                path: "series_0001.tiff",
                ext: ".tiff",
                mtime: 55,
                sizeBytes: 10,
                isSeriesLead: true,
                seriesCount: 24,
              },
            ],
            currentPath: "",
            parentPath: "",
            root: "/tmp",
            canGoUp: false,
            allowAbsolutePaths: true,
          });
        }
        return jsonResponse({
          folders: [],
          files: ["scan_0001.h5", "series_0001.tiff"],
          fileItems: [
            {
              name: "scan_0001.h5",
              path: "scan_0001.h5",
              ext: ".h5",
              mtime: 10,
              sizeBytes: 10,
              isSeriesLead: false,
              seriesCount: 1,
            },
            {
              name: "series_0001.tiff",
              path: "series_0001.tiff",
              ext: ".tiff",
              mtime: 20,
              sizeBytes: 10,
              isSeriesLead: false,
              seriesCount: 1,
            },
          ],
          currentPath: "",
          parentPath: "",
          root: "/tmp",
          canGoUp: false,
          allowAbsolutePaths: true,
        });
      },
    });

    void controller.openFileDialog();
    await flushAsyncWork();

    const formatSelect = document.getElementById("browse-format");
    const sortSelect = document.getElementById("browse-sort");
    const seriesSelect = document.getElementById("browse-series-mode");

    expect(document.getElementById("browse-format-field").classList.contains("is-hidden")).toBe(false);
    formatSelect.value = "tiff";
    formatSelect.dispatchEvent(new window.Event("change"));
    await flushAsyncWork();

    sortSelect.value = "mtime_desc";
    sortSelect.dispatchEvent(new window.Event("change"));
    await flushAsyncWork();

    seriesSelect.value = "first_only";
    seriesSelect.dispatchEvent(new window.Event("change"));
    await flushAsyncWork();

    expect(browseRequests()).toContain("/api/browse?exts=.tif%2C.tiff&sort=name_asc&series_mode=all");
    expect(browseRequests()).toContain("/api/browse?exts=.tif%2C.tiff&sort=mtime_desc&series_mode=all");
    expect(browseRequests()).toContain("/api/browse?exts=.tif%2C.tiff&sort=mtime_desc&series_mode=first_only");
    expect(document.querySelector(".browse-item-badge").textContent).toContain("24");
  });

  it("uses single click to select folders, double click to navigate, and backspace to go up", async () => {
    const onPathSelected = vi.fn();
    const controller = await createController({
      onPathSelected,
      fetchHandler: (url) => {
        const parsed = new URL(url, "http://localhost");
        const path = parsed.searchParams.get("path") || "";
        if (path === "raw") {
          return jsonResponse({
            folders: ["nested"],
            files: ["frame_0001.cbf"],
            fileItems: [
              {
                name: "frame_0001.cbf",
                path: "raw/frame_0001.cbf",
                ext: ".cbf",
                mtime: 2,
                sizeBytes: 3,
                isSeriesLead: false,
                seriesCount: 1,
              },
            ],
            currentPath: "raw",
            parentPath: "",
            root: "/tmp",
            canGoUp: true,
            allowAbsolutePaths: true,
          });
        }
        return jsonResponse({
          folders: ["raw"],
          files: [],
          fileItems: [],
          currentPath: "",
          parentPath: "",
          root: "/tmp",
          canGoUp: false,
          allowAbsolutePaths: true,
        });
      },
    });

    controller.openFileBrowser("autoload", document.createElement("input"));
    await flushAsyncWork();

    const folderButton = document.querySelector("#browse-folders-list .browse-item");
    folderButton.click();
    expect(document.getElementById("browse-path-input").value).toBe("raw");
    expect(document.getElementById("browse-select").disabled).toBe(false);

    folderButton.dispatchEvent(new window.MouseEvent("dblclick", { bubbles: true }));
    await flushAsyncWork();
    expect(browseRequests()).toContain("/api/browse?path=raw&sort=name_asc&series_mode=all");

    document.getElementById("browse-modal").dispatchEvent(
      new window.KeyboardEvent("keydown", { key: "Backspace", bubbles: true }),
    );
    await flushAsyncWork();
    expect(browseRequests().at(-1)).toBe("/api/browse?sort=name_asc&series_mode=all");
  });

  it("supports keyboard selection and enter-to-open for files", async () => {
    const controller = await createController({
      fetchHandler: () =>
        jsonResponse({
          folders: [],
          files: ["frame_0001.cbf"],
          fileItems: [
            {
              name: "frame_0001.cbf",
              path: "frame_0001.cbf",
              ext: ".cbf",
              mtime: 1,
              sizeBytes: 2,
              isSeriesLead: false,
              seriesCount: 1,
            },
          ],
          currentPath: "",
          parentPath: "",
          root: "/tmp",
          canGoUp: false,
          allowAbsolutePaths: true,
        }),
    });

    const selectionPromise = controller.openFileDialog();
    await flushAsyncWork();

    const modal = document.getElementById("browse-modal");
    modal.dispatchEvent(new window.KeyboardEvent("keydown", { key: "ArrowDown", bubbles: true }));
    modal.dispatchEvent(new window.KeyboardEvent("keydown", { key: "Enter", bubbles: true }));

    await expect(selectionPromise).resolves.toBe("frame_0001.cbf");
  });

  it("filters the current directory locally, supports search shortcuts, and clears search on navigation and reopen", async () => {
    const controller = await createController({
      fetchHandler: (url) => {
        const parsed = new URL(url, "http://localhost");
        const path = parsed.searchParams.get("path") || "";
        if (path === "raw") {
          return jsonResponse({
            folders: [],
            files: ["frame_0001.cbf"],
            fileItems: [
              {
                name: "frame_0001.cbf",
                path: "raw/frame_0001.cbf",
                ext: ".cbf",
                mtime: 10,
                sizeBytes: 2048,
                isSeriesLead: false,
                seriesCount: 1,
              },
            ],
            currentPath: "raw",
            parentPath: "",
            root: "/tmp",
            canGoUp: true,
            allowAbsolutePaths: true,
          });
        }
        return jsonResponse({
          folders: ["raw", "processed"],
          files: ["raw_scan_0001.h5", "series_0001.tiff"],
          fileItems: [
            {
              name: "raw_scan_0001.h5",
              path: "raw_scan_0001.h5",
              ext: ".h5",
              mtime: 1,
              sizeBytes: 5,
              isSeriesLead: false,
              seriesCount: 1,
            },
            {
              name: "series_0001.tiff",
              path: "series_0001.tiff",
              ext: ".tiff",
              mtime: 2,
              sizeBytes: 5,
              isSeriesLead: false,
              seriesCount: 1,
            },
          ],
          currentPath: "",
          parentPath: "",
          root: "/tmp",
          canGoUp: false,
          allowAbsolutePaths: true,
        });
      },
    });

    controller.openFileBrowser("autoload", document.createElement("input"));
    await flushAsyncWork();

    const modal = document.getElementById("browse-modal");
    const searchInput = document.getElementById("browse-search-input");
    modal.dispatchEvent(new window.KeyboardEvent("keydown", { key: "/", bubbles: true }));
    expect(document.activeElement).toBe(searchInput);

    const initialRequests = browseRequests().length;
    searchInput.value = "raw";
    searchInput.dispatchEvent(new window.Event("input", { bubbles: true }));
    await flushAsyncWork();

    expect(browseRequests()).toHaveLength(initialRequests);
    expect(Array.from(document.querySelectorAll("#browse-folders-list .browse-item")).map((item) => item.textContent)).toEqual(["raw"]);
    expect(Array.from(document.querySelectorAll("#browse-files-list .browse-item")).map((item) => item.textContent)).toEqual(["raw_scan_0001.h5"]);

    searchInput.dispatchEvent(new window.KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    await flushAsyncWork();
    expect(searchInput.value).toBe("");

    searchInput.value = "raw";
    searchInput.dispatchEvent(new window.Event("input", { bubbles: true }));
    await flushAsyncWork();
    document.querySelector("#browse-folders-list .browse-item").dispatchEvent(
      new window.MouseEvent("dblclick", { bubbles: true }),
    );
    await flushAsyncWork();

    expect(browseRequests().at(-1)).toBe("/api/browse?path=raw&sort=name_asc&series_mode=all");
    expect(searchInput.value).toBe("");

    controller.closeFileBrowser();
    controller.openFileBrowser("autoload", document.createElement("input"));
    await flushAsyncWork();
    expect(searchInput.value).toBe("");
  });

  it("persists pane width, restores stored preferences, and renders details view", async () => {
    localStorage.setItem("albis.browsePaneWidth", "396");
    localStorage.setItem("albis.browseSort", "name_desc");
    localStorage.setItem("albis.browseSeriesMode", "first_only");
    localStorage.setItem("albis.browseFormat", "tiff");
    localStorage.setItem("albis.browseViewMode", "details");

    const controller = await createController({
      fetchHandler: (url) => {
        const parsed = new URL(url, "http://localhost");
        const exts = parsed.searchParams.get("exts") || "";
        const sort = parsed.searchParams.get("sort") || "";
        const seriesMode = parsed.searchParams.get("series_mode") || "";
        if (exts === ".tif,.tiff" && sort === "name_desc" && seriesMode === "first_only") {
          return jsonResponse({
            folders: [],
            files: ["series_0001.tiff"],
            fileItems: [
              {
                name: "series_0001.tiff",
                path: "series_0001.tiff",
                ext: ".tiff",
                mtime: 60,
                sizeBytes: 2048,
                isSeriesLead: true,
                seriesCount: 12,
              },
            ],
            currentPath: "",
            parentPath: "",
            root: "/tmp",
            canGoUp: false,
            allowAbsolutePaths: true,
          });
        }
        return jsonResponse({
          folders: [],
          files: [],
          fileItems: [],
          currentPath: "",
          parentPath: "",
          root: "/tmp",
          canGoUp: false,
          allowAbsolutePaths: true,
        });
      },
    });

    void controller.openFileDialog();
    await flushAsyncWork();

    expect(browseRequests()[0]).toBe("/api/browse?exts=.tif%2C.tiff&sort=name_desc&series_mode=first_only");
    expect(document.getElementById("browse-view-mode").value).toBe("details");
    expect(document.getElementById("browse-content").style.getPropertyValue("--browse-folder-pane-width")).toBe("396px");
    expect(document.querySelector(".browse-details-header").textContent).toContain("Modified");
    expect(document.querySelector("#browse-files-list .browse-item").title).toBe("series_0001.tiff");
    expect(document.querySelector(".browse-item-badge").textContent).toContain("12");
    expect(document.querySelector("#browse-files-list .browse-item").textContent).toContain("TIFF");
    expect(document.querySelector("#browse-files-list .browse-item").textContent).toContain("2 KB");
  });

  it("updates pane width by dragging the splitter and disables splitter behavior in stacked layout", async () => {
    const controller = await createController({
      fetchHandler: () =>
        jsonResponse({
          folders: ["raw"],
          files: ["frame_0001.cbf"],
          fileItems: [
            {
              name: "frame_0001.cbf",
              path: "frame_0001.cbf",
              ext: ".cbf",
              mtime: 10,
              sizeBytes: 100,
              isSeriesLead: false,
              seriesCount: 1,
            },
          ],
          currentPath: "",
          parentPath: "",
          root: "/tmp",
          canGoUp: false,
          allowAbsolutePaths: true,
        }),
    });

    void controller.openFileDialog();
    await flushAsyncWork();

    const content = document.getElementById("browse-content");
    const splitter = document.getElementById("browse-splitter");
    splitter.dispatchEvent(new window.MouseEvent("mousedown", { clientX: 200, bubbles: true }));
    window.dispatchEvent(new window.MouseEvent("mousemove", { clientX: 280, bubbles: true }));
    window.dispatchEvent(new window.MouseEvent("mouseup", { clientX: 280, bubbles: true }));

    expect(content.style.getPropertyValue("--browse-folder-pane-width")).toBe("400px");
    expect(localStorage.getItem("albis.browsePaneWidth")).toBe("400");

    const originalWidth = window.innerWidth;
    window.innerWidth = 700;
    window.dispatchEvent(new window.Event("resize"));
    expect(content.classList.contains("is-stacked")).toBe(true);
    expect(splitter.classList.contains("is-disabled")).toBe(true);
    window.innerWidth = originalWidth;
    window.dispatchEvent(new window.Event("resize"));
  });

  it("keeps the format list populated and derives parent navigation for legacy browse responses", async () => {
    const controller = await createController({
      fetchHandler: (url) => {
        const parsed = new URL(url, "http://localhost");
        const path = parsed.searchParams.get("path") || "";
        if (path === "processed") {
          return jsonResponse({
            folders: [],
            files: ["scan_0001.h5", "scan_0002.h5"],
            currentPath: "processed",
            root: "/tmp",
            canGoUp: true,
            allowAbsolutePaths: true,
          });
        }
        return jsonResponse({
          folders: ["processed"],
          files: [],
          currentPath: "",
          root: "/tmp",
          canGoUp: false,
          allowAbsolutePaths: true,
        });
      },
    });

    controller.openFileBrowser("autoload", document.createElement("input"));
    await flushAsyncWork();
    document.querySelector("#browse-folders-list .browse-item").dispatchEvent(
      new window.MouseEvent("dblclick", { bubbles: true }),
    );
    await flushAsyncWork();

    const formatOptions = Array.from(document.querySelectorAll("#browse-format option")).map((option) => option.textContent);
    expect(formatOptions).toContain("All");
    expect(formatOptions).toContain("HDF5");
    expect(document.getElementById("browse-series-mode").disabled).toBe(true);

    document.getElementById("browse-up").click();
    await flushAsyncWork();
    expect(browseRequests().at(-1)).toBe("/api/browse?sort=name_asc&series_mode=all");
  });
});
