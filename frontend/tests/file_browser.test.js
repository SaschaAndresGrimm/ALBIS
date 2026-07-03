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
        "browse.section.folders": "Folders",
        "browse.section.image_files": "Image Files",
        "browse.path.hint": "Type or paste a path, then press Enter",
        "browse.title.select_file": "Select File",
        "browse.title.select_folder": "Select Folder",
        "browse.view.label": "View",
        "browse.view.option.details": "Details",
        "browse.view.option.list": "List",
        "file_browser.failed_load": "Failed to load folder contents",
        "file_browser.loading": "Loading {{label}}",
        "file_browser.no_folders": "No folders",
        "file_browser.no_images": "No files",
        "file_browser.path_not_found": "Path not found — showing Root",
        "file_browser.retry": "Retry",
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
        <option value="type_desc">Type (Z-A)</option>
        <option value="size_asc">Size (smallest)</option>
        <option value="size_desc">Size (largest)</option>
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
        <div class="browse-folders">
          <div class="browse-section-title" data-i18n="browse.section.folders">Folders</div>
          <div id="browse-folders-list"></div>
        </div>
        <div id="browse-splitter"></div>
        <div class="browse-files">
          <div class="browse-section-title" data-i18n="browse.section.image_files">Image Files</div>
          <div id="browse-files-list"></div>
        </div>
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
    browseSeriesField: document.getElementById("browse-series-field"),
    browseViewModeSelect: document.getElementById("browse-view-mode"),
    browseViewField: document.getElementById("browse-view-field"),
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

    expect(browseRequests()).toEqual(["/api/browse?exts=.expt&sort=name_asc&series_mode=first_only"]);
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
    // Pin the "all" default so this test exercises the all -> first_only transition.
    localStorage.setItem("albis.browseSeriesMode", "all");
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
    expect(browseRequests()).toContain("/api/browse?path=raw&sort=name_asc&series_mode=first_only");

    document.getElementById("browse-modal").dispatchEvent(
      new window.KeyboardEvent("keydown", { key: "Backspace", bubbles: true }),
    );
    await flushAsyncWork();
    expect(browseRequests().at(-1)).toBe("/api/browse?sort=name_asc&series_mode=first_only");
  });

  it("hides files and file-only controls in folder-select mode and keeps the current folder selected", async () => {
    const controller = await createController({
      fetchHandler: () =>
        jsonResponse({
          folders: ["raw", "processed"],
          files: ["scan_0001.h5"],
          fileItems: [
            {
              name: "scan_0001.h5",
              path: "scan_0001.h5",
              ext: ".h5",
              mtime: 1,
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
        }),
    });

    controller.openFileBrowser("autoload", document.createElement("input"));
    await flushAsyncWork();

    // Folder mode collapses to a folder-only browser.
    expect(document.getElementById("browse-content").classList.contains("is-folder-only")).toBe(true);
    expect(document.querySelectorAll("#browse-files-list .browse-item")).toHaveLength(0);
    expect(document.getElementById("browse-format-field").classList.contains("is-hidden")).toBe(true);
    expect(document.getElementById("browse-series-field").classList.contains("is-hidden")).toBe(true);
    expect(document.getElementById("browse-view-field").classList.contains("is-hidden")).toBe(true);

    // Folders remain browsable and the current folder is selected so Select is ready.
    expect(Array.from(document.querySelectorAll("#browse-folders-list .browse-item")).map((item) => item.textContent))
      .toEqual(["raw", "processed"]);
    expect(document.getElementById("browse-select").disabled).toBe(false);
    expect(document.getElementById("browse-path-input").value).toBe("");
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

    void controller.openFileDialog();
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
    expect(Array.from(document.querySelectorAll("#browse-files-list .browse-item")).map((item) => item.getAttribute("title"))).toEqual(["raw_scan_0001.h5"]);

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

    expect(browseRequests().at(-1)).toBe("/api/browse?path=raw&sort=name_asc&series_mode=first_only");
    expect(searchInput.value).toBe("");

    controller.closeFileBrowser();
    void controller.openFileDialog();
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

    void controller.openFileDialog();
    await flushAsyncWork();
    document.querySelector("#browse-folders-list .browse-item").dispatchEvent(
      new window.MouseEvent("dblclick", { bubbles: true }),
    );
    await flushAsyncWork();

    const formatOptions = Array.from(document.querySelectorAll("#browse-format option")).map((option) => option.textContent);
    expect(formatOptions).toContain("All");
    expect(formatOptions).toContain("HDF5");
    // HDF5 folders are series-capable (master/data collapsing), so the control is enabled.
    expect(document.getElementById("browse-series-mode").disabled).toBe(false);

    document.getElementById("browse-up").click();
    await flushAsyncWork();
    expect(browseRequests().at(-1)).toBe("/api/browse?sort=name_asc&series_mode=first_only");
  });

  it("collapses HDF5 master/data files to the master in legacy first_only responses", async () => {
    localStorage.setItem("albis.browseSeriesMode", "first_only");
    const controller = await createController({
      fetchHandler: () =>
        jsonResponse({
          folders: [],
          files: [
            "260616_CeO2_raw_master.h5",
            "260616_CeO2_raw_data_000001.h5",
            "260616_CeO2_raw_data_000002.h5",
            "series_sum_dark_20260618_081052.h5",
          ],
          currentPath: "",
          root: "/tmp",
          canGoUp: false,
          allowAbsolutePaths: true,
        }),
    });

    void controller.openFileDialog();
    await flushAsyncWork();

    const fileNames = Array.from(document.querySelectorAll("#browse-files-list .browse-item"))
      .map((item) => item.getAttribute("title"))
      .filter(Boolean);
    expect(fileNames).toContain("260616_CeO2_raw_master.h5");
    expect(fileNames).toContain("series_sum_dark_20260618_081052.h5");
    expect(fileNames).not.toContain("260616_CeO2_raw_data_000001.h5");
    expect(fileNames).not.toContain("260616_CeO2_raw_data_000002.h5");
  });

  it("jumps to a typed directory (or a file's folder) from the path field on Enter", async () => {
    const controller = await createController({
      fetchHandler: (url) => {
        const path = new URL(url, "http://localhost").searchParams.get("path") || "";
        return jsonResponse({
          folders: [],
          files: [],
          fileItems: [],
          currentPath: path,
          parentPath: "",
          root: "/tmp",
          canGoUp: Boolean(path),
          allowAbsolutePaths: true,
        });
      },
    });

    void controller.openFileDialog();
    await flushAsyncWork();
    const pathInput = document.getElementById("browse-path-input");

    pathInput.value = "processed/raw";
    pathInput.dispatchEvent(new window.KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
    await flushAsyncWork();
    expect(browseRequests().at(-1)).toBe("/api/browse?path=processed%2Fraw&sort=name_asc&series_mode=first_only");

    // A typed file path lands in its containing folder.
    pathInput.value = "beam/scan_master.h5";
    pathInput.dispatchEvent(new window.KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
    await flushAsyncWork();
    expect(browseRequests().at(-1)).toBe("/api/browse?path=beam&sort=name_asc&series_mode=first_only");
  });

  it("focuses the first entry on open for immediate keyboard navigation", async () => {
    const controller = await createController({
      fetchHandler: () =>
        jsonResponse({
          folders: ["alpha", "beta"],
          files: [],
          fileItems: [],
          currentPath: "",
          root: "/tmp",
          canGoUp: false,
          allowAbsolutePaths: true,
        }),
    });

    void controller.openFileDialog();
    await flushAsyncWork();

    const active = document.activeElement;
    expect(active?.classList.contains("browse-item")).toBe(true);
    expect(active?.getAttribute("data-browse-pane")).toBe("folders");
  });

  it("offers a retry action when a directory fails to load", async () => {
    let failNext = true;
    const controller = await createController({
      fetchHandler: () => {
        if (failNext) return { ok: false, status: 500, json: async () => ({}) };
        return jsonResponse({
          folders: ["ok"],
          files: [],
          fileItems: [],
          currentPath: "",
          root: "/tmp",
          canGoUp: false,
          allowAbsolutePaths: true,
        });
      },
    });

    void controller.openFileDialog();
    await flushAsyncWork();

    const status = document.getElementById("browse-status");
    expect(status.classList.contains("is-error")).toBe(true);
    const retryBtn = status.querySelector(".browse-retry-btn");
    expect(retryBtn).not.toBeNull();

    failNext = false;
    retryBtn.click();
    await flushAsyncWork();

    expect(status.querySelector(".browse-retry-btn")).toBeNull();
    expect(Array.from(document.querySelectorAll("#browse-folders-list .browse-item")).map((item) => item.textContent))
      .toEqual(["ok"]);
  });

  it("shows folder and file counts in the section titles", async () => {
    const controller = await createController({
      fetchHandler: () =>
        jsonResponse({
          folders: ["a", "b"],
          files: ["x.tiff", "y.tiff"],
          fileItems: [
            { name: "x.tiff", path: "x.tiff", ext: ".tiff", mtime: 1, sizeBytes: 1, isSeriesLead: false, seriesCount: 1 },
            { name: "y.tiff", path: "y.tiff", ext: ".tiff", mtime: 1, sizeBytes: 1, isSeriesLead: false, seriesCount: 1 },
          ],
          currentPath: "",
          root: "/tmp",
          canGoUp: false,
          allowAbsolutePaths: true,
        }),
    });

    void controller.openFileDialog();
    await flushAsyncWork();

    expect(document.querySelector(".browse-folders .browse-section-title").textContent).toBe("Folders (2)");
    expect(document.querySelector(".browse-files .browse-section-title").textContent).toBe("Image Files (2)");
  });

  it("sorts by clicking Details column headers and reflects direction with a caret", async () => {
    localStorage.setItem("albis.browseViewMode", "details");
    const controller = await createController({
      fetchHandler: () =>
        jsonResponse({
          folders: [],
          files: ["a.tiff", "b.tiff"],
          fileItems: [
            { name: "a.tiff", path: "a.tiff", ext: ".tiff", mtime: 10, sizeBytes: 100, isSeriesLead: false, seriesCount: 1 },
            { name: "b.tiff", path: "b.tiff", ext: ".tiff", mtime: 20, sizeBytes: 200, isSeriesLead: false, seriesCount: 1 },
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

    const sizeHeader = () => document.querySelector('.browse-details-header [data-sort-column="size"]');

    // First click on a fresh column sorts ascending.
    sizeHeader().click();
    await flushAsyncWork();
    expect(browseRequests().at(-1)).toBe("/api/browse?sort=size_asc&series_mode=first_only");
    expect(sizeHeader().getAttribute("aria-sort")).toBe("ascending");
    expect(sizeHeader().querySelector(".browse-sort-caret").textContent).toBe("▴");
    expect(document.getElementById("browse-sort").value).toBe("size_asc");

    // Clicking the active column toggles to descending.
    sizeHeader().click();
    await flushAsyncWork();
    expect(browseRequests().at(-1)).toBe("/api/browse?sort=size_desc&series_mode=first_only");
    expect(sizeHeader().getAttribute("aria-sort")).toBe("descending");
    expect(sizeHeader().querySelector(".browse-sort-caret").textContent).toBe("▾");
    expect(document.getElementById("browse-sort").value).toBe("size_desc");
  });

  it("keeps the selected file when re-sorting the same directory", async () => {
    localStorage.setItem("albis.browseViewMode", "details");
    const controller = await createController({
      fetchHandler: () =>
        jsonResponse({
          folders: [],
          files: ["a.tiff", "b.tiff"],
          fileItems: [
            { name: "a.tiff", path: "a.tiff", ext: ".tiff", mtime: 10, sizeBytes: 100, isSeriesLead: false, seriesCount: 1 },
            { name: "b.tiff", path: "b.tiff", ext: ".tiff", mtime: 20, sizeBytes: 200, isSeriesLead: false, seriesCount: 1 },
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

    const bButton = Array.from(document.querySelectorAll("#browse-files-list .browse-item"))
      .find((el) => el.getAttribute("title") === "b.tiff");
    bButton.click();
    expect(document.getElementById("browse-select").disabled).toBe(false);
    expect(document.getElementById("browse-path-input").value).toBe("b.tiff");

    document.querySelector('.browse-details-header [data-sort-column="size"]').click();
    await flushAsyncWork();

    // The selection survives the sort reload of the same directory.
    expect(document.getElementById("browse-select").disabled).toBe(false);
    expect(document.getElementById("browse-path-input").value).toBe("b.tiff");
  });

  it("preserves the chosen series mode when passing through a non-series folder", async () => {
    localStorage.setItem("albis.browseSeriesMode", "all");
    const controller = await createController({
      fetchHandler: (url) => {
        const path = new URL(url, "http://localhost").searchParams.get("path") || "";
        if (path === "expt_only") {
          return jsonResponse({
            folders: [],
            files: ["a.expt"],
            fileItems: [{ name: "a.expt", path: "expt_only/a.expt", ext: ".expt", mtime: 1, sizeBytes: 1, isSeriesLead: false, seriesCount: 1 }],
            currentPath: "expt_only",
            parentPath: "",
            root: "/tmp",
            canGoUp: true,
            allowAbsolutePaths: true,
          });
        }
        return jsonResponse({
          folders: ["expt_only"],
          files: ["x.tiff"],
          fileItems: [{ name: "x.tiff", path: "x.tiff", ext: ".tiff", mtime: 1, sizeBytes: 1, isSeriesLead: false, seriesCount: 1 }],
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

    const series = document.getElementById("browse-series-mode");
    expect(series.value).toBe("all");

    document.querySelector("#browse-folders-list .browse-item").dispatchEvent(
      new window.MouseEvent("dblclick", { bubbles: true }),
    );
    await flushAsyncWork();

    // Disabled on the incompatible folder, but the choice is not clobbered.
    expect(series.disabled).toBe(true);
    expect(series.value).toBe("all");
  });

  it("shows a not-found message when a typed path falls back to root", async () => {
    const controller = await createController({
      fetchHandler: (url) => {
        const path = new URL(url, "http://localhost").searchParams.get("path") || "";
        const missing = path === "does/not/exist";
        return jsonResponse({
          folders: ["real"],
          files: [],
          fileItems: [],
          currentPath: "",
          parentPath: "",
          root: "/tmp",
          canGoUp: false,
          allowAbsolutePaths: true,
          requestedPathMissing: missing,
        });
      },
    });

    void controller.openFileDialog();
    await flushAsyncWork();

    const pathInput = document.getElementById("browse-path-input");
    pathInput.value = "does/not/exist";
    pathInput.dispatchEvent(new window.KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
    await flushAsyncWork();

    const status = document.getElementById("browse-status");
    expect(status.textContent).toContain("Path not found");
    expect(status.classList.contains("is-error")).toBe(true);
  });

  it("selects the first search match on Enter, and opens it when it is the only one", async () => {
    const controller = await createController({
      fetchHandler: () =>
        jsonResponse({
          folders: [],
          files: ["alpha.tiff", "alphabet.tiff", "beta.tiff"],
          fileItems: [
            { name: "alpha.tiff", path: "alpha.tiff", ext: ".tiff", mtime: 1, sizeBytes: 1, isSeriesLead: false, seriesCount: 1 },
            { name: "alphabet.tiff", path: "alphabet.tiff", ext: ".tiff", mtime: 1, sizeBytes: 1, isSeriesLead: false, seriesCount: 1 },
            { name: "beta.tiff", path: "beta.tiff", ext: ".tiff", mtime: 1, sizeBytes: 1, isSeriesLead: false, seriesCount: 1 },
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
    const searchInput = document.getElementById("browse-search-input");

    // Multiple matches: Enter selects the first without opening.
    searchInput.value = "alpha";
    searchInput.dispatchEvent(new window.Event("input", { bubbles: true }));
    await flushAsyncWork();
    searchInput.dispatchEvent(new window.KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
    await flushAsyncWork();
    expect(document.getElementById("browse-path-input").value).toBe("alpha.tiff");

    // Single match: Enter opens it.
    searchInput.value = "beta";
    searchInput.dispatchEvent(new window.Event("input", { bubbles: true }));
    await flushAsyncWork();
    searchInput.dispatchEvent(new window.KeyboardEvent("keydown", { key: "Enter", bubbles: true }));

    await expect(selectionPromise).resolves.toBe("beta.tiff");
  });
});
