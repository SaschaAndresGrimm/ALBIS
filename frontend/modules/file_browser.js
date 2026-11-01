/**
 * File browser modal controller for remote filesystem browsing.
 *
 * Keeps modal state, browse requests, and dialog behavior encapsulated so
 * app.js can treat it as a small integration surface.
 */

import { t } from "./i18n.js";
import { fetchJSON } from "./http.js";
import {
  BROWSE_BREAKPOINT,
  BROWSE_MIN_FILE_WIDTH,
  BROWSE_MIN_FOLDER_WIDTH,
  BROWSE_SERIES_CAPABLE_EXTS,
  BROWSE_SERIES_CAPABLE_SET,
  BROWSE_SPLITTER_WIDTH,
  DEFAULT_BROWSE_EXTS,
  DEFAULT_BROWSE_PANE_WIDTH,
  DEFAULT_SERIES_MODE,
  DEFAULT_SORT,
  DEFAULT_VIEW_MODE,
  COLUMN_SORTS,
  FORMAT_ALL,
  FORMAT_GROUPS,
  KNOWN_BROWSE_EXTS,
  STORAGE_KEYS,
  VALID_SERIES_MODES,
  VALID_SORTS,
  VALID_VIEW_MODES,
  aggregateSeriesLocally,
  deriveParentPath,
  inferFileExt,
  intersectsExts,
  joinBrowsePath,
  matchesBrowseQuery,
  normalizeFileItems,
  normalizeRequestedExts,
  normalizeSearchQuery,
  serializeExts,
  sortFileItems,
} from "./file_browser_utils.js";
import { createFileBrowserRenderer } from "./file_browser_renderer.js";
import { bindFileBrowserEvents } from "./file_browser_bindings.js";

function detectBackendLocal(apiBase) {
  try {
    const url = new URL(apiBase, window.location.href);
    const hostname = url.hostname.toLowerCase();
    return (
      hostname === "localhost" ||
      hostname === "127.0.0.1" ||
      hostname === "[::1]" ||
      hostname === "::1"
    );
  } catch {
    return false;
  }
}

function readStoredValue(key) {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

function writeStoredValue(key, value) {
  try {
    localStorage.setItem(key, String(value));
  } catch {
    // ignore storage errors
  }
}

export function createFileBrowserController({
  apiBase,
  browseModal,
  browseTitle,
  browseBreadcrumb,
  browseUpBtn,
  browseSearchInput,
  browseSearchClearBtn,
  browseFormatField,
  browseFormatSelect,
  browseSortSelect,
  browseSeriesModeSelect,
  browseSeriesField,
  browseViewModeSelect,
  browseViewField,
  browseContent,
  browseSplitter,
  browseFoldersList,
  browseFilesList,
  browsePathInput,
  browseStatus,
  browseSelectBtn,
  browseCancelBtn,
  browseCloseBtn,
  filesystemModeEl,
  openModal,
  closeModal,
  setStatus,
  onPathSelected = null,
  onFilesystemModeChanged = null,
}) {
  const state = {
    currentPath: "",
    parentPath: "",
    canGoUp: false,
    selectedPath: "",
    selectedType: "",
    selectedPane: "",
    selectedIndex: -1,
    focusPane: "files",
    mode: null,
    inputElement: null,
    requestedExtsRaw: "",
    requestedExts: new Set(DEFAULT_BROWSE_EXTS),
    activeFormat: FORMAT_ALL,
    sort: DEFAULT_SORT,
    seriesMode: DEFAULT_SERIES_MODE,
    viewMode: DEFAULT_VIEW_MODE,
    paneWidth: DEFAULT_BROWSE_PANE_WIDTH,
    searchQuery: "",
    allFolders: [],
    folders: [],
    rawFileItems: [],
    fileBaseItems: [],
    fileItems: [],
    richMetadataAvailable: false,
  };

  let browseModalBusy = false;
  let browseRequestId = 0;
  let fileDialogPromise = null;
  let pendingInitialFocus = false;
  const browseFoldersTitle = browseFoldersList?.previousElementSibling || null;
  const browseFilesTitle = browseFilesList?.previousElementSibling || null;
  const isBackendLocal = detectBackendLocal(apiBase);
  const { renderBreadcrumb, renderFolders, renderFiles } = createFileBrowserRenderer({
    state,
    browseBreadcrumb,
    browseFoldersList,
    browseFilesList,
    applySort,
    sortForColumnClick,
    isSortModeAvailable,
    isFolderSelectMode,
    selectFolderIndex,
    selectFileIndex,
    clearSearchQuery,
    loadAndRenderBrowser,
    confirmBrowseSelection,
  });

  function setBrowseStatus(text = "", { isError = false, isLoading = false } = {}) {
    if (!browseStatus) return;
    browseStatus.textContent = text || "";
    browseStatus.classList.toggle("is-error", Boolean(isError));
    browseStatus.classList.toggle("is-loading", Boolean(isLoading));
  }

  function showBrowseLoadError(path) {
    setBrowseStatus(t("file_browser.failed_load"), { isError: true });
    if (!browseStatus) return;
    const retry = document.createElement("button");
    retry.type = "button";
    retry.className = "browse-retry-btn";
    retry.textContent = t("file_browser.retry");
    retry.addEventListener("click", () => {
      loadAndRenderBrowser(path).catch((err) => console.error(err));
    });
    browseStatus.appendChild(document.createTextNode(" "));
    browseStatus.appendChild(retry);
  }

  function updateSectionCounts() {
    const setCount = (titleEl, key, count) => {
      if (!titleEl) return;
      titleEl.textContent = count > 0 ? `${t(key)} (${count})` : t(key);
    };
    setCount(browseFoldersTitle, "browse.section.folders", state.folders.length);
    setCount(browseFilesTitle, "browse.section.image_files", state.fileItems.length);
  }

  function isStackedBrowseLayout() {
    return window.innerWidth <= BROWSE_BREAKPOINT;
  }

  function clampBrowsePaneWidth(width) {
    const containerWidth = browseContent?.getBoundingClientRect?.().width || 960;
    const maxWidth = Math.max(
      BROWSE_MIN_FOLDER_WIDTH,
      containerWidth - BROWSE_SPLITTER_WIDTH - BROWSE_MIN_FILE_WIDTH,
    );
    const numeric = Number(width);
    if (!Number.isFinite(numeric)) {
      return Math.max(BROWSE_MIN_FOLDER_WIDTH, Math.min(maxWidth, DEFAULT_BROWSE_PANE_WIDTH));
    }
    return Math.max(BROWSE_MIN_FOLDER_WIDTH, Math.min(maxWidth, Math.round(numeric)));
  }

  function persistBrowsePaneWidth() {
    writeStoredValue(STORAGE_KEYS.browsePaneWidth, state.paneWidth);
  }

  function applyBrowsePaneWidth() {
    if (!browseContent) return;
    const stacked = isStackedBrowseLayout();
    browseContent.classList.toggle("is-stacked", stacked);
    browseSplitter?.classList.toggle("is-disabled", stacked);
    if (stacked) {
      browseContent.style.removeProperty("--browse-folder-pane-width");
      return;
    }
    state.paneWidth = clampBrowsePaneWidth(state.paneWidth);
    browseContent.style.setProperty("--browse-folder-pane-width", `${state.paneWidth}px`);
  }

  function persistBrowseControlPreferences() {
    writeStoredValue(STORAGE_KEYS.browseSort, state.sort);
    writeStoredValue(STORAGE_KEYS.browseSeriesMode, state.seriesMode);
    writeStoredValue(STORAGE_KEYS.browseFormat, state.activeFormat);
    writeStoredValue(STORAGE_KEYS.browseViewMode, state.viewMode);
  }

  function restoreBrowsePreferences() {
    const storedSort = readStoredValue(STORAGE_KEYS.browseSort);
    const storedSeriesMode = readStoredValue(STORAGE_KEYS.browseSeriesMode);
    const storedFormat = readStoredValue(STORAGE_KEYS.browseFormat);
    const storedViewMode = readStoredValue(STORAGE_KEYS.browseViewMode);
    const storedPaneWidthRaw = readStoredValue(STORAGE_KEYS.browsePaneWidth);
    const storedPaneWidth = Number(storedPaneWidthRaw);

    state.sort = VALID_SORTS.has(storedSort || "") ? storedSort : DEFAULT_SORT;
    state.seriesMode = VALID_SERIES_MODES.has(storedSeriesMode || "") ? storedSeriesMode : DEFAULT_SERIES_MODE;
    state.viewMode = VALID_VIEW_MODES.has(storedViewMode || "") ? storedViewMode : DEFAULT_VIEW_MODE;
    state.paneWidth = storedPaneWidthRaw !== null && Number.isFinite(storedPaneWidth)
      ? storedPaneWidth
      : DEFAULT_BROWSE_PANE_WIDTH;

    const availableFormats = new Set(getAvailableFormatGroups().map((group) => group.value));
    state.activeFormat = storedFormat === FORMAT_ALL || availableFormats.has(storedFormat || "")
      ? (storedFormat || FORMAT_ALL)
      : FORMAT_ALL;
  }

  function syncSearchControl() {
    if (browseSearchInput) {
      browseSearchInput.value = state.searchQuery;
      browseSearchInput.disabled = browseModalBusy;
    }
    if (browseSearchClearBtn) {
      browseSearchClearBtn.disabled = browseModalBusy || !state.searchQuery;
      browseSearchClearBtn.classList.toggle("is-hidden", !state.searchQuery);
    }
  }

  function syncBrowseViewState() {
    if (!browseViewModeSelect) return;
    browseViewModeSelect.value = state.viewMode;
    browseViewModeSelect.disabled = browseModalBusy;
  }

  function clearSearchQuery({ rerender = true, focusInput = false } = {}) {
    if (!state.searchQuery) {
      syncSearchControl();
      if (focusInput) {
        browseSearchInput?.focus();
      }
      return;
    }
    state.searchQuery = "";
    syncSearchControl();
    if (rerender) {
      renderBrowseLists({ preserveSelection: true });
    }
    if (focusInput) {
      browseSearchInput?.focus();
    }
  }

  function setSearchQuery(query) {
    const nextQuery = String(query || "");
    if (state.searchQuery === nextQuery) {
      syncSearchControl();
      return;
    }
    state.searchQuery = nextQuery;
    syncSearchControl();
    renderBrowseLists({ preserveSelection: true });
  }

  function isFolderSelectMode() {
    return state.mode !== "file-open";
  }

  function applyBrowseModeLayout() {
    const folderMode = isFolderSelectMode();
    browseContent?.classList.toggle("is-folder-only", folderMode);
    browseViewField?.classList.toggle("is-hidden", folderMode);
  }

  function canConfirmBrowseSelection() {
    if (state.mode === "file-open") {
      return state.selectedType === "file" && Boolean(state.selectedPath);
    }
    return state.selectedType === "folder";
  }

  function syncBrowseSelectState() {
    if (!browseSelectBtn) return;
    browseSelectBtn.disabled = browseModalBusy || !canConfirmBrowseSelection();
  }

  function syncBrowseUpState() {
    if (!browseUpBtn) return;
    browseUpBtn.disabled = browseModalBusy || !state.canGoUp;
  }

  function canSortByMtime() {
    return state.rawFileItems.some((item) => Number(item.mtime || 0) > 0);
  }

  function canSortBySize() {
    return state.rawFileItems.some((item) => Number(item.sizeBytes || 0) > 0);
  }

  function isSortModeAvailable(sort) {
    if (sort === "mtime_asc" || sort === "mtime_desc") return canSortByMtime();
    if (sort === "size_asc" || sort === "size_desc") return canSortBySize();
    return true;
  }

  function syncBrowseSortState() {
    if (!browseSortSelect) return;
    Array.from(browseSortSelect.options).forEach((option) => {
      option.disabled = !isSortModeAvailable(option.value);
    });
    // Keep the chosen sort even when unavailable here (disabled option stays
    // selected); it applies again on folders that have the needed metadata.
    browseSortSelect.value = state.sort;
    browseSortSelect.disabled = browseModalBusy;
  }

  function applySort(nextSort) {
    const sort = VALID_SORTS.has(nextSort) ? nextSort : DEFAULT_SORT;
    if (sort === state.sort) return;
    state.sort = sort;
    if (browseSortSelect) browseSortSelect.value = state.sort;
    persistBrowseControlPreferences();
    loadAndRenderBrowser(state.currentPath).catch((err) => console.error(err));
  }

  function sortForColumnClick(column) {
    const [asc, desc] = COLUMN_SORTS[column] || [];
    if (!asc) return state.sort;
    if (state.sort === asc) return desc;
    if (state.sort === desc) return asc;
    return asc;
  }

  function setBrowseModalBusy(isBusy, statusText = "") {
    browseModalBusy = Boolean(isBusy);
    browseModal?.setAttribute("aria-busy", browseModalBusy ? "true" : "false");
    browseBreadcrumb?.classList.toggle("is-loading", browseModalBusy);
    browseFoldersList?.classList.toggle("is-loading", browseModalBusy);
    browseFilesList?.classList.toggle("is-loading", browseModalBusy);
    if (statusText) {
      setBrowseStatus(statusText, { isLoading: browseModalBusy });
    } else if (!browseModalBusy && browseStatus?.classList.contains("is-loading")) {
      setBrowseStatus("");
    }
    syncSearchControl();
    syncBrowseSortState();
    syncBrowseViewState();
    syncBrowseSelectState();
    syncBrowseUpState();
    applyBrowsePaneWidth();
  }

  function persistFilesystemMode(mode) {
    if (mode !== "local" && mode !== "remote") return;
    writeStoredValue(STORAGE_KEYS.filesystemMode, mode);
  }

  function restoreFilesystemMode() {
    if (!filesystemModeEl || isBackendLocal) return;
    const stored = readStoredValue(STORAGE_KEYS.filesystemMode);
    if (stored === "local" || stored === "remote") {
      filesystemModeEl.value = stored;
    }
  }

  function updateBrowseTitle() {
    if (!browseTitle) return;
    const key = state.mode === "file-open" ? "browse.title.select_file" : "browse.title.select_folder";
    browseTitle.dataset.i18n = key;
    browseTitle.textContent = t(key);
  }

  function getAvailableFormatGroups() {
    const effectiveRequestedExts =
      state.requestedExts instanceof Set && state.requestedExts.size
        ? state.requestedExts
        : new Set(DEFAULT_BROWSE_EXTS);
    return FORMAT_GROUPS.filter((group) => intersectsExts(effectiveRequestedExts, group.exts));
  }

  function getActiveBrowseExts() {
    if (state.activeFormat === FORMAT_ALL) {
      return new Set(state.requestedExts);
    }
    const group = FORMAT_GROUPS.find((candidate) => candidate.value === state.activeFormat);
    if (!group) {
      return new Set(state.requestedExts);
    }
    const filtered = new Set(group.exts.filter((ext) => state.requestedExts.has(ext)));
    return filtered.size ? filtered : new Set(state.requestedExts);
  }

  function effectiveExtsParam() {
    if (state.activeFormat === FORMAT_ALL) {
      return state.requestedExtsRaw ? serializeExts(state.requestedExts) : "";
    }
    return serializeExts(getActiveBrowseExts());
  }

  function syncFormatControl() {
    if (!browseFormatSelect) return;
    const availableGroups = getAvailableFormatGroups();
    const showControl = !isFolderSelectMode() && availableGroups.length > 1;
    if (!showControl) {
      state.activeFormat = FORMAT_ALL;
    } else if (
      state.activeFormat !== FORMAT_ALL &&
      !availableGroups.some((group) => group.value === state.activeFormat)
    ) {
      state.activeFormat = FORMAT_ALL;
    }

    browseFormatField?.classList.toggle("is-hidden", !showControl);
    browseFormatSelect.innerHTML = "";
    const addOption = (value, label) => {
      const option = document.createElement("option");
      option.value = value;
      option.textContent = label;
      browseFormatSelect.appendChild(option);
    };
    if (!showControl) {
      if (availableGroups.length === 1) {
        const [singleGroup] = availableGroups;
        state.activeFormat = singleGroup.value;
        addOption(singleGroup.value, t(singleGroup.labelKey));
      } else {
        state.activeFormat = FORMAT_ALL;
        addOption(FORMAT_ALL, t("browse.filter.option.all"));
      }
      browseFormatSelect.value = state.activeFormat;
      browseFormatSelect.disabled = true;
      return;
    }

    addOption(FORMAT_ALL, t("browse.filter.option.all"));
    availableGroups.forEach((group) => addOption(group.value, t(group.labelKey)));
    browseFormatSelect.disabled = browseModalBusy;
    browseFormatSelect.value = state.activeFormat;
  }

  function syncSeriesControl() {
    if (!browseSeriesModeSelect) return;
    browseSeriesField?.classList.toggle("is-hidden", isFolderSelectMode());
    const hasSeriesCapableFilter = intersectsExts(getActiveBrowseExts(), BROWSE_SERIES_CAPABLE_EXTS);
    const hasSeriesCapableFiles = state.rawFileItems.some((item) => BROWSE_SERIES_CAPABLE_SET.has(inferFileExt(item.ext || item.name)));
    // Disable (don't reset) on incompatible folders so the user's choice survives
    // navigating through them; first_only is a harmless no-op on non-series files.
    browseSeriesModeSelect.disabled = browseModalBusy
      || !hasSeriesCapableFilter
      || (state.rawFileItems.length > 0 && !hasSeriesCapableFiles);
    browseSeriesModeSelect.value = state.seriesMode;
  }

  function updatePathInput() {
    if (!browsePathInput) return;
    browsePathInput.value = state.selectedType === "folder" || state.selectedType === "file"
      ? state.selectedPath
      : state.currentPath;
  }

  function clearBrowseSelection() {
    state.selectedPane = "";
    state.selectedIndex = -1;
    state.selectedPath = "";
    state.selectedType = "";
  }

  function setCurrentFolderSelection() {
    state.selectedPane = "";
    state.selectedIndex = -1;
    state.selectedType = "folder";
    state.selectedPath = state.currentPath;
    state.focusPane = "folders";
    updatePathInput();
    syncBrowseSelectState();
  }

  function refreshBrowseSelection() {
    browseModal?.querySelectorAll(".browse-item").forEach((element) => {
      if (!(element instanceof HTMLElement)) return;
      const pane = element.dataset.browsePane || "";
      const index = Number(element.dataset.browseIndex || -1);
      const selected = pane === state.selectedPane && index === state.selectedIndex;
      element.classList.toggle("is-selected", selected);
      element.setAttribute("aria-selected", selected ? "true" : "false");
    });
  }

  function focusBrowseItem(pane, index) {
    const selector = `.browse-item[data-browse-pane="${pane}"][data-browse-index="${index}"]`;
    const button = browseModal?.querySelector(selector);
    if (button instanceof HTMLElement) {
      button.focus({ preventScroll: true });
    }
  }

  function focusInitialBrowseItem() {
    if (state.folders.length) {
      focusBrowseItem("folders", 0);
    } else if (state.fileItems.length) {
      focusBrowseItem("files", 0);
    } else {
      browseSearchInput?.focus();
    }
  }

  function selectFolderIndex(index, { focus = false } = {}) {
    const folder = state.folders[index];
    if (!folder) return;
    state.selectedPane = "folders";
    state.selectedIndex = index;
    state.selectedPath = folder.path;
    state.selectedType = "folder";
    state.focusPane = "folders";
    updatePathInput();
    refreshBrowseSelection();
    syncBrowseSelectState();
    if (focus) {
      focusBrowseItem("folders", index);
    }
  }

  function selectFileIndex(index, { focus = false } = {}) {
    if (isFolderSelectMode()) return;
    const file = state.fileItems[index];
    if (!file) return;
    state.selectedPane = "files";
    state.selectedIndex = index;
    state.selectedPath = file.path;
    state.selectedType = "file";
    state.focusPane = "files";
    updatePathInput();
    refreshBrowseSelection();
    syncBrowseSelectState();
    if (focus) {
      focusBrowseItem("files", index);
    }
  }

  function filterBrowseItems() {
    const query = normalizeSearchQuery(state.searchQuery);
    state.folders = !query
      ? [...state.allFolders]
      : state.allFolders.filter((folder) => matchesBrowseQuery(folder.name, query));
    if (isFolderSelectMode()) {
      state.fileItems = [];
      return;
    }
    state.fileItems = !query
      ? [...state.fileBaseItems]
      : state.fileBaseItems.filter((file) => matchesBrowseQuery(file.name, query));
  }

  function restoreVisibleSelection(previousSelection) {
    if (!previousSelection) return false;

    if (
      state.mode !== "file-open"
      && previousSelection.type === "folder"
      && previousSelection.path === state.currentPath
    ) {
      setCurrentFolderSelection();
      return true;
    }

    if (previousSelection.type === "folder") {
      const folderIndex = state.folders.findIndex((folder) => folder.path === previousSelection.path);
      if (folderIndex >= 0) {
        selectFolderIndex(folderIndex);
        return true;
      }
    }

    if (previousSelection.type === "file") {
      const fileIndex = state.fileItems.findIndex((file) => file.path === previousSelection.path);
      if (fileIndex >= 0) {
        selectFileIndex(fileIndex);
        return true;
      }
    }

    return false;
  }

  function applyDirectorySelectionDefaults() {
    if (state.mode === "file-open") {
      clearBrowseSelection();
      state.focusPane = "files";
      updatePathInput();
      refreshBrowseSelection();
      syncBrowseSelectState();
      return;
    }
    setCurrentFolderSelection();
    refreshBrowseSelection();
  }

  function renderBrowseLists({ preserveSelection = false } = {}) {
    const previousSelection = preserveSelection
      ? {
        type: state.selectedType,
        path: state.selectedPath,
      }
      : null;

    filterBrowseItems();
    renderFolders();
    renderFiles();
    updateSectionCounts();
    if (!restoreVisibleSelection(previousSelection)) {
      applyDirectorySelectionDefaults();
    }
    updatePathInput();
    refreshBrowseSelection();
    syncBrowseSelectState();
  }

  async function loadBrowseDirectory(path) {
    try {
      const params = new URLSearchParams();
      if (path) {
        params.set("path", path);
      }
      const exts = effectiveExtsParam();
      if (exts) {
        params.set("exts", exts);
      }
      params.set("sort", state.sort);
      params.set("series_mode", state.seriesMode);
      const query = params.toString() ? `?${params.toString()}` : "";
      return await fetchJSON(`${apiBase}/browse${query}`);
    } catch (err) {
      console.error("Browse directory error:", err);
      return null;
    }
  }

  function renderBrowseContent(data) {
    if (!data) return;
    state.currentPath = String(data.currentPath || "");
    writeStoredValue(STORAGE_KEYS.browseLastPath, state.currentPath);
    state.parentPath = String(data.parentPath ?? deriveParentPath(state.currentPath));
    state.canGoUp = Boolean(data.canGoUp ?? state.parentPath);
    state.allFolders = Array.isArray(data.folders)
      ? data.folders.map((folder) => ({
        name: String(folder || ""),
        path: joinBrowsePath(state.currentPath, folder || ""),
      }))
      : [];
    state.rawFileItems = normalizeFileItems(data, state.currentPath);
    state.richMetadataAvailable = Array.isArray(data?.fileItems) && data.fileItems.length > 0;
    let visibleItems = [...state.rawFileItems];
    if (!state.richMetadataAvailable) {
      if (state.seriesMode === "first_only") {
        visibleItems = aggregateSeriesLocally(visibleItems);
      }
      visibleItems = sortFileItems(visibleItems, state.sort);
    }
    state.fileBaseItems = visibleItems;

    updateBrowseTitle();
    renderBreadcrumb();
    // Preserve the selection by path: a sort/filter reload of the same directory
    // keeps it; navigating elsewhere drops it (the old path is no longer listed).
    renderBrowseLists({ preserveSelection: true });
    syncBrowseSortState();
    syncFormatControl();
    syncSeriesControl();
    syncSearchControl();
    syncBrowseViewState();
    syncBrowseUpState();
    applyBrowseModeLayout();
    applyBrowsePaneWidth();
  }

  async function loadAndRenderBrowser(path) {
    const requestId = ++browseRequestId;
    const label = path || t("file_browser.root");
    setBrowseModalBusy(true, t("file_browser.loading", { label }));
    try {
      const data = await loadBrowseDirectory(path);
      if (requestId !== browseRequestId) return;
      if (data) {
        renderBrowseContent(data);
        if (data.requestedPathMissing) {
          setBrowseStatus(t("file_browser.path_not_found"), { isError: true });
        } else {
          setBrowseStatus("");
        }
        if (pendingInitialFocus) {
          pendingInitialFocus = false;
          focusInitialBrowseItem();
        }
      } else {
        showBrowseLoadError(path);
      }
    } finally {
      if (requestId === browseRequestId) {
        setBrowseModalBusy(false);
        syncBrowseSortState();
        syncFormatControl();
        syncSeriesControl();
      }
    }
  }

  function settleFileDialog(selection = "") {
    if (!fileDialogPromise) return;
    const pending = fileDialogPromise;
    fileDialogPromise = null;
    pending.resolve(selection);
  }

  function rejectFileDialog(error) {
    if (!fileDialogPromise) return;
    const pending = fileDialogPromise;
    fileDialogPromise = null;
    pending.reject(error instanceof Error ? error : new Error(String(error || "File dialog failed")));
  }

  function resetBrowseFilters(exts = "") {
    state.requestedExtsRaw = String(exts || "").trim();
    state.requestedExts = normalizeRequestedExts(state.requestedExtsRaw);
    restoreBrowsePreferences();
    if (browseSortSelect) {
      browseSortSelect.value = state.sort;
    }
    if (browseSeriesModeSelect) {
      browseSeriesModeSelect.value = state.seriesMode;
    }
    if (browseViewModeSelect) {
      browseViewModeSelect.value = state.viewMode;
    }
    syncFormatControl();
    syncSeriesControl();
    syncBrowseViewState();
    applyBrowsePaneWidth();
  }

  function openFileBrowser(mode, inputElement) {
    const initialPath = readStoredValue(STORAGE_KEYS.browseLastPath) || "";
    pendingInitialFocus = true;
    state.mode = mode;
    state.inputElement = inputElement;
    state.currentPath = initialPath;
    state.parentPath = "";
    state.canGoUp = false;
    clearBrowseSelection();
    clearSearchQuery({ rerender: false });
    resetBrowseFilters("");
    updateBrowseTitle();
    applyBrowseModeLayout();
    openModal(browseModal, { focusTarget: browseCloseBtn || browseSelectBtn || browsePathInput });
    setBrowseModalBusy(true, t("file_browser.loading", { label: initialPath || t("file_browser.root") }));
    loadAndRenderBrowser(initialPath).catch((err) => console.error(err));
  }

  function openFileDialog(options = {}) {
    const exts = typeof options === "object" && options !== null ? String(options.exts || "") : "";
    return new Promise((resolve, reject) => {
      const initialPath = readStoredValue(STORAGE_KEYS.browseLastPath) || "";
      pendingInitialFocus = true;
      settleFileDialog("");
      fileDialogPromise = { resolve, reject };
      state.mode = "file-open";
      state.inputElement = null;
      state.currentPath = initialPath;
      state.parentPath = "";
      state.canGoUp = false;
      clearBrowseSelection();
      clearSearchQuery({ rerender: false });
      resetBrowseFilters(exts);
      updateBrowseTitle();
      applyBrowseModeLayout();
      openModal(browseModal, { focusTarget: browseCloseBtn || browseSelectBtn || browsePathInput });
      setBrowseModalBusy(true, t("file_browser.loading", { label: initialPath || t("file_browser.root") }));
      loadAndRenderBrowser(initialPath).catch((err) => {
        closeFileBrowser({ cancelDialog: false });
        rejectFileDialog(err);
      });
    });
  }

  function closeFileBrowser({ restoreFocus = true, cancelDialog = true } = {}) {
    browseRequestId += 1;
    clearBrowseSelection();
    state.requestedExtsRaw = "";
    state.requestedExts = normalizeRequestedExts("");
    state.allFolders = [];
    state.fileBaseItems = [];
    state.folders = [];
    state.rawFileItems = [];
    state.fileItems = [];
    clearSearchQuery({ rerender: false });
    setBrowseModalBusy(false);
    setBrowseStatus("");
    closeModal(browseModal, { restoreFocus });
    if (cancelDialog && state.mode === "file-open") {
      settleFileDialog("");
    }
  }

  function confirmBrowseSelection() {
    if (state.mode === "file-open") {
      if (state.selectedType !== "file" || !state.selectedPath) {
        setStatus(t("status.file.select_image_first"));
        return false;
      }
      const selectedPath = state.selectedPath;
      closeFileBrowser({ cancelDialog: false });
      settleFileDialog(selectedPath);
      return true;
    }

    if (state.selectedType !== "folder") {
      setStatus(t("status.file.no_selection"), { tone: "warning" });
      return false;
    }

    if (!state.inputElement) {
      closeFileBrowser();
      return true;
    }

    if (typeof onPathSelected === "function") {
      try {
        onPathSelected({ mode: state.mode, selectedPath: state.selectedPath, inputElement: state.inputElement });
      } catch (err) {
        console.error(err);
      }
    }

    closeFileBrowser();
    return true;
  }

  function navigateUp() {
    if (!state.canGoUp) return false;
    clearSearchQuery({ rerender: false });
    loadAndRenderBrowser(state.parentPath).catch((err) => console.error(err));
    return true;
  }

  function submitTypedPath() {
    if (!browsePathInput) return;
    const raw = String(browsePathInput.value || "").trim();
    // Jumping to a file path lands in its folder; a directory path opens directly.
    const target = raw && KNOWN_BROWSE_EXTS.has(inferFileExt(raw)) ? deriveParentPath(raw) : raw;
    if (target === state.currentPath) {
      updatePathInput();
      return;
    }
    clearSearchQuery({ rerender: false });
    loadAndRenderBrowser(target).catch((err) => console.error(err));
  }

  function moveSelection(direction) {
    const panes = state.focusPane === "folders" ? ["folders", "files"] : ["files", "folders"];
    let pane = panes.find((candidate) => (candidate === "folders" ? state.folders.length : state.fileItems.length));
    if (!pane) return false;

    const activeElement = document.activeElement;
    if (browseFoldersList?.contains(activeElement)) {
      pane = state.folders.length ? "folders" : pane;
    } else if (browseFilesList?.contains(activeElement)) {
      pane = state.fileItems.length ? "files" : pane;
    }

    const items = pane === "folders" ? state.folders : state.fileItems;
    if (!items.length) return false;
    const currentIndex = state.selectedPane === pane ? state.selectedIndex : -1;
    const nextIndex = currentIndex < 0
      ? (direction > 0 ? 0 : items.length - 1)
      : Math.max(0, Math.min(items.length - 1, currentIndex + direction));
    if (pane === "folders") {
      selectFolderIndex(nextIndex, { focus: true });
    } else {
      selectFileIndex(nextIndex, { focus: true });
    }
    return true;
  }

  function handleBrowseEnter() {
    if (state.selectedType === "folder" && state.selectedPath !== state.currentPath) {
      clearSearchQuery({ rerender: false });
      loadAndRenderBrowser(state.selectedPath).catch((err) => console.error(err));
      return true;
    }
    if (state.selectedType === "file" && state.mode === "file-open") {
      confirmBrowseSelection();
      return true;
    }
    return false;
  }

  function handleSearchEnter() {
    // Prefer a file match, else the first folder. Open outright only when it is
    // the sole match in a file-open dialog; otherwise just select + focus it.
    if (state.fileItems.length) {
      selectFileIndex(0, { focus: true });
      if (state.fileItems.length === 1 && !state.folders.length && state.mode === "file-open") {
        confirmBrowseSelection();
      }
      return true;
    }
    if (state.folders.length) {
      selectFolderIndex(0, { focus: true });
      return true;
    }
    return false;
  }

  bindFileBrowserEvents({
    state,
    browseModal,
    browseUpBtn,
    browseSearchInput,
    browseSearchClearBtn,
    browseFormatSelect,
    browseSortSelect,
    browseSeriesModeSelect,
    browseViewModeSelect,
    browseSplitter,
    browsePathInput,
    browseSelectBtn,
    browseCancelBtn,
    browseCloseBtn,
    filesystemModeEl,
    onFilesystemModeChanged,
    isBrowseModalBusy: () => browseModalBusy,
    confirmBrowseSelection,
    closeFileBrowser,
    navigateUp,
    setSearchQuery,
    clearSearchQuery,
    submitTypedPath,
    updatePathInput,
    persistBrowseControlPreferences,
    syncSeriesControl,
    loadAndRenderBrowser,
    applySort,
    renderBrowseLists,
    isStackedBrowseLayout,
    clampBrowsePaneWidth,
    applyBrowsePaneWidth,
    persistBrowsePaneWidth,
    handleSearchEnter,
    moveSelection,
    handleBrowseEnter,
    persistFilesystemMode,
  });

  return {
    isBackendLocal,
    openFileBrowser,
    openFileDialog,
    closeFileBrowser,
    restoreFilesystemMode,
  };
}
