/**
 * File browser modal controller for remote filesystem browsing.
 *
 * Keeps modal state, browse requests, and dialog behavior encapsulated so
 * app.js can treat it as a small integration surface.
 */

import { t } from "./i18n.js";
import { CBF_EXTS, EDF_EXTS, HDF_EXTS, SERIES_IMAGE_EXTS, TIFF_EXTS } from "./file_type_utils.js";

const DEFAULT_SORT = "name_asc";
const DEFAULT_SERIES_MODE = "all";
const FORMAT_ALL = "__all__";
const EXPT_EXTS = [".expt"];
const DEFAULT_BROWSE_EXTS = [...HDF_EXTS, ...TIFF_EXTS, ...CBF_EXTS, ...EDF_EXTS];
const FORMAT_GROUPS = [
  { value: "hdf5", labelKey: "browse.filter.option.hdf5", exts: HDF_EXTS },
  { value: "tiff", labelKey: "browse.filter.option.tiff", exts: TIFF_EXTS },
  { value: "cbf", labelKey: "browse.filter.option.cbf", exts: CBF_EXTS },
  { value: "edf", labelKey: "browse.filter.option.edf", exts: EDF_EXTS },
  { value: "geometry", labelKey: "browse.filter.option.geometry", exts: EXPT_EXTS },
];

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

function normalizeExtToken(raw) {
  let token = String(raw || "").trim().toLowerCase();
  if (!token) return "";
  if (!token.startsWith(".")) {
    token = `.${token}`;
  }
  return token;
}

function normalizeRequestedExts(raw) {
  const tokens = new Set(raw ? [] : DEFAULT_BROWSE_EXTS);
  if (!raw) {
    return tokens;
  }
  String(raw)
    .split(",")
    .map((item) => normalizeExtToken(item))
    .filter(Boolean)
    .forEach((token) => {
      tokens.add(token);
      if (token === ".cbf") {
        tokens.add(".cbf.gz");
      }
    });
  return tokens;
}

function serializeExts(exts) {
  return Array.from(exts)
    .filter(Boolean)
    .sort((a, b) => String(a).localeCompare(String(b)))
    .join(",");
}

function intersectsExts(exts, candidates) {
  return candidates.some((candidate) => exts.has(candidate));
}

function joinBrowsePath(base, name) {
  const root = String(base || "").trim();
  const child = String(name || "").trim();
  if (!root) return child;
  const separator = root.includes("\\") && !root.includes("/") ? "\\" : "/";
  return `${root.replace(/[\\/]$/, "")}${separator}${child}`;
}

function buildBreadcrumbSegments(currentPath) {
  const raw = String(currentPath || "").trim();
  if (!raw) return [];

  if (/^[A-Za-z]:\\/.test(raw)) {
    const parts = raw.split("\\").filter(Boolean);
    let accumulated = parts[0] || "";
    return parts.map((part, index) => {
      if (index === 0) {
        return { label: part, path: accumulated };
      }
      accumulated = `${accumulated}\\${part}`;
      return { label: part, path: accumulated };
    });
  }

  const unixAbsolute = raw.startsWith("/");
  const parts = raw.split("/").filter(Boolean);
  let accumulated = unixAbsolute ? "" : "";
  return parts.map((part) => {
    if (unixAbsolute) {
      accumulated = accumulated ? `${accumulated}/${part}` : `/${part}`;
    } else {
      accumulated = accumulated ? `${accumulated}/${part}` : part;
    }
    return { label: part, path: accumulated };
  });
}

function shouldIgnoreBrowseShortcuts(target) {
  if (!(target instanceof HTMLElement)) return false;
  if (target.closest(".browse-item")) return false;
  if (target.tagName === "SELECT") return true;
  if (target.tagName === "TEXTAREA") return true;
  if (target.tagName === "INPUT" && !target.readOnly) return true;
  if (target.tagName === "BUTTON") return true;
  return false;
}

export function createFileBrowserController({
  apiBase,
  browseModal,
  browseTitle,
  browseBreadcrumb,
  browseUpBtn,
  browseFormatField,
  browseFormatSelect,
  browseSortSelect,
  browseSeriesModeSelect,
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
    folders: [],
    fileItems: [],
  };

  let browseModalBusy = false;
  let browseRequestId = 0;
  let fileDialogPromise = null;
  const isBackendLocal = detectBackendLocal(apiBase);

  function setBrowseStatus(text = "", { isError = false, isLoading = false } = {}) {
    if (!browseStatus) return;
    browseStatus.textContent = text || "";
    browseStatus.classList.toggle("is-error", Boolean(isError));
    browseStatus.classList.toggle("is-loading", Boolean(isLoading));
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

  function setBrowseModalBusy(isBusy, statusText = "") {
    browseModalBusy = Boolean(isBusy);
    browseModal?.setAttribute("aria-busy", browseModalBusy ? "true" : "false");
    browseBreadcrumb?.classList.toggle("is-loading", browseModalBusy);
    browseFoldersList?.classList.toggle("is-loading", browseModalBusy);
    browseFilesList?.classList.toggle("is-loading", browseModalBusy);
    if (browseFormatSelect) browseFormatSelect.disabled = browseModalBusy;
    if (browseSortSelect) browseSortSelect.disabled = browseModalBusy;
    if (browseSeriesModeSelect) browseSeriesModeSelect.disabled = browseModalBusy;
    if (statusText) {
      setBrowseStatus(statusText, { isLoading: browseModalBusy });
    } else if (!browseModalBusy && browseStatus?.classList.contains("is-loading")) {
      setBrowseStatus("");
    }
    syncBrowseSelectState();
    syncBrowseUpState();
  }

  function persistFilesystemMode(mode) {
    if (mode !== "local" && mode !== "remote") return;
    try {
      localStorage.setItem("albis.filesystemMode", mode);
    } catch {
      // ignore storage errors
    }
  }

  function restoreFilesystemMode() {
    if (!filesystemModeEl || isBackendLocal) return;
    try {
      const stored = localStorage.getItem("albis.filesystemMode");
      if (stored === "local" || stored === "remote") {
        filesystemModeEl.value = stored;
      }
    } catch {
      // ignore storage errors
    }
  }

  function updateBrowseTitle() {
    if (!browseTitle) return;
    const key = state.mode === "file-open" ? "browse.title.select_file" : "browse.title.select_folder";
    browseTitle.dataset.i18n = key;
    browseTitle.textContent = t(key);
  }

  function getAvailableFormatGroups() {
    return FORMAT_GROUPS.filter((group) => intersectsExts(state.requestedExts, group.exts));
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
    const showControl = availableGroups.length > 1;
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
    if (!showControl) {
      browseFormatSelect.disabled = true;
      return;
    }

    const addOption = (value, label) => {
      const option = document.createElement("option");
      option.value = value;
      option.textContent = label;
      browseFormatSelect.appendChild(option);
    };

    addOption(FORMAT_ALL, t("browse.filter.option.all"));
    availableGroups.forEach((group) => addOption(group.value, t(group.labelKey)));
    browseFormatSelect.disabled = browseModalBusy;
    browseFormatSelect.value = state.activeFormat;
  }

  function syncSeriesControl() {
    if (!browseSeriesModeSelect) return;
    const hasSeriesCapableFiles = intersectsExts(getActiveBrowseExts(), SERIES_IMAGE_EXTS);
    if (!hasSeriesCapableFiles) {
      state.seriesMode = DEFAULT_SERIES_MODE;
    }
    browseSeriesModeSelect.disabled = browseModalBusy || !hasSeriesCapableFiles;
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

  function normalizeFileItems(data) {
    if (Array.isArray(data?.fileItems) && data.fileItems.length > 0) {
      return data.fileItems.map((item) => ({
        name: String(item?.name || ""),
        path: String(item?.path || joinBrowsePath(state.currentPath, item?.name || "")),
        ext: String(item?.ext || ""),
        mtime: Number(item?.mtime || 0),
        sizeBytes: Number(item?.sizeBytes || 0),
        isSeriesLead: Boolean(item?.isSeriesLead),
        seriesCount: Math.max(1, Number(item?.seriesCount || 1)),
      }));
    }
    if (Array.isArray(data?.files)) {
      return data.files.map((name) => ({
        name: String(name || ""),
        path: joinBrowsePath(state.currentPath, name || ""),
        ext: "",
        mtime: 0,
        sizeBytes: 0,
        isSeriesLead: false,
        seriesCount: 1,
      }));
    }
    return [];
  }

  function renderBreadcrumb() {
    if (!browseBreadcrumb) return;
    browseBreadcrumb.innerHTML = "";
    const rootBtn = document.createElement("button");
    rootBtn.className = "breadcrumb-btn";
    rootBtn.type = "button";
    rootBtn.textContent = t("file_browser.root");
    rootBtn.dataset.path = "";
    if (!state.currentPath) {
      rootBtn.classList.add("is-active");
    }
    rootBtn.addEventListener("click", () => loadAndRenderBrowser(""));
    browseBreadcrumb.appendChild(rootBtn);

    buildBreadcrumbSegments(state.currentPath).forEach((segment) => {
      const btn = document.createElement("button");
      btn.className = "breadcrumb-btn";
      btn.type = "button";
      btn.textContent = segment.label;
      btn.dataset.path = segment.path;
      if (segment.path === state.currentPath) {
        btn.classList.add("is-active");
      }
      btn.addEventListener("click", () => loadAndRenderBrowser(segment.path));
      browseBreadcrumb.appendChild(btn);
    });
  }

  function renderFolders() {
    if (!browseFoldersList) return;
    browseFoldersList.innerHTML = "";
    if (!state.folders.length) {
      const empty = document.createElement("div");
      empty.className = "browse-empty";
      empty.textContent = t("file_browser.no_folders");
      browseFoldersList.appendChild(empty);
      return;
    }

    state.folders.forEach((folder, index) => {
      const btn = document.createElement("button");
      btn.className = "browse-item";
      btn.type = "button";
      btn.dataset.browsePane = "folders";
      btn.dataset.browseIndex = String(index);
      btn.textContent = folder.name;
      btn.addEventListener("click", () => selectFolderIndex(index, { focus: true }));
      btn.addEventListener("dblclick", () => loadAndRenderBrowser(folder.path));
      browseFoldersList.appendChild(btn);
    });
  }

  function renderFiles() {
    if (!browseFilesList) return;
    browseFilesList.innerHTML = "";
    if (!state.fileItems.length) {
      const empty = document.createElement("div");
      empty.className = "browse-empty";
      empty.textContent = t("file_browser.no_images");
      browseFilesList.appendChild(empty);
      return;
    }

    state.fileItems.forEach((file, index) => {
      const btn = document.createElement("button");
      btn.className = "browse-item";
      btn.type = "button";
      btn.dataset.browsePane = "files";
      btn.dataset.browseIndex = String(index);

      const content = document.createElement("span");
      content.className = "browse-item-content";

      const label = document.createElement("span");
      label.className = "browse-item-label";
      label.textContent = file.name;
      content.appendChild(label);

      if (file.isSeriesLead && file.seriesCount > 1) {
        const badge = document.createElement("span");
        badge.className = "browse-item-badge";
        badge.textContent = t("file_browser.series_badge", { count: file.seriesCount });
        content.appendChild(badge);
      }

      btn.appendChild(content);
      btn.addEventListener("click", () => selectFileIndex(index, { focus: true }));
      btn.addEventListener("dblclick", () => {
        selectFileIndex(index, { focus: true });
        if (state.mode === "file-open") {
          confirmBrowseSelection();
        }
      });
      browseFilesList.appendChild(btn);
    });
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
      const res = await fetch(`${apiBase}/browse${query}`);
      if (!res.ok) {
        console.error("Failed to browse directory:", res.status);
        return null;
      }
      return await res.json();
    } catch (err) {
      console.error("Browse directory error:", err);
      return null;
    }
  }

  function renderBrowseContent(data) {
    if (!data) return;
    state.currentPath = String(data.currentPath || "");
    state.parentPath = String(data.parentPath ?? "");
    state.canGoUp = Boolean(data.canGoUp);
    state.folders = Array.isArray(data.folders)
      ? data.folders.map((folder) => ({
        name: String(folder || ""),
        path: joinBrowsePath(state.currentPath, folder || ""),
      }))
      : [];
    state.fileItems = normalizeFileItems(data);

    updateBrowseTitle();
    renderBreadcrumb();
    renderFolders();
    renderFiles();
    applyDirectorySelectionDefaults();
    syncBrowseUpState();
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
        setBrowseStatus("");
      } else {
        setBrowseStatus(t("file_browser.failed_load"), { isError: true });
      }
    } finally {
      if (requestId === browseRequestId) {
        setBrowseModalBusy(false);
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
    state.activeFormat = FORMAT_ALL;
    state.sort = DEFAULT_SORT;
    state.seriesMode = DEFAULT_SERIES_MODE;
    if (browseSortSelect) {
      browseSortSelect.value = state.sort;
    }
    if (browseSeriesModeSelect) {
      browseSeriesModeSelect.value = state.seriesMode;
    }
    syncFormatControl();
    syncSeriesControl();
  }

  function openFileBrowser(mode, inputElement) {
    state.mode = mode;
    state.inputElement = inputElement;
    state.currentPath = "";
    state.parentPath = "";
    state.canGoUp = false;
    clearBrowseSelection();
    resetBrowseFilters("");
    updateBrowseTitle();
    openModal(browseModal, { focusTarget: browseCloseBtn || browseSelectBtn || browsePathInput });
    setBrowseModalBusy(true, t("file_browser.loading", { label: t("file_browser.root") }));
    loadAndRenderBrowser("").catch((err) => console.error(err));
  }

  function openFileDialog(options = {}) {
    const exts = typeof options === "object" && options !== null ? String(options.exts || "") : "";
    return new Promise((resolve, reject) => {
      settleFileDialog("");
      fileDialogPromise = { resolve, reject };
      state.mode = "file-open";
      state.inputElement = null;
      state.currentPath = "";
      state.parentPath = "";
      state.canGoUp = false;
      clearBrowseSelection();
      resetBrowseFilters(exts);
      updateBrowseTitle();
      openModal(browseModal, { focusTarget: browseCloseBtn || browseSelectBtn || browsePathInput });
      setBrowseModalBusy(true, t("file_browser.loading", { label: t("file_browser.root") }));
      loadAndRenderBrowser("").catch((err) => {
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
      setStatus(t("status.file.no_selection"));
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
    loadAndRenderBrowser(state.parentPath).catch((err) => console.error(err));
    return true;
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
      loadAndRenderBrowser(state.selectedPath).catch((err) => console.error(err));
      return true;
    }
    if (state.selectedType === "file" && state.mode === "file-open") {
      confirmBrowseSelection();
      return true;
    }
    return false;
  }

  browseSelectBtn?.addEventListener("click", () => {
    confirmBrowseSelection();
  });

  browseCancelBtn?.addEventListener("click", () => closeFileBrowser());
  browseCloseBtn?.addEventListener("click", () => closeFileBrowser());
  browseUpBtn?.addEventListener("click", () => {
    navigateUp();
  });

  browseFormatSelect?.addEventListener("change", () => {
    state.activeFormat = browseFormatSelect.value || FORMAT_ALL;
    syncSeriesControl();
    loadAndRenderBrowser(state.currentPath).catch((err) => console.error(err));
  });

  browseSortSelect?.addEventListener("change", () => {
    state.sort = browseSortSelect.value || DEFAULT_SORT;
    loadAndRenderBrowser(state.currentPath).catch((err) => console.error(err));
  });

  browseSeriesModeSelect?.addEventListener("change", () => {
    if (browseSeriesModeSelect.disabled) return;
    state.seriesMode = browseSeriesModeSelect.value || DEFAULT_SERIES_MODE;
    loadAndRenderBrowser(state.currentPath).catch((err) => console.error(err));
  });

  browseModal?.addEventListener("keydown", (event) => {
    if (!browseModal.classList.contains("is-open") || shouldIgnoreBrowseShortcuts(event.target)) {
      return;
    }
    if ((event.key === "Backspace" || (event.altKey && event.key === "ArrowUp")) && navigateUp()) {
      event.preventDefault();
      return;
    }
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      if (moveSelection(event.key === "ArrowDown" ? 1 : -1)) {
        event.preventDefault();
      }
      return;
    }
    if (event.key === "Enter" && handleBrowseEnter()) {
      event.preventDefault();
    }
  });

  browseModal?.addEventListener("click", (event) => {
    if (event.target === browseModal || event.target.classList?.contains("modal-backdrop")) {
      closeFileBrowser();
    }
  });

  filesystemModeEl?.addEventListener("change", () => {
    const nextMode = filesystemModeEl.value;
    persistFilesystemMode(nextMode);
    if (typeof onFilesystemModeChanged === "function") {
      onFilesystemModeChanged(nextMode);
    }
  });

  return {
    isBackendLocal,
    openFileBrowser,
    openFileDialog,
    closeFileBrowser,
    restoreFilesystemMode,
  };
}
