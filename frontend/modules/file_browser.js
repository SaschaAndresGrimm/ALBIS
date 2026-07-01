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
const DEFAULT_VIEW_MODE = "list";
const FORMAT_ALL = "__all__";
const EXPT_EXTS = [".expt"];
const DEFAULT_BROWSE_EXTS = [...HDF_EXTS, ...TIFF_EXTS, ...CBF_EXTS, ...EDF_EXTS];
const LOCAL_SERIES_EXTS = new Set(SERIES_IMAGE_EXTS);
const HDF_SERIES_EXTS = new Set(HDF_EXTS);
const BROWSE_SERIES_CAPABLE_EXTS = [...SERIES_IMAGE_EXTS, ...HDF_EXTS];
const BROWSE_SERIES_CAPABLE_SET = new Set(BROWSE_SERIES_CAPABLE_EXTS);
const HDF_MASTER_RE = /^(.+?)_master\.(?:h5|hdf5)$/i;
const HDF_DATA_RE = /^(.+?)_data_(\d+)\.(?:h5|hdf5)$/i;
const VALID_SORTS = new Set(["name_asc", "name_desc", "mtime_desc", "mtime_asc", "type_asc"]);
const VALID_SERIES_MODES = new Set(["all", "first_only"]);
const VALID_VIEW_MODES = new Set(["list", "details"]);
const STORAGE_KEYS = {
  filesystemMode: "albis.filesystemMode",
  browsePaneWidth: "albis.browsePaneWidth",
  browseSort: "albis.browseSort",
  browseSeriesMode: "albis.browseSeriesMode",
  browseFormat: "albis.browseFormat",
  browseViewMode: "albis.browseViewMode",
  browseLastPath: "albis.browseLastPath",
};
const BROWSE_BREAKPOINT = 760;
const BROWSE_MIN_FOLDER_WIDTH = 220;
const BROWSE_MIN_FILE_WIDTH = 280;
const BROWSE_SPLITTER_WIDTH = 8;
const DEFAULT_BROWSE_PANE_WIDTH = 320;
const EMPTY_VALUE = "—";
const FORMAT_GROUPS = [
  { value: "hdf5", labelKey: "browse.filter.option.hdf5", exts: HDF_EXTS },
  { value: "tiff", labelKey: "browse.filter.option.tiff", exts: TIFF_EXTS },
  { value: "cbf", labelKey: "browse.filter.option.cbf", exts: CBF_EXTS },
  { value: "edf", labelKey: "browse.filter.option.edf", exts: EDF_EXTS },
  { value: "geometry", labelKey: "browse.filter.option.geometry", exts: EXPT_EXTS },
];
const NATURAL_SPLIT_RE = /(\d+)/;

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

function naturalSortKey(value) {
  return String(value || "")
    .toLowerCase()
    .split(NATURAL_SPLIT_RE)
    .filter((part) => part !== "")
    .map((part) => (/^\d+$/.test(part) ? Number(part) : part));
}

function intersectsExts(exts, candidates) {
  return candidates.some((candidate) => exts.has(candidate));
}

function compareNatural(a, b) {
  const left = naturalSortKey(a);
  const right = naturalSortKey(b);
  const length = Math.max(left.length, right.length);
  for (let index = 0; index < length; index += 1) {
    const leftPart = left[index];
    const rightPart = right[index];
    if (typeof leftPart === "undefined") return -1;
    if (typeof rightPart === "undefined") return 1;
    if (leftPart === rightPart) continue;
    if (typeof leftPart === "number" && typeof rightPart === "number") {
      return leftPart - rightPart;
    }
    return String(leftPart).localeCompare(String(rightPart));
  }
  return 0;
}

function joinBrowsePath(base, name) {
  const root = String(base || "").trim();
  const child = String(name || "").trim();
  if (!root) return child;
  const separator = root.includes("\\") && !root.includes("/") ? "\\" : "/";
  return `${root.replace(/[\\/]$/, "")}${separator}${child}`;
}

function normalizeSearchQuery(raw) {
  return String(raw || "").trim().toLowerCase();
}

function fileTypeLabelKey(ext) {
  if (HDF_EXTS.includes(ext)) return "browse.filter.option.hdf5";
  if (TIFF_EXTS.includes(ext)) return "browse.filter.option.tiff";
  if (CBF_EXTS.includes(ext)) return "browse.filter.option.cbf";
  if (EDF_EXTS.includes(ext)) return "browse.filter.option.edf";
  if (EXPT_EXTS.includes(ext)) return "browse.filter.option.geometry";
  return "";
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

function matchesBrowseQuery(name, query) {
  return !query || String(name || "").toLowerCase().includes(query);
}

function inferFileExt(name) {
  const lower = String(name || "").toLowerCase();
  if (lower.endsWith(".cbf.gz")) return ".cbf.gz";
  const dot = lower.lastIndexOf(".");
  return dot >= 0 ? lower.slice(dot) : "";
}

function splitSeriesName(name) {
  const ext = inferFileExt(name);
  const stem = ext === ".cbf.gz"
    ? String(name || "").slice(0, -ext.length)
    : String(name || "").replace(new RegExp(`${ext.replace(".", "\\.")}$`, "i"), "");
  const match = stem.match(/^(.*?)(\d+)([^\d]*)$/);
  if (!match) return null;
  return { prefix: match[1], digits: match[2], suffix: match[3] };
}

function splitHdfSeries(name) {
  const raw = String(name || "");
  const master = raw.match(HDF_MASTER_RE);
  if (master) return { kind: "master", prefix: master[1].toLowerCase(), index: 0 };
  const data = raw.match(HDF_DATA_RE);
  if (data) return { kind: "data", prefix: data[1].toLowerCase(), index: Number(data[2]) };
  return null;
}

function deriveParentPath(currentPath) {
  const raw = String(currentPath || "").trim();
  if (!raw) return "";
  if (/^[A-Za-z]:\\/.test(raw)) {
    const parts = raw.split("\\").filter(Boolean);
    return parts.length > 1 ? parts.slice(0, -1).join("\\") : "";
  }
  const isAbsolute = raw.startsWith("/");
  const parts = raw.split("/").filter(Boolean);
  if (!parts.length) return "";
  if (isAbsolute && parts.length === 1) {
    return "/";
  }
  const parent = parts.slice(0, -1).join("/");
  return isAbsolute ? (parent ? `/${parent}` : "/") : parent;
}

function sortFileItems(items, sortMode) {
  const working = [...items];
  if (sortMode === "name_desc") {
    return working.sort((left, right) => compareNatural(right.name, left.name));
  }
  if (sortMode === "mtime_desc") {
    return working.sort((left, right) => {
      const delta = Number(right.mtime || 0) - Number(left.mtime || 0);
      return delta || compareNatural(left.name, right.name);
    });
  }
  if (sortMode === "mtime_asc") {
    return working.sort((left, right) => {
      const delta = Number(left.mtime || 0) - Number(right.mtime || 0);
      return delta || compareNatural(left.name, right.name);
    });
  }
  if (sortMode === "type_asc") {
    return working.sort((left, right) => {
      const extDelta = compareNatural(left.ext, right.ext);
      return extDelta || compareNatural(left.name, right.name);
    });
  }
  return working.sort((left, right) => compareNatural(left.name, right.name));
}

function aggregateSeriesLocally(items) {
  const singles = [];
  const groups = new Map();
  const hdfGroups = new Map();
  for (const item of items) {
    const ext = inferFileExt(item.ext || item.name);
    if (HDF_SERIES_EXTS.has(ext)) {
      const parsed = splitHdfSeries(item.name);
      if (!parsed) {
        singles.push(item);
        continue;
      }
      let group = hdfGroups.get(parsed.prefix);
      if (!group) {
        group = { master: null, data: [] };
        hdfGroups.set(parsed.prefix, group);
      }
      if (parsed.kind === "master") {
        group.master = item;
      } else {
        group.data.push({ index: parsed.index, item });
      }
      continue;
    }
    if (!LOCAL_SERIES_EXTS.has(ext)) {
      singles.push(item);
      continue;
    }
    const parts = splitSeriesName(item.name);
    if (!parts) {
      singles.push(item);
      continue;
    }
    const index = Number(parts.digits);
    if (!Number.isFinite(index)) {
      singles.push(item);
      continue;
    }
    const key = `${parts.prefix}\u0000${parts.suffix}\u0000${ext}`;
    const current = groups.get(key);
    if (!current) {
      groups.set(key, {
        ...item,
        ext,
        isSeriesLead: false,
        seriesCount: 1,
        _leadIndex: index,
      });
      continue;
    }
    current.seriesCount += 1;
    current.mtime = Math.max(Number(current.mtime || 0), Number(item.mtime || 0));
    if (index < current._leadIndex) {
      current.name = item.name;
      current.path = item.path;
      current.ext = ext;
      current.sizeBytes = item.sizeBytes;
      current._leadIndex = index;
    }
  }
  for (const group of hdfGroups.values()) {
    const data = [...group.data].sort((left, right) => left.index - right.index);
    const lead = group.master || (data.length ? data[0].item : null);
    if (!lead) continue;
    const members = [group.master, ...data.map((entry) => entry.item)].filter(Boolean);
    const maxMtime = members.reduce((acc, member) => Math.max(acc, Number(member.mtime || 0)), 0);
    singles.push({
      ...lead,
      ext: inferFileExt(lead.ext || lead.name),
      mtime: maxMtime,
      seriesCount: Math.max(1, data.length),
    });
  }
  const merged = [...singles, ...groups.values()];
  return merged.map((item) => ({
    ...item,
    isSeriesLead: Number(item.seriesCount || 1) > 1,
    seriesCount: Math.max(1, Number(item.seriesCount || 1)),
  }));
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
  browseSearchInput,
  browseSearchClearBtn,
  browseFormatField,
  browseFormatSelect,
  browseSortSelect,
  browseSeriesModeSelect,
  browseViewModeSelect,
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
  const isBackendLocal = detectBackendLocal(apiBase);
  const dateTimeFormatter = (() => {
    try {
      return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" });
    } catch {
      return new Intl.DateTimeFormat();
    }
  })();

  function setBrowseStatus(text = "", { isError = false, isLoading = false } = {}) {
    if (!browseStatus) return;
    browseStatus.textContent = text || "";
    browseStatus.classList.toggle("is-error", Boolean(isError));
    browseStatus.classList.toggle("is-loading", Boolean(isLoading));
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

  function syncBrowseSortState() {
    if (!browseSortSelect) return;
    const canSortByMtime = state.rawFileItems.some((item) => Number(item.mtime || 0) > 0);
    Array.from(browseSortSelect.options).forEach((option) => {
      option.disabled = !canSortByMtime && (option.value === "mtime_desc" || option.value === "mtime_asc");
    });
    if (!canSortByMtime && (state.sort === "mtime_desc" || state.sort === "mtime_asc")) {
      state.sort = DEFAULT_SORT;
      browseSortSelect.value = state.sort;
    }
    browseSortSelect.disabled = browseModalBusy;
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
    const hasSeriesCapableFilter = intersectsExts(getActiveBrowseExts(), BROWSE_SERIES_CAPABLE_EXTS);
    const hasSeriesCapableFiles = state.rawFileItems.some((item) => BROWSE_SERIES_CAPABLE_SET.has(inferFileExt(item.ext || item.name)));
    if (!hasSeriesCapableFilter || (state.rawFileItems.length > 0 && !hasSeriesCapableFiles)) {
      state.seriesMode = DEFAULT_SERIES_MODE;
    }
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
        ext: String(item?.ext || inferFileExt(item?.name || "")),
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
        ext: inferFileExt(name),
        mtime: 0,
        sizeBytes: 0,
        isSeriesLead: false,
        seriesCount: 1,
      }));
    }
    return [];
  }

  function formatFileTypeLabel(file) {
    const key = fileTypeLabelKey(inferFileExt(file.ext || file.name));
    if (key) return t(key);
    const ext = inferFileExt(file.ext || file.name);
    return ext ? ext.replace(/^\./, "").toUpperCase() : EMPTY_VALUE;
  }

  function formatFileModified(mtime) {
    if (!(Number(mtime) > 0)) {
      return EMPTY_VALUE;
    }
    return dateTimeFormatter.format(new Date(Number(mtime) * 1000));
  }

  function formatFileSize(sizeBytes) {
    const size = Number(sizeBytes);
    if (!(size > 0)) {
      return EMPTY_VALUE;
    }
    const units = ["B", "KB", "MB", "GB", "TB"];
    let value = size;
    let unitIndex = 0;
    while (value >= 1024 && unitIndex < units.length - 1) {
      value /= 1024;
      unitIndex += 1;
    }
    const precision = value >= 10 || unitIndex === 0 || Number.isInteger(value) ? 0 : 1;
    return `${value.toFixed(precision)} ${units[unitIndex]}`;
  }

  function createSeriesBadge(file) {
    if (!file.isSeriesLead || file.seriesCount <= 1) {
      return null;
    }
    const badge = document.createElement("span");
    badge.className = "browse-item-badge";
    badge.textContent = t("file_browser.series_badge", { count: file.seriesCount });
    return badge;
  }

  function createListRowContent(file) {
    const content = document.createElement("span");
    content.className = "browse-item-content";

    const label = document.createElement("span");
    label.className = "browse-item-label";
    label.textContent = file.name;
    content.appendChild(label);

    const badge = createSeriesBadge(file);
    if (badge) {
      content.appendChild(badge);
    }
    return content;
  }

  function createDetailsHeader() {
    const header = document.createElement("div");
    header.className = "browse-details-header";

    const columns = [
      ["name", t("browse.details.header.name")],
      ["type", t("browse.details.header.type")],
      ["modified", t("browse.details.header.modified")],
      ["size", t("browse.details.header.size")],
    ];

    columns.forEach(([name, label]) => {
      const cell = document.createElement("span");
      cell.className = `browse-details-header-cell browse-details-col-${name}`;
      cell.textContent = label;
      header.appendChild(cell);
    });

    return header;
  }

  function createDetailsRowContent(file) {
    const content = document.createElement("span");
    content.className = "browse-item-content browse-item-content-details";

    const nameCell = document.createElement("span");
    nameCell.className = "browse-details-cell browse-details-col-name";
    const label = document.createElement("span");
    label.className = "browse-item-label";
    label.textContent = file.name;
    nameCell.appendChild(label);
    const badge = createSeriesBadge(file);
    if (badge) {
      nameCell.appendChild(badge);
    }
    content.appendChild(nameCell);

    const typeCell = document.createElement("span");
    typeCell.className = "browse-details-cell browse-details-col-type";
    typeCell.textContent = formatFileTypeLabel(file);
    content.appendChild(typeCell);

    const modifiedCell = document.createElement("span");
    modifiedCell.className = "browse-details-cell browse-details-col-modified";
    modifiedCell.textContent = formatFileModified(file.mtime);
    content.appendChild(modifiedCell);

    const sizeCell = document.createElement("span");
    sizeCell.className = "browse-details-cell browse-details-col-size";
    sizeCell.textContent = formatFileSize(file.sizeBytes);
    content.appendChild(sizeCell);

    return content;
  }

  function filterBrowseItems() {
    const query = normalizeSearchQuery(state.searchQuery);
    state.folders = !query
      ? [...state.allFolders]
      : state.allFolders.filter((folder) => matchesBrowseQuery(folder.name, query));
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
    rootBtn.addEventListener("click", () => {
      clearSearchQuery({ rerender: false });
      loadAndRenderBrowser("").catch((err) => console.error(err));
    });
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
      btn.addEventListener("click", () => {
        clearSearchQuery({ rerender: false });
        loadAndRenderBrowser(segment.path).catch((err) => console.error(err));
      });
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
      btn.title = folder.name;
      btn.textContent = folder.name;
      btn.addEventListener("click", () => selectFolderIndex(index, { focus: true }));
      btn.addEventListener("dblclick", () => {
        clearSearchQuery({ rerender: false });
        loadAndRenderBrowser(folder.path).catch((err) => console.error(err));
      });
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

    if (state.viewMode === "details") {
      browseFilesList.appendChild(createDetailsHeader());
    }

    state.fileItems.forEach((file, index) => {
      const btn = document.createElement("button");
      btn.className = `browse-item${state.viewMode === "details" ? " browse-item-details" : ""}`;
      btn.type = "button";
      btn.dataset.browsePane = "files";
      btn.dataset.browseIndex = String(index);
      btn.title = file.name;
      btn.appendChild(state.viewMode === "details" ? createDetailsRowContent(file) : createListRowContent(file));
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
    writeStoredValue(STORAGE_KEYS.browseLastPath, state.currentPath);
    state.parentPath = String(data.parentPath ?? deriveParentPath(state.currentPath));
    state.canGoUp = Boolean(data.canGoUp ?? state.parentPath);
    state.allFolders = Array.isArray(data.folders)
      ? data.folders.map((folder) => ({
        name: String(folder || ""),
        path: joinBrowsePath(state.currentPath, folder || ""),
      }))
      : [];
    state.rawFileItems = normalizeFileItems(data);
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
    renderBrowseLists();
    syncBrowseSortState();
    syncFormatControl();
    syncSeriesControl();
    syncSearchControl();
    syncBrowseViewState();
    syncBrowseUpState();
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
        setBrowseStatus("");
      } else {
        setBrowseStatus(t("file_browser.failed_load"), { isError: true });
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
    state.mode = mode;
    state.inputElement = inputElement;
    state.currentPath = initialPath;
    state.parentPath = "";
    state.canGoUp = false;
    clearBrowseSelection();
    clearSearchQuery({ rerender: false });
    resetBrowseFilters("");
    updateBrowseTitle();
    openModal(browseModal, { focusTarget: browseCloseBtn || browseSelectBtn || browsePathInput });
    setBrowseModalBusy(true, t("file_browser.loading", { label: initialPath || t("file_browser.root") }));
    loadAndRenderBrowser(initialPath).catch((err) => console.error(err));
  }

  function openFileDialog(options = {}) {
    const exts = typeof options === "object" && options !== null ? String(options.exts || "") : "";
    return new Promise((resolve, reject) => {
      const initialPath = readStoredValue(STORAGE_KEYS.browseLastPath) || "";
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
    clearSearchQuery({ rerender: false });
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

  browseSelectBtn?.addEventListener("click", () => {
    confirmBrowseSelection();
  });

  browseCancelBtn?.addEventListener("click", () => closeFileBrowser());
  browseCloseBtn?.addEventListener("click", () => closeFileBrowser());
  browseUpBtn?.addEventListener("click", () => {
    navigateUp();
  });

  browseSearchInput?.addEventListener("input", () => {
    setSearchQuery(browseSearchInput.value);
  });

  browseSearchClearBtn?.addEventListener("click", () => {
    clearSearchQuery({ focusInput: true });
  });

  browseFormatSelect?.addEventListener("change", () => {
    state.activeFormat = browseFormatSelect.value || FORMAT_ALL;
    persistBrowseControlPreferences();
    syncSeriesControl();
    loadAndRenderBrowser(state.currentPath).catch((err) => console.error(err));
  });

  browseSortSelect?.addEventListener("change", () => {
    state.sort = browseSortSelect.value || DEFAULT_SORT;
    persistBrowseControlPreferences();
    loadAndRenderBrowser(state.currentPath).catch((err) => console.error(err));
  });

  browseSeriesModeSelect?.addEventListener("change", () => {
    if (browseSeriesModeSelect.disabled) return;
    state.seriesMode = browseSeriesModeSelect.value || DEFAULT_SERIES_MODE;
    persistBrowseControlPreferences();
    loadAndRenderBrowser(state.currentPath).catch((err) => console.error(err));
  });

  browseViewModeSelect?.addEventListener("change", () => {
    state.viewMode = VALID_VIEW_MODES.has(browseViewModeSelect.value) ? browseViewModeSelect.value : DEFAULT_VIEW_MODE;
    persistBrowseControlPreferences();
    renderBrowseLists({ preserveSelection: true });
  });

  browseSplitter?.addEventListener("mousedown", (event) => {
    if (browseModalBusy || isStackedBrowseLayout()) {
      return;
    }
    event.preventDefault();
    const startX = event.clientX;
    const startWidth = state.paneWidth;

    function onMove(nextEvent) {
      state.paneWidth = clampBrowsePaneWidth(startWidth + (nextEvent.clientX - startX));
      applyBrowsePaneWidth();
    }

    function onUp() {
      persistBrowsePaneWidth();
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
      document.body.style.cursor = "";
    }

    document.body.style.cursor = "col-resize";
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  });

  browseModal?.addEventListener("keydown", (event) => {
    if (!browseModal.classList.contains("is-open")) {
      return;
    }

    if (event.key === "Escape" && document.activeElement === browseSearchInput && state.searchQuery) {
      clearSearchQuery({ focusInput: true });
      event.preventDefault();
      return;
    }

    if (
      (event.key === "/" && !event.metaKey && !event.ctrlKey && !event.altKey)
      || (event.key.toLowerCase() === "f" && event.ctrlKey)
    ) {
      browseSearchInput?.focus();
      browseSearchInput?.select();
      event.preventDefault();
      return;
    }

    if (shouldIgnoreBrowseShortcuts(event.target)) {
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

  window.addEventListener("resize", () => {
    applyBrowsePaneWidth();
  });

  return {
    isBackendLocal,
    openFileBrowser,
    openFileDialog,
    closeFileBrowser,
    restoreFilesystemMode,
  };
}
