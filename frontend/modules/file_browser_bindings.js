/**
 * File browser modal listeners: buttons, search, typed path, filter selects,
 * the pane splitter, keyboard navigation, the backdrop, the filesystem-mode
 * select and window resize. Forwards to the controller's functions.
 */

import { DEFAULT_SERIES_MODE, DEFAULT_SORT, DEFAULT_VIEW_MODE, FORMAT_ALL, VALID_VIEW_MODES } from "./file_browser_utils.js";

function shouldIgnoreBrowseShortcuts(target) {
  if (!(target instanceof HTMLElement)) return false;
  if (target.closest(".browse-item")) return false;
  if (target.tagName === "SELECT") return true;
  if (target.tagName === "TEXTAREA") return true;
  if (target.tagName === "INPUT" && !target.readOnly) return true;
  if (target.tagName === "BUTTON") return true;
  return false;
}

export function bindFileBrowserEvents({
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
  isBrowseModalBusy,
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
}) {
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

  browsePathInput?.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      submitTypedPath();
    }
  });

  browsePathInput?.addEventListener("blur", () => {
    // Discard any uncommitted edits so the field reflects the actual location.
    updatePathInput();
  });

  browseFormatSelect?.addEventListener("change", () => {
    state.activeFormat = browseFormatSelect.value || FORMAT_ALL;
    persistBrowseControlPreferences();
    syncSeriesControl();
    loadAndRenderBrowser(state.currentPath).catch((err) => console.error(err));
  });

  browseSortSelect?.addEventListener("change", () => {
    applySort(browseSortSelect.value || DEFAULT_SORT);
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
    if (isBrowseModalBusy() || isStackedBrowseLayout()) {
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

    const target = event.target;
    const inEditableField = target instanceof HTMLElement
      && (target.tagName === "TEXTAREA" || (target.tagName === "INPUT" && !target.readOnly));
    if (
      (event.key === "/" && !event.metaKey && !event.ctrlKey && !event.altKey && !inEditableField)
      || (event.key.toLowerCase() === "f" && event.ctrlKey)
    ) {
      browseSearchInput?.focus();
      browseSearchInput?.select();
      event.preventDefault();
      return;
    }

    if (event.key === "Enter" && event.target === browseSearchInput) {
      if (handleSearchEnter()) event.preventDefault();
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
}
