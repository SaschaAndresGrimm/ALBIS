/**
 * File browser list rendering: the breadcrumb, the folder list and the file
 * list (List or Details view, with the sortable Details header). Owns no
 * state of its own; reads the controller's state and calls back into it.
 */

import { t } from "./i18n.js";
import {
  COLUMN_SORTS,
  EMPTY_VALUE,
  SORT_COLUMN,
  buildBreadcrumbSegments,
  fileTypeLabelKey,
  formatFileSize,
  inferFileExt,
} from "./file_browser_utils.js";

export function createFileBrowserRenderer({
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
}) {
  const dateTimeFormatter = (() => {
    try {
      return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" });
    } catch {
      return new Intl.DateTimeFormat();
    }
  })();

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

    const activeColumn = SORT_COLUMN[state.sort];
    const activeDesc = state.sort.endsWith("_desc");

    columns.forEach(([name, label]) => {
      const cell = document.createElement("button");
      cell.type = "button";
      cell.className = `browse-details-header-cell browse-details-col-${name}`;
      cell.dataset.sortColumn = name;
      const isActive = name === activeColumn;
      cell.setAttribute("aria-sort", isActive ? (activeDesc ? "descending" : "ascending") : "none");
      cell.disabled = !isSortModeAvailable(COLUMN_SORTS[name][0]);

      const labelSpan = document.createElement("span");
      labelSpan.className = "browse-details-header-label";
      labelSpan.textContent = label;
      cell.appendChild(labelSpan);

      if (isActive) {
        const caret = document.createElement("span");
        caret.className = "browse-sort-caret";
        caret.setAttribute("aria-hidden", "true");
        caret.textContent = activeDesc ? "▾" : "▴";
        cell.appendChild(caret);
      }

      cell.addEventListener("click", () => {
        if (cell.disabled) return;
        applySort(sortForColumnClick(name));
      });
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
    if (isFolderSelectMode()) {
      return;
    }
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

  return { renderBreadcrumb, renderFolders, renderFiles };
}
