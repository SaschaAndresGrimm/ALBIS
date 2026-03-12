/**
 * File browser modal controller for remote filesystem browsing.
 *
 * Keeps modal state, browse requests, and dialog behavior encapsulated so
 * app.js can treat it as a small integration surface.
 */

import { t } from "./i18n.js";

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

export function createFileBrowserController({
  apiBase,
  browseModal,
  browseBreadcrumb,
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
    selectedPath: "",
    selectedType: "",
    mode: null,
    inputElement: null,
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
    return true;
  }

  function syncBrowseSelectState() {
    if (!browseSelectBtn) return;
    browseSelectBtn.disabled = browseModalBusy || !canConfirmBrowseSelection();
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
    syncBrowseSelectState();
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

  async function loadBrowseDirectory(path) {
    try {
      const query = path ? `?path=${encodeURIComponent(path)}` : "";
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

    state.currentPath = data.currentPath || "";
    if (state.mode === "file-open") {
      state.selectedPath = "";
      state.selectedType = "";
      browsePathInput.value = state.currentPath;
    } else {
      state.selectedPath = state.currentPath;
      state.selectedType = "folder";
      browsePathInput.value = state.selectedPath;
    }

    browseBreadcrumb.innerHTML = "";
    const rootBtn = document.createElement("button");
    rootBtn.className = "breadcrumb-btn";
    rootBtn.textContent = t("file_browser.root");
    rootBtn.dataset.path = "";
    if (data.currentPath === "") {
      rootBtn.classList.add("is-active");
    }
    rootBtn.addEventListener("click", () => loadAndRenderBrowser(""));
    browseBreadcrumb.appendChild(rootBtn);

    if (data.currentPath && data.currentPath !== "") {
      const parts = data.currentPath.split("/");
      let accumulated = "";
      for (const part of parts) {
        accumulated = accumulated ? `${accumulated}/${part}` : part;
        const btn = document.createElement("button");
        btn.className = "breadcrumb-btn";
        btn.textContent = part;
        btn.dataset.path = accumulated;
        if (accumulated === data.currentPath) {
          btn.classList.add("is-active");
        }
        btn.addEventListener("click", () => loadAndRenderBrowser(accumulated));
        browseBreadcrumb.appendChild(btn);
      }
    }

    browseFoldersList.innerHTML = "";
    if (data.folders && data.folders.length > 0) {
      for (const folder of data.folders) {
        const btn = document.createElement("button");
        btn.className = "browse-item";
        btn.textContent = folder;
        btn.addEventListener("click", () => {
          const newPath = state.currentPath ? `${state.currentPath}/${folder}` : folder;
          loadAndRenderBrowser(newPath);
        });
        browseFoldersList.appendChild(btn);
      }
    } else {
      const empty = document.createElement("div");
      empty.className = "browse-empty";
      empty.textContent = t("file_browser.no_folders");
      browseFoldersList.appendChild(empty);
    }

    browseFilesList.innerHTML = "";
    if (data.files && data.files.length > 0) {
      for (const file of data.files) {
        const btn = document.createElement("button");
        btn.className = "browse-item";
        btn.textContent = file;
        btn.addEventListener("click", () => {
          state.selectedPath = state.currentPath ? `${state.currentPath}/${file}` : file;
          state.selectedType = "file";
          browsePathInput.value = state.selectedPath;
          document.querySelectorAll(".browse-item.is-selected").forEach((el) => {
            el.classList.remove("is-selected");
          });
          btn.classList.add("is-selected");
          syncBrowseSelectState();
        });
        btn.addEventListener("dblclick", () => {
          state.selectedPath = state.currentPath ? `${state.currentPath}/${file}` : file;
          state.selectedType = "file";
          browsePathInput.value = state.selectedPath;
          document.querySelectorAll(".browse-item.is-selected").forEach((el) => {
            el.classList.remove("is-selected");
          });
          btn.classList.add("is-selected");
          syncBrowseSelectState();
          if (state.mode === "file-open") {
            confirmBrowseSelection();
          }
        });
        browseFilesList.appendChild(btn);
      }
    } else {
      const empty = document.createElement("div");
      empty.className = "browse-empty";
      empty.textContent = t("file_browser.no_images");
      browseFilesList.appendChild(empty);
    }

    syncBrowseSelectState();
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

  function openFileBrowser(mode, inputElement) {
    state.mode = mode;
    state.inputElement = inputElement;
    state.currentPath = "";
    state.selectedPath = "";
    state.selectedType = "";
    openModal(browseModal, { focusTarget: browseCloseBtn || browseSelectBtn || browsePathInput });
    setBrowseModalBusy(true, t("file_browser.loading", { label: t("file_browser.root") }));
    loadAndRenderBrowser("").catch((err) => console.error(err));
  }

  function openFileDialog() {
    return new Promise((resolve, reject) => {
      settleFileDialog("");
      fileDialogPromise = { resolve, reject };
      state.mode = "file-open";
      state.inputElement = null;
      state.currentPath = "";
      state.selectedPath = "";
      state.selectedType = "";
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
    setBrowseModalBusy(false);
    setBrowseStatus("");
    closeModal(browseModal, { restoreFocus });
    if (cancelDialog && state.mode === "file-open") {
      settleFileDialog("");
    }
  }

  function confirmBrowseSelection() {
    const selected = state.selectedPath;

    if (state.mode === "file-open") {
      if (!selected || state.selectedType !== "file") {
        setStatus(t("status.file.select_image_first"));
        return false;
      }
      closeFileBrowser({ cancelDialog: false });
      settleFileDialog(selected);
      return true;
    }

    if (!state.inputElement) {
      closeFileBrowser();
      return true;
    }

    if (!selected) {
      setStatus(t("status.file.no_selection"));
      return false;
    }

    if (typeof onPathSelected === "function") {
      try {
        onPathSelected({ mode: state.mode, selectedPath: selected, inputElement: state.inputElement });
      } catch (err) {
        console.error(err);
      }
    }

    closeFileBrowser();
    return true;
  }

  browseSelectBtn?.addEventListener("click", () => {
    confirmBrowseSelection();
  });

  browseCancelBtn?.addEventListener("click", () => closeFileBrowser());
  browseCloseBtn?.addEventListener("click", () => closeFileBrowser());

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
