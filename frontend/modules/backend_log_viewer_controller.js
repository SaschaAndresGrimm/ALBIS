/**
 * Backend log viewer modal controller.
 */

import { fetchJSONWithInit } from "./http.js";
import { t } from "./i18n.js";

const DEFAULT_LINE_COUNT = 500;
const POLL_INTERVAL_MS = 3000;

function normalizeLineCount(raw) {
  const value = Number(raw);
  if (value === 200 || value === 500 || value === 1000) {
    return value;
  }
  return DEFAULT_LINE_COUNT;
}

function formatModifiedAt(raw) {
  const timestamp = Number(raw);
  if (!Number.isFinite(timestamp) || timestamp <= 0) return "-";
  const date = new Date(timestamp * 1000);
  try {
    return new Intl.DateTimeFormat(undefined, {
      dateStyle: "medium",
      timeStyle: "medium",
    }).format(date);
  } catch {
    return date.toLocaleString();
  }
}

function normalizePayload(payload, lineCount) {
  return {
    path: String(payload?.path || ""),
    text: String(payload?.text || ""),
    requested_lines: normalizeLineCount(payload?.requested_lines ?? lineCount),
    returned_lines: Math.max(0, Number(payload?.returned_lines || 0)),
    truncated: Boolean(payload?.truncated),
    size_bytes: Math.max(0, Number(payload?.size_bytes || 0)),
    modified_at: Number.isFinite(Number(payload?.modified_at)) ? Number(payload.modified_at) : null,
  };
}

export function createBackendLogViewerController({
  apiBase,
  backendIsLocal = false,
  elements,
  callbacks,
}) {
  const {
    logViewerModal,
    logViewerCloseIcon,
    logViewerPathValue,
    logViewerUpdatedValue,
    logViewerMessage,
    logViewerLineCount,
    logViewerRefresh,
    logViewerFollow,
    logViewerContent,
    logViewerOpenHost,
    logViewerDownload,
    logViewerClose,
  } = elements;

  const {
    openModal,
    closeModal,
    setStatus,
  } = callbacks;

  let lineCount = DEFAULT_LINE_COUNT;
  let follow = true;
  let modalBusy = false;
  let modalMessage = "";
  let modalMessageState = "";
  let pollTimer = null;
  let requestSerial = 0;
  let payload = normalizePayload({}, DEFAULT_LINE_COUNT);

  function isOpen() {
    return Boolean(logViewerModal?.classList.contains("is-open"));
  }

  function clearPoll() {
    if (pollTimer) {
      window.clearTimeout(pollTimer);
      pollTimer = null;
    }
  }

  function schedulePoll() {
    clearPoll();
    if (!isOpen() || !follow) return;
    pollTimer = window.setTimeout(() => {
      void loadTail();
    }, POLL_INTERVAL_MS);
  }

  function setBusy(isBusy) {
    modalBusy = Boolean(isBusy);
    logViewerModal?.setAttribute("aria-busy", modalBusy ? "true" : "false");
    if (logViewerRefresh) {
      logViewerRefresh.disabled = modalBusy;
    }
    if (logViewerOpenHost) {
      logViewerOpenHost.disabled = modalBusy;
    }
  }

  function render() {
    if (logViewerLineCount) {
      logViewerLineCount.value = String(lineCount);
    }
    if (logViewerFollow) {
      logViewerFollow.checked = follow;
    }
    if (logViewerPathValue) {
      logViewerPathValue.textContent = payload.path || "-";
    }
    if (logViewerUpdatedValue) {
      logViewerUpdatedValue.textContent = formatModifiedAt(payload.modified_at);
    }
    if (logViewerMessage) {
      logViewerMessage.textContent = modalMessage;
      logViewerMessage.hidden = !modalMessage;
      logViewerMessage.classList.toggle("is-loading", modalMessageState === "loading");
      logViewerMessage.classList.toggle("is-error", modalMessageState === "error");
    }
    if (logViewerContent) {
      logViewerContent.textContent = payload.text || t("log_viewer.empty");
      logViewerContent.classList.toggle("is-empty", !payload.text);
    }
    if (logViewerOpenHost) {
      logViewerOpenHost.hidden = !backendIsLocal;
    }
    if (logViewerDownload && typeof logViewerDownload.setAttribute === "function") {
      logViewerDownload.setAttribute("href", `${apiBase}/log-file`);
    }
  }

  function scrollToBottom() {
    if (!(logViewerContent instanceof HTMLElement)) return;
    logViewerContent.scrollTop = logViewerContent.scrollHeight;
  }

  async function loadTail() {
    requestSerial += 1;
    const activeRequest = requestSerial;
    modalMessage = t("log_viewer.loading");
    modalMessageState = "loading";
    setBusy(true);
    render();

    try {
      const params = new URLSearchParams({ lines: String(lineCount) });
      const nextPayload = await fetchJSONWithInit(`${apiBase}/log-tail?${params.toString()}`, {
        cache: "no-store",
      });
      if (activeRequest !== requestSerial) return;
      payload = normalizePayload(nextPayload, lineCount);
      modalMessage = "";
      modalMessageState = "";
      render();
      if (follow) {
        window.requestAnimationFrame(() => {
          scrollToBottom();
        });
      }
    } catch (err) {
      if (activeRequest !== requestSerial) return;
      const detail = err instanceof Error ? err.message : t("log_viewer.error_unknown");
      modalMessage = t("log_viewer.load_failed", { detail });
      modalMessageState = "error";
      render();
    } finally {
      if (activeRequest === requestSerial) {
        setBusy(false);
        render();
        schedulePoll();
      }
    }
  }

  async function openOnHost() {
    modalMessage = t("log_viewer.opening_host");
    modalMessageState = "loading";
    setBusy(true);
    render();

    try {
      const nextPayload = await fetchJSONWithInit(`${apiBase}/open-log`, { method: "POST" });
      if (nextPayload?.opened === false) {
        modalMessage = t("log_viewer.host_open_failed");
        modalMessageState = "error";
      } else {
        modalMessage = t("log_viewer.host_opened");
        modalMessageState = "";
        setStatus?.(t("status.log.opened_file"));
      }
    } catch {
      modalMessage = t("log_viewer.host_open_failed");
      modalMessageState = "error";
    } finally {
      setBusy(false);
      render();
      schedulePoll();
    }
  }

  function close({ restoreFocus = true } = {}) {
    clearPoll();
    requestSerial += 1;
    setBusy(false);
    return closeModal(logViewerModal, { restoreFocus });
  }

  function open() {
    clearPoll();
    requestSerial += 1;
    lineCount = DEFAULT_LINE_COUNT;
    follow = true;
    payload = normalizePayload({}, DEFAULT_LINE_COUNT);
    modalMessage = "";
    modalMessageState = "";
    setBusy(false);
    render();
    openModal(logViewerModal, {
      focusTarget: logViewerRefresh || logViewerCloseIcon || logViewerClose,
    });
    void loadTail();
  }

  function refreshUi() {
    render();
  }

  logViewerCloseIcon?.addEventListener("click", () => {
    close();
  });
  logViewerClose?.addEventListener("click", () => {
    close();
  });
  logViewerRefresh?.addEventListener("click", () => {
    void loadTail();
  });
  logViewerOpenHost?.addEventListener("click", () => {
    void openOnHost();
  });
  logViewerLineCount?.addEventListener("change", () => {
    lineCount = normalizeLineCount(logViewerLineCount.value);
    void loadTail();
  });
  logViewerFollow?.addEventListener("change", () => {
    follow = Boolean(logViewerFollow.checked);
    render();
    if (follow) {
      void loadTail();
    } else {
      clearPoll();
    }
  });
  logViewerModal?.addEventListener("click", (event) => {
    if (event.target === logViewerModal || event.target.classList?.contains("modal-backdrop")) {
      close();
    }
  });

  render();

  return {
    close,
    isOpen,
    open,
    refreshUi,
  };
}
