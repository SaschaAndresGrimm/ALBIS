/**
 * Manual release-check modal controller.
 */

import { fetchJSONWithInit } from "./http.js";
import { t } from "./i18n.js";

const FALLBACK_RELEASES_URL = "https://github.com/SaschaAndresGrimm/ALBIS/releases";

export function createUpdateCheckController({
  apiBase,
  state,
  elements,
  callbacks,
}) {
  const {
    updateCheckModal,
    updateCheckCloseIcon,
    updateCheckMessage,
    updateCheckDetail,
    updateCheckCurrentVersionValue,
    updateCheckLatestRow,
    updateCheckLatestVersionValue,
    updateCheckAction,
    updateCheckClose,
  } = elements;

  const {
    openModal,
    closeModal,
  } = callbacks;

  let modalState = "idle";
  let requestSerial = 0;
  let latestPayload = {
    status: "unavailable",
    current_version: "",
    latest_version: "",
    release_url: FALLBACK_RELEASES_URL,
    message: "",
  };

  function getCurrentVersion() {
    return String(latestPayload.current_version || state.backendVersion || "-");
  }

  function setModalBusy(isBusy) {
    updateCheckModal?.setAttribute("aria-busy", isBusy ? "true" : "false");
    if (updateCheckAction) {
      updateCheckAction.disabled = Boolean(isBusy);
    }
  }

  function normalizePayload(payload) {
    const status = String(payload?.status || "unavailable");
    return {
      status: ["update_available", "up_to_date", "unavailable"].includes(status) ? status : "unavailable",
      current_version: String(payload?.current_version || state.backendVersion || ""),
      latest_version: String(payload?.latest_version || ""),
      release_url: String(payload?.release_url || FALLBACK_RELEASES_URL),
      message: String(payload?.message || ""),
    };
  }

  function render() {
    if (updateCheckCurrentVersionValue) {
      updateCheckCurrentVersionValue.textContent = getCurrentVersion();
    }
    if (updateCheckLatestVersionValue) {
      updateCheckLatestVersionValue.textContent = latestPayload.latest_version || "";
    }
    if (updateCheckLatestRow) {
      updateCheckLatestRow.hidden = modalState === "loading" || !latestPayload.latest_version;
    }
    if (updateCheckDetail) {
      updateCheckDetail.textContent = latestPayload.message || "";
      updateCheckDetail.hidden = !latestPayload.message;
    }
    if (updateCheckAction) {
      updateCheckAction.hidden = modalState === "loading";
      updateCheckAction.textContent = modalState === "update_available"
        ? t("update_check.action.open_release_page")
        : t("update_check.action.view_releases");
    }
    if (!updateCheckMessage) return;

    updateCheckMessage.classList.toggle("is-loading", modalState === "loading");
    updateCheckMessage.classList.toggle("is-error", modalState === "unavailable");

    if (modalState === "loading") {
      updateCheckMessage.textContent = t("update_check.loading");
      setModalBusy(true);
      return;
    }
    if (modalState === "update_available") {
      updateCheckMessage.textContent = t("update_check.status.update_available");
      setModalBusy(false);
      return;
    }
    if (modalState === "up_to_date") {
      updateCheckMessage.textContent = t("update_check.status.up_to_date");
      setModalBusy(false);
      return;
    }
    updateCheckMessage.textContent = t("update_check.status.unavailable");
    setModalBusy(false);
  }

  function openReleasePage() {
    const releaseUrl = latestPayload.release_url || FALLBACK_RELEASES_URL;
    window.open(releaseUrl, "_blank", "noopener");
  }

  function close({ restoreFocus = true } = {}) {
    setModalBusy(false);
    return closeModal(updateCheckModal, { restoreFocus });
  }

  async function openAndCheck() {
    requestSerial += 1;
    const activeRequest = requestSerial;
    modalState = "loading";
    latestPayload = {
      status: "unavailable",
      current_version: state.backendVersion || "",
      latest_version: "",
      release_url: FALLBACK_RELEASES_URL,
      message: "",
    };
    render();
    openModal(updateCheckModal, { focusTarget: updateCheckCloseIcon || updateCheckClose || updateCheckAction });

    try {
      const payload = await fetchJSONWithInit(`${apiBase}/update-check`, { cache: "no-store" });
      if (activeRequest !== requestSerial) return;
      latestPayload = normalizePayload(payload);
      modalState = latestPayload.status;
    } catch (err) {
      console.warn("Update check request failed", err);
      if (activeRequest !== requestSerial) return;
      latestPayload = {
        status: "unavailable",
        current_version: state.backendVersion || "",
        latest_version: "",
        release_url: FALLBACK_RELEASES_URL,
        message: "",
      };
      modalState = "unavailable";
    }

    render();
  }

  function refreshUi() {
    if (modalState === "idle") return;
    render();
  }

  updateCheckCloseIcon?.addEventListener("click", () => {
    close();
  });
  updateCheckClose?.addEventListener("click", () => {
    close();
  });
  updateCheckAction?.addEventListener("click", () => {
    openReleasePage();
  });
  updateCheckModal?.addEventListener("click", (event) => {
    if (event.target === updateCheckModal || event.target.classList?.contains("modal-backdrop")) {
      close();
    }
  });

  return {
    close,
    openAndCheck,
    refreshUi,
  };
}
