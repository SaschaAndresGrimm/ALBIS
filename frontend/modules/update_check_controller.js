/**
 * Release-check modal controller.
 *
 * Telling someone a newer ALBIS exists is only useful if it also tells them
 * what to do about it, and that depends entirely on how their copy was
 * installed: an AppImage is one file to replace, Windows has an installer and
 * a portable zip that must not be confused for each other, macOS has a disk
 * image per architecture, and a container updates with `docker pull` and
 * cannot be updated from inside at all. The backend resolves that into an
 * `install_kind` plus either one asset or one command, and this renders it --
 * so the dialog offers the file the user needs instead of a page listing nine
 * of them.
 *
 * Where the backend can fetch the asset itself (`download_supported`), the
 * primary button does that instead of handing the link to the browser -- and
 * the point is not convenience, it is that nobody verifies a browser download.
 * ALBIS checks the file against the release's own checksum list and says so.
 * Applying it is a further, separate step (`apply_supported`), off unless the
 * site turned it on, only offered where the platform makes it contained, and
 * refused over live work -- a viewer that replaces itself mid-experiment is a
 * hazard, not a feature. Where it is not offered, the last step stays showing
 * the user the folder the file landed in.
 */

import { fetchJSONWithInit } from "./http.js";
import { t } from "./i18n.js";

const FALLBACK_RELEASES_URL = "https://github.com/SaschaAndresGrimm/ALBIS/releases";

// A download link from this dialog goes to `window.open`. The URL arrives in a
// JSON body, and the backend already refuses anything that is not a GitHub
// release download; this repeats the check rather than trusting that it ran,
// because the cost is one comparison and the failure would be a click.
const ALLOWED_DOWNLOAD_PREFIX = "https://github.com/";

// One line of guidance per install kind. An `install_kind` not listed here --
// an older or newer backend than this interface -- simply shows no instruction
// and keeps the releases-page button, which is what the dialog did before.
const INSTRUCTION_KEYS = {
  appimage: "update_check.instruction.appimage",
  windows_installer: "update_check.instruction.windows_installer",
  windows_portable: "update_check.instruction.windows_portable",
  macos_app: "update_check.instruction.macos_app",
  docker: "update_check.instruction.docker",
  source: "update_check.instruction.source",
};

const COPY_FEEDBACK_MS = 1500;

// How often the dialog asks the backend how the download is going. Half a
// second is frequent enough for a progress bar to look continuous and slow
// enough to be free next to the transfer it is reporting on.
const DOWNLOAD_POLL_MS = 500;

const RUNNING_DOWNLOAD_STATES = ["downloading", "verifying"];

function formatBytes(bytes) {
  if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  if (bytes >= 1024) return `${Math.round(bytes / 1024)} KB`;
  return `${Math.round(bytes)} B`;
}

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
    updateCheckInstruction,
    updateCheckDownloadRow,
    updateCheckDownloadName,
    updateCheckCommandRow,
    updateCheckCommand,
    updateCheckCommandCopy,
    updateCheckProgress,
    updateCheckProgressFill,
    updateCheckProgressText,
    updateCheckVerify,
    updateCheckApplyNote,
    updateCheckCancel,
    updateCheckReveal,
    updateCheckReleaseNotes,
    updateCheckAction,
    updateCheckClose,
  } = elements;

  const {
    openModal,
    closeModal,
    // What the interface has in progress that closing ALBIS would abandon.
    // Supplied by the caller because the session lives in the browser: which
    // file is open and whether a watch is running are not facts this module
    // or the backend holds.
    getApplyBlockers,
  } = callbacks;

  let modalState = "idle";
  let requestSerial = 0;
  let copyFeedbackTimer = null;
  let pollTimer = null;
  // What the primary button does right now. Resolved in render() so the click
  // handler never has to work it out a second time and reach a different
  // answer than the label the user read.
  let primaryIntent = "release_page";
  let latestPayload = emptyPayload();
  // The backend's view of the download it is running, or null when it has
  // none. Kept separate from `latestPayload` because it outlives the dialog:
  // closing the modal stops the polling, not the transfer.
  let downloadStatus = null;
  // The backend's view of applying: its own refusal reasons plus, once
  // started, how far it got. Null until asked.
  let applyStatus = null;

  function emptyPayload() {
    return {
      status: "unavailable",
      current_version: "",
      latest_version: "",
      release_url: FALLBACK_RELEASES_URL,
      message: "",
      install_kind: "",
      download_url: "",
      download_name: "",
      update_command: "",
      download_supported: false,
      apply_supported: false,
    };
  }

  function getCurrentVersion() {
    return String(latestPayload.current_version || state.backendVersion || "-");
  }

  function setModalBusy(isBusy) {
    updateCheckModal?.setAttribute("aria-busy", isBusy ? "true" : "false");
    if (updateCheckAction) {
      updateCheckAction.disabled = Boolean(isBusy);
    }
  }

  function sanitizeDownloadUrl(raw) {
    const url = String(raw || "");
    return url.startsWith(ALLOWED_DOWNLOAD_PREFIX) ? url : "";
  }

  function normalizePayload(payload) {
    const status = String(payload?.status || "unavailable");
    return {
      status: ["update_available", "up_to_date", "unavailable"].includes(status) ? status : "unavailable",
      current_version: String(payload?.current_version || state.backendVersion || ""),
      latest_version: String(payload?.latest_version || ""),
      release_url: String(payload?.release_url || FALLBACK_RELEASES_URL),
      message: String(payload?.message || ""),
      install_kind: String(payload?.install_kind || ""),
      // A URL without a filename to show, or a filename without a URL, is not
      // an offer anyone can act on -- both halves or neither.
      download_url: sanitizeDownloadUrl(payload?.download_url),
      download_name: String(payload?.download_name || ""),
      update_command: String(payload?.update_command || ""),
      download_supported: Boolean(payload?.download_supported),
      apply_supported: Boolean(payload?.apply_supported),
    };
  }

  function normalizeApplyStatus(payload) {
    const status = String(payload?.status || "idle");
    return {
      status: ["idle", "applying", "applied", "failed"].includes(status) ? status : "idle",
      message: String(payload?.message || ""),
      refusal: String(payload?.refusal || ""),
    };
  }

  function applyBlockers() {
    try {
      const blockers = getApplyBlockers?.() || [];
      return Array.isArray(blockers) ? blockers.filter(Boolean) : [];
    } catch {
      // A caller that cannot answer must not be read as "nothing is running":
      // the safe answer is that something might be.
      return ["unknown"];
    }
  }

  function isApplyRunning() {
    return applyStatus?.status === "applying";
  }

  /**
   * Whether the Install button should be offered right now.
   *
   * Three things have to agree: the build allows it at all, the backend has no
   * reason to refuse, and the interface has no live work of its own. The last
   * one is checked here rather than sent ahead of time, because it changes
   * while the dialog is open.
   */
  function canApply() {
    if (!latestPayload.apply_supported) return false;
    if (!applyStatus || applyStatus.refusal) return false;
    if (applyStatus.status !== "idle") return false;
    return applyBlockers().length === 0;
  }

  function normalizeDownloadStatus(payload) {
    const status = String(payload?.status || "idle");
    return {
      status: ["idle", "downloading", "verifying", "ready", "failed", "cancelled"].includes(status)
        ? status
        : "idle",
      name: String(payload?.name || ""),
      path: String(payload?.path || ""),
      bytes_downloaded: Number(payload?.bytes_downloaded) || 0,
      bytes_total: Number(payload?.bytes_total) || 0,
      sha256: String(payload?.sha256 || ""),
      checksum: String(payload?.checksum || "pending"),
      signature: String(payload?.signature || "pending"),
      message: String(payload?.message || ""),
    };
  }

  /**
   * The download state worth showing, or null.
   *
   * A finished download is kept across dialog opens, which is useful -- "you
   * already have this, here it is" -- right up until a newer release appears,
   * at which point the file on disk is for the wrong version. Matching on the
   * asset name is what keeps the old one from being presented as the new one.
   */
  function activeDownload() {
    if (!downloadStatus || downloadStatus.status === "idle") return null;
    if (!latestPayload.download_name) return null;
    if (downloadStatus.name !== latestPayload.download_name) return null;
    return downloadStatus;
  }

  function isDownloadRunning() {
    const active = activeDownload();
    return Boolean(active && RUNNING_DOWNLOAD_STATES.includes(active.status));
  }

  function canDownloadInApp() {
    return Boolean(hasDownloadOffer() && latestPayload.download_supported);
  }

  function hasDownloadOffer() {
    return Boolean(
      modalState === "update_available" && latestPayload.download_url && latestPayload.download_name,
    );
  }

  function hasCommandOffer() {
    return Boolean(modalState === "update_available" && latestPayload.update_command);
  }

  function resetCopyFeedback() {
    if (copyFeedbackTimer) {
      window.clearTimeout(copyFeedbackTimer);
      copyFeedbackTimer = null;
    }
    if (updateCheckCommandCopy) {
      updateCheckCommandCopy.textContent = t("update_check.action.copy_command");
    }
  }

  function renderInstruction() {
    if (!updateCheckInstruction) return;
    // Suppressed once ALBIS is doing the install itself: telling someone to
    // replace their AppImage by hand, directly above a button that replaces
    // it for them, reads as two different instructions.
    const superseded =
      canApply() || isApplyRunning() || applyStatus?.status === "applied";
    const key =
      modalState === "update_available" && !superseded
        ? INSTRUCTION_KEYS[latestPayload.install_kind]
        : undefined;
    updateCheckInstruction.textContent = key ? t(key) : "";
    updateCheckInstruction.hidden = !key;
  }

  function renderDownloadRow() {
    const showDownload = hasDownloadOffer();
    if (updateCheckDownloadName) {
      updateCheckDownloadName.textContent = showDownload ? latestPayload.download_name : "";
    }
    if (updateCheckDownloadRow) {
      updateCheckDownloadRow.hidden = !showDownload;
    }
  }

  function renderCommandRow() {
    const showCommand = hasCommandOffer();
    if (updateCheckCommand) {
      updateCheckCommand.textContent = showCommand ? latestPayload.update_command : "";
    }
    if (updateCheckCommandRow) {
      updateCheckCommandRow.hidden = !showCommand;
    }
    if (showCommand) {
      resetCopyFeedback();
    }
  }

  function renderProgress() {
    const active = activeDownload();
    const running = Boolean(active && RUNNING_DOWNLOAD_STATES.includes(active.status));
    if (updateCheckProgress) {
      updateCheckProgress.hidden = !running;
    }
    if (!running || !updateCheckProgressFill || !updateCheckProgressText) return;

    if (active.status === "verifying") {
      updateCheckProgressFill.style.width = "100%";
      updateCheckProgressText.textContent = t("update_check.progress.verifying");
      return;
    }
    // A release without a Content-Length gets a bar it cannot fill, so it
    // reports the bytes it has instead of a percentage it would have to invent.
    if (active.bytes_total > 0) {
      const ratio = Math.min(1, Math.max(0, active.bytes_downloaded / active.bytes_total));
      updateCheckProgressFill.style.width = `${Math.round(ratio * 100)}%`;
      updateCheckProgressText.textContent = t("update_check.progress.downloading", {
        percent: Math.round(ratio * 100),
        done: formatBytes(active.bytes_downloaded),
        total: formatBytes(active.bytes_total),
      });
      return;
    }
    updateCheckProgressFill.style.width = "0%";
    updateCheckProgressText.textContent = t("update_check.progress.downloading_unsized", {
      done: formatBytes(active.bytes_downloaded),
    });
  }

  function verifyLine(text, variant) {
    const line = document.createElement("div");
    if (variant) line.className = variant;
    line.textContent = text;
    return line;
  }

  function verifyLineWithCode(text, code, variant) {
    const line = document.createElement("div");
    if (variant) line.className = variant;
    line.textContent = `${text} `;
    const mono = document.createElement("code");
    // textContent throughout: the digest and the path come from the backend,
    // and a DOM node built from text cannot be read as markup whatever is in
    // them.
    mono.textContent = code;
    line.appendChild(mono);
    return line;
  }

  function renderVerify() {
    if (!updateCheckVerify) return;
    const active = activeDownload();
    updateCheckVerify.replaceChildren();

    if (!active || RUNNING_DOWNLOAD_STATES.includes(active.status)) {
      updateCheckVerify.hidden = true;
      return;
    }

    if (active.status === "failed") {
      updateCheckVerify.appendChild(
        verifyLine(active.message || t("update_check.download.failed"), "is-warning"),
      );
      updateCheckVerify.hidden = false;
      return;
    }
    if (active.status === "cancelled") {
      updateCheckVerify.hidden = true;
      return;
    }

    if (active.checksum === "verified") {
      updateCheckVerify.appendChild(
        verifyLineWithCode(
          t("update_check.verify.checksum_ok"),
          active.sha256.slice(0, 16),
          "is-verified",
        ),
      );
    } else {
      updateCheckVerify.appendChild(
        verifyLine(active.message || t("update_check.verify.checksum_unavailable"), "is-warning"),
      );
    }

    // Reported only when it says something. An absent signature check is the
    // normal case on macOS and Windows, where the OS does the equivalent at
    // install time, and claiming "not verified" there would read as a problem.
    if (active.signature === "verified") {
      updateCheckVerify.appendChild(verifyLine(t("update_check.verify.signature_ok"), "is-verified"));
    } else if (active.signature === "invalid") {
      updateCheckVerify.appendChild(
        verifyLine(t("update_check.verify.signature_invalid"), "is-warning"),
      );
    }

    if (active.path) {
      updateCheckVerify.appendChild(verifyLineWithCode(t("update_check.verify.saved_to"), active.path));
    }
    updateCheckVerify.hidden = false;
  }

  function renderApplyNote() {
    if (!updateCheckApplyNote) return;
    const active = activeDownload();
    const ready = Boolean(active && active.status === "ready");
    let text = "";
    let warning = false;

    if (isApplyRunning()) {
      text = t("update_check.apply.applying");
    } else if (applyStatus?.status === "applied") {
      text = applyStatus.message || t("update_check.apply.applied");
    } else if (applyStatus?.status === "failed") {
      text = applyStatus.message || t("update_check.apply.failed");
      warning = true;
    } else if (ready && latestPayload.apply_supported) {
      // "Busy" can come from either side: a live watch this interface knows
      // about, or a job only the backend can see. Both say the same thing to
      // the user.
      if (applyBlockers().length > 0 || applyStatus?.refusal === "busy") {
        text = t("update_check.apply.refusal.busy");
        warning = true;
      } else if (applyStatus?.refusal === "unverified_download") {
        text = t("update_check.apply.refusal.unverified_download");
        warning = true;
      } else if (canApply()) {
        // Said before the click, not after: the window disappearing is the
        // part a user needs warning about.
        text = t("update_check.apply.note");
      }
    }

    updateCheckApplyNote.classList.toggle("is-warning", warning);
    updateCheckApplyNote.textContent = text;
    updateCheckApplyNote.hidden = !text;
  }

  /**
   * Which of the four things the primary button is, this render.
   *
   * Resolved once, here, and stored: the click handler reads the same answer
   * the label was written from, so the button can never do something other
   * than what it says.
   */
  function resolvePrimaryIntent() {
    if (modalState !== "update_available") return "release_page";
    // Applying supersedes everything: the process is about to end, and the
    // only useful button left is Close.
    if (isApplyRunning() || applyStatus?.status === "applied") return "busy";
    const active = activeDownload();
    if (active) {
      if (RUNNING_DOWNLOAD_STATES.includes(active.status)) return "busy";
      if (active.status === "ready") return canApply() ? "apply" : "reveal";
      // A failed or cancelled download leaves the release page as the way out,
      // and the user can start over from there.
      if (active.status === "failed") return "release_page";
    }
    if (canDownloadInApp()) return "download";
    // The backend will not fetch it, so the browser does -- the Tier-0 path,
    // and what `ui.allow_update_download: false` falls back to.
    if (hasDownloadOffer()) return "open_download_link";
    return "release_page";
  }

  function renderActions() {
    primaryIntent = resolvePrimaryIntent();

    if (updateCheckAction) {
      updateCheckAction.hidden = modalState === "loading" || primaryIntent === "busy";
      if (primaryIntent === "download" || primaryIntent === "open_download_link") {
        updateCheckAction.textContent = t("update_check.action.download");
      } else if (primaryIntent === "apply") {
        updateCheckAction.textContent = t("update_check.action.apply");
      } else if (primaryIntent === "reveal") {
        updateCheckAction.textContent = t("update_check.action.reveal");
      } else if (modalState === "update_available") {
        updateCheckAction.textContent = t("update_check.action.open_release_page");
      } else {
        updateCheckAction.textContent = t("update_check.action.view_releases");
      }
    }

    if (updateCheckCancel) {
      // A download can be given up on. An apply cannot: by the time it is
      // running, the file has been replaced or the installer has started.
      updateCheckCancel.hidden = !(primaryIntent === "busy" && isDownloadRunning());
      updateCheckCancel.textContent = t("common.cancel");
    }

    // The file is on disk either way, so the folder stays reachable while the
    // primary button offers the install instead.
    if (updateCheckReveal) {
      updateCheckReveal.hidden = primaryIntent !== "apply";
      updateCheckReveal.textContent = t("update_check.action.reveal");
    }

    // Only worth its own button when the primary one is not already the
    // release page, and not once ALBIS is on its way out.
    const showReleaseNotes =
      ["download", "open_download_link", "reveal"].includes(primaryIntent) ||
      (primaryIntent === "busy" && isDownloadRunning());
    if (updateCheckReleaseNotes) {
      updateCheckReleaseNotes.hidden = !showReleaseNotes;
      updateCheckReleaseNotes.textContent = t("update_check.action.release_notes");
    }
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

    renderInstruction();
    renderDownloadRow();
    renderCommandRow();
    renderProgress();
    renderVerify();
    renderApplyNote();
    renderActions();

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

  function openDownload() {
    const downloadUrl = sanitizeDownloadUrl(latestPayload.download_url);
    if (!downloadUrl) {
      openReleasePage();
      return;
    }
    window.open(downloadUrl, "_blank", "noopener");
  }

  /**
   * Copy without the Clipboard API, which needs a secure context.
   *
   * ALBIS is routinely reached over plain HTTP on a LAN, so
   * `navigator.clipboard` being absent is the ordinary case for a remote
   * session rather than an edge case -- and a `docker pull` line is exactly
   * the text someone in that session wants to paste into a shell.
   */
  function copyViaSelection(text) {
    if (typeof document === "undefined" || !document.body) return false;
    const scratch = document.createElement("textarea");
    scratch.value = text;
    scratch.setAttribute("aria-hidden", "true");
    scratch.style.position = "fixed";
    scratch.style.top = "-1000px";
    scratch.style.opacity = "0";
    document.body.appendChild(scratch);
    let copied = false;
    try {
      scratch.select();
      copied = document.execCommand?.("copy") === true;
    } catch {
      copied = false;
    } finally {
      scratch.remove();
    }
    return copied;
  }

  async function copyUpdateCommand() {
    const command = String(latestPayload.update_command || "");
    if (!command || !updateCheckCommandCopy) return false;

    let copied = false;
    try {
      if (navigator?.clipboard?.writeText) {
        await navigator.clipboard.writeText(command);
        copied = true;
      }
    } catch {
      copied = false;
    }
    if (!copied) {
      copied = copyViaSelection(command);
    }
    // When both routes refuse, the command is still on screen and selectable,
    // so the button says so rather than silently doing nothing.
    updateCheckCommandCopy.textContent = copied
      ? t("update_check.action.copied")
      : t("update_check.action.copy_unavailable");
    if (copyFeedbackTimer) window.clearTimeout(copyFeedbackTimer);
    copyFeedbackTimer = window.setTimeout(() => {
      copyFeedbackTimer = null;
      if (updateCheckCommandCopy) {
        updateCheckCommandCopy.textContent = t("update_check.action.copy_command");
      }
    }, COPY_FEEDBACK_MS);
    return copied;
  }

  function stopPolling() {
    if (pollTimer) {
      window.clearTimeout(pollTimer);
      pollTimer = null;
    }
  }

  async function refreshDownloadStatus() {
    try {
      const payload = await fetchJSONWithInit(`${apiBase}/update-download/status`, {
        cache: "no-store",
      });
      downloadStatus = normalizeDownloadStatus(payload);
    } catch {
      // The transfer runs in the backend; a failed poll says nothing about it,
      // so the last known state is kept rather than being reported as an error.
    }
    return downloadStatus;
  }

  async function refreshApplyStatus() {
    if (!latestPayload.apply_supported) return applyStatus;
    try {
      const payload = await fetchJSONWithInit(`${apiBase}/update-apply/status`, {
        cache: "no-store",
      });
      applyStatus = normalizeApplyStatus(payload);
    } catch {
      // Once the update has been applied ALBIS is shutting down, so a failed
      // poll is the expected end of this conversation rather than an error to
      // report. The last known state is kept.
    }
    return applyStatus;
  }

  function schedulePoll() {
    stopPolling();
    pollTimer = window.setTimeout(async () => {
      pollTimer = null;
      await refreshDownloadStatus();
      // Asked for whenever a download has landed: the backend's verdict on
      // whether that file may be installed is part of what the ready state
      // has to render.
      if (!isDownloadRunning()) await refreshApplyStatus();
      render();
      if (isDownloadRunning() || isApplyRunning()) schedulePoll();
    }, DOWNLOAD_POLL_MS);
  }

  async function applyUpdate() {
    if (!canApply()) return;
    applyStatus = normalizeApplyStatus({ status: "applying" });
    render();
    try {
      const payload = await fetchJSONWithInit(`${apiBase}/update-apply/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ busy: applyBlockers() }),
      });
      applyStatus = normalizeApplyStatus(payload);
    } catch (err) {
      console.warn("Update could not be applied", err);
      // A 409 carries the backend's reason code, which is authoritative: it
      // can see work this interface cannot.
      const refusal = typeof err?.detail === "string" ? err.detail : "";
      applyStatus = normalizeApplyStatus({
        status: refusal ? "idle" : "failed",
        refusal,
        message: refusal ? "" : t("update_check.apply.failed"),
      });
    }
    render();
    if (isApplyRunning()) schedulePoll();
  }

  async function startDownload() {
    if (!canDownloadInApp()) {
      openDownload();
      return;
    }
    // Shown as running before the request resolves, so a slow start does not
    // leave the button looking unclicked.
    downloadStatus = normalizeDownloadStatus({
      status: "downloading",
      name: latestPayload.download_name,
    });
    render();
    try {
      const payload = await fetchJSONWithInit(`${apiBase}/update-download/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          url: latestPayload.download_url,
          name: latestPayload.download_name,
        }),
      });
      downloadStatus = normalizeDownloadStatus(payload);
    } catch (err) {
      console.warn("Update download could not be started", err);
      downloadStatus = normalizeDownloadStatus({
        status: "failed",
        name: latestPayload.download_name,
        message: t("update_check.download.failed"),
      });
    }
    render();
    if (isDownloadRunning()) schedulePoll();
  }

  async function cancelDownload() {
    stopPolling();
    try {
      const payload = await fetchJSONWithInit(`${apiBase}/update-download/cancel`, {
        method: "POST",
      });
      downloadStatus = normalizeDownloadStatus(payload);
    } catch (err) {
      console.warn("Update download could not be cancelled", err);
    }
    render();
    // The backend acknowledges the request, not the stop: the worker notices
    // between chunks, so the state is still polled until it settles.
    if (isDownloadRunning()) schedulePoll();
  }

  async function revealDownload() {
    try {
      await fetchJSONWithInit(`${apiBase}/update-download/reveal`, { method: "POST" });
    } catch (err) {
      console.warn("Downloaded update could not be revealed", err);
      // Most likely the file was moved or deleted after it was downloaded, in
      // which case the backend has stopped offering it -- re-reading the state
      // is what turns the button back into something that works.
      await refreshDownloadStatus();
      render();
    }
  }

  function close({ restoreFocus = true } = {}) {
    setModalBusy(false);
    resetCopyFeedback();
    // The transfer is the backend's, and closing the dialog is not a decision
    // to abandon it -- only the polling stops.
    stopPolling();
    return closeModal(updateCheckModal, { restoreFocus });
  }

  async function openAndCheck() {
    requestSerial += 1;
    const activeRequest = requestSerial;
    modalState = "loading";
    latestPayload = { ...emptyPayload(), current_version: state.backendVersion || "" };
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
      latestPayload = { ...emptyPayload(), current_version: state.backendVersion || "" };
      modalState = "unavailable";
    }

    // A download started earlier is still running, or finished, in the
    // backend. Reading it here is what lets the dialog reopen onto the
    // progress bar rather than onto a button offering to start again.
    if (latestPayload.download_name) {
      await refreshDownloadStatus();
      if (activeRequest !== requestSerial) return;
      await refreshApplyStatus();
      if (activeRequest !== requestSerial) return;
    }

    render();
    if (isDownloadRunning() || isApplyRunning()) schedulePoll();
  }

  async function checkOnStartup({ enabled = true } = {}) {
    if (!enabled) return null;
    requestSerial += 1;
    const activeRequest = requestSerial;

    try {
      const payload = await fetchJSONWithInit(`${apiBase}/update-check`, { cache: "no-store" });
      if (activeRequest !== requestSerial) return null;
      const normalized = normalizePayload(payload);
      if (normalized.status !== "update_available") {
        return normalized;
      }
      latestPayload = normalized;
      modalState = "update_available";
      render();
      openModal(updateCheckModal, { focusTarget: updateCheckCloseIcon || updateCheckClose || updateCheckAction });
      return normalized;
    } catch {
      return null;
    }
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
    if (primaryIntent === "download") {
      void startDownload();
      return;
    }
    if (primaryIntent === "open_download_link") {
      openDownload();
      return;
    }
    if (primaryIntent === "apply") {
      void applyUpdate();
      return;
    }
    if (primaryIntent === "reveal") {
      void revealDownload();
      return;
    }
    openReleasePage();
  });
  updateCheckReveal?.addEventListener("click", () => {
    void revealDownload();
  });
  updateCheckCancel?.addEventListener("click", () => {
    void cancelDownload();
  });
  updateCheckReleaseNotes?.addEventListener("click", () => {
    openReleasePage();
  });
  updateCheckCommandCopy?.addEventListener("click", () => {
    void copyUpdateCommand();
  });
  updateCheckModal?.addEventListener("click", (event) => {
    if (event.target === updateCheckModal || event.target.classList?.contains("modal-backdrop")) {
      close();
    }
  });

  return {
    checkOnStartup,
    close,
    openAndCheck,
    refreshUi,
  };
}
