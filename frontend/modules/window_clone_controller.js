/**
 * File > Duplicate Window, and the two things a new window may be asked to do
 * when it opens: restore a duplicated window, or open the file ALBIS was
 * launched with (both named in the URL fragment).
 *
 * What travels between windows, and how, is window_clone_state.js; this is
 * the sending and receiving, which needs the viewer's own operations, passed
 * in as callbacks.
 */

import { t } from "./i18n.js";
import { readLaunchTargetFromHash } from "./launch_target.js";
import {
  CLONE_HASH_PREFIX,
  captureWindowState,
  claimClonePayload,
  discardClonePayload,
  readCloneTokenFromHash,
  stashClonePayload,
  sweepStaleClonePayloads,
} from "./window_clone_state.js";

export function createWindowCloneController({ state, analysisState, roiState, callbacks, core }) {
  const {
    fileCommandUnavailableReason,
    setStatus,
    getViewport,
    applyViewportCenter,
    openPathInViewer,
    isHdfFile,
    selectDatasetIfPresent,
    setThresholdIndex,
    requestFrame,
    updateMaskUI,
    applySyncedContrast,
    applySyncedRoi,
    setRings,
    updateRingsSectionState,
    updatePeaksSectionState,
    schedulePeakFinder,
    scheduleResolutionOverlay,
    schedulePeakOverlay,
  } = callbacks;

  /**
   * File > Duplicate Window: open this image again, set up the same way.
   *
   * The snapshot goes through a single-use localStorage slot named by a nonce in
   * the URL fragment, not through the URL itself -- a path in the address bar
   * would end up in history and in anything that logs `location.href`.
   */
  function duplicateWindow() {
    const reason = fileCommandUnavailableReason("duplicate-window");
    if (reason) {
      setStatus(reason, { tone: "warning" });
      return;
    }
    const payload = captureWindowState({
      state,
      analysisState,
      roiState,
      viewport: getViewport(),
    });
    if (!payload) {
      setStatus(t("status.duplicate_window.failed"), { tone: "warning" });
      return;
    }
    // Sweep first: a slot whose popup was blocked earlier would otherwise sit
    // there until its TTL, and one per blocked attempt adds up.
    sweepStaleClonePayloads();
    const token = stashClonePayload(payload);
    if (!token) {
      setStatus(t("status.duplicate_window.failed"), { tone: "warning" });
      return;
    }
    const url = new URL(window.location.href);
    url.hash = CLONE_HASH_PREFIX + token;
    const opened = window.open(url.toString(), "_blank");
    if (!opened) {
      // Pop-up blocked: take the slot back rather than leaving the snapshot for
      // a window that will never come.
      discardClonePayload(token);
      setStatus(t("status.duplicate_window.blocked"), { tone: "warning" });
    }
  }

  /**
   * The receiving half, run once at boot when the fragment names a slot.
   *
   * Order matters and every step is awaited: loadMetadata resets the frame and
   * threshold indices and re-derives pixelAspect, and the mask load can force the
   * mask on, so anything applied before them would be quietly undone. The
   * viewport goes last because it is expressed as an image-space centre and needs
   * the frame's real dimensions to convert into a scroll position.
   */
  async function restoreClonedWindow(payload) {
    const source = payload.source || {};
    await openPathInViewer(source.file, { refreshFileList: true });

    if (source.dataset && isHdfFile(source.file)) {
      await selectDatasetIfPresent(source.dataset);
    }

    if (Number(state.thresholdCount) > 1 && Number(source.thresholdIndex) > 0) {
      await setThresholdIndex(Number(source.thresholdIndex));
    }

    const frameIndex = Number(source.frameIndex) || 0;
    if (frameIndex > 0 && frameIndex < Number(state.frameCount)) {
      await requestFrame(frameIndex);
    }

    const mask = payload.mask || {};
    state.maskAuto = Boolean(mask.auto);
    state.maskSaturatedEnabled = Boolean(mask.saturated);
    state.maskEnabled = Boolean(mask.enabled);
    updateMaskUI();

    if (payload.contrast) applySyncedContrast(payload.contrast);

    const analysis = payload.analysis || {};
    analysisState.ringsEnabled = Boolean(analysis.ringsEnabled);
    analysisState.ringMode = analysis.ringMode || analysisState.ringMode;
    if (Array.isArray(analysis.rings)) {
      setRings(analysis.rings);
    }
    // The detector geometry does not travel: the file the clone just opened
    // states its own, and the geometry override lives in localStorage, which
    // the clone shares with this window.
    analysisState.peaksEnabled = Boolean(analysis.peaksEnabled);
    analysisState.peakCount = Number(analysis.peakCount) || analysisState.peakCount;
    analysisState.peakMinSnr = Number(analysis.peakMinSnr) || 0;
    updateRingsSectionState();
    updatePeaksSectionState();
    if (analysisState.peaksEnabled) schedulePeakFinder();

    if (payload.roi?.start && payload.roi?.end) applySyncedRoi(payload.roi);

    if (payload.viewport) applyViewportCenter(payload.viewport);

    scheduleResolutionOverlay();
    schedulePeakOverlay();
  }

  /** Clear one key out of the fragment, leaving any others in place. */
  function stripHashKey(prefix) {
    try {
      const url = new URL(window.location.href);
      const rest = url.hash
        .replace(/^#/, "")
        .split("&")
        .filter((part) => part && !part.startsWith(prefix));
      url.hash = rest.length ? rest.join("&") : "";
      window.history.replaceState(null, "", url.toString());
    } catch {
      // A browser that refuses replaceState is not a reason to skip the open.
    }
  }

  /**
   * Open the file ALBIS was launched with, if it was launched with one.
   *
   * Runs before the clone restore: that strips the whole fragment once it finds
   * its own token, and the two cannot both be honoured anyway -- a duplicated
   * window restores a file of its own.
   */
  async function openLaunchTargetFromHash() {
    const target = readLaunchTargetFromHash(window.location.hash);
    if (!target) return;
    // Strip first, so a reload gives a clean window instead of reopening the
    // file the user may since have navigated away from.
    stripHashKey("albis-open=");
    try {
      // The window restored the saved data source, which may be a live one --
      // the SIMPLON monitor, still running. A file opened with ALBIS is the
      // source now, as with File -> Open. The detector's monitor is left alone:
      // another ALBIS window may still be showing it live.
      await core.autoloadOrchestration.ensureFileMode({ disableMonitor: false });
      await openPathInViewer(target, { refreshFileList: true });
    } catch (err) {
      console.error(err);
      setStatus(t("status.launch_target.failed", { path: target }), { tone: "error" });
    }
  }

  /** Consume a clone slot if this window was opened as one. */
  async function restoreCloneFromHash() {
    const token = readCloneTokenFromHash(window.location.hash);
    if (!token) {
      sweepStaleClonePayloads();
      return;
    }
    const payload = claimClonePayload(token);
    // Strip the fragment either way: a reload should give a clean window rather
    // than re-running a restore whose slot is already gone.
    try {
      const url = new URL(window.location.href);
      url.hash = "";
      window.history.replaceState(null, "", url.toString());
    } catch {
      // A browser that refuses replaceState is not a reason to skip the restore.
    }
    if (!payload) return;
    try {
      await restoreClonedWindow(payload);
    } catch (err) {
      console.warn("Failed restoring duplicated window", err);
      setStatus(t("status.duplicate_window.restore_failed"), { tone: "warning" });
    }
  }

  return { duplicateWindow, openLaunchTargetFromHash, restoreCloneFromHash };
}
