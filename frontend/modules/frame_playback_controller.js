/**
 * Frame playback and navigation orchestration.
 */

import { t } from "./i18n.js";

export function createFramePlaybackController({
  state,
  elements,
  callbacks,
}) {
  const {
    frameRange,
    frameIndex,
  } = elements;

  const {
    updatePlayButtons,
    setLoading,
    isViewportInteractionActive,
    cancelActiveFrameLoad,
    updateToolbar,
    loadFrame,
    queuePendingFrameRequest: queuePendingFrameRequestCallback,
    consumePendingFrameRequest: consumePendingFrameRequestCallback,
    isFrameLoading: isFrameLoadingCallback,
  } = callbacks;

  function queuePendingFrameRequest(index) {
    if (queuePendingFrameRequestCallback) {
      queuePendingFrameRequestCallback(index);
      return;
    }
    state.pendingFrame = index;
  }

  function consumePendingFrameRequest() {
    if (consumePendingFrameRequestCallback) {
      return consumePendingFrameRequestCallback();
    }
    if (state.pendingFrame === null) return null;
    const next = state.pendingFrame;
    state.pendingFrame = null;
    return next;
  }

  function isFrameLoading() {
    if (isFrameLoadingCallback) {
      return isFrameLoadingCallback();
    }
    return Boolean(state.isLoading);
  }

  function currentFrameStatusText() {
    const total = Math.max(1, Number(state.frameCount) || 1);
    const index = Math.max(0, Math.min(total - 1, Number(state.frameIndex) || 0));
    return t("status.frame.position", { current: index + 1, total });
  }

  function updateFrameControls() {
    const total = Math.max(1, state.frameCount || 1);
    const displayValue = Math.max(1, Math.min(total, (state.frameIndex || 0) + 1));
    if (frameRange) {
      frameRange.min = "1";
      frameRange.max = String(total);
      frameRange.value = String(displayValue);
      frameRange.disabled = total <= 1;
    }
    if (frameIndex) {
      frameIndex.min = "1";
      frameIndex.max = String(total);
      frameIndex.value = String(displayValue);
      frameIndex.disabled = total <= 1;
    }
  }

  function stopPlayback() {
    if (state.playTimer) {
      window.clearTimeout(state.playTimer);
      state.playTimer = null;
    }
    state.playing = false;
    updatePlayButtons();
  }

  function requestFrame(index) {
    const hasSeries = Array.isArray(state.seriesFiles) && state.seriesFiles.length > 0;
    if (!state.frameCount || (!state.dataset && !hasSeries) || !state.file) return;
    const clamped = Math.max(0, Math.min(state.frameCount - 1, index));
    if (state.playing && isViewportInteractionActive()) {
      queuePendingFrameRequest(clamped);
      return;
    }
    if (isFrameLoading()) {
      queuePendingFrameRequest(clamped);
      // Interactive navigation wants the newest index and nothing in between, so
      // dropping the stale request is right. Playback is the opposite: every frame
      // is wanted, and aborting the one in flight would mean no frame ever lands
      // once a load outlasts the tick interval. Every interactive caller stops
      // playback first, so `playing` separates the two cleanly.
      if (!state.playing) {
        cancelActiveFrameLoad();
      }
      return;
    }
    state.frameIndex = clamped;
    updateFrameControls();
    updateToolbar();
    loadFrame();
  }

  // Playback paces itself instead of running on a fixed interval. A tick that
  // finds the previous frame still in flight waits for it rather than starting a
  // competing request, so a slow source slows playback down instead of starving
  // it: the selected fps is a ceiling, not a promise. Re-arming is idempotent, so
  // overlapping callers cannot stack up two timers, and reading `fps` at arm time
  // means a speed change applies from the next tick without restarting playback.
  function scheduleNextPlaybackTick() {
    if (!state.playing || state.playTimer) return;
    state.playTimer = window.setTimeout(() => {
      state.playTimer = null;
      if (!state.playing) return;
      if (!isFrameLoading()) {
        const step = Math.max(1, state.step);
        const next = (state.frameIndex + step) % state.frameCount;
        requestFrame(next);
      }
      scheduleNextPlaybackTick();
    }, Math.max(1000 / state.fps, 50));
  }

  function startPlayback() {
    if (state.playing || state.frameCount <= 1) return;
    state.playing = true;
    updatePlayButtons();
    setLoading(false);
    scheduleNextPlaybackTick();
  }

  function processPendingFrameRequest(appliedFrame) {
    const next = consumePendingFrameRequest();
    if (next === null) return;
    if (next !== state.frameIndex || !appliedFrame) {
      requestFrame(next);
    }
  }

  return {
    currentFrameStatusText,
    updateFrameControls,
    startPlayback,
    stopPlayback,
    processPendingFrameRequest,
    requestFrame,
  };
}
