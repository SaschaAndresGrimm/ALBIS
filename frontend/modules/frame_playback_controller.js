/**
 * Frame playback and navigation orchestration.
 */

import { t } from "./i18n.js";
import { syncRangeFill } from "./range_fill.js";

// The fastest playback offered. A 1 Mpx frame takes about 10 ms from file to
// screen, a 4M one about four times that; playback waits for a frame still
// loading, so a speed the machine cannot keep up with only becomes a ceiling.
export const MAX_PLAYBACK_FPS = 30;

const LIVE_MODES = new Set(["simplon", "remote", "jungfraujoch"]);

// A live source steps through its own history, not a series; another window's
// frame means nothing there.
function isLiveHistory(state) {
  const autoload = state.autoload;
  return Boolean(
    autoload && LIVE_MODES.has(autoload.mode) && Array.isArray(autoload.historyEntries) && autoload.historyEntries.length > 0,
  );
}

export function createFramePlaybackController({
  state,
  elements,
  callbacks,
  core,
}) {
  const {
    frameRange,
    frameIndex,
  } = elements;

  const {
    updatePlayButtons,
    setLoading,
    isViewportInteractionActive,
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
      syncRangeFill(frameRange);
    }
    if (frameIndex) {
      frameIndex.min = "1";
      frameIndex.max = String(total);
      frameIndex.value = String(displayValue);
      frameIndex.disabled = total <= 1;
      // As wide as the largest frame number: "2000" was cut to "200".
      frameIndex.style.setProperty("--frame-digits", String(String(total).length));
    }
  }

  // The frame this window is going to because a linked window asked. Its
  // arrival is not sent back out: that would echo, and in a group of three the
  // third window would be moved twice.
  let followTarget = null;

  function clearPlayTimer() {
    if (state.playTimer) {
      window.clearTimeout(state.playTimer);
      state.playTimer = null;
    }
  }

  // `announce: false` when another window stopped playback: it already knows.
  function stopPlayback({ announce = true } = {}) {
    const wasPlaying = state.playing;
    clearPlayTimer();
    state.playing = false;
    updatePlayButtons();
    if (wasPlaying && announce) {
      core.viewerSync?.handleFrameChanged?.(state.frameIndex, "stop");
    }
  }

  function goToFrame(index, { follow = false } = {}) {
    const hasSeries = Array.isArray(state.seriesFiles) && state.seriesFiles.length > 0;
    if (!state.frameCount || (!state.dataset && !hasSeries) || !state.file) return;
    const clamped = Math.max(0, Math.min(state.frameCount - 1, index));
    if (follow) {
      followTarget = clamped;
    }
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
      // playback first, so `playing` separates the two cleanly. Following a
      // linked window is like playback: the messages can come faster than a
      // frame loads, and cancelling each time would mean no frame ever lands.
      // The newest one waits instead, and the ones in between are skipped.
      if (!state.playing && !follow) {
        core.fileDataPipeline?.cancelActiveLoad();
      }
      return;
    }
    const previousIndex = state.frameIndex;
    state.frameIndex = clamped;
    updateFrameControls();
    core.chromeToolbar?.updateToolbar();
    if (followTarget === clamped) {
      followTarget = null;
    } else if (clamped !== previousIndex) {
      core.viewerSync?.handleFrameChanged?.(previousIndex);
    }
    core.fileDataPipeline.loadFrame();
  }

  function requestFrame(index) {
    followTarget = null;
    goToFrame(index);
  }

  /**
   * Go to a frame because a linked window moved. While that window plays, this
   * one counts as playing too, with no timer of its own: the play button shows
   * it, Stop here stops both, and a frame still loading is never cancelled.
   */
  function followFrame(index, { playing = false } = {}) {
    if (isLiveHistory(state)) return;
    if (playing) {
      clearPlayTimer();
      if (!state.playing && state.frameCount > 1) {
        state.playing = true;
        updatePlayButtons();
      }
    } else if (state.playing) {
      stopPlayback({ announce: false });
    }
    if (index === state.frameIndex && !isFrameLoading()) return;
    goToFrame(index, { follow: true });
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
    }, Math.max(1000 / state.fps, 1000 / MAX_PLAYBACK_FPS));
  }

  function startPlayback() {
    if (state.playing || state.frameCount <= 1) return;
    state.playing = true;
    updatePlayButtons();
    setLoading(false);
    scheduleNextPlaybackTick();
    core.viewerSync?.handleFrameChanged?.(state.frameIndex, "play");
  }

  function processPendingFrameRequest(appliedFrame) {
    const next = consumePendingFrameRequest();
    if (next === null) return;
    if (next !== state.frameIndex || !appliedFrame) {
      goToFrame(next);
    }
  }

  return {
    currentFrameStatusText,
    updateFrameControls,
    startPlayback,
    stopPlayback,
    processPendingFrameRequest,
    requestFrame,
    followFrame,
  };
}
