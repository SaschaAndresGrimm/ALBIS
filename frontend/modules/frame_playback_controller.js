/**
 * Frame playback and navigation orchestration.
 */

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
  } = callbacks;

  function currentFrameStatusText() {
    const total = Math.max(1, Number(state.frameCount) || 1);
    const index = Math.max(0, Math.min(total - 1, Number(state.frameIndex) || 0));
    return `Frame ${index + 1} / ${total}`;
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
      window.clearInterval(state.playTimer);
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
      state.pendingFrame = clamped;
      return;
    }
    if (state.isLoading) {
      state.pendingFrame = clamped;
      cancelActiveFrameLoad();
      return;
    }
    state.frameIndex = clamped;
    updateFrameControls();
    updateToolbar();
    loadFrame();
  }

  function startPlayback() {
    if (state.playing || state.frameCount <= 1) return;
    state.playing = true;
    updatePlayButtons();
    setLoading(false);
    state.playTimer = window.setInterval(() => {
      if (!state.playing) return;
      const step = Math.max(1, state.step);
      const next = (state.frameIndex + step) % state.frameCount;
      requestFrame(next);
    }, Math.max(1000 / state.fps, 50));
  }

  function processPendingFrameRequest(appliedFrame) {
    if (state.pendingFrame === null) return;
    const next = state.pendingFrame;
    state.pendingFrame = null;
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
