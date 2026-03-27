/**
 * Shared transient frame-load state helpers.
 */

export function createTransientFrameLoadState(state) {
  function isFrameLoading() {
    return Boolean(state.isLoading);
  }

  function startFrameLoad() {
    if (state.isLoading) return false;
    state.isLoading = true;
    return true;
  }

  function finishFrameLoad() {
    state.isLoading = false;
  }

  function queuePendingFrame(index) {
    state.pendingFrame = index;
  }

  function hasPendingFrameRequest() {
    return state.pendingFrame !== null;
  }

  function consumePendingFrameRequest() {
    if (state.pendingFrame === null) return null;
    const next = state.pendingFrame;
    state.pendingFrame = null;
    return next;
  }

  function clearPendingFrameRequest() {
    state.pendingFrame = null;
  }

  function resetTransientFrameLoadState() {
    clearPendingFrameRequest();
    finishFrameLoad();
  }

  return {
    isFrameLoading,
    startFrameLoad,
    finishFrameLoad,
    queuePendingFrame,
    hasPendingFrameRequest,
    consumePendingFrameRequest,
    clearPendingFrameRequest,
    resetTransientFrameLoadState,
  };
}
