/**
 * Data-source and frame control bindings.
 */

export function bindDataControlInteractions({
  state,
  elements,
  callbacks,
  core,
}) {
  const {
    fileSelect,
    datasetSelect,
    thresholdSelect,
    toolbarThresholdSelect,
    frameRange,
    frameIndex,
    frameStep,
    fpsSelect,
  } = elements;

  const {
    ensureFileMode,
    syncSeriesSumOutputPath,
    isHdfFile,
    loadDatasets,
    loadImageSeries,
    loadMetadata,
    setThresholdIndex,
    requestFrame,
    setFrameStep,
    closeToolbarPlaybackPopover,
    setFps,
    recordRecentFile,
  } = callbacks;

  fileSelect.addEventListener("change", async (event) => {
    await ensureFileMode();
    state.file = event.target.value;
    if (!state.file) return;
    syncSeriesSumOutputPath();
    core.framePlayback?.stopPlayback();
    if (isHdfFile(state.file)) {
      await loadDatasets();
    } else {
      await loadImageSeries(state.file);
    }
    recordRecentFile?.(state.file);
  });

  datasetSelect.addEventListener("change", async (event) => {
    state.dataset = event.target.value;
    core.framePlayback?.stopPlayback();
    await loadMetadata();
  });

  thresholdSelect?.addEventListener("change", async (event) => {
    const value = Math.max(0, Number(event.target.value || 0));
    await setThresholdIndex(value);
  });

  toolbarThresholdSelect?.addEventListener("change", async (event) => {
    const value = Math.max(0, Number(event.target.value || 0));
    await setThresholdIndex(value);
  });

  frameRange.addEventListener("input", async (event) => {
    core.framePlayback?.stopPlayback();
    const value = Math.round(Number(event.target.value || 1));
    requestFrame(value - 1);
  });

  frameIndex.addEventListener("change", async (event) => {
    core.framePlayback?.stopPlayback();
    const value = Math.round(Number(event.target.value || 1));
    requestFrame(value - 1);
  });

  frameStep?.addEventListener("change", () => {
    setFrameStep(frameStep.value);
    closeToolbarPlaybackPopover();
  });

  fpsSelect?.addEventListener("change", () => {
    setFps(Number(fpsSelect.value));
    closeToolbarPlaybackPopover();
  });
}
