/**
 * Window-level UI bindings.
 */

export function bindWindowUiInteractions({
  state,
  callbacks,
  core,
}) {
  const {
    setZoom,
    drawHistogram,
    drawSplash,
    applyPanelState,
    scheduleHistogram,
    scheduleRoiOverlay,
  } = callbacks;

  window.addEventListener("resize", () => {
    if (state.hasFrame) {
      setZoom(state.zoom);
    }
    core.chromeToolbar?.updateToolbar();
    if (state.histogram) {
      drawHistogram(state.histogram);
    }
    drawSplash();
    applyPanelState();
    core.overviewViewport?.scheduleOverview();
    scheduleHistogram();
    core.overlayRender.schedulePixelOverlay();
    scheduleRoiOverlay();
    core.roiInteraction.scheduleRoiUpdate();
    core.overlayRender.scheduleResolutionOverlay();
    core.overlayRender.schedulePeakOverlay();
  });
}
