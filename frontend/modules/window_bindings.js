/**
 * Window-level UI bindings.
 */

export function bindWindowUiInteractions({
  state,
  callbacks,
}) {
  const {
    setZoom,
    updateToolbar,
    drawHistogram,
    drawSplash,
    applyPanelState,
    scheduleOverview,
    scheduleHistogram,
    schedulePixelOverlay,
    scheduleRoiOverlay,
    scheduleRoiUpdate,
    scheduleResolutionOverlay,
    schedulePeakOverlay,
  } = callbacks;

  window.addEventListener("resize", () => {
    if (state.hasFrame) {
      setZoom(state.zoom);
    }
    updateToolbar();
    if (state.histogram) {
      drawHistogram(state.histogram);
    }
    drawSplash();
    applyPanelState();
    scheduleOverview();
    scheduleHistogram();
    schedulePixelOverlay();
    scheduleRoiOverlay();
    scheduleRoiUpdate();
    scheduleResolutionOverlay();
    schedulePeakOverlay();
  });
}
