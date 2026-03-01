/**
 * Window-level UI bindings.
 */

export function bindWindowUiInteractions({
  state,
  elements,
  callbacks,
}) {
  const { exportBtn } = elements;

  const {
    exportFullImage,
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

  exportBtn?.addEventListener("click", () => {
    exportFullImage();
  });

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
