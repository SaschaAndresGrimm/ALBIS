/**
 * Window-level UI bindings.
 */

import { THEME_CHANGE_EVENT } from "./theme.js";

export function bindWindowUiInteractions({
  state,
  callbacks,
  core,
}) {
  const {
    drawHistogram,
    drawSplash,
    applyPanelState,
    scheduleHistogram,
    scheduleRoiOverlay,
  } = callbacks;

  window.addEventListener("resize", () => {
    if (state.hasFrame) {
      core.overviewViewport?.setZoom(state.zoom);
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

  // The plots are drawn on canvas in the theme's colours: draw them again.
  window.addEventListener(THEME_CHANGE_EVENT, () => {
    if (state.histogram) {
      drawHistogram(state.histogram);
    }
    core.overviewViewport?.scheduleOverview();
    core.roiInteraction?.scheduleRoiUpdate();
  });
}
