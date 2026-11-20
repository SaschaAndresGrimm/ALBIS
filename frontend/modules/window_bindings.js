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

  function handleViewportResize() {
    // First, so a new pixel ratio keeps the zoom the user sees.
    const ratioChanged = core.overviewViewport?.syncDevicePixelRatio?.();
    if (state.hasFrame && !ratioChanged) {
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
  }

  window.addEventListener("resize", handleViewportResize);

  // Moving the window to a screen with another pixel ratio need not resize
  // it; a media query on the current ratio says when it changes.
  function watchDevicePixelRatio() {
    if (typeof window.matchMedia !== "function") return;
    const query = window.matchMedia(`(resolution: ${window.devicePixelRatio || 1}dppx)`);
    query?.addEventListener?.("change", () => {
      handleViewportResize();
      watchDevicePixelRatio();
    }, { once: true });
  }
  watchDevicePixelRatio();

  // The plots are drawn on canvas in the theme's colours: draw them again.
  window.addEventListener(THEME_CHANGE_EVENT, () => {
    if (state.histogram) {
      drawHistogram(state.histogram);
    }
    core.overviewViewport?.scheduleOverview();
    core.roiInteraction?.scheduleRoiUpdate();
  });
}
