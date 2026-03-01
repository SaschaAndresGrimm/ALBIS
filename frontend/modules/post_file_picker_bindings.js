/**
 * Post-file-picker binding orchestration.
 */

import { bindAutoloadControls } from "./autoload_controls.js";
import { bindViewerControls } from "./viewer_controls_bindings.js";
import { bindRoiControlInteractions } from "./roi_controls_bindings.js";
import { bindPanelAndSectionInteractions } from "./panel_tab_bindings.js";
import { bindViewportInteractions } from "./viewport_bindings.js";
import { bindRoiPlotInteractions } from "./roi_plot_bindings.js";
import { bindOverviewInteractions } from "./overview_bindings.js";
import { bindHistogramDragInteractions } from "./histogram_drag_bindings.js";
import { bindWindowUiInteractions } from "./window_bindings.js";

export function initializePostFilePickerBindings({
  autoloadBinding,
  viewerBinding,
  roiControlBinding,
  panelBinding,
  viewportBinding,
  roiPlotBinding,
  overviewBinding,
  histogramDragBinding,
  windowUiBinding,
}) {
  bindAutoloadControls(autoloadBinding);
  bindViewerControls(viewerBinding);
  bindRoiControlInteractions(roiControlBinding);
  bindPanelAndSectionInteractions(panelBinding);
  bindViewportInteractions(viewportBinding);
  bindRoiPlotInteractions(roiPlotBinding);
  bindOverviewInteractions(overviewBinding);
  bindHistogramDragInteractions(histogramDragBinding);
  bindWindowUiInteractions(windowUiBinding);
}
