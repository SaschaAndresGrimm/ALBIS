/**
 * Build ROI CSV payloads independently from DOM/browser download side effects.
 */

import { buildCsvTable, csvProvenanceLines, plotSeriesColumns } from "./csv_export_utils.js";
import { t } from "./i18n.js";

/** The ROI itself, for the provenance block: what these numbers were measured over. */
function roiGeometryNote(roiState) {
  const mode = roiState?.mode || "";
  const point = (p) =>
    p && Number.isFinite(p.x) && Number.isFinite(p.y) ? `${Math.round(p.x)} ${Math.round(p.y)}` : "";
  if (mode === "circle" || mode === "annulus") {
    const centre = point(roiState.start);
    const radii =
      mode === "annulus"
        ? `r ${roiState.innerRadius ?? 0} to ${roiState.outerRadius ?? 0}`
        : `r ${roiState.outerRadius ?? 0}`;
    return `ROI: ${mode} centre ${centre} ${radii}`.trim();
  }
  const start = point(roiState?.start);
  const end = point(roiState?.end);
  if (!start || !end) return `ROI: ${mode}`.trim();
  return `ROI: ${mode} from ${start} to ${end}`;
}

/**
 * Why the ROI CSV export cannot run, or "" when it can.
 *
 * Shared by the Export CSV button, which greys out with this as its tooltip,
 * and by the export itself, which still refuses the call: a click can land
 * between a state change and the repaint that greys the button.
 */
export function roiCsvExportUnavailableReason(state, roiState) {
  if (!state.hasFrame) return t("roi.section.load_frame");
  if (!roiState.enabled) return t("roi.section.disabled");
  if (!roiState.active) return t("status.roi.no_data");
  return "";
}

export function buildRoiCsvExportPayload({
  state,
  roiState,
  lineMeta,
  xMeta,
  yMeta,
  histMeta,
}) {
  if (!roiState?.enabled || !roiState?.active) {
    return null;
  }

  // Every plot the panel is currently showing, in the order it shows them, so
  // the columns come out in the order the eye already reads.
  // The axis fallbacks were localized before this export was columnar and stay
  // so; a plot meta without labels is not a case that happens, but an English
  // "x" appearing in an otherwise translated header would be a visible seam.
  const axisFallbacks = { fallbackX: t("csv.axis.index"), fallbackY: t("csv.axis.value") };
  const columns = [
    ...plotSeriesColumns({
      ...axisFallbacks,
      title: roiState.mode === "line" ? t("roi.plot.line_profile") : t("roi.plot.radial_profile"),
      data: roiState.lineProfile,
      meta: lineMeta,
    }),
    ...plotSeriesColumns({
      ...axisFallbacks,
      title: t("csv.section.x_projection"),
      data: roiState.xProjection,
      meta: xMeta,
    }),
    ...plotSeriesColumns({
      ...axisFallbacks,
      title: t("csv.section.y_projection"),
      data: roiState.yProjection,
      meta: yMeta,
    }),
    ...plotSeriesColumns({
      ...axisFallbacks,
      title: t("analysis.roi.plot.histogram"),
      data: roiState.histogramDistribution,
      meta: histMeta,
    }),
  ];

  const table = buildCsvTable(columns);
  if (!table.length) {
    return null;
  }
  const lines = [...csvProvenanceLines(state, [roiGeometryNote(roiState)]), ...table];

  const base = (state.file || "roi").split("/").pop().replace(/\.[^.]+$/, "");
  const thresholdSuffix = state.thresholdCount > 1 ? `_thr${state.thresholdIndex + 1}` : "";
  const filename = `${base}_frame_${state.frameIndex + 1}${thresholdSuffix}_roi_${roiState.mode}.csv`;
  return {
    filename,
    content: lines.join("\n"),
  };
}
