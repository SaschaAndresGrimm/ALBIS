/**
 * Build ROI CSV payloads, and the statistics table Copy puts on the
 * clipboard, independently from DOM/browser download side effects.
 */

import { buildCsvTable, csvProvenanceLines, plotSeriesColumns } from "./csv_export_utils.js";
import { formatLevelValue } from "./format_utils.js";
import { t } from "./i18n.js";

/** The ROI itself, for the provenance block: what these numbers were measured over. */
export function roiGeometryNote(roiState) {
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

/** Pixels too far out in a tail to be binned (roi_histogram_engine.js), so the histogram columns do not hold them. */
export function histogramOutsideNotes(histMeta) {
  const below = histMeta?.outsideBelow || 0;
  const above = histMeta?.outsideAbove || 0;
  if (!below && !above) return [];
  return [`Histogram: ${below} pixels below the first bin and ${above} above the last are not in its columns`];
}

/**
 * The Below BG / BG to FG / Above FG counts, with the levels they were
 * counted against: they change with BG and FG, so the levels belong next to
 * them. In English, like the rest of the provenance block.
 */
export function levelBandNotes(levelBands) {
  const total = levelBands?.total;
  if (!total) return [];
  const { bg, fg } = levelBands;
  const share = (count) => `${count} of ${total} (${((count / total) * 100).toFixed(2)} %)`;
  return [
    `Below BG ${bg}: ${share(levelBands.below)}`,
    `BG ${bg} to FG ${fg}: ${share(levelBands.between)}`,
    `Above FG ${fg}: ${share(levelBands.above)}`,
  ];
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
  const notes = [roiGeometryNote(roiState), ...levelBandNotes(roiState.levelBands), ...histogramOutsideNotes(histMeta)];
  const lines = [...csvProvenanceLines(state, notes), ...table];

  const base = (state.file || "roi").split("/").pop().replace(/\.[^.]+$/, "");
  const thresholdSuffix = state.thresholdCount > 1 ? `_thr${state.thresholdIndex + 1}` : "";
  const filename = `${base}_frame_${state.frameIndex + 1}${thresholdSuffix}_roi_${roiState.mode}.csv`;
  return {
    filename,
    content: lines.join("\n"),
  };
}

/**
 * The statistics panel as a two-column, tab-separated table for the
 * clipboard: it pastes into a spreadsheet as cells and into notes as aligned
 * text. Above it the CSV's comment lines, so a pasted block still says which
 * file, frame and ROI it describes. The values are the full ones, not the four
 * digits the panel rounds to. Labels are in the interface's language, as the
 * panel shows them; null when there are no statistics to copy.
 */
export function buildRoiStatsTsv(state, roiState) {
  const stats = roiState?.statistics;
  if (!stats) return null;
  const region = roiState.enabled && roiState.active ? roiGeometryNote(roiState) : "ROI: none, whole frame";
  const rows = [];
  const add = (label, value) => {
    if (value !== undefined && value !== null && !Number.isNaN(value)) rows.push(`${label}\t${value}`);
  };
  const counts = roiState.pixelCounts;
  add(t("analysis.roi.stats.total_pixels"), counts?.total);
  add(t("analysis.roi.stats.gap_pixels"), counts?.gap);
  add(t("analysis.roi.stats.defective_pixels"), counts?.defective);
  add(t("analysis.roi.stats.saturated_pixels"), counts?.saturated);
  ["min", "max", "sum", "median", "mean", "std"].forEach((key) => add(t(`analysis.roi.stats.${key}`), stats[key]));
  const bands = roiState.levelBands;
  if (bands?.total) {
    // The levels as the panel labels them ("FG 37"); they name the rows, and
    // 36.95234009337005 there would be noise.
    const levels = { bg: formatLevelValue(bands.bg, state?.dtype), fg: formatLevelValue(bands.fg, state?.dtype) };
    add(t("analysis.roi.stats.below_bg_at", levels), bands.below);
    add(t("analysis.roi.stats.bg_to_fg_at", levels), bands.between);
    add(t("analysis.roi.stats.above_fg_at", levels), bands.above);
  }
  return [...csvProvenanceLines(state, [region]), ...rows].join("\n") + "\n";
}
