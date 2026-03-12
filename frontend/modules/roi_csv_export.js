/**
 * Build ROI CSV payloads independently from DOM/browser download side effects.
 */

import { t } from "./i18n.js";

function formatCsvNumber(value) {
  return Number.isFinite(value) ? String(value) : "";
}

function addCsvSection(lines, title, data, meta, allowEmpty = false) {
  if (!allowEmpty && (!data || !data.length)) return;
  const xLabel = meta?.xLabel || t("csv.axis.index");
  const yLabel = meta?.yLabel || t("csv.axis.value");
  const xStart = Number.isFinite(meta?.xStart) ? meta.xStart : 0;
  const xStep = Number.isFinite(meta?.xStep) && meta.xStep !== 0 ? meta.xStep : 1;
  lines.push(`# ${title}`);
  lines.push(`${xLabel},${yLabel}`);
  if (data && data.length) {
    data.forEach((value, idx) => {
      const xValue = xStart + idx * xStep;
      lines.push(`${formatCsvNumber(xValue)},${formatCsvNumber(value)}`);
    });
  }
  lines.push("");
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

  const lines = [];
  if (roiState.lineProfile && roiState.lineProfile.length) {
    addCsvSection(
      lines,
      roiState.mode === "line" ? t("roi.plot.line_profile") : t("roi.plot.radial_profile"),
      roiState.lineProfile,
      lineMeta,
    );
  }

  const allowBoxEmpty = roiState.mode === "box";
  if (roiState.xProjection && roiState.xProjection.length) {
    addCsvSection(lines, t("csv.section.x_projection"), roiState.xProjection, xMeta, allowBoxEmpty);
  } else if (allowBoxEmpty) {
    addCsvSection(lines, t("csv.section.x_projection"), roiState.xProjection || [], xMeta, true);
  }

  if (roiState.yProjection && roiState.yProjection.length) {
    addCsvSection(lines, t("csv.section.y_projection"), roiState.yProjection, yMeta, allowBoxEmpty);
  } else if (allowBoxEmpty) {
    addCsvSection(lines, t("csv.section.y_projection"), roiState.yProjection || [], yMeta, true);
  }

  if (roiState.histogramDistribution && roiState.histogramDistribution.length) {
    addCsvSection(lines, t("analysis.roi.plot.histogram"), roiState.histogramDistribution, histMeta);
  }

  if (!lines.length) {
    return null;
  }

  const base = (state.file || "roi").split("/").pop().replace(/\.[^.]+$/, "");
  const thresholdSuffix = state.thresholdCount > 1 ? `_thr${state.thresholdIndex + 1}` : "";
  const filename = `${base}_frame_${state.frameIndex + 1}${thresholdSuffix}_roi_${roiState.mode}.csv`;
  return {
    filename,
    content: lines.join("\n"),
  };
}
