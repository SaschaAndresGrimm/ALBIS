/**
 * Saving one ROI plot as a PNG: its file name, the caption that says what it
 * was measured on, and the picture itself (title and caption above the plot).
 *
 * The plot canvas holds only the axes and the data; its title is HTML beside
 * it. A PNG on its own in a report has nothing around it, so the title and
 * the caption are drawn into the image.
 */

import { roiGeometryNote } from "./roi_csv_export.js";
import { canvasFont } from "./canvas_fonts.js";

const PLOT_FILE_PARTS = { x: "profile_x", y: "profile_y", hist: "histogram", surface: "3d_view" };

/** `<file>_frame_<n>[_thr<k>]_roi_<mode>_<plot>.png`, beside the CSV export's name. */
export function roiPlotPngFilename(state, roiState, plotKey) {
  const base = String(state?.file || "roi").split("/").pop().replace(/\.[^.]+$/, "") || "roi";
  const frame = Number.isFinite(state?.frameIndex) ? state.frameIndex + 1 : 1;
  const thresholdSuffix = state?.thresholdCount > 1 ? `_thr${state.thresholdIndex + 1}` : "";
  const mode = roiState?.mode || "box";
  const plot = PLOT_FILE_PARTS[plotKey] || (mode === "line" ? "line_profile" : "radial_profile");
  return `${base}_frame_${frame}${thresholdSuffix}_roi_${mode}_${plot}.png`;
}

/** One line: file, dataset, frame, threshold and the ROI, in English like the CSV's provenance. */
export function roiPlotPngCaption(state, roiState) {
  const parts = [];
  const source = String(state?.file || "").split("/").pop();
  if (source) parts.push(source);
  if (state?.dataset) parts.push(String(state.dataset));
  if (Number.isFinite(state?.frameIndex)) {
    const count = Number.isFinite(state?.frameCount) ? state.frameCount : 0;
    parts.push(`frame ${state.frameIndex + 1}${count > 1 ? `/${count}` : ""}`);
  }
  if (state?.thresholdCount > 1) {
    parts.push(`threshold ${state.thresholdIndex + 1}/${state.thresholdCount}`);
  }
  parts.push(roiGeometryNote(roiState));
  return parts.filter(Boolean).join(" · ");
}

/** `text`, cut with an ellipsis to fit `maxWidth` as `ctx` measures it. */
export function fitCanvasText(ctx, text, maxWidth) {
  const value = String(text ?? "");
  if (ctx.measureText(value).width <= maxWidth) return value;
  let low = 0;
  let high = value.length;
  while (low < high) {
    const mid = Math.ceil((low + high) / 2);
    if (ctx.measureText(`${value.slice(0, mid)}…`).width <= maxWidth) low = mid;
    else high = mid - 1;
  }
  return `${value.slice(0, low)}…`;
}

// CSS pixels; scaled by the source canvas's device-pixel ratio.
const PAD = 10;
const TITLE_SIZE = 13;
const CAPTION_SIZE = 11;
const LINE_GAP = 4;

/**
 * The plot as it is on screen, with its title and caption above it, on an
 * opaque background (the plot's own fill is slightly see-through).
 *
 * `source` is the plot canvas, already at the screen's pixel ratio `dpr`;
 * the header is drawn at that ratio too so the text matches the axis labels.
 * `colors` are { background, plot, text, muted }. Returns the new canvas, or
 * null when there is nothing to draw.
 */
export function composeRoiPlotPng({ source, title, caption, dpr = 1, colors, createCanvas }) {
  const width = Number(source?.width);
  const height = Number(source?.height);
  if (!(width > 0) || !(height > 0)) return null;
  const scale = dpr > 0 ? dpr : 1;
  const headerHeight = Math.round((PAD + TITLE_SIZE + (caption ? LINE_GAP + CAPTION_SIZE : 0) + PAD / 2) * scale);
  const canvas = createCanvas(width, height + headerHeight);
  const ctx = canvas?.getContext?.("2d");
  if (!ctx) return null;

  ctx.fillStyle = colors.background;
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = colors.plot;
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  const maxText = width - 2 * PAD * scale;
  ctx.textBaseline = "top";
  ctx.fillStyle = colors.text;
  ctx.font = canvasFont(Math.round(TITLE_SIZE * scale), 600);
  ctx.fillText(fitCanvasText(ctx, title, maxText), PAD * scale, PAD * scale);
  if (caption) {
    ctx.fillStyle = colors.muted;
    ctx.font = canvasFont(Math.round(CAPTION_SIZE * scale));
    ctx.fillText(fitCanvasText(ctx, caption, maxText), PAD * scale, (PAD + TITLE_SIZE + LINE_GAP) * scale);
  }
  ctx.drawImage(source, 0, headerHeight);
  return canvas;
}
