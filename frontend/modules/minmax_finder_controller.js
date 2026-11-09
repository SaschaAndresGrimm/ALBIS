/**
 * Analysis -> Min/Max Finder, the highest and lowest pixels of the frame on
 * screen, listed and marked on the image.
 *
 * One of the Analysis tools (analysis_tools.js), in the general group: it
 * answers a question about any frame rather than one kind of work -- which
 * pixels read the most, or the least -- so it takes none of the Peak
 * Finder's tests for a spot (see minmax_finder_utils.js). Masked pixels, and saturated ones while
 * Mask saturated is on, are left out as the Peak Finder leaves them out,
 * unless "Include masked pixels" is ticked: a hot pixel that is masked
 * already is a fair thing to look for. Left in, the gaps would fill the low
 * end.
 *
 * Find: Max, Min or Both. Both is one table, the highest pixels and then the
 * lowest, each numbered on its own and marked ▲ or ▼, rather than two tables:
 * the panel is long already, and one list keeps one selection and one CSV.
 *
 * It runs whenever the Peak Finder would (a new frame, a mask change): the
 * Peak Finder's schedule calls schedule() here, so there is no second list of
 * places that change the frame to keep in step.
 */

import { onLanguageChange, t } from "./i18n.js";
import { setControlAvailability } from "./control_availability.js";
import { buildCsvTable, csvProvenanceLines } from "./csv_export_utils.js";
import { clampMinMaxCount, findExtremePixels, normalizeMinMaxMode, parseMinMaxCutoff } from "./minmax_finder_utils.js";

const KIND_GLYPH = { max: "▲", min: "▼" };

// A pixel can be in both lists on a small frame, so a selection names the
// list too.
const pixelKey = (pixel) => `${pixel.kind}:${pixel.index}`;

export function createMinMaxFinderController({ core, state, analysisState, elements, constants, callbacks }) {
  const {
    minmaxToggle,
    minmaxStateEl,
    minmaxSummaryEl,
    minmaxModeSelect,
    minmaxCountInput,
    minmaxIncludeMasked,
    minmaxLowCutoff,
    minmaxHighCutoff,
    minmaxExportBtn,
    minmaxList,
    minmaxBody,
  } = elements;
  const { badMaskBits } = constants;
  const { setSectionBadgeState, setSummaryChip, formatStat, getActiveSaturationMax, isSaturatedValue, setStatus } =
    callbacks;

  let scheduled = false;
  // How many pixels share the highest and the lowest value, from the last run.
  let shared = { max: 0, min: 0 };

  // The mask that goes with this frame, whether or not it is in use: the Mask
  // column shows it either way.
  function frameMask() {
    const ready =
      state.maskAvailable &&
      state.maskRaw &&
      state.maskShape &&
      state.maskShape[0] === state.height &&
      state.maskShape[1] === state.width;
    return ready ? state.maskRaw : null;
  }

  function ignoreFilter(data) {
    if (analysisState.minmaxIncludeMasked) return null;
    const mask = state.maskEnabled ? frameMask() : null;
    const satMax = state.maskSaturatedEnabled ? getActiveSaturationMax() : null;
    if (!mask && !Number.isFinite(satMax)) return null;
    return (index) => Boolean(mask && mask[index] & badMaskBits) || isSaturatedValue(data[index], satMax);
  }

  function find() {
    shared = { max: 0, min: 0 };
    if (!analysisState.minmaxEnabled || !state.hasFrame || !state.dataRaw) return [];
    const mask = frameMask();
    const mode = analysisState.minmaxMode;
    const ignore = ignoreFilter(state.dataRaw);
    const kinds = mode === "both" ? ["max", "min"] : [mode];
    return kinds.flatMap((kind) => {
      const { pixels, extremeCount } = findExtremePixels(state.dataRaw, state.width, state.height, {
        count: analysisState.minmaxCount,
        ignore,
        lowest: kind === "min",
        lowCutoff: analysisState.minmaxLowCutoff,
        highCutoff: analysisState.minmaxHighCutoff,
      });
      shared[kind] = extremeCount;
      return pixels.map((pixel, rank) => ({ ...pixel, kind, rank: rank + 1, mask: mask ? mask[pixel.index] : null }));
    });
  }

  // Why there is nothing to export, or "" when there is: the same states the
  // badge describes.
  function unavailableReason() {
    if (!analysisState.minmaxEnabled) return t("minmax.state.enable_hint");
    if (!state.hasFrame) return t("minmax.state.load_frame");
    if (!analysisState.minmaxFound.length) {
      if (analysisState.minmaxLowCutoff !== null || analysisState.minmaxHighCutoff !== null) {
        return t("minmax.state.none_in_range");
      }
      return t(analysisState.minmaxMode === "max" ? "minmax.state.none_above_zero" : "minmax.state.none_on_frame");
    }
    return "";
  }

  // One end, as the badge says it: where the extreme pixel is, or, when
  // several share its value, how many -- at the low end of a dark frame that
  // can be most of the detector, and naming one of them would mislead.
  function extremeText(kind) {
    const top = analysisState.minmaxFound.find((pixel) => pixel.kind === kind);
    const value = formatStat(top.value);
    const count = shared[kind];
    if (count > 1) return t(`minmax.state.${kind}_shared`, { value, count });
    return t(`minmax.state.${kind}_one`, { value, x: top.x, y: top.y });
  }

  function badgeText() {
    const has = (kind) => analysisState.minmaxFound.some((pixel) => pixel.kind === kind);
    if (has("max") && has("min")) {
      const top = (kind) => formatStat(analysisState.minmaxFound.find((pixel) => pixel.kind === kind).value);
      return t("minmax.state.both", { max: top("max"), maxCount: shared.max, min: top("min"), minCount: shared.min });
    }
    return extremeText(has("max") ? "max" : "min");
  }

  function updateSectionState() {
    setControlAvailability(minmaxExportBtn, unavailableReason());
    const count = analysisState.minmaxFound.length;
    if (!analysisState.minmaxEnabled) {
      setSectionBadgeState(minmaxStateEl, "empty", t("minmax.state.enable_hint"));
      setSummaryChip(minmaxSummaryEl, t("summary.off"));
    } else if (!state.hasFrame) {
      setSectionBadgeState(minmaxStateEl, "loading", t("minmax.state.load_frame"));
      setSummaryChip(minmaxSummaryEl, t("summary.waiting_frame"));
    } else if (!count) {
      setSectionBadgeState(minmaxStateEl, "empty", unavailableReason());
      setSummaryChip(minmaxSummaryEl, t("minmax.summary.none"));
    } else {
      setSectionBadgeState(minmaxStateEl, "active", badgeText());
      setSummaryChip(minmaxSummaryEl, t("minmax.summary.count", { count }), "active");
    }
  }

  function emptyRow(text) {
    const empty = document.createElement("div");
    empty.className = "peaks-empty";
    empty.textContent = text;
    return empty;
  }

  // The Peak Finder's selection: click picks one (or clears it when it is the
  // only one picked), Shift adds the range from the last click, Cmd/Ctrl
  // toggles one.
  function select(idx, event) {
    const selected = analysisState.minmaxSelected;
    const anchor = analysisState.minmaxSelectionAnchor;
    if (event.shiftKey && Number.isInteger(anchor) && anchor < analysisState.minmaxFound.length) {
      const start = Math.min(anchor, idx);
      analysisState.minmaxSelected = Array.from({ length: Math.abs(anchor - idx) + 1 }, (_, i) => start + i);
    } else if (event.metaKey || event.ctrlKey) {
      analysisState.minmaxSelected = selected.includes(idx)
        ? selected.filter((value) => value !== idx)
        : [...selected, idx].sort((a, b) => a - b);
      analysisState.minmaxSelectionAnchor = idx;
    } else if (selected.length === 1 && selected[0] === idx) {
      analysisState.minmaxSelected = [];
      analysisState.minmaxSelectionAnchor = null;
    } else {
      analysisState.minmaxSelected = [idx];
      analysisState.minmaxSelectionAnchor = idx;
    }
    render();
    core.overlayRender.schedulePeakOverlay();
  }

  function render() {
    updateSectionState();
    if (!minmaxBody) return;
    minmaxBody.innerHTML = "";
    const found = analysisState.minmaxFound;
    if (!found.length) {
      minmaxBody.appendChild(emptyRow(unavailableReason()));
      return;
    }
    const hasMask = found.some((pixel) => pixel.mask !== null);
    const marked = analysisState.minmaxMode === "both";
    minmaxList?.classList.toggle("has-mask", hasMask);
    found.forEach((pixel, idx) => {
      const row = document.createElement("button");
      row.type = "button";
      row.className = "peaks-row";
      const selected = analysisState.minmaxSelected.includes(idx);
      row.classList.toggle("is-selected", selected);
      row.setAttribute("aria-pressed", String(selected));
      const rank = marked ? `${KIND_GLYPH[pixel.kind]}${pixel.rank}` : String(pixel.rank);
      const kindName = marked ? `${t(`minmax.kind.${pixel.kind}`)} ` : "";
      const maskText = pixel.mask ? String(pixel.mask) : "—";
      const value = formatStat(pixel.value);
      row.setAttribute(
        "aria-label",
        `${kindName}#${pixel.rank}, X ${pixel.x}, Y ${pixel.y}, ${t("minmax.column.value")} ${value}` +
          (hasMask ? `, ${t("minmax.column.mask")} ${maskText}` : ""),
      );
      row.innerHTML = `<span>${rank}</span><span>${pixel.x}</span><span>${pixel.y}</span><span>${value}</span><span>${maskText}</span>`;
      row.addEventListener("click", (event) => select(idx, event));
      minmaxBody.appendChild(row);
    });
  }

  function run() {
    scheduled = false;
    // Keep the selection on the same pixels while they are still listed: a
    // new frame reorders the list.
    const before = analysisState.minmaxFound;
    const keyAt = (row) => (before[row] ? pixelKey(before[row]) : null);
    const picked = new Set(analysisState.minmaxSelected.map(keyAt));
    const anchorKey = keyAt(analysisState.minmaxSelectionAnchor);
    analysisState.minmaxFound = find();
    analysisState.minmaxSelected = [];
    analysisState.minmaxSelectionAnchor = null;
    analysisState.minmaxFound.forEach((pixel, row) => {
      const key = pixelKey(pixel);
      if (picked.has(key)) analysisState.minmaxSelected.push(row);
      if (key === anchorKey) analysisState.minmaxSelectionAnchor = row;
    });
    render();
    core.overlayRender.schedulePeakOverlay();
  }

  function schedule() {
    if (scheduled || (!analysisState.minmaxEnabled && !analysisState.minmaxFound.length)) return;
    scheduled = true;
    window.setTimeout(run, 0);
  }

  function exportCsv() {
    const unavailable = unavailableReason();
    if (unavailable) {
      setStatus?.(unavailable, { tone: "warning" });
      return;
    }
    const found = analysisState.minmaxFound;
    const mode = analysisState.minmaxMode;
    const hasMask = found.some((pixel) => pixel.mask !== null);
    // Column names stay in English, as the peak list's do: scripts read them.
    const columns = [
      { label: "kind", values: found.map((pixel) => pixel.kind) },
      { label: "rank", values: found.map((pixel) => pixel.rank) },
      { label: "x", values: found.map((pixel) => pixel.x) },
      { label: "y", values: found.map((pixel) => pixel.y) },
      { label: "value", values: found.map((pixel) => pixel.value) },
    ];
    if (hasMask) columns.push({ label: "mask", values: found.map((pixel) => pixel.mask ?? "") });
    const provenance = [`Min/max pixels: ${mode}, ${found.length}`];
    if (analysisState.minmaxLowCutoff !== null) provenance.push(`Low cutoff: ${analysisState.minmaxLowCutoff}`);
    if (analysisState.minmaxHighCutoff !== null) provenance.push(`High cutoff: ${analysisState.minmaxHighCutoff}`);
    if (shared.max > 1) provenance.push(`Highest value shared by: ${shared.max}`);
    if (shared.min > 1) provenance.push(`Lowest value shared by: ${shared.min}`);
    const lines = [...csvProvenanceLines(state, provenance), ...buildCsvTable(columns)];
    const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
    const link = document.createElement("a");
    const url = URL.createObjectURL(blob);
    const base = (state.file || "frame").split("/").pop().replace(/\.[^.]+$/, "");
    const thresholdSuffix = state.thresholdCount > 1 ? `_thr${state.thresholdIndex + 1}` : "";
    const suffix = mode === "both" ? "minmax" : mode;
    link.href = url;
    link.download = `${base}_frame_${state.frameIndex + 1}${thresholdSuffix}_${suffix}_pixels.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  }

  minmaxToggle?.addEventListener("change", () => {
    analysisState.minmaxEnabled = minmaxToggle.checked;
    analysisState.minmaxSelected = [];
    analysisState.minmaxSelectionAnchor = null;
    if (!analysisState.minmaxEnabled) analysisState.minmaxFound = [];
    run();
  });
  minmaxModeSelect?.addEventListener("change", () => {
    analysisState.minmaxMode = normalizeMinMaxMode(minmaxModeSelect.value);
    schedule();
  });
  minmaxCountInput?.addEventListener("change", () => {
    analysisState.minmaxCount = clampMinMaxCount(minmaxCountInput.value, analysisState.minmaxCount);
    minmaxCountInput.value = String(analysisState.minmaxCount);
    schedule();
  });
  minmaxIncludeMasked?.addEventListener("change", () => {
    analysisState.minmaxIncludeMasked = minmaxIncludeMasked.checked;
    schedule();
  });
  // An empty box means no cutoff; text that is not a number clears the box.
  for (const [input, key] of [
    [minmaxLowCutoff, "minmaxLowCutoff"],
    [minmaxHighCutoff, "minmaxHighCutoff"],
  ]) {
    input?.addEventListener("change", () => {
      analysisState[key] = parseMinMaxCutoff(input.value);
      input.value = analysisState[key] === null ? "" : String(analysisState[key]);
      schedule();
    });
  }
  minmaxExportBtn?.addEventListener("click", exportCsv);
  onLanguageChange(render);

  if (minmaxModeSelect) minmaxModeSelect.value = analysisState.minmaxMode;
  if (minmaxCountInput) minmaxCountInput.value = String(analysisState.minmaxCount);
  render();

  return { schedule, render, exportCsv };
}
