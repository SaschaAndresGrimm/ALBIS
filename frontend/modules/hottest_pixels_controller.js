/**
 * Beta: Analysis -> Hottest Pixels, the brightest pixels of the frame on
 * screen, listed and marked on the image.
 *
 * Shown only while `ui.hottest_pixels` is on (Settings -> Tools). It answers a
 * detector question rather than a diffraction one -- which pixels read the
 * most -- so it takes none of the Peak Finder's tests for a spot (see
 * hottest_pixels_utils.js). Masked pixels, and saturated ones while Mask
 * saturated is on, are left out as the Peak Finder leaves them out, unless
 * "Include masked pixels" is ticked: a hot pixel that is masked already is a
 * fair thing to look for.
 *
 * It runs whenever the Peak Finder would (a new frame, a mask change): the
 * Peak Finder's schedule calls schedule() here, so there is no second list of
 * places that change the frame to keep in step.
 */

import { onLanguageChange, t } from "./i18n.js";
import { setControlAvailability } from "./control_availability.js";
import { buildCsvTable, csvProvenanceLines } from "./csv_export_utils.js";
import { clampHottestPixelCount, findHottestPixels } from "./hottest_pixels_utils.js";

export function createHottestPixelsController({ core, state, analysisState, elements, constants, callbacks }) {
  const {
    hottestSection,
    hottestToggle,
    hottestStateEl,
    hottestSummaryEl,
    hottestCountInput,
    hottestIncludeMasked,
    hottestExportBtn,
    hottestList,
    hottestBody,
  } = elements;
  const { badMaskBits } = constants;
  const { setSectionBadgeState, setSummaryChip, formatStat, getActiveSaturationMax, isSaturatedValue, setStatus } =
    callbacks;

  let scheduled = false;

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
    if (analysisState.hottestIncludeMasked) return null;
    const mask = state.maskEnabled ? frameMask() : null;
    const satMax = state.maskSaturatedEnabled ? getActiveSaturationMax() : null;
    if (!mask && !Number.isFinite(satMax)) return null;
    return (index) => Boolean(mask && mask[index] & badMaskBits) || isSaturatedValue(data[index], satMax);
  }

  function find() {
    if (!analysisState.hottestEnabled || !state.hasFrame || !state.dataRaw) return [];
    const mask = frameMask();
    return findHottestPixels(state.dataRaw, state.width, state.height, {
      count: analysisState.hottestCount,
      ignore: ignoreFilter(state.dataRaw),
    }).map((pixel) => ({ ...pixel, mask: mask ? mask[pixel.index] : null }));
  }

  // Why there is nothing to export, or "" when there is: the same states the
  // badge describes.
  function unavailableReason() {
    if (!analysisState.hottestEnabled) return t("hottest.state.enable_hint");
    if (!state.hasFrame) return t("hottest.state.load_frame");
    if (!analysisState.hottestFound.length) return t("hottest.state.none_on_frame");
    return "";
  }

  function updateSectionState() {
    setControlAvailability(hottestExportBtn, unavailableReason());
    const count = analysisState.hottestFound.length;
    if (!analysisState.hottestEnabled) {
      setSectionBadgeState(hottestStateEl, "empty", t("hottest.state.enable_hint"));
      setSummaryChip(hottestSummaryEl, t("summary.off"));
    } else if (!state.hasFrame) {
      setSectionBadgeState(hottestStateEl, "loading", t("hottest.state.load_frame"));
      setSummaryChip(hottestSummaryEl, t("summary.waiting_frame"));
    } else if (!count) {
      setSectionBadgeState(hottestStateEl, "empty", t("hottest.state.none_on_frame"));
      setSummaryChip(hottestSummaryEl, t("hottest.summary.none"));
    } else {
      // The chip already says how many; the line names the brightest one.
      const [top] = analysisState.hottestFound;
      setSectionBadgeState(
        hottestStateEl,
        "active",
        t("hottest.state.brightest", { value: formatStat(top.value), x: top.x, y: top.y }),
      );
      setSummaryChip(hottestSummaryEl, t("hottest.summary.count", { count }), "active");
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
    const selected = analysisState.hottestSelected;
    const anchor = analysisState.hottestSelectionAnchor;
    if (event.shiftKey && Number.isInteger(anchor) && anchor < analysisState.hottestFound.length) {
      const start = Math.min(anchor, idx);
      analysisState.hottestSelected = Array.from({ length: Math.abs(anchor - idx) + 1 }, (_, i) => start + i);
    } else if (event.metaKey || event.ctrlKey) {
      analysisState.hottestSelected = selected.includes(idx)
        ? selected.filter((value) => value !== idx)
        : [...selected, idx].sort((a, b) => a - b);
      analysisState.hottestSelectionAnchor = idx;
    } else if (selected.length === 1 && selected[0] === idx) {
      analysisState.hottestSelected = [];
      analysisState.hottestSelectionAnchor = null;
    } else {
      analysisState.hottestSelected = [idx];
      analysisState.hottestSelectionAnchor = idx;
    }
    render();
    core.overlayRender.schedulePeakOverlay();
  }

  function render() {
    updateSectionState();
    if (!hottestBody) return;
    hottestBody.innerHTML = "";
    const found = analysisState.hottestFound;
    if (!found.length) {
      hottestBody.appendChild(emptyRow(unavailableReason()));
      return;
    }
    const hasMask = found.some((pixel) => pixel.mask !== null);
    hottestList?.classList.toggle("has-mask", hasMask);
    found.forEach((pixel, idx) => {
      const row = document.createElement("button");
      row.type = "button";
      row.className = "peaks-row";
      const selected = analysisState.hottestSelected.includes(idx);
      row.classList.toggle("is-selected", selected);
      row.setAttribute("aria-pressed", String(selected));
      const maskText = pixel.mask ? String(pixel.mask) : "—";
      const value = formatStat(pixel.value);
      row.setAttribute(
        "aria-label",
        `#${idx + 1}, X ${pixel.x}, Y ${pixel.y}, ${t("hottest.column.value")} ${value}` +
          (hasMask ? `, ${t("hottest.column.mask")} ${maskText}` : ""),
      );
      row.innerHTML = `<span>${idx + 1}</span><span>${pixel.x}</span><span>${pixel.y}</span><span>${value}</span><span>${maskText}</span>`;
      row.addEventListener("click", (event) => select(idx, event));
      hottestBody.appendChild(row);
    });
  }

  function run() {
    scheduled = false;
    // Keep the selection on the same pixels while they are still among the
    // hottest: a new frame reorders the list.
    const rowOf = (found, row) => found[row]?.index;
    const before = analysisState.hottestFound;
    const picked = new Set(analysisState.hottestSelected.map((row) => rowOf(before, row)));
    const anchorPixel = rowOf(before, analysisState.hottestSelectionAnchor);
    analysisState.hottestFound = find();
    analysisState.hottestSelected = [];
    analysisState.hottestSelectionAnchor = null;
    analysisState.hottestFound.forEach((pixel, row) => {
      if (picked.has(pixel.index)) analysisState.hottestSelected.push(row);
      if (pixel.index === anchorPixel) analysisState.hottestSelectionAnchor = row;
    });
    render();
    core.overlayRender.schedulePeakOverlay();
  }

  function schedule() {
    if (scheduled || (!analysisState.hottestEnabled && !analysisState.hottestFound.length)) return;
    scheduled = true;
    window.setTimeout(run, 0);
  }

  /** Show or hide the section; hiding it switches the tool off. */
  function setEnabled(enabled) {
    if (hottestSection) hottestSection.hidden = !enabled;
    if (enabled || !hottestToggle?.checked) return;
    hottestToggle.checked = false;
    hottestToggle.dispatchEvent(new window.Event("change", { bubbles: true }));
  }

  function exportCsv() {
    const unavailable = unavailableReason();
    if (unavailable) {
      setStatus?.(unavailable, { tone: "warning" });
      return;
    }
    const found = analysisState.hottestFound;
    const hasMask = found.some((pixel) => pixel.mask !== null);
    // Column names stay in English, as the peak list's do: scripts read them.
    const columns = [
      { label: "index", values: found.map((_, idx) => idx + 1) },
      { label: "x", values: found.map((pixel) => pixel.x) },
      { label: "y", values: found.map((pixel) => pixel.y) },
      { label: "value", values: found.map((pixel) => pixel.value) },
    ];
    if (hasMask) columns.push({ label: "mask", values: found.map((pixel) => pixel.mask ?? "") });
    const lines = [...csvProvenanceLines(state, [`Hottest pixels: ${found.length}`]), ...buildCsvTable(columns)];
    const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
    const link = document.createElement("a");
    const url = URL.createObjectURL(blob);
    const base = (state.file || "frame").split("/").pop().replace(/\.[^.]+$/, "");
    const thresholdSuffix = state.thresholdCount > 1 ? `_thr${state.thresholdIndex + 1}` : "";
    link.href = url;
    link.download = `${base}_frame_${state.frameIndex + 1}${thresholdSuffix}_hottest_pixels.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  }

  hottestToggle?.addEventListener("change", () => {
    analysisState.hottestEnabled = hottestToggle.checked;
    analysisState.hottestSelected = [];
    analysisState.hottestSelectionAnchor = null;
    if (!analysisState.hottestEnabled) analysisState.hottestFound = [];
    run();
  });
  hottestCountInput?.addEventListener("change", () => {
    analysisState.hottestCount = clampHottestPixelCount(hottestCountInput.value, analysisState.hottestCount);
    hottestCountInput.value = String(analysisState.hottestCount);
    schedule();
  });
  hottestIncludeMasked?.addEventListener("change", () => {
    analysisState.hottestIncludeMasked = hottestIncludeMasked.checked;
    schedule();
  });
  hottestExportBtn?.addEventListener("click", exportCsv);
  onLanguageChange(render);

  if (hottestCountInput) hottestCountInput.value = String(analysisState.hottestCount);
  setEnabled(Boolean(state.hottestPixels));
  render();

  return { schedule, setEnabled, render, exportCsv };
}
