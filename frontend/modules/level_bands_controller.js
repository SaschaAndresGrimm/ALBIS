/**
 * The Below BG / BG to FG / Above FG rows of the statistics: how many of the
 * pixels the statistics count lie below, between and above the levels set in
 * View -> Histogram.
 *
 * They count what the rest of the panel counts: the ROI's pixels while one is
 * drawn, the whole frame otherwise, masked and saturated pixels left out as
 * the statistics leave them out.
 *
 * The pixels are prepared once per frame or ROI into a "level sample" (see
 * level_bands_utils.js), and refresh() counts that against the current BG and
 * FG. refresh() runs on every redraw, so it returns at once when neither the
 * levels nor the pixels changed. What costs a pass over a large frame waits
 * until things have been still for SETTLE_MS instead of running on every step
 * of a drag or every frame of playback:
 *
 * - counting a float sample of more than LARGE_PIXELS values against new
 *   levels (an integer sample is two lookups and always counts at once);
 * - building the whole frame's sample when the frame has more than
 *   LARGE_PIXELS pixels. Until it is built the rows show "…", not the
 *   previous frame's numbers.
 *
 * The labels carry the levels the numbers were counted against ("Below BG
 * 12", "BG 12 to FG 340", "Above FG 340"), formatted as the histogram shows
 * them, so the counts never have to be read against another tab.
 */

import { formatLevelValue } from "./format_utils.js";
import { onLanguageChange, t } from "./i18n.js";
import { countLevelBands, formatLevelBand } from "./level_bands_utils.js";

export const LARGE_PIXELS = 2_000_000;
export const SETTLE_MS = 150;
const PENDING = "…";

const LABEL_KEYS = {
  below: "analysis.roi.stats.below_bg",
  between: "analysis.roi.stats.bg_to_fg",
  above: "analysis.roi.stats.above_fg",
};

const sameParts = (a, b) => Boolean(a && b) && a.length === b.length && a.every((part, i) => part === b[i]);

export function createLevelBandsController({ state, roiState, elements, callbacks }) {
  const { below: belowEl, between: betweenEl, above: aboveEl } = elements || {};
  // wholeFrameKey(): what the whole frame's sample depends on, as a list
  // compared part by part.
  const { getWholeFrameSample, wholeFrameKey } = callbacks;
  // Each value's label: the span beside it in its row.
  const labelOf = (el) => el?.closest(".meta-row")?.querySelector("span") || null;
  const labels = { below: labelOf(belowEl), between: labelOf(betweenEl), above: labelOf(aboveEl) };
  let source = null; // { sample } for an ROI, or "frame"
  let frameSample = null;
  let frameKey = null;
  let counted = null; // { sample, bg, fg }
  let timer = null;

  function setText(text) {
    if (belowEl) belowEl.textContent = text("below");
    if (betweenEl) betweenEl.textContent = text("between");
    if (aboveEl) aboveEl.textContent = text("above");
  }

  // With the levels when there are counts to go with them, plain otherwise.
  function setLabels(counts) {
    const bg = counts ? formatLevelValue(counts.bg, state.dtype) : "";
    const fg = counts ? formatLevelValue(counts.fg, state.dtype) : "";
    for (const [band, label] of Object.entries(labels)) {
      if (label) label.textContent = counts ? t(`${LABEL_KEYS[band]}_at`, { bg, fg }) : t(LABEL_KEYS[band]);
    }
  }

  function show(counts) {
    roiState.levelBands = counts;
    setText((band) => (counts ? formatLevelBand(counts[band], counts.total) : "-"));
    setLabels(counts);
  }

  function showPending() {
    roiState.levelBands = null;
    counted = null;
    setText(() => PENDING);
    setLabels(null);
  }

  function later(run) {
    if (timer) window.clearTimeout(timer);
    timer = window.setTimeout(() => {
      timer = null;
      run();
    }, SETTLE_MS);
  }

  function count(sample, bg, fg) {
    counted = { sample, bg, fg };
    show({ ...countLevelBands(sample, bg, fg), bg, fg });
  }

  // The whole frame's sample, or null while a large frame's waits to be built.
  function wholeFrame(settled) {
    const key = wholeFrameKey();
    if (sameParts(key, frameKey)) return frameSample;
    if (!settled && (state.dataRaw?.length || 0) > LARGE_PIXELS) {
      showPending();
      later(() => refresh(true));
      return null;
    }
    frameSample = getWholeFrameSample();
    frameKey = key;
    return frameSample;
  }

  function refresh(settled = false) {
    if (!source || !state.hasFrame) return;
    const sample = source === "frame" ? wholeFrame(settled) : source.sample;
    if (!sample) return;
    const bg = state.min;
    const fg = state.max;
    if (!Number.isFinite(bg) || !Number.isFinite(fg)) {
      show(null);
      return;
    }
    if (counted && counted.sample === sample && counted.bg === bg && counted.fg === fg) return;
    const newSample = !counted || counted.sample !== sample;
    // A new sample is counted at once: the numbers on screen belong to the
    // previous frame or ROI.
    if (settled || newSample || sample.kind === "table" || sample.total <= LARGE_PIXELS) {
      if (timer) window.clearTimeout(timer);
      timer = null;
      count(sample, bg, fg);
    } else {
      later(() => refresh(true));
    }
  }

  // A language change rewrites the labels from their data-i18n keys first.
  onLanguageChange(() => setLabels(roiState.levelBands));

  return {
    /** The ROI's counted values, as a level sample (buildLevelSample). */
    showRoi(sample) {
      source = { sample };
      refresh();
    },
    showWholeFrame() {
      source = "frame";
      refresh();
    },
    clear() {
      source = null;
      counted = null;
      if (timer) window.clearTimeout(timer);
      timer = null;
      show(null);
    },
    refresh: () => refresh(),
  };
}
