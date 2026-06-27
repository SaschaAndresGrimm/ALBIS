/**
 * Rings and peak analysis orchestration.
 */

import { t } from "./i18n.js";
import { applyGeometryOverrides, getGeometryResolutionAtPixel } from "./ring_geometry_utils.js";

export function createAnalysisOverlayController({
  state,
  analysisState,
  elements,
  constants,
  callbacks,
}) {
  const {
    ringsDistance,
    ringsPixel,
    ringsEnergy,
    ringsCenterX,
    ringsCenterY,
    ringInputs,
    ringsSectionStateEl,
    ringsSummaryEl,
    peaksSectionStateEl,
    peaksSummaryEl,
    peaksBody,
    peaksCountInput,
    peaksCountHint,
  } = elements;

  const {
    defaultRingCount,
    peakBadMaskBits,
  } = constants;

  const {
    setSectionBadgeState,
    setSummaryChip,
    buildSkeletonList,
    formatStat,
    schedulePeakOverlay,
    setFieldHint,
    getActiveSaturationMax,
    isSaturatedValue,
  } = callbacks;

  let peakFinderScheduled = false;

  function parseNumericInputValue(inputEl) {
    if (!inputEl) return null;
    const raw = String(inputEl.value ?? "").trim();
    if (!raw) return null;
    const value = Number(raw);
    return Number.isFinite(value) ? value : null;
  }

  function getDefaultCenter() {
    if (Array.isArray(state.shape) && state.shape.length >= 2) {
      const width = state.shape[state.shape.length - 1];
      const height = state.shape[state.shape.length - 2];
      if (Number.isFinite(width) && Number.isFinite(height)) {
        return { x: width / 2, y: height / 2 };
      }
    }
    if (state.width && state.height) {
      return { x: state.width / 2, y: state.height / 2 };
    }
    return { x: 0, y: 0 };
  }

  function getRingParams() {
    const geometryActive = analysisState.ringMode === "geometry" && analysisState.ringGeometry;
    const distanceInput = parseNumericInputValue(ringsDistance);
    const pixelInput = parseNumericInputValue(ringsPixel);
    const energyInput = parseNumericInputValue(ringsEnergy);
    const centerX = parseNumericInputValue(ringsCenterX);
    const centerY = parseNumericInputValue(ringsCenterY);
    const distanceMm = Number.isFinite(distanceInput) ? distanceInput : analysisState.distanceMm;
    const pixelSizeUm = Number.isFinite(pixelInput) ? pixelInput : analysisState.pixelSizeUm;
    const energyEv = Number.isFinite(energyInput) ? energyInput : analysisState.energyEv;
    const centerKnown =
      Number.isFinite(centerX) ||
      Number.isFinite(centerY) ||
      Number.isFinite(analysisState.centerX) ||
      Number.isFinite(analysisState.centerY);
    const center = {
      x: Number.isFinite(centerX) ? centerX : analysisState.centerX,
      y: Number.isFinite(centerY) ? centerY : analysisState.centerY,
    };
    const fallback = getDefaultCenter();
    if (!Number.isFinite(center.x)) center.x = fallback.x;
    if (!Number.isFinite(center.y)) center.y = fallback.y;
    const maxCount = Math.max(1, ringInputs.length);
    const ringLimit = Math.max(1, Math.min(maxCount, defaultRingCount));
    const rings = ringInputs
      .map((input, index) => {
        const value = Number(input?.value || analysisState.rings[index]);
        return Number.isFinite(value) && value > 0 ? value : null;
      })
      .filter((value) => value !== null)
      .slice(0, ringLimit);
    return {
      mode: geometryActive ? "geometry" : "planar",
      geometry: geometryActive ? analysisState.ringGeometry : null,
      geometrySource: geometryActive ? String(analysisState.ringGeometrySource || "") : "",
      distanceMm: Number.isFinite(distanceMm) && distanceMm > 0 ? distanceMm : null,
      pixelSizeUm: Number.isFinite(pixelSizeUm) && pixelSizeUm > 0 ? pixelSizeUm : null,
      energyEv: Number.isFinite(energyEv) && energyEv > 0 ? energyEv : null,
      centerX: center.x,
      centerY: center.y,
      centerKnown,
      rings,
    };
  }

  function getResolutionAtPixel(ix, iy, params = getRingParams()) {
    if (!Number.isFinite(ix) || !Number.isFinite(iy)) return null;
    if (!params) return null;
    if (params.mode === "geometry" && params.geometry) {
      const geometry = applyGeometryOverrides(params.geometry, {
        centerX: params.centerX,
        centerY: params.centerY,
        distanceMm: params.distanceMm,
      });
      return getGeometryResolutionAtPixel(ix, iy, geometry, params.energyEv);
    }
    if (!params.distanceMm || !params.pixelSizeUm || !params.energyEv) return null;
    const lambda = 12398.4193 / params.energyEv;
    if (!Number.isFinite(lambda) || lambda <= 0) return null;
    const pixelSizeMm = params.pixelSizeUm / 1000;
    if (!Number.isFinite(pixelSizeMm) || pixelSizeMm <= 0) return null;

    const dxPx = ix - params.centerX;
    const dyPx = iy - params.centerY;
    const radiusPx = Math.hypot(dxPx, dyPx);
    const radiusMm = radiusPx * pixelSizeMm;
    const twoTheta = Math.atan2(radiusMm, params.distanceMm);
    const sinArg = Math.sin(twoTheta / 2);
    if (!Number.isFinite(sinArg) || sinArg <= 0) return null;
    const d = lambda / (2 * sinArg);
    return Number.isFinite(d) && d > 0 ? d : null;
  }

  function updateRingsSectionState() {
    if (!ringsSectionStateEl) return;
    if (!analysisState.ringsEnabled) {
      setSectionBadgeState(ringsSectionStateEl, "empty", t("rings.state.enable_hint"));
      setSummaryChip(ringsSummaryEl, t("summary.off"));
      return;
    }
    if (!state.hasFrame) {
      setSectionBadgeState(ringsSectionStateEl, "loading", t("rings.state.load_frame"));
      setSummaryChip(ringsSummaryEl, t("summary.waiting_frame"));
      return;
    }
    const params = getRingParams();
    const planarMissing = !params.distanceMm || !params.pixelSizeUm || !params.energyEv;
    const geometryMissing = params.mode === "geometry" && !params.energyEv;
    if ((params.mode === "geometry" && geometryMissing) || (params.mode !== "geometry" && planarMissing)) {
      setSectionBadgeState(
        ringsSectionStateEl,
        "empty",
        t("rings.state.missing_geometry"),
      );
      setSummaryChip(ringsSummaryEl, t("rings.summary.missing_geometry"), "warning");
      return;
    }
    if (!params.rings.length) {
      setSectionBadgeState(ringsSectionStateEl, "empty", t("rings.state.no_rings"));
      setSummaryChip(ringsSummaryEl, t("rings.summary.no_rings"), "warning");
      return;
    }
    setSectionBadgeState(
      ringsSectionStateEl,
      "active",
      t("rings.state.showing_count", { count: params.rings.length }),
    );
    setSummaryChip(ringsSummaryEl, t("rings.summary.count", { count: params.rings.length }), "active");
  }

  function updatePeaksSectionState() {
    if (!peaksSectionStateEl) return;
    if (!analysisState.peaksEnabled) {
      setSectionBadgeState(peaksSectionStateEl, "empty", t("peaks.state.enable_hint"));
      setSummaryChip(peaksSummaryEl, t("summary.off"));
      return;
    }
    if (!state.hasFrame) {
      setSectionBadgeState(peaksSectionStateEl, "loading", t("peaks.state.load_frame"));
      setSummaryChip(peaksSummaryEl, t("summary.waiting_frame"));
      return;
    }
    if (state.playing || state.isLoading || peakFinderScheduled) {
      setSectionBadgeState(peaksSectionStateEl, "active", t("peaks.state.active_updating"));
      setSummaryChip(peaksSummaryEl, t("summary.active"), "active");
      return;
    }
    if (!analysisState.peaks.length) {
      setSectionBadgeState(peaksSectionStateEl, "empty", t("peaks.state.none_on_frame"));
      setSummaryChip(peaksSummaryEl, t("peaks.summary.none"));
      return;
    }
    setSectionBadgeState(
      peaksSectionStateEl,
      "active",
      t("peaks.state.detected_count", { count: analysisState.peaks.length }),
    );
    setSummaryChip(
      peaksSummaryEl,
      t("peaks.summary.count", { count: analysisState.peaks.length }),
      "active",
    );
  }

  function renderPeakList() {
    if (!peaksBody) return;
    // Playback can start loading the next frame before the deferred peak-finder
    // repaint runs for the current one. Keep the table blocked only while a
    // fresh peak-detection pass is still pending.
    const peakResultsPending = analysisState.peaksEnabled && state.hasFrame && peakFinderScheduled;
    if (peakResultsPending && peaksBody.childElementCount > 0) {
      updatePeaksSectionState();
      return;
    }
    peaksBody.innerHTML = "";
    if (peakResultsPending) {
      peaksBody.appendChild(buildSkeletonList(6));
      updatePeaksSectionState();
      return;
    }
    if (!analysisState.peaksEnabled) {
      const empty = document.createElement("div");
      empty.className = "peaks-empty";
      empty.textContent = t("analysis.peaks.enable_hint");
      peaksBody.appendChild(empty);
      updatePeaksSectionState();
      return;
    }
    if (!analysisState.peaks.length) {
      const empty = document.createElement("div");
      empty.className = "peaks-empty";
      empty.textContent = state.hasFrame ? t("analysis.peaks.none_detected") : t("analysis.peaks.load_frame");
      peaksBody.appendChild(empty);
      updatePeaksSectionState();
      return;
    }
    analysisState.peaks.forEach((peak, idx) => {
      const row = document.createElement("button");
      row.type = "button";
      row.className = "peaks-row";
      const isSelected = analysisState.selectedPeaks.includes(idx);
      if (isSelected) {
        row.classList.add("is-selected");
      }
      row.innerHTML = `<span>${idx + 1}</span><span>${peak.x}</span><span>${peak.y}</span><span>${formatStat(peak.intensity)}</span>`;
      row.addEventListener("click", (event) => {
        const anchor = analysisState.peakSelectionAnchor;
        if (event.shiftKey && Number.isInteger(anchor) && anchor >= 0 && anchor < analysisState.peaks.length) {
          const start = Math.min(anchor, idx);
          const end = Math.max(anchor, idx);
          const range = [];
          for (let i = start; i <= end; i += 1) {
            range.push(i);
          }
          analysisState.selectedPeaks = range;
        } else if (event.metaKey || event.ctrlKey) {
          if (analysisState.selectedPeaks.includes(idx)) {
            analysisState.selectedPeaks = analysisState.selectedPeaks.filter((v) => v !== idx);
          } else {
            analysisState.selectedPeaks = [...analysisState.selectedPeaks, idx].sort((a, b) => a - b);
          }
          analysisState.peakSelectionAnchor = idx;
        } else if (isSelected && analysisState.selectedPeaks.length === 1) {
          analysisState.selectedPeaks = [];
          analysisState.peakSelectionAnchor = null;
        } else {
          analysisState.selectedPeaks = [idx];
          analysisState.peakSelectionAnchor = idx;
        }
        if (!Number.isInteger(analysisState.peakSelectionAnchor) && analysisState.selectedPeaks.length) {
          analysisState.peakSelectionAnchor = idx;
        }
        renderPeakList();
        schedulePeakOverlay();
      });
      peaksBody.appendChild(row);
    });
    updatePeaksSectionState();
  }

  // Background window geometry (pixels) for the local signal-to-noise test.
  // The signal is integrated over a small box around the candidate; the local
  // background is estimated from an annulus around it (outer box minus an inner
  // guard box) so the peak's own tails never contaminate the background.
  const PEAK_SIGNAL_RADIUS = 1; // 3x3 signal box
  const PEAK_BG_INNER_RADIUS = 3; // guard region excluded from the background
  const PEAK_BG_OUTER_RADIUS = 8; // outer edge of the background annulus

  // Build a padded summed-area table (integral image) so the sum/count over any
  // rectangle is four lookups regardless of window size. Invalid pixels (masked,
  // saturated, non-finite) contribute zero to both the value sum and the pixel
  // count, so the background mean is computed only over real measurements.
  // Returns null if the scratch buffers cannot be allocated (very large frames),
  // in which case the caller falls back to ranking by absolute intensity.
  function buildPeakIntegralImages(data, width, height, shouldIgnorePixel) {
    const w1 = width + 1;
    const h1 = height + 1;
    let sum;
    let count;
    try {
      sum = new Float64Array(w1 * h1);
      count = new Uint32Array(w1 * h1);
    } catch {
      return null;
    }
    for (let y = 0; y < height; y += 1) {
      const rowOff = y * width;
      const prevRow = y * w1;
      const curRow = (y + 1) * w1;
      for (let x = 0; x < width; x += 1) {
        const idx = rowOff + x;
        const v = data[idx];
        let value = 0;
        let valid = 0;
        if (Number.isFinite(v) && !shouldIgnorePixel(idx)) {
          value = v;
          valid = 1;
        }
        const col = x + 1;
        sum[curRow + col] = value + sum[curRow + x] + sum[prevRow + col] - sum[prevRow + x];
        count[curRow + col] = valid + count[curRow + x] + count[prevRow + col] - count[prevRow + x];
      }
    }
    const w = w1;
    // Inclusive rectangle [x0..x1] x [y0..y1], clamped to the image bounds.
    const rectSum = (table, x0, y0, x1, y1) => {
      const ax = x0 < 0 ? 0 : x0;
      const ay = y0 < 0 ? 0 : y0;
      const bx = (x1 >= width ? width - 1 : x1) + 1;
      const by = (y1 >= height ? height - 1 : y1) + 1;
      return table[by * w + bx] - table[ay * w + bx] - table[by * w + ax] + table[ay * w + ax];
    };
    return {
      valueSum: (x0, y0, x1, y1) => rectSum(sum, x0, y0, x1, y1),
      pixelCount: (x0, y0, x1, y1) => rectSum(count, x0, y0, x1, y1),
    };
  }

  function detectPeaks(maxPeaks, minSnr) {
    if (!state.hasFrame || !state.dataRaw || !state.width || !state.height) return [];
    const width = state.width;
    const height = state.height;
    if (width < 3 || height < 3 || maxPeaks < 1) return [];

    const data = state.dataRaw;
    const maskReady =
      state.maskEnabled &&
      state.maskAvailable &&
      state.maskRaw &&
      state.maskShape &&
      state.maskShape[0] === height &&
      state.maskShape[1] === width;
    const mask = maskReady ? state.maskRaw : null;
    const satMaskEnabled = Boolean(state.maskSaturatedEnabled);
    const satMax = getActiveSaturationMax();

    const shouldIgnorePixel = (index) => {
      if (mask && (mask[index] & peakBadMaskBits)) return true;
      if (satMaskEnabled && isSaturatedValue(data[index], satMax)) return true;
      return false;
    };

    // Local signal-to-noise gate. Real Bragg spots stand out from their *local*
    // surroundings; ranking by absolute intensity alone promotes noise that sits
    // on a bright background (around the beam stop or hot modules) and buries
    // genuine but faint reflections. The integral images make this test O(1) per
    // candidate, keeping the whole pass linear in the number of pixels.
    const snrThreshold = Number.isFinite(minSnr) && minSnr > 0 ? minSnr : 0;
    const integrals = snrThreshold > 0 ? buildPeakIntegralImages(data, width, height, shouldIgnorePixel) : null;

    function scorePeak(x, y, value) {
      // No SNR test requested (or buffers unavailable): rank by intensity.
      if (!integrals) return value;
      const bgOuter = integrals.valueSum(
        x - PEAK_BG_OUTER_RADIUS, y - PEAK_BG_OUTER_RADIUS,
        x + PEAK_BG_OUTER_RADIUS, y + PEAK_BG_OUTER_RADIUS,
      );
      const bgInner = integrals.valueSum(
        x - PEAK_BG_INNER_RADIUS, y - PEAK_BG_INNER_RADIUS,
        x + PEAK_BG_INNER_RADIUS, y + PEAK_BG_INNER_RADIUS,
      );
      const cntOuter = integrals.pixelCount(
        x - PEAK_BG_OUTER_RADIUS, y - PEAK_BG_OUTER_RADIUS,
        x + PEAK_BG_OUTER_RADIUS, y + PEAK_BG_OUTER_RADIUS,
      );
      const cntInner = integrals.pixelCount(
        x - PEAK_BG_INNER_RADIUS, y - PEAK_BG_INNER_RADIUS,
        x + PEAK_BG_INNER_RADIUS, y + PEAK_BG_INNER_RADIUS,
      );
      const bgCount = cntOuter - cntInner;
      if (bgCount < 4) return Number.NEGATIVE_INFINITY; // too little background to judge
      const bgMean = (bgOuter - bgInner) / bgCount;

      const sigSum = integrals.valueSum(
        x - PEAK_SIGNAL_RADIUS, y - PEAK_SIGNAL_RADIUS,
        x + PEAK_SIGNAL_RADIUS, y + PEAK_SIGNAL_RADIUS,
      );
      const sigCount = integrals.pixelCount(
        x - PEAK_SIGNAL_RADIUS, y - PEAK_SIGNAL_RADIUS,
        x + PEAK_SIGNAL_RADIUS, y + PEAK_SIGNAL_RADIUS,
      );
      if (sigCount < 1) return Number.NEGATIVE_INFINITY;

      // Background-subtracted integrated intensity and its Poisson variance:
      // var = var(signal counts) + (signal area)^2 * var(background mean).
      const excess = sigSum - bgMean * sigCount;
      const variance = sigSum + (sigCount * sigCount * bgMean) / bgCount;
      const noise = Math.sqrt(variance > 1 ? variance : 1);
      return excess / noise;
    }

    const candidates = [];
    const candidateLimit = Math.min(4096, Math.max(128, maxPeaks * 24));
    let minCandidateScore = Number.POSITIVE_INFINITY;
    let minCandidateIndex = -1;

    function pushCandidate(x, y, value, score) {
      if (candidates.length < candidateLimit) {
        candidates.push({ x, y, intensity: value, score });
        if (score < minCandidateScore) {
          minCandidateScore = score;
          minCandidateIndex = candidates.length - 1;
        }
        return;
      }
      if (score <= minCandidateScore || minCandidateIndex < 0) return;
      candidates[minCandidateIndex] = { x, y, intensity: value, score };
      minCandidateScore = Number.POSITIVE_INFINITY;
      minCandidateIndex = -1;
      for (let i = 0; i < candidates.length; i += 1) {
        if (candidates[i].score < minCandidateScore) {
          minCandidateScore = candidates[i].score;
          minCandidateIndex = i;
        }
      }
    }

    for (let y = 1; y < height - 1; y += 1) {
      const row = y * width;
      for (let x = 1; x < width - 1; x += 1) {
        const idx = row + x;
        const v = data[idx];
        if (!Number.isFinite(v) || v <= 0) continue;
        if (shouldIgnorePixel(idx)) continue;

        let left = data[idx - 1];
        let right = data[idx + 1];
        let up = data[idx - width];
        let down = data[idx + width];
        if (shouldIgnorePixel(idx - 1)) left = Number.NEGATIVE_INFINITY;
        if (shouldIgnorePixel(idx + 1)) right = Number.NEGATIVE_INFINITY;
        if (shouldIgnorePixel(idx - width)) up = Number.NEGATIVE_INFINITY;
        if (shouldIgnorePixel(idx + width)) down = Number.NEGATIVE_INFINITY;
        if (!Number.isFinite(left)) left = Number.NEGATIVE_INFINITY;
        if (!Number.isFinite(right)) right = Number.NEGATIVE_INFINITY;
        if (!Number.isFinite(up)) up = Number.NEGATIVE_INFINITY;
        if (!Number.isFinite(down)) down = Number.NEGATIVE_INFINITY;

        if (!(v > left && v >= right && v > up && v >= down)) continue;

        const score = scorePeak(x, y, v);
        if (integrals && score < snrThreshold) continue; // below local SNR gate
        pushCandidate(x, y, v, score);
      }
    }

    candidates.sort((a, b) => b.score - a.score);
    const selected = [];
    const minSeparation = Math.max(4, Math.round(Math.min(width, height) * 0.004));
    const minSeparationSq = minSeparation * minSeparation;
    for (let i = 0; i < candidates.length; i += 1) {
      const candidate = candidates[i];
      let tooClose = false;
      for (let j = 0; j < selected.length; j += 1) {
        const dx = candidate.x - selected[j].x;
        const dy = candidate.y - selected[j].y;
        if (dx * dx + dy * dy < minSeparationSq) {
          tooClose = true;
          break;
        }
      }
      if (!tooClose) {
        selected.push({ x: candidate.x, y: candidate.y, intensity: candidate.intensity });
        if (selected.length >= maxPeaks) break;
      }
    }
    return selected;
  }

  function runPeakFinder() {
    peakFinderScheduled = false;
    if (!analysisState.peaksEnabled) {
      analysisState.peaks = [];
      analysisState.selectedPeaks = [];
      analysisState.peakSelectionAnchor = null;
      renderPeakList();
      schedulePeakOverlay();
      return;
    }
    const requested = Math.max(1, Math.min(1000, Math.round(Number(peaksCountInput?.value || analysisState.peakCount || 25))));
    analysisState.peakCount = requested;
    if (peaksCountInput) {
      peaksCountInput.value = String(requested);
      setFieldHint(peaksCountInput, peaksCountHint, "");
    }
    const minSnr = Math.max(0, Math.min(50, Number(analysisState.peakMinSnr) || 0));
    analysisState.peaks = detectPeaks(requested, minSnr);
    analysisState.selectedPeaks = analysisState.selectedPeaks.filter(
      (idx) => Number.isInteger(idx) && idx >= 0 && idx < analysisState.peaks.length,
    );
    if (
      !Number.isInteger(analysisState.peakSelectionAnchor) ||
      analysisState.peakSelectionAnchor < 0 ||
      analysisState.peakSelectionAnchor >= analysisState.peaks.length
    ) {
      analysisState.peakSelectionAnchor = analysisState.selectedPeaks.length
        ? analysisState.selectedPeaks[analysisState.selectedPeaks.length - 1]
        : null;
    }
    if (!analysisState.peaks.length) {
      analysisState.peakSelectionAnchor = null;
    }
    renderPeakList();
    schedulePeakOverlay();
  }

  function schedulePeakFinder() {
    if (peakFinderScheduled) return;
    peakFinderScheduled = true;
    updatePeaksSectionState();
    renderPeakList();
    window.setTimeout(runPeakFinder, 0);
  }

  function exportPeakCsv() {
    if (!analysisState.peaks.length) return;
    const rows = ["index,x,y,intensity"];
    analysisState.peaks.forEach((peak, idx) => {
      rows.push(`${idx + 1},${peak.x},${peak.y},${peak.intensity}`);
    });
    const blob = new Blob([rows.join("\n")], { type: "text/csv;charset=utf-8" });
    const link = document.createElement("a");
    const url = URL.createObjectURL(blob);
    const base = (state.file || "peaks").split("/").pop().replace(/\.[^.]+$/, "");
    const thresholdSuffix = state.thresholdCount > 1 ? `_thr${state.thresholdIndex + 1}` : "";
    link.href = url;
    link.download = `${base}_frame_${state.frameIndex + 1}${thresholdSuffix}_peaks.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  }

  return {
    getDefaultCenter,
    getRingParams,
    getResolutionAtPixel,
    updateRingsSectionState,
    updatePeaksSectionState,
    renderPeakList,
    detectPeaks,
    runPeakFinder,
    schedulePeakFinder,
    exportPeakCsv,
  };
}
