/**
 * Animated GIF export workflow.
 *
 * Renders a frame range of the current image series (or multi-frame HDF5
 * dataset) into an animated GIF that matches the on-screen view: the active
 * colormap, contrast (BG/FG), invert, mask and saturation highlighting are all
 * applied per frame. Frames are fetched independently of the live viewer so the
 * export never disturbs the user's current frame, and are streamed into the
 * encoder one at a time to keep memory bounded.
 */

import { t } from "./i18n.js";
import { GifWriter } from "./gif_encoder.js";
import { SATURATED_PIXEL_RGBA } from "./viewer_overlay_colors.js";

// Number of colormap colours reserved in the GIF global colour table. The
// remaining three slots carry the mask (black + flagged blue) and saturation
// highlight colours, keeping the table within the 256-entry GIF limit.
const COLORMAP_STEPS = 252;
const MASK_BLACK_RGB = [0, 0, 0];
const MASK_FLAG_RGB = [25, 50, 120];
const GIF_TYPES = [{ accept: { "image/gif": [".gif"] } }];

export function createAnimationExportController({
  apiBase,
  state,
  elements,
  callbacks,
}) {
  const {
    modal,
    closeBtn,
    source,
    frameMode,
    rangeStartField,
    rangeEndField,
    rangeStart,
    rangeEnd,
    frameStep,
    regionSelect,
    fpsSelect,
    loopCheckbox,
    scaleSelect,
    summary,
    progress,
    progressFill,
    progressText,
    startBtn,
    cancelBtn,
  } = elements;

  const {
    buildPalette,
    getPaletteColorCount,
    mapValueToNorm,
    getActiveSaturationMax,
    isSaturatedValue,
    parseDtype,
    parseShape,
    typedArrayFrom,
    getVisibleRegion,
    openModal,
    closeModal,
    setStatus,
  } = callbacks;

  let activeController = null;

  function isReady() {
    const total = Math.round(Number(state.frameCount) || 1);
    if (total <= 1) return false;
    const hasSeries = Array.isArray(state.seriesFiles) && state.seriesFiles.length > 0;
    return Boolean(state.file && (hasSeries || state.dataset));
  }

  function fileStem() {
    const raw = state.file ? String(state.file) : "";
    const base = raw ? raw.replace(/.*[\\/]/, "").replace(/\.[^.]+$/, "") : "animation";
    return base || "animation";
  }

  function selectedFrameNumbers() {
    const total = Math.max(1, Math.round(Number(state.frameCount) || 1));
    const mode = String(frameMode?.value || "all").toLowerCase();
    let start = 1;
    let end = total;
    if (mode === "range") {
      start = Math.max(1, Math.min(total, Math.round(Number(rangeStart?.value || 1))));
      end = Math.max(1, Math.min(total, Math.round(Number(rangeEnd?.value || total))));
    }
    if (start > end) return [];
    const step = Math.max(1, Math.round(Number(frameStep?.value || 1)));
    const frames = [];
    for (let f = start; f <= end; f += step) frames.push(f - 1);
    return frames;
  }

  function selectedRegion() {
    const useVisible = String(regionSelect?.value || "full") === "visible";
    if (useVisible) {
      const region = getVisibleRegion?.();
      if (region && region.width > 0 && region.height > 0) return region;
    }
    return { x: 0, y: 0, width: state.width, height: state.height };
  }

  function selectedScale() {
    const value = Number(scaleSelect?.value || 1);
    return Number.isFinite(value) && value > 0 ? value : 1;
  }

  function outputSize(region, scale) {
    const width = Math.max(1, Math.round(region.width * scale));
    const height = Math.max(1, Math.round(region.height * scale));
    return { width, height };
  }

  function formatBytes(bytes) {
    if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
    if (bytes >= 1024) return `${Math.round(bytes / 1024)} KB`;
    return `${Math.round(bytes)} B`;
  }

  function updateSummary() {
    if (!summary) return;
    if (!isReady()) {
      summary.textContent = t("animation_export.summary.empty");
      return;
    }
    const frames = selectedFrameNumbers();
    if (!frames.length) {
      summary.textContent = t("animation_export.summary.empty");
      return;
    }
    const region = selectedRegion();
    const { width, height } = outputSize(region, selectedScale());
    // Rough estimate: colormapped frames compress well under LZW; ~0.5 B/px is a
    // deliberately conservative heuristic, hence the "~" in the localized string.
    const estBytes = frames.length * width * height * 0.5;
    summary.textContent = t("animation_export.summary", {
      frames: frames.length,
      width,
      height,
      size: formatBytes(estBytes),
    });
  }

  function setProgress(value, text) {
    const clamped = Number.isFinite(value) ? Math.max(0, Math.min(1, value)) : 0;
    if (progress) progress.classList.toggle("is-loading", Boolean(state.animationExport.running));
    if (progressFill) progressFill.style.width = `${(clamped * 100).toFixed(1)}%`;
    if (progressText) {
      progressText.textContent = state.animationExport.running
        ? `${Math.round(clamped * 100)}%  ${text || ""}`.trim()
        : text || t("animation_export.progress.idle");
    }
  }

  function updateUi() {
    const ready = isReady();
    const running = Boolean(state.animationExport.running);
    const showRange = String(frameMode?.value || "all").toLowerCase() === "range";
    const total = Math.max(1, Math.round(Number(state.frameCount) || 1));

    if (source) {
      source.textContent = state.file
        ? `${state.file}${state.dataset ? `  ${state.dataset}` : ""}`
        : t("animation_export.source.none");
    }
    rangeStartField?.classList.toggle("is-hidden", !showRange);
    rangeEndField?.classList.toggle("is-hidden", !showRange);
    if (rangeStart) {
      rangeStart.min = "1";
      rangeStart.max = String(total);
      if (!rangeStart.value) rangeStart.value = "1";
    }
    if (rangeEnd) {
      rangeEnd.min = "1";
      rangeEnd.max = String(total);
      if (!rangeEnd.value) rangeEnd.value = String(total);
    }
    [frameMode, frameStep, regionSelect, fpsSelect, loopCheckbox, scaleSelect].forEach((el) => {
      if (el) el.disabled = running || !ready;
    });
    if (rangeStart) rangeStart.disabled = running || !ready || !showRange;
    if (rangeEnd) rangeEnd.disabled = running || !ready || !showRange;
    if (startBtn) {
      startBtn.disabled = running || !ready;
      startBtn.textContent = running
        ? t("animation_export.button.exporting")
        : t("animation_export.button.export");
    }
    if (cancelBtn) {
      cancelBtn.classList.toggle("is-hidden", !running);
      cancelBtn.disabled = !running || state.animationExport.cancelling;
      cancelBtn.textContent = state.animationExport.cancelling
        ? t("animation_export.button.cancelling")
        : t("animation_export.button.cancel");
    }
    updateSummary();
  }

  // Build the GIF global colour table from the active colormap, sampled down to
  // COLORMAP_STEPS entries, plus the reserved mask/saturation colours.
  function buildGifPalette() {
    const src = buildPalette(state.colormap);
    const srcCount = Math.max(1, getPaletteColorCount(src));
    const palette = new Uint8Array((COLORMAP_STEPS + 3) * 3);
    for (let i = 0; i < COLORMAP_STEPS; i += 1) {
      const tNorm = COLORMAP_STEPS > 1 ? i / (COLORMAP_STEPS - 1) : 0;
      const srcIdx = Math.min(srcCount - 1, Math.round(tNorm * (srcCount - 1))) * 4;
      palette[i * 3] = src[srcIdx];
      palette[i * 3 + 1] = src[srcIdx + 1];
      palette[i * 3 + 2] = src[srcIdx + 2];
    }
    const black = COLORMAP_STEPS * 3;
    palette[black] = MASK_BLACK_RGB[0];
    palette[black + 1] = MASK_BLACK_RGB[1];
    palette[black + 2] = MASK_BLACK_RGB[2];
    const flag = (COLORMAP_STEPS + 1) * 3;
    palette[flag] = MASK_FLAG_RGB[0];
    palette[flag + 1] = MASK_FLAG_RGB[1];
    palette[flag + 2] = MASK_FLAG_RGB[2];
    const sat = (COLORMAP_STEPS + 2) * 3;
    palette[sat] = SATURATED_PIXEL_RGBA.byte[0];
    palette[sat + 1] = SATURATED_PIXEL_RGBA.byte[1];
    palette[sat + 2] = SATURATED_PIXEL_RGBA.byte[2];
    return palette;
  }

  // Convert one decoded frame into palette indices for the chosen region/scale,
  // mirroring renderRegionToCanvas' per-pixel decision order.
  function frameToIndices(data, frameWidth, frameHeight, region, out, ow, oh) {
    const idxBlack = COLORMAP_STEPS;
    const idxFlag = COLORMAP_STEPS + 1;
    const idxSat = COLORMAP_STEPS + 2;
    const satMax = getActiveSaturationMax();
    const maskSaturatedEnabled = Boolean(state.maskSaturatedEnabled && Number.isFinite(satMax));
    const maskReady =
      state.maskEnabled &&
      state.maskAvailable &&
      state.maskRaw &&
      state.maskShape &&
      state.maskShape[0] === frameHeight &&
      state.maskShape[1] === frameWidth;
    const maskData = maskReady ? state.maskRaw : null;

    for (let oy = 0; oy < oh; oy += 1) {
      const srcY = region.y + Math.min(region.height - 1, Math.floor((oy * region.height) / oh));
      const rowOffset = srcY * frameWidth;
      const outRow = oy * ow;
      for (let ox = 0; ox < ow; ox += 1) {
        const srcX = region.x + Math.min(region.width - 1, Math.floor((ox * region.width) / ow));
        const srcIdx = rowOffset + srcX;
        const v = data[srcIdx];
        let outIdx;
        if (maskData) {
          const mv = maskData[srcIdx];
          if (mv & 1) outIdx = idxBlack;
          else if (mv & 0x1e) outIdx = idxFlag;
        }
        if (outIdx === undefined) {
          if (maskSaturatedEnabled && isSaturatedValue(v, satMax)) {
            outIdx = idxSat;
          } else {
            const norm = mapValueToNorm(v);
            const clamped = norm < 0 ? 0 : norm > 1 ? 1 : norm;
            outIdx = Math.min(COLORMAP_STEPS - 1, Math.floor(clamped * (COLORMAP_STEPS - 1)));
          }
        }
        out[outRow + ox] = outIdx;
      }
    }
  }

  function frameUrl(frameNumber) {
    const hasSeries = Array.isArray(state.seriesFiles) && state.seriesFiles.length > 0;
    if (hasSeries) {
      const file = state.seriesFiles[frameNumber];
      return file ? `${apiBase}/image?file=${encodeURIComponent(file)}` : null;
    }
    if (!state.dataset) return null;
    const threshold = Number(state.thresholdCount) > 1 ? `&threshold=${state.thresholdIndex}` : "";
    return `${apiBase}/frame?file=${encodeURIComponent(state.file)}&dataset=${encodeURIComponent(
      state.dataset
    )}&index=${frameNumber}${threshold}`;
  }

  async function fetchFrame(frameNumber, signal) {
    const url = frameUrl(frameNumber);
    if (!url) return null;
    const res = await fetch(url, { signal, cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const buffer = await res.arrayBuffer();
    const dtype = parseDtype(res.headers.get("X-Dtype"));
    const shape = parseShape(res.headers.get("X-Shape"));
    return { data: typedArrayFrom(buffer, dtype), height: shape[0], width: shape[1] };
  }

  async function renderFrames(frames, region, scale, fps, loop) {
    const palette = buildGifPalette();
    let writer = null;
    let ow = 0;
    let oh = 0;
    let indices = null;
    const delayCs = Math.max(2, Math.round(100 / Math.max(1, fps)));
    activeController = new AbortController();

    for (let i = 0; i < frames.length; i += 1) {
      if (state.animationExport.cancelling) return null;
      setProgress(
        i / frames.length,
        t("animation_export.progress.rendering", { current: i + 1, total: frames.length })
      );
      // Yield so the progress bar repaints between frames.
      await new Promise((resolve) => window.setTimeout(resolve, 0));
      const frame = await fetchFrame(frames[i], activeController.signal);
      if (!frame) continue;
      // Guard against a frame whose dimensions differ from the region's basis.
      const safeRegion =
        frame.width === state.width && frame.height === state.height
          ? region
          : { x: 0, y: 0, width: frame.width, height: frame.height };
      if (!writer) {
        const size = outputSize(safeRegion, scale);
        ow = size.width;
        oh = size.height;
        indices = new Uint8Array(ow * oh);
        writer = new GifWriter({ width: ow, height: oh, palette, loop });
      }
      frameToIndices(frame.data, frame.width, frame.height, safeRegion, indices, ow, oh);
      writer.addFrame(indices, delayCs);
    }
    if (!writer) return null;
    setProgress(1, t("animation_export.progress.encoding"));
    return writer.finish();
  }

  async function saveGif(bytes, handle, suggestedName) {
    const blob = new Blob([bytes], { type: "image/gif" });
    if (handle) {
      const writable = await handle.createWritable();
      await writable.write(blob);
      await writable.close();
      setStatus(t("status.animation_export.saved", { filename: handle.name }), { tone: "success" });
      return;
    }
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = suggestedName;
    a.click();
    URL.revokeObjectURL(a.href);
    setStatus(t("status.animation_export.saved", { filename: suggestedName }), { tone: "success" });
  }

  async function startExport() {
    if (state.animationExport.running) return;
    if (!isReady()) {
      setStatus(t("status.animation_export.not_series"));
      return;
    }
    const frames = selectedFrameNumbers();
    if (!frames.length) {
      setStatus(t("status.animation_export.range_invalid"));
      return;
    }
    const region = selectedRegion();
    const scale = selectedScale();
    const fps = Math.max(1, Number(fpsSelect?.value || state.fps || 5));
    // loop 0 = repeat forever (Netscape block); -1 = omit the block, play once.
    const loop = loopCheckbox && !loopCheckbox.checked ? -1 : 0;
    const suggestedName = `${fileStem()}_animation.gif`;

    // Open the save picker first, while we still hold the click's user
    // activation; the (slow) frame rendering then runs before we write.
    let handle = null;
    if (typeof window.showSaveFilePicker === "function") {
      try {
        handle = await window.showSaveFilePicker({ suggestedName, types: GIF_TYPES });
      } catch (err) {
        if (err && err.name === "AbortError") return; // user cancelled
        handle = null; // unsupported context -> anchor download fallback
      }
    }

    state.animationExport.running = true;
    state.animationExport.cancelling = false;
    setProgress(0, t("animation_export.progress.rendering", { current: 0, total: frames.length }));
    updateUi();
    try {
      const bytes = await renderFrames(frames, region, scale, fps, loop);
      if (state.animationExport.cancelling || !bytes) {
        setStatus(t("status.animation_export.cancelled"));
        return;
      }
      await saveGif(bytes, handle, suggestedName);
    } catch (err) {
      if (err?.name !== "AbortError") {
        console.error(err);
        setStatus(t("status.animation_export.failed"), { tone: "error" });
      } else {
        setStatus(t("status.animation_export.cancelled"));
      }
    } finally {
      state.animationExport.running = false;
      state.animationExport.cancelling = false;
      activeController = null;
      setProgress(0, t("animation_export.progress.idle"));
      updateUi();
    }
  }

  function cancelExport() {
    if (!state.animationExport.running || state.animationExport.cancelling) return;
    state.animationExport.cancelling = true;
    if (activeController) {
      try {
        activeController.abort();
      } catch {
        // ignore
      }
    }
    updateUi();
  }

  function openDialog() {
    if (!state.file) {
      setStatus(t("status.animation_export.no_file"));
      return;
    }
    if (!isReady()) {
      setStatus(t("status.animation_export.not_series"));
      return;
    }
    if (fpsSelect && !state.animationExport.running) {
      const fps = String(Math.max(1, Math.round(Number(state.fps) || 5)));
      if (Array.from(fpsSelect.options).some((opt) => opt.value === fps)) {
        fpsSelect.value = fps;
      }
    }
    if (!state.animationExport.running) {
      setProgress(0, t("animation_export.progress.idle"));
    }
    updateUi();
    openModal(modal, { focusTarget: startBtn });
  }

  function closeDialog() {
    closeModal(modal);
  }

  closeBtn?.addEventListener("click", closeDialog);
  modal?.addEventListener("click", (event) => {
    if (event.target === modal || event.target.classList?.contains("modal-backdrop")) {
      closeDialog();
    }
  });
  frameMode?.addEventListener("change", updateUi);
  [rangeStart, rangeEnd, frameStep, regionSelect, scaleSelect].forEach((el) => {
    el?.addEventListener("change", updateSummary);
    el?.addEventListener("input", updateSummary);
  });
  startBtn?.addEventListener("click", startExport);
  cancelBtn?.addEventListener("click", cancelExport);

  return {
    openDialog,
    closeDialog,
    updateUi,
  };
}
