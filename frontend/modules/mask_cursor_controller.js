/**
 * Mask lifecycle, histogram tooltip, and cursor probe interactions.
 */

import { t } from "./i18n.js";
import { fetchResponse, readHeaderText } from "./http.js";
import { normalizeSimplonUrlInput } from "./simplon_url_utils.js";

export function createMaskCursorController({
  apiBase,
  state,
  elements,
  callbacks,
}) {
  const {
    canvasWrap,
    canvasShell,
    cursorOverlay,
    histTooltip,
    maskToggle,
    maskSaturatedToggle,
    simplonUrl,
    simplonVersion,
  } = elements;

  const {
    isHdfFile,
    parseDtype,
    parseShape,
    typedArrayFrom,
    getActiveSaturationMax,
    updateGlobalStats,
    redraw,
    scheduleRoiUpdate,
    getDtypeInfo,
    formatValue,
    isSaturatedValue,
    getInpaintedFrame = () => null,
    getCursorClearance = () => 0,
    getResolutionAtPixel,
    getEffectiveScrollLeft,
    getEffectiveScrollTop,
    setAutoloadStatus,
    // Data -> Pixel Mask (beta): a mask the user chose, used instead of the
    // data's own until they go back to it; null when there is none.
    getPinnedMask = () => null,
    onMaskChanged = () => {},
    // " · masked by you" for a pixel changed by hand, or "".
    getCursorNote = () => "",
  } = callbacks;

  const PINNED_MASK_KEY = "__pixel_mask__";

  function formatCursorValue(value) {
    if (!Number.isFinite(value)) return "";
    const dtypeInfo = getDtypeInfo(state.dtype || "");
    if (dtypeInfo?.kind === "f" && Number.isInteger(value)) {
      return String(value);
    }
    return formatValue(value);
  }

  function getImagePointFromEvent(event, options = {}) {
    if (!state.hasFrame || !canvasWrap) return null;
    const { allowOutside = false, allowOutsideViewport = false } = options;
    const rect = canvasWrap.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const y = event.clientY - rect.top;
    if (!allowOutsideViewport && (x < 0 || y < 0 || x > rect.width || y > rect.height)) return null;
    const zoom = state.zoom || 1;
    const zoomY = zoom * (state.pixelAspect || 1);
    const offsetX = state.renderOffsetX || 0;
    const offsetY = state.renderOffsetY || 0;
    const imgX = (getEffectiveScrollLeft() + x - offsetX) / zoom;
    const imgY = (getEffectiveScrollTop() + y - offsetY) / zoomY;
    if (allowOutside) {
      return { x: Math.floor(imgX), y: Math.floor(imgY) };
    }
    // ROI selection should use pixel-cell containment (same convention as cursor readout),
    // not nearest-center rounding.
    const clampedX = Math.max(0, Math.min(state.width - Number.EPSILON, imgX));
    const clampedY = Math.max(0, Math.min(state.height - Number.EPSILON, imgY));
    const ix = Math.floor(clampedX);
    const iy = Math.floor(clampedY);
    return { x: ix, y: iy };
  }

  function normalizeMaskData(data) {
    if (!data) return null;
    if (data instanceof Uint32Array) return data;
    const out = new Uint32Array(data.length);
    for (let i = 0; i < data.length; i += 1) {
      out[i] = data[i];
    }
    return out;
  }

  function buildNegativeMask(data) {
    if (!data || !data.length) return null;
    let hasMask = false;
    const mask = new Uint32Array(data.length);
    for (let i = 0; i < data.length; i += 1) {
      const value = data[i];
      if (!Number.isFinite(value)) continue;
      if (value < 0) {
        hasMask = true;
        mask[i] = value === -1 ? 1 : 0x1e;
      }
    }
    return hasMask ? mask : null;
  }

  // Flag whole columns (x = channel) as defective across every row (y = frame).
  // Used for strip-detector bad channels; `base` is OR-combined when provided.
  function buildColumnMask(width, height, columns, base = null) {
    if (!width || !height || !Array.isArray(columns) || !columns.length) {
      return base;
    }
    const mask = base || new Uint32Array(width * height);
    let applied = false;
    for (const col of columns) {
      const x = Math.trunc(col);
      if (!Number.isFinite(x) || x < 0 || x >= width) continue;
      for (let y = 0; y < height; y += 1) {
        mask[y * width + x] |= 0x1e;
      }
      applied = true;
    }
    return applied || base ? mask : null;
  }

  /** Put the chosen mask in place of whatever the data brought. */
  function installPinnedMask(pinned) {
    maskRequestId += 1;
    state.maskRaw = pinned.data;
    state.maskShape = pinned.shape.slice();
    state.maskFile = PINNED_MASK_KEY;
    state.maskPath = pinned.name || "";
  }

  function alignMaskToFrame() {
    // Every frame passes through here: a file change or a live frame that
    // brought its own mask does not replace the one chosen.
    const pinned = getPinnedMask();
    if (pinned && state.maskFile !== PINNED_MASK_KEY) installPinnedMask(pinned);
    if (
      !state.maskRaw ||
      !Array.isArray(state.maskShape) ||
      state.maskShape.length !== 2 ||
      !state.width ||
      !state.height
    ) {
      return;
    }
    const [maskH, maskW] = state.maskShape;
    if (maskH === state.height && maskW === state.width) {
      return;
    }
    if (maskH === state.width && maskW === state.height) {
      const transposed = new Uint32Array(state.width * state.height);
      for (let y = 0; y < state.height; y += 1) {
        for (let x = 0; x < state.width; x += 1) {
          transposed[y * state.width + x] = state.maskRaw[x * state.height + y];
        }
      }
      state.maskRaw = transposed;
      state.maskShape = [state.height, state.width];
    }
  }

  // A checkbox stays natively disabled -- one that looks unavailable but still
  // toggles is worse than one that cannot explain itself -- so the reason goes
  // on the label around it, which is hoverable and is a help target in its own
  // right. On the checkbox it was unreachable: a disabled control receives no
  // pointer events, so neither tooltip ever opened.
  function setCheckboxUnavailableReason(checkbox, reason) {
    const target = checkbox?.closest("label") || checkbox;
    if (!target) return;
    if (reason) target.dataset.helpReason = reason;
    else delete target.dataset.helpReason;
  }

  function updateMaskUI() {
    if (maskToggle) {
      maskToggle.disabled = !state.maskAvailable;
      maskToggle.checked = Boolean(state.maskEnabled && state.maskAvailable);
      setCheckboxUnavailableReason(
        maskToggle,
        state.maskAvailable ? "" : t("view.mask_unavailable_hint")
      );
    }
    if (maskSaturatedToggle) {
      const hasSatMax = Number.isFinite(getActiveSaturationMax());
      maskSaturatedToggle.disabled = !hasSatMax;
      maskSaturatedToggle.checked = Boolean(state.maskSaturatedEnabled && hasSatMax);
      setCheckboxUnavailableReason(
        maskSaturatedToggle,
        hasSatMax ? "" : t("view.mask_saturated_unavailable_hint")
      );
    }
    onMaskChanged();
  }

  function syncMaskAvailability(forceEnable = false) {
    const matches =
      state.maskRaw &&
      Array.isArray(state.maskShape) &&
      state.maskShape.length === 2 &&
      state.width &&
      state.height &&
      state.maskShape[0] === state.height &&
      state.maskShape[1] === state.width;
    state.maskAvailable = Boolean(matches);
    if (!state.maskAvailable) {
      state.maskEnabled = false;
    } else if (forceEnable || state.maskAuto) {
      state.maskEnabled = true;
    }
    updateMaskUI();
  }

  // Bumped for every mask request so a response that lost the race can tell.
  // Without it, switching files while a mask is in flight lets the previous
  // file's mask land on the new one: `alignMaskToFrame` only corrects shape and
  // orientation, so two files from the same detector -- the normal case when
  // stepping between runs -- pass straight through. It does not even self-heal,
  // because `state.maskFile` already names the new file, so the early return
  // below treats the wrong mask as cached for the rest of the session.
  let maskRequestId = 0;

  function clearMaskState() {
    // Also invalidates any mask still in flight: this runs when the session is
    // reset or the file changes, so a response arriving afterwards describes a
    // file that is no longer open.
    maskRequestId += 1;
    state.maskRaw = null;
    state.maskShape = null;
    state.maskAvailable = false;
    state.maskEnabled = false;
    state.maskAuto = true;
    state.maskFile = "";
    state.maskPath = "";
    updateMaskUI();
  }

  function redrawWithMask() {
    if (!state.hasFrame) return;
    updateGlobalStats();
    redraw();
    scheduleRoiUpdate();
  }

  /**
   * Use the chosen mask now, or -- with none chosen any more -- the data's own
   * again. Pinning switches Apply mask on: choosing a mask is asking to see it.
   */
  async function refreshPinnedMask() {
    const pinned = getPinnedMask();
    if (pinned) {
      installPinnedMask(pinned);
      alignMaskToFrame();
      syncMaskAvailability(true);
      redrawWithMask();
      return;
    }
    if (state.maskFile !== PINNED_MASK_KEY) return;
    clearMaskState();
    await loadMask(true);
    redrawWithMask();
  }

  async function loadMask(forceEnable = false) {
    if (getPinnedMask()) {
      if (state.maskFile !== PINNED_MASK_KEY) installPinnedMask(getPinnedMask());
      alignMaskToFrame();
      syncMaskAvailability(forceEnable);
      return;
    }
    if (!state.file || !isHdfFile(state.file)) {
      clearMaskState();
      return;
    }
    const hasThresholdDimension = Array.isArray(state.shape) && state.shape.length === 4;
    const maskKey =
      hasThresholdDimension ? `${state.file}#${state.thresholdIndex}` : state.file;
    if (state.maskFile === maskKey && state.maskRaw) {
      syncMaskAvailability(forceEnable);
      return;
    }
    const requestId = ++maskRequestId;
    const requestFile = state.file;
    state.maskFile = maskKey;
    state.maskRaw = null;
    state.maskShape = null;
    state.maskAvailable = false;
    if (forceEnable) {
      state.maskEnabled = true;
    }
    updateMaskUI();
    try {
      const thresholdParam = hasThresholdDimension ? `&threshold=${state.thresholdIndex}` : "";
      const res = await fetchResponse(`${apiBase}/mask?file=${encodeURIComponent(requestFile)}${thresholdParam}`);
      if (requestId !== maskRequestId) return;
      const buffer = await res.arrayBuffer();
      if (requestId !== maskRequestId) return;
      const dtype = parseDtype(res.headers.get("X-Dtype"));
      const shape = parseShape(res.headers.get("X-Shape"));
      const data = typedArrayFrom(buffer, dtype);
      state.maskRaw = normalizeMaskData(data);
      state.maskShape = shape;
      state.maskPath = readHeaderText(res.headers, "X-Mask-Path");
      alignMaskToFrame();
      syncMaskAvailability(forceEnable);
      if (state.hasFrame) {
        updateGlobalStats();
        redraw();
        scheduleRoiUpdate();
      }
    } catch (err) {
      if (requestId !== maskRequestId) return;
      // A file without a mask answers 404, which is routine; only a failure
      // to ask is worth logging.
      if (!(err?.status >= 400)) console.error(err);
      state.maskEnabled = false;
      state.maskAvailable = false;
      updateMaskUI();
    }
  }

  function snapHistogramValue(value) {
    if (!Number.isFinite(value)) return value;
    const info = getDtypeInfo(state.dtype);
    if (info && (info.kind === "u" || info.kind === "i")) {
      return Math.round(value);
    }
    return value;
  }

  function showHistTooltip(text, x, y) {
    if (!histTooltip) return;
    histTooltip.textContent = text;
    histTooltip.style.left = `${Math.round(x)}px`;
    histTooltip.style.top = `${Math.round(y)}px`;
    histTooltip.classList.add("is-visible");
    histTooltip.setAttribute("aria-hidden", "false");
  }

  function hideHistTooltip() {
    if (!histTooltip) return;
    histTooltip.classList.remove("is-visible");
    histTooltip.setAttribute("aria-hidden", "true");
  }

  // Where the mouse was when the readout was last placed, so it can be placed
  // again when the magnifier's lens appears, goes or changes size under a
  // mouse that has not moved.
  let cursorAt = null;

  function showCursorOverlay(text, clientX, clientY) {
    if (!cursorOverlay || !canvasShell) return;
    cursorOverlay.textContent = text;
    cursorOverlay.classList.add("is-visible");
    cursorOverlay.setAttribute("aria-hidden", "false");
    placeCursorOverlay(clientX, clientY);
  }

  /** Place the readout again for the lens as it is now, if it shows. */
  function repositionCursorOverlay() {
    if (!cursorOverlay?.classList.contains("is-visible") || !cursorAt) return;
    placeCursorOverlay(cursorAt.clientX, cursorAt.clientY);
  }

  function placeCursorOverlay(clientX, clientY) {
    cursorAt = { clientX, clientY };
    const shellRect = canvasShell.getBoundingClientRect();
    // Beside the magnifier's lens when it shows, not under it.
    const offset = 12 + (getCursorClearance?.() || 0);
    let left = clientX - shellRect.left + offset;
    let top = clientY - shellRect.top + offset;
    const maxLeft = shellRect.width - cursorOverlay.offsetWidth - 6;
    const maxTop = shellRect.height - cursorOverlay.offsetHeight - 6;
    left = Math.min(maxLeft, Math.max(6, left));
    top = Math.min(maxTop, Math.max(6, top));
    cursorOverlay.style.left = `${left}px`;
    cursorOverlay.style.top = `${top}px`;
  }

  function hideCursorOverlay() {
    if (!cursorOverlay) return;
    cursorOverlay.classList.remove("is-visible");
    cursorOverlay.setAttribute("aria-hidden", "true");
  }

  function updateCursorOverlay(event) {
    if (!state.hasFrame || !state.dataRaw || !state.width || !state.height) {
      hideCursorOverlay();
      return;
    }
    if (!canvasWrap || !canvasShell) {
      hideCursorOverlay();
      return;
    }
    const rect = canvasWrap.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const y = event.clientY - rect.top;
    if (x < 0 || y < 0 || x > rect.width || y > rect.height) {
      hideCursorOverlay();
      return;
    }
    const zoom = state.zoom || 1;
    const zoomY = zoom * (state.pixelAspect || 1);
    const offsetX = state.renderOffsetX || 0;
    const offsetY = state.renderOffsetY || 0;
    const imgX = (getEffectiveScrollLeft() + x - offsetX) / zoom;
    const imgY = (getEffectiveScrollTop() + y - offsetY) / zoomY;
    const ix = Math.floor(imgX);
    const iy = Math.floor(imgY);
    if (ix < 0 || iy < 0 || ix >= state.width || iy >= state.height) {
      hideCursorOverlay();
      return;
    }
    const idx = iy * state.width + ix;
    let labelValue = formatCursorValue(state.dataRaw[idx]);
    const satMax = getActiveSaturationMax();
    if (
      state.maskEnabled &&
      state.maskAvailable &&
      state.maskRaw &&
      state.maskShape &&
      state.maskShape[0] === state.height &&
      state.maskShape[1] === state.width
    ) {
      const maskValue = state.maskRaw[idx];
      if (maskValue & 1) {
        labelValue = "G";
      } else if (maskValue & 0x1e) {
        labelValue = "D";
      }
    }
    if (state.maskSaturatedEnabled && labelValue !== "G" && labelValue !== "D" && isSaturatedValue(state.dataRaw[idx], satMax)) {
      labelValue = "S";
    }
    // Flatfield correction or inpainting on: the value as shown, with the
    // measured one beside it -- "Value 0.437 (measured 1234)". A gap,
    // defective or saturated pixel keeps its marker unless inpainting filled it.
    const shownFrame = getInpaintedFrame?.() || null;
    if (shownFrame?.data) {
      const filled = Boolean(shownFrame.holes && shownFrame.holes[idx]);
      const marked = labelValue === "G" || labelValue === "D" || labelValue === "S";
      if (filled || !marked) {
        const shown = shownFrame.data[idx];
        const shownText = Number.isFinite(shown) ? formatCursorValue(shown) : "–";
        labelValue = `${shownText}${t("cursor.measured", { value: labelValue })}`;
      }
    }
    const resolutionValue = getResolutionAtPixel(ix, iy);
    const resolutionText = Number.isFinite(resolutionValue)
      ? t("cursor.resolution", { value: resolutionValue.toFixed(1) })
      : "";
    const label = t("cursor.readout", {
      x: ix,
      y: iy,
      value: labelValue,
      resolutionText,
    }) + (getCursorNote(idx) || "");
    showCursorOverlay(label, event.clientX, event.clientY);
  }

  async function fetchSimplonMask() {
    if (!simplonUrl || getPinnedMask()) return;
    const baseUrl = normalizeSimplonUrlInput(simplonUrl);
    if (!baseUrl) return;
    const version = simplonVersion?.value?.trim() || "1.8.0";
    try {
      const res = await fetchResponse(
        `${apiBase}/simplon/mask?url=${encodeURIComponent(baseUrl)}&version=${encodeURIComponent(version)}`,
      );
      if (res.status === 204) {
        return;
      }
      const buffer = await res.arrayBuffer();
      const dtype = parseDtype(res.headers.get("X-Dtype"));
      const shape = parseShape(res.headers.get("X-Shape"));
      const data = typedArrayFrom(buffer, dtype);
      state.maskRaw = normalizeMaskData(data);
      state.maskShape = shape;
      state.maskAuto = true;
      state.maskFile = "__simplon__";
      alignMaskToFrame();
      syncMaskAvailability(true);
      if (state.hasFrame) {
        updateGlobalStats();
        redraw();
        scheduleRoiUpdate();
      }
    } catch (err) {
      // The detector answering without a mask is not ALBIS failing.
      if (err?.status >= 400) {
        setAutoloadStatus(t("status.autoload.simplon_mask_unavailable"));
        return;
      }
      console.error(err);
      setAutoloadStatus(t("status.autoload.simplon_mask_failed"));
    }
  }

  return {
    getImagePointFromEvent,
    normalizeMaskData,
    buildNegativeMask,
    buildColumnMask,
    alignMaskToFrame,
    updateMaskUI,
    syncMaskAvailability,
    clearMaskState,
    loadMask,
    refreshPinnedMask,
    isPinnedMaskInUse: () => state.maskFile === PINNED_MASK_KEY,
    fetchSimplonMask,
    snapHistogramValue,
    showHistTooltip,
    hideHistTooltip,
    hideCursorOverlay,
    updateCursorOverlay,
    repositionCursorOverlay,
  };
}
