/**
 * Mask lifecycle, histogram tooltip, and cursor probe interactions.
 */

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
    getResolutionAtPixel,
    getEffectiveScrollLeft,
    getEffectiveScrollTop,
    setAutoloadStatus,
  } = callbacks;

  function getImagePointFromEvent(event) {
    if (!state.hasFrame || !canvasWrap) return null;
    const rect = canvasWrap.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const y = event.clientY - rect.top;
    if (x < 0 || y < 0 || x > rect.width || y > rect.height) return null;
    const zoom = state.zoom || 1;
    const offsetX = state.renderOffsetX || 0;
    const offsetY = state.renderOffsetY || 0;
    const imgX = (getEffectiveScrollLeft() + x - offsetX) / zoom;
    const imgY = (getEffectiveScrollTop() + y - offsetY) / zoom;
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

  function alignMaskToFrame() {
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

  function updateMaskUI() {
    if (maskToggle) {
      maskToggle.disabled = !state.maskAvailable;
      maskToggle.checked = Boolean(state.maskEnabled && state.maskAvailable);
    }
    if (maskSaturatedToggle) {
      const hasSatMax = Number.isFinite(getActiveSaturationMax());
      maskSaturatedToggle.disabled = !hasSatMax;
      maskSaturatedToggle.checked = Boolean(state.maskSaturatedEnabled && hasSatMax);
    }
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

  function clearMaskState() {
    state.maskRaw = null;
    state.maskShape = null;
    state.maskAvailable = false;
    state.maskEnabled = false;
    state.maskAuto = true;
    state.maskFile = "";
    state.maskPath = "";
    updateMaskUI();
  }

  async function loadMask(forceEnable = false) {
    if (!state.file || !isHdfFile(state.file)) {
      clearMaskState();
      return;
    }
    const maskKey =
      state.thresholdCount > 1 ? `${state.file}#${state.thresholdIndex}` : state.file;
    if (state.maskFile === maskKey && state.maskRaw) {
      syncMaskAvailability(forceEnable);
      return;
    }
    state.maskFile = maskKey;
    state.maskRaw = null;
    state.maskShape = null;
    state.maskAvailable = false;
    if (forceEnable) {
      state.maskEnabled = true;
    }
    updateMaskUI();
    try {
      const thresholdParam =
        state.thresholdCount > 1 ? `&threshold=${state.thresholdIndex}` : "";
      const res = await fetch(`${apiBase}/mask?file=${encodeURIComponent(state.file)}${thresholdParam}`);
      if (!res.ok) {
        state.maskEnabled = false;
        updateMaskUI();
        return;
      }
      const buffer = await res.arrayBuffer();
      const dtype = parseDtype(res.headers.get("X-Dtype"));
      const shape = parseShape(res.headers.get("X-Shape"));
      const data = typedArrayFrom(buffer, dtype);
      state.maskRaw = normalizeMaskData(data);
      state.maskShape = shape;
      state.maskPath = res.headers.get("X-Mask-Path") || "";
      alignMaskToFrame();
      syncMaskAvailability(forceEnable);
      if (state.hasFrame) {
        updateGlobalStats();
        redraw();
        scheduleRoiUpdate();
      }
    } catch (err) {
      console.error(err);
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

  function showCursorOverlay(text, clientX, clientY) {
    if (!cursorOverlay || !canvasShell) return;
    cursorOverlay.textContent = text;
    cursorOverlay.classList.add("is-visible");
    cursorOverlay.setAttribute("aria-hidden", "false");
    const shellRect = canvasShell.getBoundingClientRect();
    let left = clientX - shellRect.left + 12;
    let top = clientY - shellRect.top + 12;
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
    const offsetX = state.renderOffsetX || 0;
    const offsetY = state.renderOffsetY || 0;
    const imgX = (getEffectiveScrollLeft() + x - offsetX) / zoom;
    const imgY = (getEffectiveScrollTop() + y - offsetY) / zoom;
    const ix = Math.floor(imgX);
    const iy = Math.floor(imgY);
    if (ix < 0 || iy < 0 || ix >= state.width || iy >= state.height) {
      hideCursorOverlay();
      return;
    }
    const idx = iy * state.width + ix;
    let labelValue = formatValue(state.dataRaw[idx]);
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
    const resolutionValue = getResolutionAtPixel(ix, iy);
    const resolutionText = Number.isFinite(resolutionValue) ? `  d ${resolutionValue.toFixed(1)} Å` : "";
    const label = `X ${ix}  Y ${iy}  Value ${labelValue}${resolutionText}`;
    showCursorOverlay(label, event.clientX, event.clientY);
  }

  async function fetchSimplonMask() {
    if (!simplonUrl) return;
    const baseUrl = simplonUrl.value.trim();
    if (!baseUrl) return;
    const version = simplonVersion?.value?.trim() || "1.8.0";
    try {
      const res = await fetch(
        `${apiBase}/simplon/mask?url=${encodeURIComponent(baseUrl)}&version=${encodeURIComponent(version)}`,
      );
      if (res.status === 204) {
        return;
      }
      if (!res.ok) {
        setAutoloadStatus("SIMPLON: mask unavailable");
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
      console.error(err);
      setAutoloadStatus("SIMPLON: mask failed");
    }
  }

  return {
    getImagePointFromEvent,
    normalizeMaskData,
    buildNegativeMask,
    alignMaskToFrame,
    updateMaskUI,
    syncMaskAvailability,
    clearMaskState,
    loadMask,
    fetchSimplonMask,
    snapHistogramValue,
    showHistTooltip,
    hideHistTooltip,
    hideCursorOverlay,
    updateCursorOverlay,
  };
}
