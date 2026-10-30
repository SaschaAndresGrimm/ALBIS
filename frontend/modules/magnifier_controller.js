/**
 * The magnifier: a round lens at the mouse that shows the pixels under it
 * enlarged, drawn from the data rather than from the screen -- with the whole
 * detector on screen at 0.2×, the lens still shows each pixel as a crisp
 * square. Same colour map, contrast and mask colours as the image, and the
 * frame as shown (flatfield-corrected, inpainted).
 *
 * Held with Z: the lens is there while the key is down. The toolbar's 🔍 (or
 * the command palette) keeps it on. While it shows, the mouse wheel changes
 * its magnification and Shift+wheel its size, instead of zooming the image;
 * both are remembered. Never part of an export.
 */

import { t } from "./i18n.js";
import { paintPixelLabels } from "./overlay_painters.js";
import { currentMaskColors } from "./mask_colors.js";

export const MAGNIFICATIONS = [2, 3, 4, 6, 8, 12, 16, 24, 32];
export const LENS_SIZES = [140, 180, 220, 280, 340, 420];
const STORAGE_KEY = "albis.magnifier";
const DEFAULTS = { magnification: 4, size: 220 };
const OUTSIDE_RGB = [13, 19, 28];
// Pixel values in the lens: from this cell size (screen pixels), up to this font.
const LABEL_MIN_CELL = 16;
const LABEL_MAX_FONT = 24;
// A label that would come out smaller than this (screen pixels) is left out.
const LABEL_MIN_FONT = 7;

/** One step along `steps` from `value` (the nearest one), clamped. */
export function stepAlong(steps, value, direction) {
  let index = 0;
  for (let i = 0; i < steps.length; i += 1) {
    if (Math.abs(steps[i] - value) < Math.abs(steps[index] - value)) index = i;
  }
  return steps[Math.max(0, Math.min(steps.length - 1, index + Math.sign(direction)))];
}

/**
 * The image pixel shown at lens position (ox, oy), out of a lens `size`
 * across, centred on image point (cx, cy): `scaleX`/`scaleY` screen pixels per
 * image pixel in the lens. Returns [ix, iy], possibly outside the image.
 */
export function lensSourcePixel(ox, oy, size, cx, cy, scaleX, scaleY) {
  return [Math.floor(cx + (ox - size / 2) / scaleX), Math.floor(cy + (oy - size / 2) / scaleY)];
}

function readSettings() {
  try {
    const stored = JSON.parse(window.localStorage?.getItem(STORAGE_KEY) || "{}") || {};
    return {
      magnification: MAGNIFICATIONS.includes(stored.magnification) ? stored.magnification : DEFAULTS.magnification,
      size: LENS_SIZES.includes(stored.size) ? stored.size : DEFAULTS.size,
    };
  } catch {
    return { ...DEFAULTS };
  }
}

// Controls that take a typed Z: text fields and dropdowns (which jump to an
// entry starting with Z). A checkbox, button or slider does not.
const NON_TEXT_INPUTS = new Set(["checkbox", "radio", "button", "submit", "reset", "range", "color", "file", "image"]);

export function isTypingTarget(target) {
  if (!target || typeof target !== "object") return false;
  const tag = String(target.tagName || "").toLowerCase();
  if (tag === "input") return !NON_TEXT_INPUTS.has(String(target.type || "text").toLowerCase());
  return tag === "textarea" || tag === "select" || Boolean(target.isContentEditable);
}

export function createMagnifierController({ state, elements, callbacks }) {
  const { canvasWrap, canvasShell, toggleBtn } = elements;
  const {
    buildPalette,
    getPaletteColorCount,
    mapValueToNorm,
    getActiveSaturationMax,
    isSaturatedValue,
    getInpaintedFrame = () => null,
    getEffectiveScrollLeft,
    getEffectiveScrollTop,
    getPixelLabelSettings = () => null,
    pixelLabelsForFrame = null,
    // Called when clearance() changes: the lens appeared, went or changed
    // size. The cursor readout moves only with the mouse, so without this it
    // stayed under a lens that appeared beneath a still mouse.
    onClearanceChange = () => {},
  } = callbacks;

  let settings = readSettings();
  let held = false;
  let locked = false;
  let pointer = null;
  let frameRequest = 0;

  const lens = document.createElement("canvas");
  lens.className = "magnifier-lens";
  lens.setAttribute("aria-hidden", "true");
  lens.hidden = true;
  canvasShell?.appendChild(lens);

  function saveSettings() {
    try {
      window.localStorage?.setItem(STORAGE_KEY, JSON.stringify(settings));
    } catch {
      // Remembered for this session only.
    }
  }

  function active() {
    return (held || locked) && Boolean(state.hasFrame && state.dataRaw && pointer);
  }

  function clearance() {
    return lens.hidden ? 0 : Math.round(settings.size / 2 * 0.71);
  }

  let reportedClearance = 0;
  function reportClearance() {
    const now = clearance();
    if (now === reportedClearance) return;
    reportedClearance = now;
    onClearanceChange();
  }

  function hide() {
    lens.hidden = true;
    reportClearance();
  }

  function schedule() {
    if (frameRequest) return;
    frameRequest = window.requestAnimationFrame(() => {
      frameRequest = 0;
      draw();
    });
  }

  function draw() {
    if (!active() || !canvasWrap || !canvasShell) {
      hide();
      return;
    }
    const wrapRect = canvasWrap.getBoundingClientRect();
    const x = pointer.clientX - wrapRect.left;
    const y = pointer.clientY - wrapRect.top;
    if (x < 0 || y < 0 || x > wrapRect.width || y > wrapRect.height) {
      hide();
      return;
    }
    const zoom = state.zoom || 1;
    const zoomY = zoom * (state.pixelAspect || 1);
    const cx = (getEffectiveScrollLeft() + x - (state.renderOffsetX || 0)) / zoom;
    const cy = (getEffectiveScrollTop() + y - (state.renderOffsetY || 0)) / zoomY;
    const size = settings.size;
    const dpr = Math.max(1, Math.min(3, window.devicePixelRatio || 1));
    const px = Math.round(size * dpr);
    if (lens.width !== px) {
      lens.width = px;
      lens.height = px;
      lens.style.width = `${size}px`;
      lens.style.height = `${size}px`;
    }
    const ctx = lens.getContext("2d");
    if (!ctx) return;

    // The frame as shown; mask colours unless inpainting filled those pixels.
    const view = getInpaintedFrame() || null;
    const values = view?.data || state.dataRaw;
    const raw = state.dataRaw;
    const maskReady =
      !view?.holes &&
      state.maskEnabled &&
      state.maskAvailable &&
      state.maskRaw &&
      state.maskShape &&
      state.maskShape[0] === state.height &&
      state.maskShape[1] === state.width;
    const satMax = getActiveSaturationMax();
    const markSaturated = Boolean(state.maskSaturatedEnabled && Number.isFinite(satMax));
    const palette = buildPalette(state.colormap);
    const maxIdx = getPaletteColorCount(palette) - 1;
    const scaleX = zoom * settings.magnification * dpr;
    const scaleY = zoomY * settings.magnification * dpr;

    const maskColors = currentMaskColors(state);
    const image = ctx.createImageData(px, px);
    const out = image.data;
    // One source pixel spans several lens pixels: look each up once per row run.
    for (let oy = 0; oy < px; oy += 1) {
      const iy = Math.floor(cy + (oy - px / 2) / scaleY);
      const rowInside = iy >= 0 && iy < state.height;
      for (let ox = 0; ox < px; ox += 1) {
        const ix = Math.floor(cx + (ox - px / 2) / scaleX);
        const j = (oy * px + ox) * 4;
        let r = OUTSIDE_RGB[0];
        let g = OUTSIDE_RGB[1];
        let b = OUTSIDE_RGB[2];
        if (rowInside && ix >= 0 && ix < state.width) {
          const idx = iy * state.width + ix;
          const flags = maskReady ? state.maskRaw[idx] : 0;
          if (flags & 1) {
            [r, g, b] = maskColors.gap;
          } else if (flags & 0x1e) {
            [r, g, b] = maskColors.defective;
          } else if (markSaturated && !(view?.holes && view.holes[idx]) && isSaturatedValue(raw[idx], satMax)) {
            [r, g, b] = maskColors.saturated;
          } else {
            const p = Math.floor(mapValueToNorm(values[idx]) * maxIdx) * 4;
            r = palette[p];
            g = palette[p + 1];
            b = palette[p + 2];
          }
        }
        out[j] = r;
        out[j + 1] = g;
        out[j + 2] = b;
        out[j + 3] = 255;
      }
    }
    ctx.putImageData(image, 0, 0);

    // Pixel values, with Pixel values on, once the lens makes a pixel big
    // enough to read (LABEL_MIN_CELL screen pixels): larger than the viewer's,
    // up to LABEL_MAX_FONT, each shrunk to fit its cell.
    const labelSettings = getPixelLabelSettings();
    const cellCss = Math.min(zoom, zoomY) * settings.magnification;
    if (labelSettings?.enabled && pixelLabelsForFrame && cellCss >= LABEL_MIN_CELL) {
      const lensView = { scaleX, scaleY, offsetX: px / 2 - cx * scaleX, offsetY: px / 2 - cy * scaleY };
      const x0 = Math.max(0, Math.floor(cx - px / 2 / scaleX));
      const y0 = Math.max(0, Math.floor(cy - px / 2 / scaleY));
      const x1 = Math.min(state.width, Math.ceil(cx + px / 2 / scaleX) + 1);
      const y1 = Math.min(state.height, Math.ceil(cy + px / 2 / scaleY) + 1);
      const { labelAt, float } = pixelLabelsForFrame(
        {
          data: raw,
          values: view?.data || null,
          filled: view?.holes || null,
          width: state.width,
          height: state.height,
          dtype: state.dtype,
        },
        // Room for every digit: the fitting below decides the size.
        scaleX * 4,
      );
      const fitFontPx = Math.min(LABEL_MAX_FONT, cellCss * 0.5) * dpr;
      paintPixelLabels(ctx, {
        view: lensView,
        x0,
        y0,
        x1,
        y1,
        frameWidth: state.width,
        labelAt,
        float,
        fitFontPx,
        fitMinPx: LABEL_MIN_FONT * dpr,
      });
    }

    // The centre pixel, outlined, and the magnification on the rim.
    const cellW = scaleX;
    const cellH = scaleY;
    const centreX = (Math.floor(cx) - cx) * scaleX + px / 2;
    const centreY = (Math.floor(cy) - cy) * scaleY + px / 2;
    ctx.lineWidth = Math.max(1, dpr);
    ctx.strokeStyle = "rgba(0, 0, 0, 0.75)";
    ctx.strokeRect(centreX - ctx.lineWidth, centreY - ctx.lineWidth, cellW + 2 * ctx.lineWidth, cellH + 2 * ctx.lineWidth);
    ctx.strokeStyle = "rgba(255, 255, 255, 0.95)";
    ctx.strokeRect(centreX, centreY, cellW, cellH);
    const caption = `${settings.magnification}×`;
    ctx.font = `600 ${Math.round(12 * dpr)}px system-ui, sans-serif`;
    ctx.textAlign = "center";
    ctx.textBaseline = "bottom";
    // On a small dark backing, readable over pixel values too.
    const captionWidth = ctx.measureText(caption).width + 12 * dpr;
    const captionHeight = 18 * dpr;
    ctx.fillStyle = "rgba(13, 19, 28, 0.82)";
    ctx.beginPath();
    ctx.roundRect?.(px / 2 - captionWidth / 2, px - 8 * dpr - captionHeight + 2 * dpr, captionWidth, captionHeight, 6 * dpr);
    ctx.fill();
    ctx.fillStyle = "#ffffff";
    ctx.fillText(caption, px / 2, px - 8 * dpr);

    const shellRect = canvasShell.getBoundingClientRect();
    lens.style.left = `${pointer.clientX - shellRect.left - size / 2}px`;
    lens.style.top = `${pointer.clientY - shellRect.top - size / 2}px`;
    lens.hidden = false;
    reportClearance();
  }

  function syncButton() {
    if (!toggleBtn) return;
    toggleBtn.classList.toggle("is-active", locked);
    toggleBtn.setAttribute("aria-pressed", String(locked));
    toggleBtn.title = t("toolbar.magnifier");
    toggleBtn.setAttribute("aria-label", t("toolbar.magnifier"));
  }

  /** Keep the lens on (the toolbar button, the command palette), or off. */
  function toggleLocked(force) {
    locked = typeof force === "boolean" ? force : !locked;
    syncButton();
    if (active()) schedule();
    else hide();
    return locked;
  }

  canvasWrap?.addEventListener("pointermove", (event) => {
    pointer = { clientX: event.clientX, clientY: event.clientY };
    if (held || locked) schedule();
  });
  // A click on the image is a return to it: keyboard focus leaves whatever
  // panel control had it, so Z (and the other single-key shortcuts) reach the
  // viewer again instead of the last field or dropdown used.
  canvasWrap?.addEventListener("pointerdown", () => {
    const focused = document.activeElement;
    if (focused && focused !== document.body && !canvasWrap.contains(focused) && typeof focused.blur === "function") {
      focused.blur();
    }
  });
  canvasWrap?.addEventListener("pointerleave", () => {
    pointer = null;
    hide();
  });
  // Ahead of the view's own wheel zoom (capture), and only while the lens shows.
  canvasWrap?.addEventListener(
    "wheel",
    (event) => {
      if (!active()) return;
      event.preventDefault();
      event.stopImmediatePropagation();
      const direction = event.deltaY < 0 ? 1 : -1;
      if (event.shiftKey) settings = { ...settings, size: stepAlong(LENS_SIZES, settings.size, direction) };
      else settings = { ...settings, magnification: stepAlong(MAGNIFICATIONS, settings.magnification, direction) };
      saveSettings();
      schedule();
    },
    { passive: false, capture: true },
  );
  window.addEventListener("keydown", (event) => {
    if (event.key !== "z" && event.key !== "Z") return;
    if (event.metaKey || event.ctrlKey || event.altKey || isTypingTarget(event.target)) return;
    if (!held) {
      held = true;
      schedule();
    }
  });
  window.addEventListener("keyup", (event) => {
    if (event.key !== "z" && event.key !== "Z") return;
    held = false;
    if (!locked) hide();
  });
  // A key released in another window never reaches this one.
  window.addEventListener("blur", () => {
    held = false;
    if (!locked) hide();
  });
  toggleBtn?.addEventListener("click", () => toggleLocked());
  syncButton();

  return {
    toggleLocked,
    /** Redraw for a new frame or contrast, if it is showing. */
    refresh() {
      if (active()) schedule();
    },
    get settings() {
      return { ...settings };
    },
    get isShowing() {
      return !lens.hidden;
    },
    /** How far the cursor readout must stay from the mouse: past the lens. */
    clearance,
  };
}
