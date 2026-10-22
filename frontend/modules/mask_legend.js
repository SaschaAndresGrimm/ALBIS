/**
 * The key under the histogram's colour bar: a swatch for each mask colour the
 * image is drawn with -- Gap, Defective, Saturated -- listed only while such
 * pixels are on screen. A colour that is a marker then says so, instead of
 * being read as a value on the scale.
 *
 * The conditions mirror render_engine_controller's redraw(): the mask applies
 * only when it is on, available and the frame's size, not where inpainting
 * filled the masked pixels; saturated pixels only with Mask saturated and a
 * known saturation value. Finding which classes occur is a scan, so it is kept
 * per mask and per frame.
 */

import { t } from "./i18n.js";
import { MASK_CLASSES, currentMaskColors, rgbToHex } from "./mask_colors.js";

const LABEL_KEYS = {
  gap: "legend.mask.gap",
  defective: "legend.mask.defective",
  saturated: "legend.mask.saturated",
};

export function createMaskLegend({ state, element, getInpaintedFrame, getActiveSaturationMax, isSaturatedValue }) {
  let maskScan = { mask: null, gap: false, defective: false };
  let satScan = { data: null, satMax: null, found: false };

  function scanMask(mask) {
    if (maskScan.mask === mask) return maskScan;
    let gap = false;
    let defective = false;
    for (let i = 0; i < mask.length && !(gap && defective); i += 1) {
      const bits = mask[i];
      if (bits & 1) gap = true;
      else if (bits & 0x1e) defective = true;
    }
    maskScan = { mask, gap, defective };
    return maskScan;
  }

  function scanSaturated(data, satMax) {
    if (satScan.data === data && satScan.satMax === satMax) return satScan.found;
    let found = false;
    for (let i = 0; i < data.length; i += 1) {
      if (isSaturatedValue(data[i], satMax)) {
        found = true;
        break;
      }
    }
    satScan = { data, satMax, found };
    return found;
  }

  /** The mask colours the current image is drawn with. */
  function presentClasses() {
    if (!state.dataRaw) return [];
    const view = getInpaintedFrame?.() || null;
    const present = new Set();
    const maskReady =
      state.maskEnabled &&
      state.maskAvailable &&
      state.maskRaw &&
      state.maskShape &&
      state.maskShape[0] === state.height &&
      state.maskShape[1] === state.width &&
      !view?.holes;
    if (maskReady) {
      const { gap, defective } = scanMask(state.maskRaw);
      if (gap) present.add("gap");
      if (defective) present.add("defective");
    }
    const satMax = getActiveSaturationMax?.();
    if (state.maskSaturatedEnabled && Number.isFinite(satMax)) {
      if (scanSaturated(view?.data || state.dataRaw, satMax)) present.add("saturated");
    }
    return MASK_CLASSES.filter((cls) => present.has(cls));
  }

  function update() {
    if (!element) return;
    const classes = presentClasses();
    const colors = currentMaskColors(state);
    element.hidden = classes.length === 0;
    element.replaceChildren(
      ...classes.map((cls) => {
        const item = document.createElement("span");
        item.className = "mask-legend-item";
        const swatch = document.createElement("span");
        swatch.className = "mask-legend-swatch";
        swatch.style.background = rgbToHex(colors[cls]);
        const label = document.createElement("span");
        label.textContent = t(LABEL_KEYS[cls]);
        item.append(swatch, label);
        return item;
      }),
    );
  }

  return { update, presentClasses };
}
