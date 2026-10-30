/**
 * The mask colours under the histogram's colour bar: one chip per class --
 * Gap, Defective, Saturated -- that is both the key and the way to change it.
 *
 * A chip shows the colour the image is drawn with; a class with no such pixel
 * on screen is dimmed ("None in this image"), so the row still reads as a key.
 * Clicking a chip opens a small popover with few, ready choices rather than a
 * full colour picker:
 *   - Stands out: colours chosen to contrast (mask_colors.CANDIDATES);
 *   - From the colour map: its low end, middle and high end -- to make a class
 *     blend in, gaps into the background, say (on a map with the same colour at
 *     both ends, its most different colour instead of the high end);
 *   - Auto for that class.
 * A colour that blends into the map is labelled so, not refused: hiding is a
 * legitimate choice. Reset colours at the end of the row, shown only once a
 * colour has been hand-set, drops every hand-set colour.
 *
 * Which classes count as on screen mirrors render_engine_controller's
 * redraw(): the mask applies only when on, available and the frame's size,
 * not where inpainting filled it; saturated pixels only with Mask saturated
 * and a known saturation value. The scans are kept per mask and per frame.
 */

import { t } from "./i18n.js";
import {
  CANDIDATES,
  MASK_CLASSES,
  blendsWithMap,
  currentMaskColors,
  mapEndColors,
  readMaskColorOverrides,
  rgbToHex,
  setMaskColorOverride,
  storeMaskColorOverrides,
} from "./mask_colors.js";

const LABEL_KEYS = {
  gap: "legend.mask.gap",
  defective: "legend.mask.defective",
  saturated: "legend.mask.saturated",
};

export function createMaskLegend({
  state,
  element,
  getInpaintedFrame,
  getActiveSaturationMax,
  isSaturatedValue,
  onChange,
}) {
  if (!state.maskColorOverrides) state.maskColorOverrides = readMaskColorOverrides();
  let maskScan = { mask: null, gap: false, defective: false };
  let satScan = { data: null, satMax: null, found: false };
  let popover = null;
  let openFor = null;
  let lastSignature = "";

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

  function changed() {
    render(true);
    if (popover && openFor) fillPopover(openFor.cls);
    onChange?.();
  }

  // ---- the popover -------------------------------------------------------

  function swatchButton(rgb, title, onPick, current) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "mask-swatch";
    btn.style.background = rgbToHex(rgb);
    const blends = blendsWithMap(state, rgb);
    btn.title = blends ? `${title} · ${t("mask.popover.blends")}` : title;
    btn.setAttribute("aria-label", btn.title);
    if (current && rgbToHex(current) === rgbToHex(rgb)) btn.setAttribute("aria-pressed", "true");
    btn.addEventListener("click", onPick);
    return btn;
  }

  function fillPopover(cls) {
    const colors = currentMaskColors(state);
    const current = colors[cls];
    const pick = (rgb) => () => {
      setMaskColorOverride(state, cls, rgbToHex(rgb));
      changed();
    };
    const title = document.createElement("div");
    title.className = "mask-popover-title";
    title.textContent = t("mask.popover.title", { name: t(LABEL_KEYS[cls]) });

    const contrastLabel = document.createElement("div");
    contrastLabel.className = "mask-popover-label";
    contrastLabel.textContent = t("mask.popover.contrasting");
    const contrast = document.createElement("div");
    contrast.className = "mask-popover-swatches";
    // Only what really stands out on this map: on Grey Scale, black and white
    // do not. Colours that blend in are offered below, as the map's own.
    contrast.append(
      ...CANDIDATES.filter((rgb) => !blendsWithMap(state, rgb)).map((rgb) =>
        swatchButton(rgb, rgbToHex(rgb), pick(rgb), current),
      ),
    );

    const mapLabel = document.createElement("div");
    mapLabel.className = "mask-popover-label";
    mapLabel.textContent = t("mask.popover.from_map");
    const map = document.createElement("div");
    map.className = "mask-popover-swatches";
    map.append(
      ...Object.entries(mapEndColors(state)).map(([where, rgb]) =>
        swatchButton(rgb, t(`mask.popover.map_${where}`), pick(rgb), current),
      ),
    );

    const actions = document.createElement("div");
    actions.className = "mask-popover-actions";
    const auto = document.createElement("button");
    auto.type = "button";
    auto.className = "btn btn-secondary inpaint-value-btn";
    auto.textContent = t("view.mask_colors.auto");
    auto.disabled = !state.maskColorOverrides?.[cls];
    auto.addEventListener("click", () => {
      setMaskColorOverride(state, cls, null);
      changed();
    });
    actions.append(auto);

    popover.replaceChildren(title, contrastLabel, contrast, mapLabel, map, actions);
  }

  function closePopover({ restoreFocus = true } = {}) {
    if (!popover) return;
    popover.remove();
    popover = null;
    document.removeEventListener("pointerdown", onOutside, true);
    document.removeEventListener("keydown", onKey, true);
    const chip = openFor?.chip;
    openFor = null;
    if (restoreFocus) chip?.focus();
  }

  function onOutside(event) {
    if (popover?.contains(event.target) || event.target.closest?.(".mask-chip")) return;
    closePopover({ restoreFocus: false });
  }

  function onKey(event) {
    if (event.key === "Escape") {
      event.stopPropagation();
      closePopover();
    }
  }

  function openPopover(cls, chip) {
    if (openFor?.cls === cls) {
      closePopover();
      return;
    }
    closePopover({ restoreFocus: false });
    popover = document.createElement("div");
    popover.className = "mask-popover";
    popover.setAttribute("role", "dialog");
    popover.setAttribute("aria-label", t("mask.popover.title", { name: t(LABEL_KEYS[cls]) }));
    openFor = { cls, chip };
    fillPopover(cls);
    document.body.append(popover);
    chip.setAttribute("aria-expanded", "true");
    const rect = chip.getBoundingClientRect();
    const width = popover.offsetWidth || 260;
    const left = Math.max(8, Math.min(window.innerWidth - width - 8, rect.left));
    popover.style.left = `${left + window.scrollX}px`;
    popover.style.top = `${rect.bottom + 6 + window.scrollY}px`;
    popover.querySelector(".mask-swatch")?.focus();
    document.addEventListener("pointerdown", onOutside, true);
    document.addEventListener("keydown", onKey, true);
  }

  // ---- the row -----------------------------------------------------------

  // Called on every redraw: rebuilt only when what it shows changed, so a
  // frame arriving while a chip has the keyboard focus does not take it away.
  function render(force = false) {
    if (!element) return;
    const wrapper = element.parentElement?.classList.contains("hist-mask-key") ? element.parentElement : element;
    if (!state.dataRaw) {
      wrapper.hidden = true;
      element.replaceChildren();
      lastSignature = "";
      return;
    }
    const present = new Set(presentClasses());
    const colors = currentMaskColors(state);
    const signature = JSON.stringify([[...present], colors, state.maskColorOverrides, openFor?.cls || ""]);
    if (!force && signature === lastSignature && element.childElementCount) return;
    lastSignature = signature;
    const chips = MASK_CLASSES.map((cls) => {
      const chip = document.createElement("button");
      chip.type = "button";
      chip.className = "mask-chip";
      chip.dataset.maskClass = cls;
      chip.classList.toggle("is-absent", !present.has(cls));
      chip.classList.toggle("is-custom", Boolean(state.maskColorOverrides?.[cls]));
      chip.setAttribute("aria-haspopup", "dialog");
      chip.setAttribute("aria-expanded", String(openFor?.cls === cls));
      const name = t(LABEL_KEYS[cls]);
      chip.title = present.has(cls)
        ? t("mask.chip_title", { name })
        : `${t("mask.chip_title", { name })} · ${t("legend.mask.none_here")}`;
      const swatch = document.createElement("span");
      swatch.className = "mask-legend-swatch";
      swatch.style.background = rgbToHex(colors[cls]);
      const label = document.createElement("span");
      label.textContent = name;
      chip.append(swatch, label);
      chip.addEventListener("click", () => openPopover(cls, chip));
      if (openFor?.cls === cls) openFor.chip = chip;
      return chip;
    });
    const resetAll = document.createElement("button");
    resetAll.type = "button";
    resetAll.className = "btn btn-secondary inpaint-value-btn mask-colors-auto";
    resetAll.id = "mask-colors-auto";
    resetAll.textContent = t("view.mask_colors.reset");
    resetAll.title = t("mask.auto_all_title");
    resetAll.hidden = MASK_CLASSES.every((cls) => !state.maskColorOverrides?.[cls]);
    resetAll.addEventListener("click", () => {
      state.maskColorOverrides = { gap: null, defective: null, saturated: null };
      storeMaskColorOverrides(state.maskColorOverrides);
      changed();
    });
    wrapper.hidden = false;
    element.replaceChildren(...chips, resetAll);
  }

  return { update: () => render(), refresh: () => render(true), presentClasses, closePopover };
}
