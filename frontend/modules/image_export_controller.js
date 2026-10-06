/**
 * File → Export Image…: the current frame as a PNG for slides and papers.
 *
 * The quick exports under Save As write one detector pixel per PNG pixel. That
 * is exact, and on a slide it is blurry: the program showing it enlarges it and
 * smooths the pixels together. This dialog enlarges by whole factors without
 * smoothing (image_export.js), or at the viewer's zoom, applies the pixel
 * aspect, can draw the overlays and pixel values the viewer shows, and stamps a
 * print resolution into the file.
 */

import { t } from "./i18n.js";
import { canSaveImage } from "./command_availability.js";
import {
  DEFAULT_EXPORT_DPI,
  EXPORT_DPI_CHOICES,
  SCREEN_SCALE,
  defaultExportScale,
  exportScaleOptions,
  exportSize,
  formatZoom,
  pixelValuesAvailability,
  printSizeCm,
  setPngDpi,
  withinExportLimits,
} from "./image_export.js";
import {
  buildGeometryRingCache,
  overlayUiScale,
  paintPeakMarkers,
  paintPixelLabels,
  paintResolutionRings,
  paintRoiOutline,
  regionView,
} from "./overlay_painters.js";

export function createImageExportController({ state, elements, callbacks }) {
  const {
    modal,
    closeBtn,
    source,
    regionSelect,
    scaleSelect,
    dpiSelect,
    overlaysCheckbox,
    overlaysField,
    pixelValuesCheckbox,
    pixelValuesField,
    overlaysHint,
    pixelValuesHint,
    summary,
    startBtn,
  } = elements;

  const {
    getVisibleRegion,
    renderRegionToCanvas,
    canvasToBlob,
    saveBlobAs,
    defaultExportName,
    getOverlaySnapshot,
    getPixelLabelSettings,
    pixelLabelsForFrame,
    createCanvas = (width, height) => Object.assign(document.createElement("canvas"), { width, height }),
    openModal,
    closeModal,
    setStatus,
  } = callbacks;

  // What the user last chose for pixel values; a size too small for them
  // unticks the box without forgetting the choice.
  let pixelValuesWanted = Boolean(pixelValuesCheckbox?.checked);

  function pixelAspect() {
    return Number(state.pixelAspect) > 0 ? Number(state.pixelAspect) : 1;
  }

  function selectedRegion() {
    if (String(regionSelect?.value || "full") === "visible") {
      const region = getVisibleRegion?.();
      if (region && region.width > 0 && region.height > 0) return region;
    }
    return { x: 0, y: 0, width: state.width, height: state.height };
  }

  function screenZoom() {
    return Number(state.zoom) > 0 ? Number(state.zoom) : 1;
  }

  function selectedScale() {
    if (scaleSelect?.value === SCREEN_SCALE) return screenZoom();
    const value = Number(scaleSelect?.value || 1);
    return Number.isFinite(value) && value > 0 ? value : 1;
  }

  function selectedDpi() {
    const value = Number(dpiSelect?.value ?? DEFAULT_EXPORT_DPI);
    return Number.isFinite(value) && value > 0 ? value : 0;
  }

  /** What the viewer shows that the image could carry, or null. */
  function overlaySnapshot() {
    const snap = getOverlaySnapshot?.();
    if (!snap) return null;
    const rings = Boolean(snap.ringParams);
    const peaks = Array.isArray(snap.peaks) && snap.peaks.length > 0;
    const roi = Boolean(snap.roi);
    return rings || peaks || roi ? snap : null;
  }

  // The scale list states each option's size, so the choice is made knowing
  // what it produces; a size a browser cannot produce is shown, but disabled.
  // "As on screen" exports at the viewer's zoom: a zoomed-in view comes out as
  // it looks, pixel values included.
  function populateScales({ resetToDefault = false } = {}) {
    if (!scaleSelect) return;
    const region = selectedRegion();
    const previous = scaleSelect.value;
    scaleSelect.innerHTML = "";
    const add = (value, label, allowed) => {
      const option = document.createElement("option");
      option.value = value;
      option.textContent = label;
      option.disabled = !allowed;
      if (!allowed) option.title = t("image_export.scale.too_large");
      scaleSelect.appendChild(option);
    };
    exportScaleOptions(region, pixelAspect()).forEach(({ scale, width, height, allowed }) => {
      add(String(scale), t("image_export.scale.option", { scale, width, height }), allowed);
    });
    const screen = exportSize(region, screenZoom(), pixelAspect());
    add(
      SCREEN_SCALE,
      t("export.scale.screen", { zoom: formatZoom(screenZoom()), ...screen }),
      withinExportLimits(screen.width, screen.height),
    );
    const keep = Array.from(scaleSelect.options).find((option) => option.value === previous && !option.disabled);
    scaleSelect.value = !resetToDefault && keep ? previous : String(defaultExportScale(region, pixelAspect()));
  }

  function pixelValuesState(scale = selectedScale()) {
    const settings = typeof pixelLabelsForFrame === "function" ? getPixelLabelSettings?.() : null;
    const result = pixelValuesAvailability(settings, scale, pixelAspect());
    return {
      available: result.available,
      reason: result.available ? "" : t(`export.pixel_values.${result.reason}`, { min: settings?.minCellPx }),
    };
  }

  function updateSummary() {
    if (!summary) return;
    if (!canSaveImage(state)) {
      summary.textContent = t("status.export.no_image");
      return;
    }
    const { width, height } = exportSize(selectedRegion(), selectedScale(), pixelAspect());
    const print = printSizeCm(width, height, selectedDpi());
    summary.textContent = print
      ? t("image_export.summary.print", {
          width,
          height,
          printWidth: print.width.toFixed(1),
          printHeight: print.height.toFixed(1),
          dpi: selectedDpi(),
        })
      : t("image_export.summary", { width, height });
  }

  // Why an option is unavailable, written under it rather than left to a
  // hover nobody tries on a greyed-out box.
  function showReason(field, hint, reason) {
    if (hint) {
      hint.textContent = reason;
      if (field) field.title = "";
    } else if (field) {
      field.title = reason;
    }
  }

  function updateUi() {
    const ready = canSaveImage(state);
    if (source) {
      source.textContent = state.file
        ? `${state.file}${state.dataset ? `  ${state.dataset}` : ""}`
        : t("animation_export.source.none");
    }
    [regionSelect, scaleSelect, dpiSelect].forEach((el) => {
      if (el) el.disabled = !ready;
    });
    const overlaysAvailable = Boolean(overlaySnapshot());
    if (overlaysCheckbox) {
      overlaysCheckbox.disabled = !ready || !overlaysAvailable;
      if (!overlaysAvailable) overlaysCheckbox.checked = false;
    }
    overlaysField?.classList.toggle("is-disabled", !overlaysAvailable);
    showReason(overlaysField, overlaysHint, overlaysAvailable ? "" : t("image_export.overlays.unavailable"));
    const pixelValues = pixelValuesState();
    if (pixelValuesCheckbox) {
      pixelValuesCheckbox.disabled = !ready || !pixelValues.available;
      pixelValuesCheckbox.checked = pixelValues.available && pixelValuesWanted;
    }
    pixelValuesField?.classList.toggle("is-disabled", !pixelValues.available);
    showReason(pixelValuesField, pixelValuesHint, pixelValues.reason);
    if (startBtn) startBtn.disabled = !ready;
    updateSummary();
  }

  /** The PNG, enlarged without smoothing, with overlays and a resolution. */
  async function renderPng({ region, scale, dpi, withOverlays, withPixelValues = false }) {
    const native = renderRegionToCanvas(region);
    if (!native) return null;
    const { width, height } = exportSize(region, scale, pixelAspect());
    const canvas = createCanvas(width, height);
    const ctx = canvas.getContext("2d");
    if (!ctx) return null;
    ctx.imageSmoothingEnabled = false;
    ctx.drawImage(native, 0, 0, width, height);

    const snap = withOverlays ? overlaySnapshot() : null;
    const view = regionView(region, width, height);
    const uiScale = overlayUiScale(width, height);
    if (snap) {
      if (snap.ringParams) {
        paintResolutionRings(ctx, {
          params: snap.ringParams,
          view,
          geometryCache: buildGeometryRingCache(snap.ringParams),
          pixelAspect: pixelAspect(),
          uiScale,
        });
      }
      if (snap.peaks?.length) {
        paintPeakMarkers(ctx, {
          peaks: snap.peaks,
          // A selection is a viewer state, not part of the figure.
          selectedPeaks: [],
          thinWithDensity: false,
          view,
          width,
          height,
          uiScale,
        });
      }
    }
    // Stacked as in the viewer: pixel values above rings and markers, the ROI
    // on top.
    if (withPixelValues && pixelValuesState(scale).available && state.dataRaw) {
      const frame = { data: state.dataRaw, width: state.width, height: state.height, dtype: state.dtype };
      const { labelAt, float } = pixelLabelsForFrame(frame, view.scaleX);
      paintPixelLabels(ctx, {
        view,
        x0: region.x,
        y0: region.y,
        x1: region.x + region.width,
        y1: region.y + region.height,
        frameWidth: state.width,
        labelAt,
        float,
      });
    }
    if (snap?.roi) {
      paintRoiOutline(ctx, { roi: snap.roi, view, outerRadius: snap.outerRadius, uiScale });
    }

    const blob = await canvasToBlob(canvas);
    if (!blob) return null;
    const bytes = setPngDpi(new Uint8Array(await blob.arrayBuffer()), dpi);
    return new Blob([bytes], { type: "image/png" });
  }

  function suggestedName(region, scale) {
    const visible = region.x !== 0 || region.y !== 0 || region.width !== state.width || region.height !== state.height;
    const base = defaultExportName(visible ? "view" : "full");
    return scale !== 1 ? base.replace(/\.png$/i, `_${formatZoom(scale)}x.png`) : base;
  }

  function startExport() {
    if (!canSaveImage(state)) {
      setStatus(t("status.export.no_image"), { tone: "warning" });
      return undefined;
    }
    const region = selectedRegion();
    const scale = selectedScale();
    const options = {
      region,
      scale,
      dpi: selectedDpi(),
      withOverlays: Boolean(overlaysCheckbox?.checked),
      withPixelValues: Boolean(pixelValuesCheckbox?.checked),
    };
    closeModal(modal);
    // saveBlobAs opens the native Save panel before rendering, so it keeps the
    // click's user activation; the image is only made once a place is chosen.
    return saveBlobAs(suggestedName(region, scale), () => renderPng(options));
  }

  function openDialog() {
    if (!canSaveImage(state)) {
      setStatus(t("status.export.no_image"), { tone: "warning" });
      return;
    }
    if (dpiSelect && !dpiSelect.options.length) {
      EXPORT_DPI_CHOICES.forEach((dpi) => {
        const option = document.createElement("option");
        option.value = String(dpi);
        option.textContent = dpi > 0 ? t("image_export.dpi.option", { dpi }) : t("image_export.dpi.none");
        dpiSelect.appendChild(option);
      });
      dpiSelect.value = String(DEFAULT_EXPORT_DPI);
    }
    populateScales({ resetToDefault: true });
    updateUi();
    openModal(modal, { focusTarget: startBtn });
  }

  function closeDialog() {
    closeModal(modal);
  }

  closeBtn?.addEventListener("click", closeDialog);
  modal?.addEventListener("click", (event) => {
    if (event.target === modal || event.target.classList?.contains("modal-backdrop")) closeDialog();
  });
  regionSelect?.addEventListener("change", () => {
    populateScales({ resetToDefault: true });
    updateUi();
  });
  scaleSelect?.addEventListener("change", updateUi);
  dpiSelect?.addEventListener("change", updateSummary);
  pixelValuesCheckbox?.addEventListener("change", () => {
    pixelValuesWanted = pixelValuesCheckbox.checked;
  });
  startBtn?.addEventListener("click", () => {
    void startExport();
  });

  return { openDialog, closeDialog, startExport, renderPng, updateUi };
}
