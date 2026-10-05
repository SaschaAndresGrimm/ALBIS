/**
 * File → Export Image…: the current frame as a PNG for slides and papers.
 *
 * The quick exports under Save As write one detector pixel per PNG pixel. That
 * is exact, and on a slide it is blurry: the program showing it enlarges it and
 * smooths the pixels together. This dialog enlarges by whole factors without
 * smoothing (image_export.js), applies the pixel aspect, can draw the overlays
 * the viewer shows, and stamps a print resolution into the file.
 */

import { t } from "./i18n.js";
import { canSaveImage } from "./command_availability.js";
import {
  DEFAULT_EXPORT_DPI,
  EXPORT_DPI_CHOICES,
  defaultExportScale,
  exportScaleOptions,
  exportSize,
  printSizeCm,
  setPngDpi,
} from "./image_export.js";
import {
  buildGeometryRingCache,
  overlayUiScale,
  paintPeakMarkers,
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
    createCanvas = (width, height) => Object.assign(document.createElement("canvas"), { width, height }),
    openModal,
    closeModal,
    setStatus,
  } = callbacks;

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

  function selectedScale() {
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
  function populateScales({ resetToDefault = false } = {}) {
    if (!scaleSelect) return;
    const region = selectedRegion();
    const options = exportScaleOptions(region, pixelAspect());
    const previous = selectedScale();
    scaleSelect.innerHTML = "";
    options.forEach(({ scale, width, height, allowed }) => {
      const option = document.createElement("option");
      option.value = String(scale);
      option.textContent = t("image_export.scale.option", { scale, width, height });
      option.disabled = !allowed;
      if (!allowed) option.title = t("image_export.scale.too_large");
      scaleSelect.appendChild(option);
    });
    const keep = options.find((option) => option.scale === previous && option.allowed);
    scaleSelect.value = String(
      !resetToDefault && keep ? previous : defaultExportScale(region, pixelAspect()),
    );
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
    if (overlaysField) {
      overlaysField.title = overlaysAvailable ? "" : t("image_export.overlays.unavailable");
    }
    if (startBtn) startBtn.disabled = !ready;
    updateSummary();
  }

  /** The PNG, enlarged without smoothing, with overlays and a resolution. */
  async function renderPng({ region, scale, dpi, withOverlays }) {
    const native = renderRegionToCanvas(region);
    if (!native) return null;
    const { width, height } = exportSize(region, scale, pixelAspect());
    const canvas = createCanvas(width, height);
    const ctx = canvas.getContext("2d");
    if (!ctx) return null;
    ctx.imageSmoothingEnabled = false;
    ctx.drawImage(native, 0, 0, width, height);

    const snap = withOverlays ? overlaySnapshot() : null;
    if (snap) {
      const view = regionView(region, width, height);
      const uiScale = overlayUiScale(width, height);
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
      if (snap.roi) {
        paintRoiOutline(ctx, { roi: snap.roi, view, outerRadius: snap.outerRadius, uiScale });
      }
    }

    const blob = await canvasToBlob(canvas);
    if (!blob) return null;
    const bytes = setPngDpi(new Uint8Array(await blob.arrayBuffer()), dpi);
    return new Blob([bytes], { type: "image/png" });
  }

  function suggestedName(region, scale) {
    const visible = region.x !== 0 || region.y !== 0 || region.width !== state.width || region.height !== state.height;
    const base = defaultExportName(visible ? "view" : "full");
    return scale > 1 ? base.replace(/\.png$/i, `_${scale}x.png`) : base;
  }

  function startExport() {
    if (!canSaveImage(state)) {
      setStatus(t("status.export.no_image"), { tone: "warning" });
      return undefined;
    }
    const region = selectedRegion();
    const scale = selectedScale();
    const options = { region, scale, dpi: selectedDpi(), withOverlays: Boolean(overlaysCheckbox?.checked) };
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
    updateSummary();
  });
  [scaleSelect, dpiSelect].forEach((el) => el?.addEventListener("change", updateSummary));
  startBtn?.addEventListener("click", () => {
    void startExport();
  });

  return { openDialog, closeDialog, startExport, renderPng, updateUi };
}
