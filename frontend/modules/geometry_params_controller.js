/**
 * Data → Detector Geometry: shows the geometry ALBIS calculates with, and lets
 * the user override it.
 *
 * The values themselves are combined in geometry_params.js. This owns the
 * section's fields, the "Override manually" switch, the geometry-file picker,
 * and the one-line summaries the Resolution Rings and Peak Finder sections show
 * of what is in effect, so an override is visible where its effect is.
 *
 * It writes the values in effect back to `analysisState` (`distanceMm`,
 * `pixelSizeUm` for X, `pixelSizeYUm`, `energyEv`, `centerX`, `centerY`) and
 * `state.pixelAspect`, which is what the rings, the cursor readout, the peak
 * table and a series sum already read. Nothing else writes those fields.
 */

import { getLanguage, t } from "./i18n.js";
import {
  GEOMETRY_KEYS,
  GEOMETRY_OVERRIDE_STORAGE_KEY,
  createGeometryOverride,
  emptyGeometryValues,
  geometryValueOrNull,
  loadGeometryOverride,
  pixelAspectFrom,
  resolveGeometryParams,
  sanitizeGeometryValues,
  saveGeometryOverride,
} from "./geometry_params.js";
import { isExptPath } from "./geometry_override_utils.js";

const VALIDATION_KEYS = {
  distanceMm: "validation.rings.distance_positive",
  pixelSizeXUm: "validation.rings.pixel_size_positive",
  pixelSizeYUm: "validation.rings.pixel_size_positive",
  energyEv: "validation.rings.photon_energy_positive",
};
const DIGITS = { distanceMm: 2, pixelSizeXUm: 2, pixelSizeYUm: 2, energyEv: 0, centerX: 2, centerY: 2 };

export function formatGeometryNumber(value, digits = 2) {
  if (!Number.isFinite(value)) return "";
  return String(Number(value.toFixed(digits)));
}

export function formatGeometrySource(raw) {
  const text = String(raw || "").trim();
  if (!text) return "";
  const parts = text.replace(/\\/g, "/").split("/").filter(Boolean);
  return parts.length <= 2 ? parts.join("/") : parts.slice(-2).join("/");
}

export function createGeometryParamsController({
  apiBase,
  state,
  analysisState,
  elements,
  callbacks = {},
  storage,
  isBackendLocal = () => false,
}) {
  const {
    sectionStateEl,
    summaryChipEl,
    overrideToggle,
    overrideHint,
    inputs = {},
    hints = {},
    resetButton,
    geometryFile,
    geometryFileHint,
    geometryBrowse,
    geometryClear,
    geometryStatusEl,
    inlineSummaries = [],
  } = elements;

  const {
    onParamsChanged,
    onPixelAspectChanged,
    reloadGeometry,
    revealSection,
    setSectionBadgeState,
    setSummaryChip,
    setStatus,
    openFileDialog,
  } = callbacks;

  const invalid = new Set();
  let resolved = resolveGeometryParams({});

  analysisState.geometrySource = analysisState.geometrySource || { values: emptyGeometryValues(), origin: "" };
  analysisState.geometryReference = analysisState.geometryReference || null;
  analysisState.geometryPoseFromFile = Boolean(analysisState.geometryPoseFromFile);
  analysisState.geometryOverride = loadGeometryOverride(storage);

  function override() {
    return analysisState.geometryOverride;
  }

  function overrideInEffect() {
    const current = override();
    if (!current.enabled) return false;
    return Boolean(current.geometryFile) || GEOMETRY_KEYS.some((key) => current.values[key] !== null);
  }

  /** The geometry file to request, or "" -- only while the override is on. */
  function getActiveGeometryFile() {
    const current = override();
    return current.enabled ? current.geometryFile : "";
  }

  function persist() {
    saveGeometryOverride(override(), storage);
  }

  function recompute({ notify = true } = {}) {
    const previousAspect = state.pixelAspect || 1;
    resolved = resolveGeometryParams({
      source: analysisState.geometrySource.values,
      sourceOrigin: analysisState.geometrySource.origin,
      reference: analysisState.geometryReference,
      poseFromGeometry: analysisState.geometryPoseFromFile,
      override: override(),
    });
    const values = resolved.values;
    analysisState.distanceMm = values.distanceMm;
    analysisState.pixelSizeUm = values.pixelSizeXUm;
    analysisState.pixelSizeYUm = values.pixelSizeYUm;
    analysisState.energyEv = values.energyEv;
    analysisState.centerX = values.centerX;
    analysisState.centerY = values.centerY;
    state.pixelAspect = pixelAspectFrom(values);
    render();
    if (!notify) return;
    onParamsChanged?.();
    // The image is drawn stretched by the pixel aspect. A new one has to lay
    // the canvas out again, not only repaint it: before, a changed pixel size
    // showed only after the image was panned.
    if (state.pixelAspect !== previousAspect && state.hasFrame) {
      onPixelAspectChanged?.();
    }
  }

  /**
   * What the open file or live stream states. A file replaces everything; a
   * live frame keeps, for a field it does not carry, the last value from the
   * same stream.
   */
  function setSource(values, origin, { live = false } = {}) {
    const next = sanitizeGeometryValues(values);
    const current = analysisState.geometrySource;
    if (live && current.origin === origin) {
      for (const key of GEOMETRY_KEYS) {
        if (next[key] === null) next[key] = current.values[key];
      }
    }
    analysisState.geometrySource = { values: next, origin: String(origin || "") };
    recompute();
  }

  function clearSource() {
    analysisState.geometrySource = { values: emptyGeometryValues(), origin: "" };
    recompute();
  }

  /** The pose of the detector geometry in use, or null for a flat detector. */
  function setGeometryReference(reference, { poseFromGeometry = false } = {}) {
    analysisState.geometryReference = reference
      ? {
          distanceMm: geometryValueOrNull("distanceMm", reference.distanceMm),
          centerX: geometryValueOrNull("centerX", reference.centerX),
          centerY: geometryValueOrNull("centerY", reference.centerY),
        }
      : null;
    analysisState.geometryPoseFromFile = Boolean(reference && poseFromGeometry);
    recompute();
  }

  function setOverrideEnabled(enabled) {
    const current = override();
    const changedFile = Boolean(current.geometryFile) && current.enabled !== Boolean(enabled);
    current.enabled = Boolean(enabled);
    if (!current.enabled) invalid.clear();
    persist();
    recompute();
    if (changedFile) reloadGeometry?.();
  }

  /** Override some values, switching the override on: an explicit edit is one. */
  function setOverrideValues(partial) {
    const current = override();
    const wasEnabled = current.enabled;
    current.enabled = true;
    for (const [key, value] of Object.entries(partial || {})) {
      if (GEOMETRY_KEYS.includes(key)) current.values[key] = geometryValueOrNull(key, value);
    }
    persist();
    recompute();
    if (!wasEnabled && current.geometryFile) reloadGeometry?.();
  }

  /** Dragging the beam centre on the image is a manual edit like typing it. */
  function setBeamCenter(x, y) {
    setOverrideValues({ centerX: x, centerY: y });
  }

  function resetOverride() {
    const current = override();
    const hadFile = Boolean(current.geometryFile) && current.enabled;
    const fresh = createGeometryOverride();
    fresh.enabled = current.enabled;
    analysisState.geometryOverride = fresh;
    invalid.clear();
    persist();
    recompute();
    if (hadFile) reloadGeometry?.();
  }

  function setGeometryFile(path) {
    const current = override();
    current.geometryFile = String(path || "").trim();
    if (current.geometryFile) current.enabled = true;
    persist();
    recompute();
    reloadGeometry?.();
  }

  function clearGeometryFile() {
    const current = override();
    if (!current.geometryFile) return;
    current.geometryFile = "";
    persist();
    recompute();
    reloadGeometry?.();
  }

  // ---- rendering -----------------------------------------------------------

  function showHint(hintEl, inputEls, message, { isInvalid = false } = {}) {
    inputEls.filter(Boolean).forEach((el) => el.classList.toggle("is-invalid", isInvalid));
    if (!hintEl) return;
    hintEl.textContent = message || "";
    hintEl.classList.toggle("is-hidden", !message);
    hintEl.classList.toggle("is-info", Boolean(message) && !isInvalid);
  }

  function sourceText(keys) {
    const parts = keys.map((key) => resolved.sourceValues[key]);
    if (parts.every((value) => value === null)) return t("geometry.field.not_in_metadata");
    const shown = parts.map((value, index) => (value === null ? "—" : formatGeometryNumber(value, DIGITS[keys[index]])));
    return t("geometry.field.from_metadata", { value: shown.join(keys.length === 2 ? ", " : "") });
  }

  function renderFieldHint(hintKey, keys) {
    const hintEl = hints[hintKey];
    const inputEls = keys.map((key) => inputs[key]);
    const badKey = keys.find((key) => invalid.has(key));
    if (badKey) {
      showHint(hintEl, inputEls, t(VALIDATION_KEYS[badKey]), { isInvalid: true });
      return;
    }
    // Only an overridden field needs a line of its own, to say what the
    // metadata had. A value the metadata lacks shows as "—" in the field.
    const overridden = keys.some((key) => resolved.origins[key] === "override");
    showHint(hintEl, inputEls, overridden ? sourceText(keys) : "");
  }

  function hasSource() {
    return Boolean(analysisState.geometrySource.origin) || Boolean(state.file);
  }

  function renderInputs() {
    const editable = override().enabled;
    const geometryActive = analysisState.ringMode === "geometry" && Boolean(analysisState.ringGeometry);
    const focused = typeof document !== "undefined" ? document.activeElement : null;
    for (const key of GEOMETRY_KEYS) {
      const input = inputs[key];
      if (!input) continue;
      const pixelKey = key === "pixelSizeXUm" || key === "pixelSizeYUm";
      // A geometry file states its own pixel size; the field would do nothing.
      input.disabled = pixelKey && geometryActive;
      input.readOnly = !editable;
      input.classList.toggle("is-overridden", resolved.origins[key] === "override");
      if (input === focused && editable) continue;
      if (invalid.has(key)) continue;
      input.value = formatGeometryNumber(resolved.values[key], DIGITS[key]);
    }
    renderFieldHint("distanceMm", ["distanceMm"]);
    renderFieldHint("pixelSize", ["pixelSizeXUm", "pixelSizeYUm"]);
    renderFieldHint("energyEv", ["energyEv"]);
    renderFieldHint("center", ["centerX", "centerY"]);
  }

  function renderGeometryFile() {
    const current = override();
    if (geometryFile && geometryFile !== (typeof document !== "undefined" ? document.activeElement : null)) {
      geometryFile.value = current.geometryFile;
    }
    if (geometryFile) geometryFile.readOnly = !current.enabled;
    if (geometryBrowse) geometryBrowse.disabled = false;
    if (geometryClear) geometryClear.disabled = !current.geometryFile;
    if (!geometryStatusEl) return;
    const geometryActive = analysisState.ringMode === "geometry" && analysisState.ringGeometry;
    if (!geometryActive) {
      geometryStatusEl.classList.add("is-hidden");
      geometryStatusEl.textContent = "";
      geometryStatusEl.removeAttribute("title");
      return;
    }
    const source = formatGeometrySource(analysisState.ringGeometrySource) || t("common.ready");
    const statusKey = analysisState.geometryOverrideActive
      ? "rings.geometry.status_manual"
      : "rings.geometry.status_auto";
    geometryStatusEl.textContent = t(statusKey, { source });
    if (analysisState.ringGeometrySource) {
      geometryStatusEl.title = analysisState.ringGeometrySource;
    } else {
      geometryStatusEl.removeAttribute("title");
    }
    geometryStatusEl.classList.remove("is-hidden");
  }

  function missingRequired() {
    const values = resolved.values;
    const geometryActive = analysisState.ringMode === "geometry" && analysisState.ringGeometry;
    const required = geometryActive
      ? ["energyEv"]
      : ["distanceMm", "pixelSizeXUm", "energyEv", "centerX", "centerY"];
    return required.some((key) => values[key] === null);
  }

  function renderState() {
    if (overrideToggle) overrideToggle.checked = override().enabled;
    // What the switch does is worth saying while it is off; once it is on,
    // the state line above says the same thing.
    overrideHint?.classList.toggle("is-hidden", override().enabled);
    if (resetButton) resetButton.disabled = !overrideInEffect();
    if (overrideInEffect()) {
      setSectionBadgeState?.(sectionStateEl, "active", t("geometry.state.override"));
      setSummaryChip?.(summaryChipEl, t("geometry.summary.override"), "warning");
    } else if (!hasSource()) {
      setSectionBadgeState?.(sectionStateEl, "empty", t("geometry.state.no_file"));
      setSummaryChip?.(summaryChipEl, "");
    } else if (missingRequired()) {
      setSectionBadgeState?.(sectionStateEl, "warning", t("geometry.state.missing"));
      setSummaryChip?.(summaryChipEl, t("geometry.summary.missing"), "warning");
    } else {
      setSectionBadgeState?.(sectionStateEl, "active", t("geometry.state.from_metadata"));
      setSummaryChip?.(summaryChipEl, t("geometry.summary.from_metadata"), "active");
    }
  }

  /** "260.13 mm · 172 µm · 4500 eV · beam 1080, 2595", with — for what is missing. */
  function inlineText() {
    const v = resolved.values;
    const show = (key) => (v[key] === null ? "—" : formatGeometryNumber(v[key], DIGITS[key]));
    const pixel =
      v.pixelSizeXUm !== null && v.pixelSizeYUm !== null && v.pixelSizeXUm !== v.pixelSizeYUm
        ? `${show("pixelSizeXUm")} × ${show("pixelSizeYUm")}`
        : show("pixelSizeXUm");
    return t("geometry.inline.values", {
      distance: show("distanceMm"),
      pixel,
      energy: show("energyEv"),
      center: v.centerX === null && v.centerY === null ? "—" : `${show("centerX")}, ${show("centerY")}`,
    });
  }

  function renderInlineSummaries() {
    const text = inlineText();
    const manual = overrideInEffect();
    inlineSummaries.filter(Boolean).forEach((root) => {
      const valuesEl = root.querySelector("[data-geometry-inline-values]");
      const manualEl = root.querySelector("[data-geometry-inline-manual]");
      if (valuesEl) valuesEl.textContent = text;
      if (manualEl) manualEl.classList.toggle("is-hidden", !manual);
      root.classList.toggle("is-overridden", manual);
    });
  }

  function render() {
    renderInputs();
    renderGeometryFile();
    renderState();
    renderInlineSummaries();
  }

  // ---- interaction ---------------------------------------------------------

  function onInput(key) {
    const input = inputs[key];
    if (!input || !override().enabled) return;
    const raw = String(input.value ?? "").trim();
    if (!raw) {
      // An emptied field follows the file again.
      invalid.delete(key);
      setOverrideValues({ [key]: null });
      return;
    }
    const value = geometryValueOrNull(key, raw);
    if (value === null) {
      invalid.add(key);
      render();
      return;
    }
    invalid.delete(key);
    setOverrideValues({ [key]: value });
  }

  for (const key of GEOMETRY_KEYS) {
    inputs[key]?.addEventListener("input", () => onInput(key));
    // Leaving a field shows the value in effect again, formatted.
    inputs[key]?.addEventListener("change", () => {
      if (!invalid.has(key)) render();
    });
  }

  overrideToggle?.addEventListener("change", () => setOverrideEnabled(overrideToggle.checked));
  resetButton?.addEventListener("click", () => resetOverride());

  function applyPickedGeometryFile(path) {
    const picked = String(path || "").trim();
    if (!picked) return;
    if (!isExptPath(picked)) {
      showHint(geometryFileHint, [geometryFile], t("validation.rings.geometry_expt_required"), { isInvalid: true });
      return;
    }
    showHint(geometryFileHint, [geometryFile], "");
    setGeometryFile(picked);
  }

  geometryFile?.addEventListener("change", () => {
    const raw = String(geometryFile.value || "").trim();
    if (!raw) {
      showHint(geometryFileHint, [geometryFile], "");
      clearGeometryFile();
      return;
    }
    applyPickedGeometryFile(raw);
  });

  geometryBrowse?.addEventListener("click", async () => {
    try {
      if (isBackendLocal()) {
        const res = await fetch(`${apiBase}/choose-file?exts=.expt&lang=${encodeURIComponent(getLanguage())}`);
        if (res.status === 204) return;
        if (!res.ok) {
          setStatus?.(
            res.status === 409 ? t("status.file_picker.unavailable") : t("status.analysis.geometry_picker_failed"),
          );
          return;
        }
        const data = await res.json();
        applyPickedGeometryFile(data?.path);
        return;
      }
      applyPickedGeometryFile(await openFileDialog?.({ exts: ".expt" }));
    } catch (err) {
      console.error(err);
      setStatus?.(t("status.analysis.geometry_picker_failed"), { tone: "error" });
    }
  });

  geometryClear?.addEventListener("click", () => {
    showHint(geometryFileHint, [geometryFile], "");
    clearGeometryFile();
  });

  inlineSummaries.filter(Boolean).forEach((root) => {
    root.querySelector("[data-geometry-edit]")?.addEventListener("click", () => revealSection?.());
  });

  // Another window of this ALBIS changed the override: follow it, so two
  // windows never draw the same data with different geometry.
  if (typeof window !== "undefined") {
    window.addEventListener("storage", (event) => {
      if (event.key !== GEOMETRY_OVERRIDE_STORAGE_KEY) return;
      const previousFile = getActiveGeometryFile();
      analysisState.geometryOverride = loadGeometryOverride(storage);
      invalid.clear();
      recompute();
      if (getActiveGeometryFile() !== previousFile) reloadGeometry?.();
    });
  }

  // A saved override applies from the start, before any file is open. Without
  // notifying: this runs while the app is still being built, before the
  // overlays it would schedule exist (app.js creates this controller early, so
  // a notification here threw and stopped the app from starting).
  recompute({ notify: false });

  return {
    recompute,
    render,
    setSource,
    clearSource,
    setGeometryReference,
    setOverrideEnabled,
    setOverrideValues,
    setBeamCenter,
    resetOverride,
    setGeometryFile,
    clearGeometryFile,
    getActiveGeometryFile,
    overrideInEffect,
    getResolved: () => resolved,
  };
}
