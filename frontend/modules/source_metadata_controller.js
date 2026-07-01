/**
 * Source metadata parsing and UI synchronization.
 */

import { t } from "./i18n.js";
import {
  getActiveGeometryOverridePath,
  getGeometryScopeKey,
  isGeometryLockActive,
} from "./geometry_override_utils.js";
import { getGeometryReferencePose, prepareRingGeometry } from "./ring_geometry_utils.js";

export function createSourceMetadataController({
  state,
  analysisState,
  elements,
  callbacks,
}) {
  const {
    simplonMetaPanel,
    simplonSeriesEl,
    simplonImageEl,
    simplonTimeEl,
    simplonEnergyEl,
    simplonThresholdEl,
    simplonWavelengthEl,
    simplonDistanceEl,
    simplonCenterEl,
    remoteMetaPanel,
    remoteSourceEl,
    remoteSeqEl,
    remoteSeriesEl,
    remoteImageEl,
    remoteTimeEl,
    remoteEnergyEl,
    remoteWavelengthEl,
    remoteDistanceEl,
    remoteCenterEl,
    remotePeakSetsEl,
    jfjochMetaPanel,
    jfjochSourceEl,
    jfjochSeqEl,
    jfjochSeriesEl,
    jfjochImageEl,
    jfjochTimeEl,
    jfjochReflectionsEl,
    jfjochChannelMetaEl,
    jfjochBridgeStatusEl,
    ringsDistance,
    ringsPixel,
    ringsEnergy,
    ringsCenterX,
    ringsCenterY,
    ringsGeometryFile,
    ringsGeometryFileHint,
    ringsGeometryBrowse,
    ringsGeometryClear,
    ringsGeometryStatusEl,
    ringsGeometryLockEl,
    ringsGeometryLockLabel,
    ringsGeometryLockReset,
  } = elements;

  const { scheduleResolutionOverlay, schedulePeakOverlay, refreshPeakResolutions } = callbacks;

  // Any geometry change (distance/center/energy or a geometry file) repaints the
  // resolution rings and must also refresh the peak-list d-spacings, which are
  // geometry-derived. refreshPeakResolutions is lightweight (no re-detection)
  // and no-ops when nothing actually changed.
  function scheduleGeometryDependentOverlays() {
    scheduleResolutionOverlay();
    refreshPeakResolutions?.();
  }

  function formatNumberInput(value, digits = 2) {
    if (!Number.isFinite(value)) return "";
    const rounded = Number(value.toFixed(digits));
    return Number.isInteger(rounded) ? String(rounded) : String(rounded);
  }

  function hasGeometryManualOverride(flagName) {
    const activeKey = String(analysisState.ringGeometryKey || "");
    const manualKey = String(analysisState.geometryManualKey || "");
    return Boolean(analysisState[flagName] && activeKey && manualKey && activeKey === manualKey);
  }

  function setDistanceInputValue(value) {
    if (ringsDistance && Number.isFinite(value)) {
      ringsDistance.value = formatNumberInput(value, 2);
    }
  }

  function setCenterInputValue(inputEl, value) {
    if (inputEl && Number.isFinite(value)) {
      inputEl.value = formatNumberInput(value, 2);
    }
  }

  function clearGeometryManualOverrides() {
    analysisState.geometryManualKey = "";
    analysisState.geometryDistanceManual = false;
    analysisState.geometryCenterXManual = false;
    analysisState.geometryCenterYManual = false;
  }

  function parseHeaderFloat(headers, key) {
    if (!headers) return null;
    const raw = headers.get(key);
    if (raw === null || raw === undefined || raw === "") return null;
    const value = Number(raw);
    return Number.isFinite(value) ? value : null;
  }

  function cloneMetaObject(value) {
    return value && typeof value === "object" ? { ...value } : {};
  }

  function clonePeakSets(peakSets) {
    if (!Array.isArray(peakSets)) return [];
    return peakSets.map((set) => ({
      ...cloneMetaObject(set),
      points: Array.isArray(set?.points)
        ? set.points.map((point) => ({
            ...cloneMetaObject(point),
          }))
        : [],
    }));
  }

  function normalizeAnalysis(analysis) {
    const source = analysis && typeof analysis === "object" ? analysis : {};
    return {
      distanceMm: Number.isFinite(Number(source.distanceMm)) ? Number(source.distanceMm) : null,
      pixelSizeUm: Number.isFinite(Number(source.pixelSizeUm)) ? Number(source.pixelSizeUm) : null,
      energyEv: Number.isFinite(Number(source.energyEv)) ? Number(source.energyEv) : null,
      centerX: Number.isFinite(Number(source.centerX)) ? Number(source.centerX) : null,
      centerY: Number.isFinite(Number(source.centerY)) ? Number(source.centerY) : null,
    };
  }

  function parseSimplonTimestamp(raw) {
    if (!raw) return null;
    const cleaned = String(raw).replace(/\.(\d{3})\d+Z$/, ".$1Z");
    const parsed = new Date(cleaned);
    if (!Number.isNaN(parsed.valueOf())) return parsed;
    return null;
  }

  function formatSimplonTimestamp(raw) {
    const parsed = parseSimplonTimestamp(raw);
    if (parsed) {
      return parsed.toLocaleString();
    }
    return raw ? String(raw) : "";
  }

  function formatSimplonValue(value, digits = 2) {
    if (value === null || value === undefined || value === "") return "-";
    if (typeof value === "number" && Number.isFinite(value)) {
      if (Number.isInteger(value)) return String(value);
      return value.toFixed(digits);
    }
    return String(value);
  }

  function formatGeometrySource(raw) {
    const text = String(raw || "").trim();
    if (!text) return "";
    const normalized = text.replace(/\\/g, "/");
    const parts = normalized.split("/").filter(Boolean);
    if (parts.length <= 2) return parts.join("/");
    return parts.slice(-2).join("/");
  }

  function currentGeometryScopeKey() {
    return getGeometryScopeKey(state, state.file || "");
  }

  function visibleGeometryOverridePath() {
    return getActiveGeometryOverridePath(analysisState, currentGeometryScopeKey());
  }

  function updateGeometryUi() {
    const geometryActive = analysisState.ringMode === "geometry" && analysisState.ringGeometry;
    const scopeKey = currentGeometryScopeKey();
    const overridePath = visibleGeometryOverridePath();
    if (ringsPixel) {
      ringsPixel.disabled = Boolean(geometryActive);
    }
    if (ringsGeometryFile) {
      ringsGeometryFile.value = overridePath;
    }
    if (ringsGeometryFileHint && !overridePath) {
      ringsGeometryFileHint.classList.add("is-hidden");
      ringsGeometryFileHint.textContent = "";
    }
    if (ringsGeometryBrowse) {
      ringsGeometryBrowse.disabled = !scopeKey;
    }
    if (ringsGeometryClear) {
      ringsGeometryClear.disabled = !overridePath;
    }
    if (!ringsGeometryStatusEl) return;
    if (!geometryActive) {
      ringsGeometryStatusEl.classList.add("is-hidden");
      ringsGeometryStatusEl.textContent = "";
      ringsGeometryStatusEl.removeAttribute("title");
      return;
    }
    const source = formatGeometrySource(analysisState.ringGeometrySource) || t("common.ready");
    const statusKey = analysisState.geometryOverrideActive
      ? "rings.geometry.status_manual"
      : "rings.geometry.status_auto";
    ringsGeometryStatusEl.textContent = t(statusKey, { source });
    if (analysisState.ringGeometrySource) {
      ringsGeometryStatusEl.title = analysisState.ringGeometrySource;
    } else {
      ringsGeometryStatusEl.removeAttribute("title");
    }
    ringsGeometryStatusEl.classList.remove("is-hidden");
  }

  function updateSimplonMetaUI(meta) {
    if (!simplonMetaPanel) return;
    if (state.autoload.mode !== "simplon") {
      simplonMetaPanel.classList.add("is-hidden");
      return;
    }
    simplonMetaPanel.classList.remove("is-hidden");
    if (simplonSeriesEl) simplonSeriesEl.textContent = formatSimplonValue(meta.series);
    if (simplonImageEl) simplonImageEl.textContent = formatSimplonValue(meta.image);
    if (simplonTimeEl) simplonTimeEl.textContent = formatSimplonTimestamp(meta.date) || "-";
    if (simplonEnergyEl) simplonEnergyEl.textContent = formatSimplonValue(meta.energyEv, 1);
    if (simplonThresholdEl) simplonThresholdEl.textContent = formatSimplonValue(meta.thresholdEv, 1);
    if (simplonWavelengthEl) simplonWavelengthEl.textContent = formatSimplonValue(meta.wavelengthA, 4);
    if (simplonDistanceEl) simplonDistanceEl.textContent = formatSimplonValue(meta.distanceMm, 2);
    if (simplonCenterEl) {
      if (Number.isFinite(meta.centerX) && Number.isFinite(meta.centerY)) {
        simplonCenterEl.textContent = `${Math.round(meta.centerX)}, ${Math.round(meta.centerY)}`;
      } else {
        simplonCenterEl.textContent = "-";
      }
    }
  }

  function updateRemoteMetaUI(meta) {
    if (!remoteMetaPanel) return;
    if (state.autoload.mode !== "remote") {
      remoteMetaPanel.classList.add("is-hidden");
      return;
    }
    remoteMetaPanel.classList.remove("is-hidden");
    if (remoteSourceEl) remoteSourceEl.textContent = formatSimplonValue(meta.source);
    if (remoteSeqEl) remoteSeqEl.textContent = formatSimplonValue(meta.seq);
    if (remoteSeriesEl) remoteSeriesEl.textContent = formatSimplonValue(meta.series);
    if (remoteImageEl) remoteImageEl.textContent = formatSimplonValue(meta.image);
    if (remoteTimeEl) remoteTimeEl.textContent = formatSimplonTimestamp(meta.date) || "-";
    if (remoteEnergyEl) remoteEnergyEl.textContent = formatSimplonValue(meta.energyEv, 1);
    if (remoteWavelengthEl) remoteWavelengthEl.textContent = formatSimplonValue(meta.wavelengthA, 4);
    if (remoteDistanceEl) remoteDistanceEl.textContent = formatSimplonValue(meta.distanceMm, 2);
    if (remoteCenterEl) {
      if (Number.isFinite(meta.centerX) && Number.isFinite(meta.centerY)) {
        remoteCenterEl.textContent = `${Math.round(meta.centerX)}, ${Math.round(meta.centerY)}`;
      } else {
        remoteCenterEl.textContent = "-";
      }
    }
    if (remotePeakSetsEl) {
      const count = Number(meta.peakSets || 0);
      remotePeakSetsEl.textContent = Number.isFinite(count) ? String(Math.max(0, Math.round(count))) : "-";
    }
  }

  function updateJfjochMetaUI(meta, status = {}) {
    if (!jfjochMetaPanel) return;
    if (state.autoload.mode !== "jungfraujoch") {
      jfjochMetaPanel.classList.add("is-hidden");
      return;
    }
    jfjochMetaPanel.classList.remove("is-hidden");
    if (jfjochSourceEl) jfjochSourceEl.textContent = formatSimplonValue(meta.source || state.autoload.jfjochSourceId);
    if (jfjochSeqEl) jfjochSeqEl.textContent = formatSimplonValue(meta.seq);
    if (jfjochSeriesEl) jfjochSeriesEl.textContent = formatSimplonValue(meta.series);
    if (jfjochImageEl) jfjochImageEl.textContent = formatSimplonValue(meta.image);
    if (jfjochTimeEl) jfjochTimeEl.textContent = formatSimplonTimestamp(meta.date) || "-";
    if (jfjochReflectionsEl) {
      const count = Number(meta.reflections ?? meta.peakSets ?? 0);
      jfjochReflectionsEl.textContent = Number.isFinite(count)
        ? String(Math.max(0, Math.round(count)))
        : "-";
    }
    if (jfjochChannelMetaEl) jfjochChannelMetaEl.textContent = formatSimplonValue(meta.channel || "-");
    if (jfjochBridgeStatusEl) {
      if (status?.last_error) {
        jfjochBridgeStatusEl.textContent = String(status.last_error);
      } else if (status?.running) {
        jfjochBridgeStatusEl.textContent = t("source.bridge.running");
      } else {
        jfjochBridgeStatusEl.textContent = t("source.bridge.stopped");
      }
    }
  }

  function applyAnalysisMeta({ distanceMm, pixelSizeUm, energyEv, centerX, centerY }) {
    // Image/remote sources expose a single pixel size and are treated as
    // square; per-axis ("strixel") aspect only comes from HDF master files.
    state.pixelAspect = 1;
    if (isGeometryLockActive(analysisState, state)) {
      updateGeometryLockUi();
      return;
    }
    let updated = false;
    if (Number.isFinite(distanceMm) && ringsDistance && !hasGeometryManualOverride("geometryDistanceManual")) {
      analysisState.distanceMm = distanceMm;
      setDistanceInputValue(distanceMm);
      updated = true;
    }
    if (Number.isFinite(pixelSizeUm) && ringsPixel) {
      analysisState.pixelSizeUm = pixelSizeUm;
      ringsPixel.value = pixelSizeUm.toFixed(2);
      updated = true;
    }
    if (Number.isFinite(energyEv) && ringsEnergy) {
      analysisState.energyEv = energyEv;
      ringsEnergy.value = String(Math.round(energyEv));
      updated = true;
    }
    if (Number.isFinite(centerX) && ringsCenterX && !hasGeometryManualOverride("geometryCenterXManual")) {
      analysisState.centerX = centerX;
      setCenterInputValue(ringsCenterX, centerX);
      updated = true;
    }
    if (Number.isFinite(centerY) && ringsCenterY && !hasGeometryManualOverride("geometryCenterYManual")) {
      analysisState.centerY = centerY;
      setCenterInputValue(ringsCenterY, centerY);
      updated = true;
    }
    if (updated) {
      scheduleGeometryDependentOverlays();
    }
    updateGeometryLockUi();
  }

  function liveSourceActive() {
    return Boolean(state.autoload?.running) && analysisState.ringMode !== "geometry";
  }

  function updateGeometryLockUi() {
    if (!ringsGeometryLockEl) return;
    if (!liveSourceActive()) {
      ringsGeometryLockEl.classList.add("is-hidden");
      ringsGeometryLockEl.classList.remove("is-locked");
      return;
    }
    const locked = isGeometryLockActive(analysisState, state);
    ringsGeometryLockEl.classList.remove("is-hidden");
    ringsGeometryLockEl.classList.toggle("is-locked", locked);
    if (ringsGeometryLockLabel) {
      ringsGeometryLockLabel.textContent = locked ? t("rings.lock.locked") : t("rings.lock.live");
    }
    if (ringsGeometryLockReset) {
      ringsGeometryLockReset.classList.toggle("is-hidden", !locked);
    }
  }

  // Re-apply the most recent live metadata so locked fields snap back to the
  // values currently arriving from the source.
  function reapplyLiveAnalysis() {
    const auto = state.autoload || {};
    const mode = String(auto.mode || "");
    let meta = null;
    if (mode === "simplon") meta = auto.simplonMeta;
    else if (mode === "remote") meta = auto.remoteMeta;
    else if (mode === "jungfraujoch") meta = auto.jfjochMeta;
    if (!meta || typeof meta !== "object") return;
    applyAnalysisMeta(
      normalizeAnalysis({
        distanceMm: meta.distanceMm,
        pixelSizeUm: meta.pixelSizeUm ?? null,
        energyEv: meta.energyEv,
        centerX: meta.centerX,
        centerY: meta.centerY,
      }),
    );
  }

  function resetGeometryLock() {
    analysisState.geometryLocked = false;
    analysisState.geometryLockKey = "";
    reapplyLiveAnalysis();
    updateGeometryLockUi();
    scheduleGeometryDependentOverlays();
  }

  function parseSimplonMeta(headers) {
    if (!headers) {
      return {
        analysis: normalizeAnalysis({}),
        meta: {},
      };
    }
    const distanceMm = parseHeaderFloat(headers, "X-Simplon-DetectorDistance-MM");
    const energyEv = parseHeaderFloat(headers, "X-Simplon-Energy-Ev");
    const thresholdEv = parseHeaderFloat(headers, "X-Simplon-Threshold-Ev");
    const wavelengthA = parseHeaderFloat(headers, "X-Simplon-Wavelength-A");
    const centerX = parseHeaderFloat(headers, "X-Simplon-BeamCenter-X");
    const centerY = parseHeaderFloat(headers, "X-Simplon-BeamCenter-Y");

    return {
      analysis: normalizeAnalysis({ distanceMm, pixelSizeUm: null, energyEv, centerX, centerY }),
      meta: {
        series: headers.get("X-Simplon-Series") || "",
        image: headers.get("X-Simplon-Image") || "",
        date: headers.get("X-Simplon-Date") || "",
        energyEv,
        thresholdEv,
        wavelengthA,
        distanceMm,
        centerX,
        centerY,
      },
    };
  }

  function parseRemoteMeta(headers) {
    if (!headers) {
      return {
        analysis: normalizeAnalysis({}),
        meta: {},
      };
    }
    const distanceMm = parseHeaderFloat(headers, "X-Remote-DetectorDistance-MM");
    const pixelSizeUm = parseHeaderFloat(headers, "X-Remote-PixelSize-UM");
    let energyEv = parseHeaderFloat(headers, "X-Remote-Energy-Ev");
    const wavelengthA = parseHeaderFloat(headers, "X-Remote-Wavelength-A");
    const centerX = parseHeaderFloat(headers, "X-Remote-BeamCenter-X");
    const centerY = parseHeaderFloat(headers, "X-Remote-BeamCenter-Y");
    if (!Number.isFinite(energyEv) && Number.isFinite(wavelengthA) && wavelengthA > 0) {
      energyEv = 12398.4193 / wavelengthA;
    }

    return {
      analysis: normalizeAnalysis({ distanceMm, pixelSizeUm, energyEv, centerX, centerY }),
      meta: {
        source: headers.get("X-Remote-Source") || state.autoload.remoteSourceId || "",
        seq: Number(headers.get("X-Remote-Seq") || 0),
        displayName: headers.get("X-Remote-Display") || "",
        series: headers.get("X-Remote-Series") || "",
        image: headers.get("X-Remote-Image") || "",
        date: headers.get("X-Remote-Date") || "",
        energyEv,
        wavelengthA,
        distanceMm,
        centerX,
        centerY,
        peakSets: Number(headers.get("X-Remote-PeakSets") || 0),
      },
    };
  }

  function createLiveSourceSnapshot({
    sourceKind,
    analysis = {},
    simplonMeta = {},
    remoteMeta = {},
    jfjochMeta = {},
    jfjochStatus = {},
    externalPeakSets = [],
  } = {}) {
    return {
      sourceKind: String(sourceKind || ""),
      analysis: normalizeAnalysis(analysis),
      simplonMeta: cloneMetaObject(simplonMeta),
      remoteMeta: cloneMetaObject(remoteMeta),
      jfjochMeta: cloneMetaObject(jfjochMeta),
      jfjochStatus: cloneMetaObject(jfjochStatus),
      externalPeakSets: clonePeakSets(externalPeakSets),
    };
  }

  function applyLiveSourceSnapshot(snapshot) {
    const source = snapshot && typeof snapshot === "object" ? snapshot : {};
    const sourceKind = String(source.sourceKind || "");
    applyAnalysisMeta(normalizeAnalysis(source.analysis));

    if (sourceKind === "simplon") {
      const meta = cloneMetaObject(source.simplonMeta);
      state.autoload.simplonMeta = meta;
      updateSimplonMetaUI(meta);
    } else if (sourceKind === "remote") {
      const meta = cloneMetaObject(source.remoteMeta);
      state.autoload.remoteMeta = meta;
      if (Number.isFinite(Number(meta.seq))) {
        state.autoload.remoteSeq = Number(meta.seq);
      }
      updateRemoteMetaUI(meta);
    } else if (sourceKind === "jungfraujoch") {
      const meta = cloneMetaObject(source.jfjochMeta);
      const status = cloneMetaObject(source.jfjochStatus);
      state.autoload.jfjochMeta = meta;
      state.autoload.jfjochStatus = status;
      updateJfjochMetaUI(meta, status);
    }

    analysisState.externalPeakSets = clonePeakSets(source.externalPeakSets);
    schedulePeakOverlay?.();
  }

  function clearImageGeometry({ clearKey = true } = {}) {
    analysisState.ringMode = "planar";
    analysisState.ringGeometry = null;
    analysisState.ringGeometrySource = "";
    analysisState.geometryOverrideActive = false;
    clearGeometryManualOverrides();
    if (clearKey) {
      analysisState.ringGeometryKey = "";
    }
    updateGeometryUi();
    scheduleGeometryDependentOverlays();
  }

  function applyImageGeometry(payload, cacheKey = "", { overrideActive = false } = {}) {
    analysisState.ringGeometryKey = cacheKey ? String(cacheKey) : "";
    const prepared = prepareRingGeometry(payload);
    if (!prepared) {
      analysisState.ringMode = "planar";
      analysisState.ringGeometry = null;
      analysisState.ringGeometrySource = "";
      analysisState.geometryOverrideActive = false;
      clearGeometryManualOverrides();
      updateGeometryUi();
      scheduleGeometryDependentOverlays();
      return;
    }
    const reference = getGeometryReferencePose(prepared);
    analysisState.ringMode = "geometry";
    analysisState.ringGeometry = prepared;
    analysisState.ringGeometrySource = String(prepared.source || "");
    analysisState.geometryOverrideActive = Boolean(overrideActive);
    const shouldSeedFromManualOverride = Boolean(overrideActive);
    if (
      reference &&
      !hasGeometryManualOverride("geometryDistanceManual") &&
      (shouldSeedFromManualOverride || !Number.isFinite(analysisState.distanceMm) || analysisState.distanceMm <= 0)
    ) {
      analysisState.distanceMm = reference.distanceMm;
      setDistanceInputValue(reference.distanceMm);
    }
    if (
      reference &&
      !hasGeometryManualOverride("geometryCenterXManual") &&
      (shouldSeedFromManualOverride || !Number.isFinite(analysisState.centerX))
    ) {
      analysisState.centerX = reference.centerX;
      setCenterInputValue(ringsCenterX, reference.centerX);
    }
    if (
      reference &&
      !hasGeometryManualOverride("geometryCenterYManual") &&
      (shouldSeedFromManualOverride || !Number.isFinite(analysisState.centerY))
    ) {
      analysisState.centerY = reference.centerY;
      setCenterInputValue(ringsCenterY, reference.centerY);
    }
    updateGeometryUi();
    scheduleGeometryDependentOverlays();
  }

  function applyImageMeta(headers) {
    if (!headers) return;
    const distanceMm = parseHeaderFloat(headers, "X-Image-DetectorDistance-MM");
    const pixelSizeUm = parseHeaderFloat(headers, "X-Image-PixelSize-UM");
    let energyEv = parseHeaderFloat(headers, "X-Image-Energy-Ev");
    const wavelengthA = parseHeaderFloat(headers, "X-Image-Wavelength-A");
    const centerX = parseHeaderFloat(headers, "X-Image-BeamCenter-X");
    const centerY = parseHeaderFloat(headers, "X-Image-BeamCenter-Y");
    if (!Number.isFinite(energyEv) && Number.isFinite(wavelengthA) && wavelengthA > 0) {
      energyEv = 12398.4193 / wavelengthA;
    }
    applyAnalysisMeta({ distanceMm, pixelSizeUm, energyEv, centerX, centerY });
  }

  function applySimplonMeta(headers) {
    const parsed = parseSimplonMeta(headers);
    applyLiveSourceSnapshot(
      createLiveSourceSnapshot({
        sourceKind: "simplon",
        analysis: parsed.analysis,
        simplonMeta: parsed.meta,
      }),
    );
    return parsed.meta;
  }

  function applyRemoteMeta(headers) {
    const parsed = parseRemoteMeta(headers);
    applyLiveSourceSnapshot(
      createLiveSourceSnapshot({
        sourceKind: "remote",
        analysis: parsed.analysis,
        remoteMeta: parsed.meta,
        externalPeakSets: analysisState.externalPeakSets,
      }),
    );
    return parsed.meta;
  }

  return {
    parseHeaderFloat,
    formatSimplonTimestamp,
    updateSimplonMetaUI,
    updateRemoteMetaUI,
    updateJfjochMetaUI,
    updateGeometryUi,
    updateGeometryLockUi,
    resetGeometryLock,
    parseSimplonMeta,
    parseRemoteMeta,
    createLiveSourceSnapshot,
    applyLiveSourceSnapshot,
    applyImageGeometry,
    applyImageMeta,
    applySimplonMeta,
    applyRemoteMeta,
    clearImageGeometry,
  };
}
