/**
 * Drag interactions for the resolution-ring overlay: move the beam center and
 * resize individual rings directly on the canvas.
 *
 * The ring geometry has no canvas-side state of its own — it is derived every
 * frame by getRingParams(). A ring drag sets that ring's d-spacing through
 * the ring list (setRingValue, by its index in analysisState.rings), which
 * shows it in its field; the list is sorted again when the drag ends. A beam-centre drag is a manual geometry override, the same as
 * typing a centre in Data → Detector Geometry, so it goes to setBeamCenter,
 * which switches the override on and keeps the centre across files.
 */

import { braggTwoTheta, wavelengthFromEnergy } from "./ring_geometry_utils.js";

const CENTER_HIT_PX = 12;
const RING_HIT_PX = 7;

export function createResolutionRingInteractionController({
  state,
  analysisState,
  elements,
  callbacks,
  core,
}) {
  const { canvasWrap } = elements;
  const {
    setBeamCenter,
    getEffectiveScrollLeft,
    getEffectiveScrollTop,
    getRingParams,
    getResolutionAtPixel,
    setRingValue,
    commitRings,
  } = callbacks;

  let ringEditing = false;
  let activeHandle = null;
  let hoverHandle = null;

  function getPointerCanvasPos(event) {
    if (!canvasWrap) return null;
    const rect = canvasWrap.getBoundingClientRect();
    return { x: event.clientX - rect.left, y: event.clientY - rect.top };
  }

  function imageToScreen(ix, iy) {
    const zoom = state.zoom || 1;
    const zoomY = zoom * (state.pixelAspect || 1);
    const offsetX = state.renderOffsetX || 0;
    const offsetY = state.renderOffsetY || 0;
    const viewX = getEffectiveScrollLeft() / zoom;
    const viewY = getEffectiveScrollTop() / zoomY;
    return { x: (ix - viewX) * zoom + offsetX, y: (iy - viewY) * zoomY + offsetY };
  }

  function screenToImage(px, py) {
    const zoom = state.zoom || 1;
    const zoomY = zoom * (state.pixelAspect || 1);
    const viewX = getEffectiveScrollLeft() / zoom;
    const viewY = getEffectiveScrollTop() / zoomY;
    return {
      x: (px - (state.renderOffsetX || 0)) / zoom + viewX,
      y: (py - (state.renderOffsetY || 0)) / zoomY + viewY,
    };
  }

  // The ring under the pointer when the rings are not circles -- panels, a
  // detector on a 2θ arm: how far, in screen pixels, the pointer's scattering
  // angle is from each ring's, by the angle's local rate of change. Works on
  // any geometry; a pointer in a gap between panels has no angle and finds none.
  function getGeometryRingAt(params, pointer) {
    const at = screenToImage(pointer.x, pointer.y);
    const angle = (ix, iy) => {
      const d = getResolutionAtPixel(ix, iy, params);
      return Number.isFinite(d) && d > 0 ? braggTwoTheta(d, params.energyEv) : null;
    };
    const here = angle(at.x, at.y);
    const samples = [
      angle(at.x + 1, at.y),
      angle(at.x - 1, at.y),
      angle(at.x, at.y + 1),
      angle(at.x, at.y - 1),
    ];
    if (here === null || samples.some((value) => value === null)) return null;
    const gradient = Math.hypot((samples[0] - samples[1]) / 2, (samples[2] - samples[3]) / 2);
    if (!(gradient > 0)) return null;
    const zoom = state.zoom || 1;
    let best = null;
    params.rings.forEach((value, index) => {
      const d = Number(value);
      const ringAngle = braggTwoTheta(d, params.energyEv);
      if (ringAngle === null) return;
      const delta = (Math.abs(here - ringAngle) / gradient) * zoom;
      if (delta <= RING_HIT_PX && (!best || delta < best.delta)) {
        best = { type: "ring", index, d, delta };
      }
    });
    return best ? { type: "ring", index: best.index, d: best.d } : null;
  }

  // Screen-space radius (px) of every ring that resolves to a valid
  // d-spacing. Planar mode only -- geometry-mode rings are not concentric
  // circles, and getGeometryRingAt finds those.
  function getScreenRings(params) {
    if (params.mode === "geometry") return [];
    if (!params.energyEv || !params.distanceMm || !params.pixelSizeUm) return [];
    if (!wavelengthFromEnergy(params.energyEv)) return [];
    const pixelSizeMm = params.pixelSizeUm / 1000;
    if (!Number.isFinite(pixelSizeMm) || pixelSizeMm <= 0) return [];
    const zoom = state.zoom || 1;
    const rings = [];
    params.rings.forEach((value, index) => {
      const d = Number(value);
      if (!Number.isFinite(d) || d <= 0) return;
      const twoTheta = braggTwoTheta(d, params.energyEv);
      if (twoTheta === null) return;
      const radiusMm = params.distanceMm * Math.tan(twoTheta);
      const radiusPx = radiusMm / pixelSizeMm;
      if (!Number.isFinite(radiusPx) || radiusPx <= 0) return;
      rings.push({ index, d, radius: radiusPx * zoom });
    });
    return rings;
  }

  function getRingHandleAt(event) {
    if (!analysisState.ringsEnabled || !state.hasFrame) return null;
    const pointer = getPointerCanvasPos(event);
    if (!pointer) return null;
    const params = getRingParams();
    if (!params.energyEv) return null;

    const center = imageToScreen(params.markerX ?? params.centerX, params.markerY ?? params.centerY);
    const distToCenter = Math.hypot(pointer.x - center.x, pointer.y - center.y);
    if (params.centerKnown && distToCenter <= CENTER_HIT_PX) {
      return { type: "center" };
    }

    if (params.mode === "geometry") {
      return params.geometry ? getGeometryRingAt(params, pointer) : null;
    }

    let best = null;
    getScreenRings(params).forEach((ring) => {
      const delta = Math.abs(distToCenter - ring.radius);
      if (delta <= RING_HIT_PX && (!best || delta < best.delta)) {
        best = { type: "ring", index: ring.index, d: ring.d, delta };
      }
    });
    if (best) {
      return { type: "ring", index: best.index, d: best.d };
    }
    return null;
  }

  function startRingEdit(handle) {
    if (!handle) return;
    ringEditing = true;
    activeHandle = handle;
    hoverHandle = null;
    canvasWrap.classList.add("is-ring-dragging");
    canvasWrap.classList.remove("is-ring-target");
    core.overlayRender.scheduleResolutionOverlay();
  }

  function applyRingEdit(point) {
    if (!ringEditing || !activeHandle || !point) return;
    if (activeHandle.type === "center") {
      // The marker sits where the beam meets the detector; the field holds the
      // 2θ = 0 centre, a fixed offset from it whatever the centre is moved to.
      const params = getRingParams();
      const offsetX = Number.isFinite(params.markerX) ? params.markerX - params.centerX : 0;
      const offsetY = Number.isFinite(params.markerY) ? params.markerY - params.centerY : 0;
      setBeamCenter?.(Math.round(point.x - offsetX), Math.round(point.y - offsetY));
      return;
    }
    if (activeHandle.type === "ring") {
      const d = getResolutionAtPixel(point.x, point.y);
      if (!Number.isFinite(d) || d <= 0) return;
      activeHandle.d = d;
      setRingValue?.(activeHandle.index, d);
    }
  }

  function stopRingEdit(event) {
    if (!ringEditing) return;
    const draggedRing = activeHandle?.type === "ring";
    ringEditing = false;
    activeHandle = null;
    if (draggedRing) commitRings?.();
    canvasWrap.classList.remove("is-ring-dragging");
    if (event && Number.isInteger(event.pointerId) && canvasWrap.hasPointerCapture?.(event.pointerId)) {
      canvasWrap.releasePointerCapture(event.pointerId);
    }
    core.overlayRender.scheduleResolutionOverlay();
  }

  function isRingEditing() {
    return ringEditing;
  }

  function handleKey(handle) {
    if (!handle) return "";
    return handle.type === "ring" ? `ring:${handle.index}` : handle.type;
  }

  // Hover affordance: switch the cursor and highlight the targeted handle.
  // Returns true when a handle is under the pointer so the caller can suppress
  // its own cursor handling.
  function updateRingHover(event) {
    if (ringEditing) return true;
    const handle = getRingHandleAt(event);
    const changed = handleKey(handle) !== handleKey(hoverHandle);
    hoverHandle = handle;
    canvasWrap.classList.toggle("is-ring-target", Boolean(handle));
    if (changed) core.overlayRender.scheduleResolutionOverlay();
    return Boolean(handle);
  }

  function clearRingHover() {
    if (!hoverHandle) return;
    hoverHandle = null;
    canvasWrap.classList.remove("is-ring-target");
    core.overlayRender.scheduleResolutionOverlay();
  }

  // Consumed by the overlay renderer to highlight the active/hovered handle.
  function getRingInteractionState() {
    return { handle: ringEditing ? activeHandle : hoverHandle };
  }

  return {
    getRingHandleAt,
    startRingEdit,
    applyRingEdit,
    stopRingEdit,
    isRingEditing,
    updateRingHover,
    clearRingHover,
    getRingInteractionState,
  };
}
