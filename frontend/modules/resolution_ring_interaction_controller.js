/**
 * Drag interactions for the resolution-ring overlay: move the beam center and
 * resize individual rings directly on the canvas.
 *
 * The ring geometry has no canvas-side state of its own — it is derived every
 * frame by getRingParams(). A ring drag writes the new d-spacing into that
 * ring's input and dispatches a native "input" event, so it behaves exactly
 * like typing. A beam-centre drag is a manual geometry override, the same as
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
}) {
  const { canvasWrap, ringInputs } = elements;
  const {
    setBeamCenter,
    getEffectiveScrollLeft,
    getEffectiveScrollTop,
    getRingParams,
    getResolutionAtPixel,
    scheduleResolutionOverlay,
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

  // Screen-space radius (px) of every ring input that resolves to a valid
  // d-spacing. Planar mode only — geometry-mode rings are panel-clipped
  // polylines, not concentric circles, so only the center is draggable there.
  function getScreenRings(params) {
    if (params.mode === "geometry") return [];
    if (!params.energyEv || !params.distanceMm || !params.pixelSizeUm) return [];
    if (!wavelengthFromEnergy(params.energyEv)) return [];
    const pixelSizeMm = params.pixelSizeUm / 1000;
    if (!Number.isFinite(pixelSizeMm) || pixelSizeMm <= 0) return [];
    const zoom = state.zoom || 1;
    const rings = [];
    ringInputs.forEach((input, index) => {
      const d = Number(input?.value);
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

    const center = imageToScreen(params.centerX, params.centerY);
    const distToCenter = Math.hypot(pointer.x - center.x, pointer.y - center.y);
    if (params.centerKnown && distToCenter <= CENTER_HIT_PX) {
      return { type: "center" };
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

  function commitInput(input, value) {
    if (!input) return;
    input.value = value;
    input.dispatchEvent(new window.Event("input", { bubbles: true }));
  }

  function startRingEdit(handle) {
    if (!handle) return;
    ringEditing = true;
    activeHandle = handle;
    hoverHandle = null;
    canvasWrap.classList.add("is-ring-dragging");
    canvasWrap.classList.remove("is-ring-target");
    scheduleResolutionOverlay();
  }

  function applyRingEdit(point) {
    if (!ringEditing || !activeHandle || !point) return;
    if (activeHandle.type === "center") {
      setBeamCenter?.(Math.round(point.x), Math.round(point.y));
      return;
    }
    if (activeHandle.type === "ring") {
      const d = getResolutionAtPixel(point.x, point.y);
      if (!Number.isFinite(d) || d <= 0) return;
      const input = ringInputs[activeHandle.index];
      activeHandle.d = d;
      commitInput(input, String(Number(d.toFixed(3))));
    }
  }

  function stopRingEdit(event) {
    if (!ringEditing) return;
    ringEditing = false;
    activeHandle = null;
    canvasWrap.classList.remove("is-ring-dragging");
    if (event && Number.isInteger(event.pointerId) && canvasWrap.hasPointerCapture?.(event.pointerId)) {
      canvasWrap.releasePointerCapture(event.pointerId);
    }
    scheduleResolutionOverlay();
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
    if (changed) scheduleResolutionOverlay();
    return Boolean(handle);
  }

  function clearRingHover() {
    if (!hoverHandle) return;
    hoverHandle = null;
    canvasWrap.classList.remove("is-ring-target");
    scheduleResolutionOverlay();
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
