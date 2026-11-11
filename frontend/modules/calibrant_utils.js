/**
 * Reference rings: which rings of a calibrant (or ice, or a metal) to draw.
 *
 * A material has dozens to thousands of reflections (pyFAI's LaB6 lists 1,441),
 * but a detector sees only those down to the d-spacing at its far corner.
 * Those are drawn, nearest the beam first, at most MAX_REFERENCE_RINGS of them
 * -- so a dense pattern at high energy stays readable, and the user's own
 * rings, which have their own limit, are not crowded out.
 */

import { CALIBRANTS } from "./calibrant_data.js";
import {
  applyGeometryOverrides,
  braggDSpacing,
  getGeometryResolutionAtPixel,
} from "./ring_geometry_utils.js";

// Keep in step with MAX_LINES in scripts/build_calibrant_data.py.
export const MAX_REFERENCE_RINGS = 60;

/** The calibrant with this id, or null. */
export function getCalibrant(id) {
  if (!id) return null;
  return CALIBRANTS.find((entry) => entry.id === id) || null;
}

/** The label's leading formula: "LaB₆" for "LaB₆ (SRM 660a)". */
export function calibrantShortName(calibrant) {
  return String(calibrant?.label || calibrant?.id || "").split(" (")[0];
}

/** What a reference ring is labelled with on the image: "Al 111", or its d. */
export function referenceRingLabel(calibrant, d, hkl) {
  const name = calibrantShortName(calibrant);
  if (hkl) return `${name} ${hkl}`;
  return `${name} ${Number(d).toFixed(2)} Å`;
}

function panelEdgePoints(panel) {
  const [x0, y0] = panel.raw_offset_px || [0, 0];
  const [w, h] = panel.image_size_px || [0, 0];
  if (!(w > 0 && h > 0)) return [];
  const x1 = x0 + w - 1;
  const y1 = y0 + h - 1;
  const xm = x0 + (w - 1) / 2;
  const ym = y0 + (h - 1) / 2;
  return [
    [x0, y0], [x1, y0], [x0, y1], [x1, y1],
    [xm, y0], [xm, y1], [x0, ym], [x1, ym],
  ];
}

/**
 * The smallest d-spacing on the detector -- at its corner farthest from the
 * beam -- for the ring parameters analysis_overlay_controller builds, or null
 * when the geometry is incomplete. Panel corners rather than image corners for
 * a multi-panel geometry: an image corner can fall in a gap between panels.
 */
export function detectorMinDSpacing(params, { width, height, pixelAspect = 1 } = {}) {
  if (!params?.energyEv) return null;
  let best = null;
  const consider = (d) => {
    if (Number.isFinite(d) && d > 0 && (best === null || d < best)) best = d;
  };
  if (params.mode === "geometry" && params.geometry) {
    const geometry = applyGeometryOverrides(params.geometry, {
      centerX: params.centerX,
      centerY: params.centerY,
      distanceMm: params.distanceMm,
      twoThetaDeg: params.twoThetaDeg,
    });
    for (const panel of geometry?.panels || []) {
      for (const [x, y] of panelEdgePoints(panel)) {
        consider(getGeometryResolutionAtPixel(x, y, geometry, params.energyEv));
      }
    }
    return best;
  }
  if (!params.distanceMm || !params.pixelSizeUm || !(width > 0 && height > 0)) return null;
  const aspect = Number.isFinite(pixelAspect) && pixelAspect > 0 ? pixelAspect : 1;
  const pixelMmX = params.pixelSizeUm / 1000;
  const pixelMmY = pixelMmX * aspect;
  let radiusMm = 0;
  for (const [x, y] of [[0, 0], [width, 0], [0, height], [width, height]]) {
    const r = Math.hypot((x - params.centerX) * pixelMmX, (y - params.centerY) * pixelMmY);
    if (Number.isFinite(r)) radiusMm = Math.max(radiusMm, r);
  }
  if (!(radiusMm > 0)) return null;
  consider(braggDSpacing(Math.atan2(radiusMm, params.distanceMm), params.energyEv));
  return best;
}

/**
 * The rings of a calibrant to draw: `[{ d, hkl, label }]`, highest d first,
 * those at or above `dMin` (all of them when it is unknown), at most `max`.
 */
export function selectReferenceRings(calibrant, dMin, max = MAX_REFERENCE_RINGS) {
  if (!calibrant || !Array.isArray(calibrant.lines)) return [];
  const floor = Number.isFinite(dMin) && dMin > 0 ? dMin * (1 - 1e-6) : 0;
  const out = [];
  for (const [d, hkl] of calibrant.lines) {
    if (!(d > 0) || d < floor) continue;
    out.push({ d, hkl: hkl || "", label: referenceRingLabel(calibrant, d, hkl) });
    if (out.length >= max) break;
  }
  return out;
}
