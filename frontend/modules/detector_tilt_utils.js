/**
 * A detector on a 2θ arm, built from the flat fields of Data → Detector
 * Geometry plus the arm's angle and the direction it swings.
 *
 * An NXmx file that records its transformation chain places a tilted detector
 * by itself (backend/services/nxmx_geometry.py). Other files do not: a CBF, a
 * TIFF, and older DECTRIS masters, which store the angle (`two_theta`) but not
 * the axis it turns about -- that belongs to the beamline's arm. So the user
 * states the direction, and this builds the same single panel the ring code
 * already draws for a file's own chain.
 *
 * The fields mean what they mean for such a file: the beam centre the detector
 * has at 2θ = 0, the distance along the arm. The arm turns the detector about
 * an axis through the sample, perpendicular to the beam and to the swing, so
 * the pixel at that beam centre sits at 2θ and higher angles lie the way the
 * arm swings. Frame: the beam along +z, as in NeXus and the ring code.
 */

const DIRECTION_VECTORS = Object.freeze({
  "+x": [1, 0, 0],
  "-x": [-1, 0, 0],
  "+y": [0, 1, 0],
  "-y": [0, -1, 0],
});

// Below this the detector is square to the beam for any purpose rings serve,
// as backend/services/nxmx_geometry.py decides for a file's chain.
const MIN_TILT_DEG = 0.1;

function rotate(vector, axis, angle) {
  // Rodrigues: v cos + (k × v) sin + k (k · v)(1 − cos).
  const [kx, ky, kz] = axis;
  const [vx, vy, vz] = vector;
  const c = Math.cos(angle);
  const s = Math.sin(angle);
  const dotKV = kx * vx + ky * vy + kz * vz;
  return [
    vx * c + (ky * vz - kz * vy) * s + kx * dotKV * (1 - c),
    vy * c + (kz * vx - kx * vz) * s + ky * dotKV * (1 - c),
    vz * c + (kx * vy - ky * vx) * s + kz * dotKV * (1 - c),
  ];
}

function positive(value) {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? number : null;
}

/**
 * The tilted detector as a ring-geometry payload, or null when there is
 * nothing to tilt (no direction, an angle under 0.1°) or a field is missing.
 *
 * Pixel coordinates are ALBIS's: pixel i's centre at (i + 0.5) pixel sizes from
 * the panel's corner, so the beam centre field is a pixel-centre coordinate.
 */
export function buildTiltedPanelGeometry({
  widthPx,
  heightPx,
  pixelSizeXUm,
  pixelSizeYUm,
  distanceMm,
  centerX,
  centerY,
  twoThetaDeg,
  direction,
} = {}) {
  const swing = DIRECTION_VECTORS[String(direction || "").toLowerCase()];
  const angle = Number(twoThetaDeg);
  const width = positive(widthPx);
  const height = positive(heightPx);
  const pixelX = positive(pixelSizeXUm);
  const pixelY = positive(pixelSizeYUm) ?? pixelX;
  const distance = positive(distanceMm);
  // null and "" are missing, not 0: Number(null) is 0.
  const cx = centerX === null || centerX === "" ? NaN : Number(centerX);
  const cy = centerY === null || centerY === "" ? NaN : Number(centerY);
  if (!swing || !Number.isFinite(angle) || Math.abs(angle) < MIN_TILT_DEG) return null;
  if (!width || !height || !pixelX || !pixelY || !distance) return null;
  if (!Number.isFinite(cx) || !Number.isFinite(cy)) return null;

  const pixelMm = [pixelX / 1000, pixelY / 1000];
  // Square to the beam first: the beam through the beam-centre pixel's centre.
  const flatOrigin = [-(cx + 0.5) * pixelMm[0], -(cy + 0.5) * pixelMm[1], distance];
  // Then swung: about beam × swing, which turns the beam toward the swing.
  const axis = [-swing[1], swing[0], 0];
  const radians = (angle * Math.PI) / 180;
  return {
    mode: "geometry",
    detector: "",
    source: `manual 2θ ${angle}° ${direction}`,
    panels: [
      {
        name: "manual",
        origin_mm: rotate(flatOrigin, axis, radians),
        fast_axis: rotate([1, 0, 0], axis, radians),
        slow_axis: rotate([0, 1, 0], axis, radians),
        pixel_size_mm: pixelMm,
        image_size_px: [Math.round(width), Math.round(height)],
        raw_offset_px: [0, 0],
      },
    ],
    reference_center_x_px: cx,
    reference_center_y_px: cy,
    reference_distance_mm: distance,
    distance_axis: rotate([0, 0, 1], axis, radians),
    // Its own arm, at the angle it was built with: the field's value turns it
    // by nothing more.
    two_theta_deg: angle,
    two_theta_axis: axis,
  };
}
