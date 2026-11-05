import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  HC_EV_ANGSTROM,
  applyGeometryOverrides,
  getGeometryBeamPosition,
  getGeometryResolutionAtPixel,
  prepareRingGeometry,
  serializeGeometryPayload,
} from "../modules/ring_geometry_utils.js";

// Diamond I19-2's Eiger2 4M on its 2θ arm at 20°: the panel the backend reads
// from the NXmx file, and dxtbx's d-spacings for the same file, without
// parallax correction since ALBIS applies none (tests/test_nxmx_geometry.py).
const here = dirname(fileURLToPath(import.meta.url));
const FIXTURE = JSON.parse(
  readFileSync(resolve(here, "../../tests/fixtures/dls_i19_2_two_theta_dxtbx.json"), "utf8"),
);
const ENERGY_EV = HC_EV_ANGSTROM / FIXTURE.wavelength_a;
// What the Detector Geometry fields hold for this file: its own values.
const FILE_FIELDS = { centerX: 985.0666666666667, centerY: 1419.4666666666665, distanceMm: 85 };

function geometryWith(fields = FILE_FIELDS) {
  return applyGeometryOverrides(prepareRingGeometry(FIXTURE.albis_geometry), fields);
}

describe("a detector on a 2θ arm (DLS I19-2, 20°)", () => {
  it.each(FIXTURE.dxtbx_d_spacings.filter(([, , d]) => d < 100))(
    "matches dxtbx at pixel (%i, %i)",
    (x, y, expected) => {
      const d = getGeometryResolutionAtPixel(x, y, geometryWith(), ENERGY_EV);
      expect(Math.abs(d - expected) / expected).toBeLessThan(1e-9);
    },
  );

  it("measures its fields against the file's 2θ = 0 beam centre, not the beam's hit", () => {
    // Taking the beam's hit (572.6, 1419.5) as the reference made the file's
    // own beam centre shift the detector by 413 px.
    const geometry = prepareRingGeometry(FIXTURE.albis_geometry);
    expect(geometry.reference_center_x_px).toBeCloseTo(985.0667, 4);
    expect(geometry.reference_distance_mm).toBe(85);
    expect(applyGeometryOverrides(geometry, FILE_FIELDS)).toBe(geometry);
  });

  it("puts the stated beam centre at 20° 2θ, where it really is", () => {
    const d = getGeometryResolutionAtPixel(985, 1419, geometryWith(), ENERGY_EV);
    expect(d).toBeCloseTo(1.39758, 4);
  });

  it("finds the beam where dxtbx does, which is where the marker goes", () => {
    // dxtbx counts from the first pixel's corner, ALBIS from its centre.
    const beam = getGeometryBeamPosition(geometryWith());
    expect(beam.x).toBeCloseTo(FIXTURE.dxtbx_beam_centre_px[0] - 0.5, 6);
    expect(beam.y).toBeCloseTo(FIXTURE.dxtbx_beam_centre_px[1] - 0.5, 6);
    const moved = getGeometryBeamPosition(geometryWith({ ...FILE_FIELDS, centerX: FILE_FIELDS.centerX + 10 }));
    expect(moved.x - beam.x).toBeCloseTo(10, 9);
  });

  it("moves the detector along the arm when the distance changes", () => {
    const before = geometryWith().panels[0].origin_mm;
    const after = geometryWith({ ...FILE_FIELDS, distanceMm: 95 }).panels[0].origin_mm;
    const axis = FIXTURE.albis_geometry.distance_axis;
    after.forEach((value, i) => expect(value - before[i]).toBeCloseTo(10 * axis[i], 9));
  });

  it("moves the image under the rings when the beam centre changes", () => {
    const shifted = geometryWith({ ...FILE_FIELDS, centerX: FILE_FIELDS.centerX + 10 });
    const original = getGeometryResolutionAtPixel(1500, 700, geometryWith(), ENERGY_EV);
    expect(getGeometryResolutionAtPixel(1510, 700, shifted, ENERGY_EV)).toBeCloseTo(original, 12);
  });

  it("keeps its reference and arm when serialized, as a sum embeds it", () => {
    const serialized = serializeGeometryPayload(geometryWith());
    expect(serialized.reference_center_x_px).toBeCloseTo(985.0667, 4);
    expect(serialized.distance_axis).toEqual(FIXTURE.albis_geometry.distance_axis);
    const roundTrip = prepareRingGeometry(serialized);
    const d = getGeometryResolutionAtPixel(250, 700, roundTrip, ENERGY_EV);
    expect(d).toBeCloseTo(getGeometryResolutionAtPixel(250, 700, geometryWith(), ENERGY_EV), 12);
  });
});
