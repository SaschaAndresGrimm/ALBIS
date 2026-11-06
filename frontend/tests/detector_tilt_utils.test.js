import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { buildTiltedPanelGeometry } from "../modules/detector_tilt_utils.js";
import {
  HC_EV_ANGSTROM,
  applyGeometryOverrides,
  getGeometryBeamPosition,
  getGeometryResolutionAtPixel,
  prepareRingGeometry,
} from "../modules/ring_geometry_utils.js";

// The I19-2 2θ = 20° collection, as dxtbx sees its NeXus file. A hand-entered
// 2θ with the same angle, distance and direction must give the same detector.
const here = dirname(fileURLToPath(import.meta.url));
const FIXTURE = JSON.parse(
  readFileSync(resolve(here, "../../tests/fixtures/dls_i19_2_two_theta_dxtbx.json"), "utf8"),
);
const ENERGY_EV = HC_EV_ANGSTROM / FIXTURE.wavelength_a;

// The file's beam centre counts from the first pixel's corner, as NeXus and
// dxtbx do; the Beam centre field counts from its centre: half a pixel less.
const I19_2_BY_HAND = {
  widthPx: 2068,
  heightPx: 2162,
  pixelSizeXUm: 75,
  pixelSizeYUm: 75,
  distanceMm: 85,
  centerX: 985.0666666666667 - 0.5,
  centerY: 1419.4666666666665 - 0.5,
  twoThetaDeg: 20,
  // At I19-2 the beam moved from 985 to 572: higher angles lie toward +X.
  direction: "+x",
};

function prepared(overrides = {}) {
  return prepareRingGeometry(buildTiltedPanelGeometry({ ...I19_2_BY_HAND, ...overrides }));
}

describe("a hand-entered 2θ", () => {
  it.each(FIXTURE.dxtbx_d_spacings.filter(([, , d]) => d < 100))(
    "rebuilds the I19-2 detector: dxtbx's d at pixel (%i, %i)",
    (x, y, expected) => {
      const d = getGeometryResolutionAtPixel(x, y, prepared(), ENERGY_EV);
      expect(Math.abs(d - expected) / expected).toBeLessThan(1e-9);
    },
  );

  it("puts the beam where dxtbx finds it", () => {
    const beam = getGeometryBeamPosition(prepared());
    expect(beam.x).toBeCloseTo(FIXTURE.dxtbx_beam_centre_px[0] - 0.5, 6);
    expect(beam.y).toBeCloseTo(FIXTURE.dxtbx_beam_centre_px[1] - 0.5, 6);
  });

  it("measures its own fields from the values it was built with", () => {
    const geometry = prepared();
    expect(applyGeometryOverrides(geometry, {
      centerX: I19_2_BY_HAND.centerX,
      centerY: I19_2_BY_HAND.centerY,
      distanceMm: 85,
    })).toBe(geometry);
  });

  it.each([
    ["+x", (beam, c) => beam.x < c.centerX],
    ["-x", (beam, c) => beam.x > c.centerX],
    ["+y", (beam, c) => beam.y < c.centerY],
    ["-y", (beam, c) => beam.y > c.centerY],
  ])("swings %s: higher angles that way, the beam the other", (direction, beamIsOpposite) => {
    const beam = getGeometryBeamPosition(prepared({ direction }));
    expect(beamIsOpposite(beam, I19_2_BY_HAND)).toBe(true);
  });

  it.each([
    ["no direction", { direction: "" }],
    ["an unknown direction", { direction: "up" }],
    ["no angle", { twoThetaDeg: 0 }],
    ["an angle too small to matter", { twoThetaDeg: 0.05 }],
    ["no distance", { distanceMm: null }],
    ["no beam centre", { centerX: null }],
    ["no image size", { widthPx: 0 }],
  ])("builds nothing with %s", (_label, overrides) => {
    expect(buildTiltedPanelGeometry({ ...I19_2_BY_HAND, ...overrides })).toBeNull();
  });
});
