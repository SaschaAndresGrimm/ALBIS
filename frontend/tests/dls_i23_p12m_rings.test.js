import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  HC_EV_ANGSTROM,
  applyGeometryOverrides,
  getGeometryResolutionAtPixel,
  prepareRingGeometry,
} from "../modules/ring_geometry_utils.js";

// The I23 PILATUS 12M's 24 rows as dials.import wrote them for real I23 data
// (DLS_I23_P12M_thau_00001.cbf.gz from dials_data), which is also what the
// backend's built-in geometry reproduces -- tests/test_detector_profiles.py.
const here = dirname(fileURLToPath(import.meta.url));
const panels = JSON.parse(
  readFileSync(resolve(here, "../../tests/fixtures/dls_i23_p12m_imported_panels.json"), "utf8"),
);

// That frame's header: Wavelength 2.75520 A, Beam_xy (1080.00, 2595.00),
// Detector_distance 0.01000 m. The backend reports the distance as the
// blueprint's beam-hit distance plus that 10 mm offset.
const WAVELENGTH_A = 2.7552;
const DISTANCE_MM = 250.13456873525593 + 10.0;
const CENTER = { x: 1080, y: 2595 };

// d-spacings dxtbx gives at these pixels, from the panel frames dials.show
// prints for the same frame (the blueprint composed with the header's offset
// and beam centre), without parallax correction since ALBIS applies none.
const DXTBX_D_SPACINGS = [
  [100, 20, 1.832877],
  [1080, 400, 2.057649],
  [2300, 1100, 2.563079],
  [1080, 2300, 14.188554],
  [600, 2700, 8.794866],
  [1500, 3200, 5.797369],
  [200, 4000, 2.789354],
  [2400, 4700, 2.077587],
  [1080, 5000, 1.908148],
];

describe("DLS I23 PILATUS 12M resolution", () => {
  const geometry = applyGeometryOverrides(prepareRingGeometry({ mode: "geometry", panels }), {
    centerX: CENTER.x,
    centerY: CENTER.y,
    distanceMm: DISTANCE_MM,
  });
  const energyEv = HC_EV_ANGSTROM / WAVELENGTH_A;

  // Measured: at most 0.18%. What is left is half a pixel of beam centre:
  // ALBIS reads Beam_xy as pixel-centre coordinates for every PILATUS header,
  // dxtbx as pixel-corner ones. For scale, the raw 10 mm header distance ALBIS
  // used before was off by up to 85%, and the reference pose a hand-loaded
  // .expt seeds, which ignores the header's offset, by up to 3.9%.
  it.each(DXTBX_D_SPACINGS)("matches dxtbx at pixel (%i, %i)", (x, y, expected) => {
    const d = getGeometryResolutionAtPixel(x, y, geometry, energyEv);
    expect(Math.abs(d - expected) / expected).toBeLessThan(0.0025);
  });
});
