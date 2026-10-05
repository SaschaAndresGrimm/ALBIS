import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  HC_EV_ANGSTROM,
  getGeometryReferencePose,
  getGeometryResolutionAtPixel,
  prepareRingGeometry,
} from "../modules/ring_geometry_utils.js";

// A flat PILATUS's imported.expt as dxtbx wrote it, after the backend's
// loader turned it into ALBIS's frame (tests/test_dials_expt_geometry.py keeps
// the two in step), and dxtbx's own d-spacings at the same pixels. Before the
// loader composed the hierarchy and faced the panel to the beam, these came
// out as 0.84 A where dxtbx gives 1.92 A, and the beam centre was not found.
const here = dirname(fileURLToPath(import.meta.url));
const fixture = JSON.parse(
  readFileSync(resolve(here, "../../tests/fixtures/dials_pilatus_flat_albis.json"), "utf8"),
);

describe("a flat detector's DIALS geometry file", () => {
  const geometry = prepareRingGeometry({ mode: "geometry", panels: fixture.albis_panels });
  const energyEv = HC_EV_ANGSTROM / fixture.wavelength_a;

  it("puts the beam where the header does", () => {
    const pose = getGeometryReferencePose(geometry);

    expect(pose.distanceMm).toBeCloseTo(40, 6);
    expect(pose.centerX).toBeCloseTo(243.5, 6);
    expect(pose.centerY).toBeCloseTo(307.5, 6);
  });

  // Measured: at most 0.16%, the near-beam pixel included.
  it.each(fixture.dxtbx_d_spacings)("matches dxtbx at pixel (%i, %i)", (x, y, _panel, expected) => {
    const d = getGeometryResolutionAtPixel(x, y, geometry, energyEv);
    expect(Math.abs(d - expected) / expected).toBeLessThan(0.0025);
  });
});
