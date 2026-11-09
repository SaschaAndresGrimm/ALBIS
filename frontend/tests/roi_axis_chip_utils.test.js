import { describe, expect, it } from "vitest";
import { axisChipState, toggleAxisChip } from "../modules/roi_axis_chip_utils.js";

const manual = { xMin: 1, xMax: 5, yMin: null, yMax: 3 };
const none = { xMin: null, xMax: null, yMin: null, yMax: null };

describe("roi_axis_chip_utils", () => {
  it("clears manual limits and stashes them, then restores them", () => {
    const off = toggleAxisChip(manual, null);
    expect(off).toEqual({ limits: none, stash: manual });
    expect(toggleAxisChip(off.limits, off.stash)).toEqual({ limits: manual, stash: null });
  });

  it("does nothing on auto axes with no stash", () => {
    expect(toggleAxisChip(none, null)).toBeNull();
  });

  it("reports the chip state", () => {
    expect(axisChipState(manual, null)).toBe("manual");
    expect(axisChipState(none, manual)).toBe("auto");
    expect(axisChipState(none, null)).toBe("none");
  });
});
