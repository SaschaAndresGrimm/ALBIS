import { describe, expect, it } from "vitest";

import { formatPixelLabelValue } from "../modules/intensity_scale_utils.js";

describe("intensity_scale_utils", () => {
  it("keeps integer auto labels unchanged", () => {
    expect(formatPixelLabelValue(12.7, 40, "auto", "uint16")).toBe("13");
    expect(formatPixelLabelValue(1234567, 40, "auto", "uint16")).toBe("1.2M");
  });

  it("shows decimal labels for float auto mode when space allows", () => {
    expect(formatPixelLabelValue(1.2345, 40, "auto", "float32")).toBe("1.234");
    expect(formatPixelLabelValue(0.999791, 40, "auto", "float32")).toBe("1.000");
    expect(formatPixelLabelValue(1.2345, 28, "auto", "float32")).toBe("1.23");
    expect(formatPixelLabelValue(0.1234, 28, "auto", "float32")).toBe("0.12");
  });

  it("falls back to compact float representations as space gets tighter", () => {
    expect(formatPixelLabelValue(123456.789, 40, "auto", "float32")).toBe("1.23e5");
    expect(formatPixelLabelValue(1.2345, 18, "auto", "float32")).toBe("1");
  });

  it("preserves explicit integer and scientific modes for float data", () => {
    expect(formatPixelLabelValue(1.2345, 40, "integer", "float32")).toBe("1");
    expect(formatPixelLabelValue(0.0001234, 60, "scientific", "float32")).toBe("1.234e-4");
  });
});
