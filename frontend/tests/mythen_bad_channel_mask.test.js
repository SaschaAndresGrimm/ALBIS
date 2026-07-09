import { describe, expect, it, vi } from "vitest";

vi.mock("../modules/i18n.js", () => ({
  t: (key) => key,
}));

import { parseBadChannels } from "../modules/file_data_pipeline_controller.js";
import { createMaskCursorController } from "../modules/mask_cursor_controller.js";

function makeMaskController() {
  const el = () => document.createElement("div");
  const input = () => document.createElement("input");
  return createMaskCursorController({
    apiBase: "/api",
    state: {},
    elements: {
      canvasWrap: el(),
      canvasShell: el(),
      cursorOverlay: el(),
      histTooltip: el(),
      maskToggle: input(),
      maskSaturatedToggle: input(),
      simplonUrl: input(),
      simplonVersion: input(),
    },
    callbacks: {
      isHdfFile: vi.fn(),
      parseDtype: vi.fn(),
      parseShape: vi.fn(),
      typedArrayFrom: vi.fn(),
      getActiveSaturationMax: vi.fn(() => null),
      updateGlobalStats: vi.fn(),
      redraw: vi.fn(),
      scheduleRoiUpdate: vi.fn(),
      getDtypeInfo: vi.fn(),
      formatValue: vi.fn(),
      isSaturatedValue: vi.fn(() => false),
      getResolutionAtPixel: vi.fn(() => null),
      getEffectiveScrollLeft: vi.fn(() => 0),
      getEffectiveScrollTop: vi.fn(() => 0),
      setAutoloadStatus: vi.fn(),
    },
  });
}

describe("parseBadChannels", () => {
  it("parses a comma-separated header into non-negative integers", () => {
    expect(parseBadChannels("0,1,737,1278,1279")).toEqual([0, 1, 737, 1278, 1279]);
  });

  it("ignores blanks and invalid tokens", () => {
    expect(parseBadChannels("0, , 5, x, -3, 9")).toEqual([0, 5, 9]);
  });

  it("returns an empty list for missing headers", () => {
    expect(parseBadChannels(null)).toEqual([]);
    expect(parseBadChannels("")).toEqual([]);
  });
});

describe("buildColumnMask", () => {
  it("flags whole channel columns as defective across every frame row", () => {
    const { buildColumnMask } = makeMaskController();
    const width = 4;
    const height = 3;
    const mask = buildColumnMask(width, height, [1, 3]);

    expect(mask).toBeInstanceOf(Uint32Array);
    for (let y = 0; y < height; y += 1) {
      expect(mask[y * width + 0]).toBe(0);
      expect(mask[y * width + 2]).toBe(0);
      // 0x1e => "defective pixel" class picked up by the renderer.
      expect(mask[y * width + 1] & 0x1e).toBeTruthy();
      expect(mask[y * width + 3] & 0x1e).toBeTruthy();
    }
  });

  it("OR-combines onto an existing base mask and skips out-of-range columns", () => {
    const { buildColumnMask } = makeMaskController();
    const width = 3;
    const height = 2;
    const base = new Uint32Array(width * height);
    base[0] = 1; // pre-existing gap flag at (0,0)
    const mask = buildColumnMask(width, height, [0, 99], base);

    expect(mask).toBe(base);
    expect(mask[0] & 1).toBeTruthy(); // gap flag preserved
    expect(mask[0] & 0x1e).toBeTruthy(); // defective flag added
    expect(mask[height * width - 1]).toBe(0); // column 99 ignored
  });

  it("returns the base unchanged when no columns are supplied", () => {
    const { buildColumnMask } = makeMaskController();
    expect(buildColumnMask(4, 4, [])).toBeNull();
    const base = new Uint32Array(4);
    expect(buildColumnMask(2, 2, [], base)).toBe(base);
  });
});
