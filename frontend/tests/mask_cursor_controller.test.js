import { describe, expect, it, vi } from "vitest";

vi.mock("../modules/i18n.js", () => ({
  t: (_key, vars = {}) => `X ${vars.x} Y ${vars.y} Value ${vars.value}${vars.resolutionText || ""}`,
}));

import { createMaskCursorController } from "../modules/mask_cursor_controller.js";

describe("mask_cursor_controller", () => {
  it("returns off-image coordinates for circular ROI drags when explicitly allowed", () => {
    const canvasWrap = document.createElement("div");
    canvasWrap.getBoundingClientRect = () => ({ left: 20, top: 30, width: 100, height: 100 });

    const controller = createMaskCursorController({
      apiBase: "/api",
      state: {
        hasFrame: true,
        width: 100,
        height: 100,
        zoom: 1,
        renderOffsetX: 0,
        renderOffsetY: 0,
      },
      elements: {
        canvasWrap,
        canvasShell: document.createElement("div"),
        cursorOverlay: document.createElement("div"),
        histTooltip: document.createElement("div"),
        maskToggle: document.createElement("input"),
        maskSaturatedToggle: document.createElement("input"),
        simplonUrl: document.createElement("input"),
        simplonVersion: document.createElement("input"),
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

    const point = controller.getImagePointFromEvent(
      { clientX: 10, clientY: 25 },
      { allowOutside: true, allowOutsideViewport: true },
    );

    expect(point).toEqual({ x: -10, y: -5 });
  });

  it("renders whole-number float samples without trailing decimals in the cursor readout", () => {
    const canvasWrap = document.createElement("div");
    canvasWrap.getBoundingClientRect = () => ({ left: 20, top: 30, width: 100, height: 100 });
    const canvasShell = document.createElement("div");
    canvasShell.getBoundingClientRect = () => ({ left: 20, top: 30, width: 100, height: 100 });
    const cursorOverlay = document.createElement("div");

    const controller = createMaskCursorController({
      apiBase: "/api",
      state: {
        hasFrame: true,
        width: 1,
        height: 1,
        zoom: 1,
        renderOffsetX: 0,
        renderOffsetY: 0,
        dataRaw: new Float32Array([24]),
        dtype: "<f4",
        maskEnabled: false,
        maskAvailable: false,
        maskRaw: null,
        maskShape: null,
        maskSaturatedEnabled: false,
      },
      elements: {
        canvasWrap,
        canvasShell,
        cursorOverlay,
        histTooltip: document.createElement("div"),
        maskToggle: document.createElement("input"),
        maskSaturatedToggle: document.createElement("input"),
        simplonUrl: document.createElement("input"),
        simplonVersion: document.createElement("input"),
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
        getDtypeInfo: vi.fn(() => ({ kind: "f", bits: 32 })),
        formatValue: vi.fn(() => "24.000"),
        isSaturatedValue: vi.fn(() => false),
        getResolutionAtPixel: vi.fn(() => null),
        getEffectiveScrollLeft: vi.fn(() => 0),
        getEffectiveScrollTop: vi.fn(() => 0),
        setAutoloadStatus: vi.fn(),
      },
    });

    controller.updateCursorOverlay({ clientX: 20.5, clientY: 30.5 });

    expect(cursorOverlay.textContent).toBe("X 0 Y 0 Value 24");
  });
});

describe("mask_cursor_controller mask request races", () => {
  // Two files from the same detector produce same-shaped masks, so
  // alignMaskToFrame cannot tell them apart. Tag the contents to see which one
  // actually landed.
  function setup() {
    const state = {
      file: "A.h5",
      shape: [10, 64, 64],
      width: 64,
      height: 64,
      hasFrame: true,
      maskFile: "",
      maskRaw: null,
      maskShape: null,
      maskAvailable: false,
      maskEnabled: false,
      maskAuto: true,
      maskPath: "",
    };
    const release = {};
    const fetchMock = vi.fn((url) => {
      const file = decodeURIComponent(String(url).split("file=")[1].split("&")[0]);
      return new Promise((resolve) => {
        release[file] = () =>
          resolve({
            ok: true,
            arrayBuffer: async () => new Uint32Array(64 * 64).fill(file === "A.h5" ? 111 : 222).buffer,
            headers: {
              get: (name) =>
                name === "X-Dtype" ? "<u4" : name === "X-Shape" ? "64,64" : `${file}:/mask`,
            },
          });
      });
    });
    global.fetch = fetchMock;

    const input = () => document.createElement("input");
    const controller = createMaskCursorController({
      apiBase: "/api",
      state,
      elements: {
        canvasWrap: document.createElement("div"),
        canvasShell: document.createElement("div"),
        cursorOverlay: document.createElement("div"),
        histTooltip: document.createElement("div"),
        maskToggle: input(),
        maskSaturatedToggle: input(),
        simplonUrl: input(),
        simplonVersion: input(),
      },
      callbacks: {
        isHdfFile: () => true,
        parseDtype: () => "<u4",
        parseShape: (value) => String(value).split(",").map(Number),
        typedArrayFrom: (buffer) => new Uint32Array(buffer),
        getActiveSaturationMax: () => null,
        updateGlobalStats: vi.fn(),
        redraw: vi.fn(),
        scheduleRoiUpdate: vi.fn(),
        getDtypeInfo: () => null,
        formatValue: String,
        isSaturatedValue: () => false,
        getResolutionAtPixel: () => null,
        getEffectiveScrollLeft: () => 0,
        getEffectiveScrollTop: () => 0,
        setAutoloadStatus: vi.fn(),
      },
    });
    return { state, controller, release, fetchMock };
  }

  it("ignores a mask that arrives after the file has changed", async () => {
    const { state, controller, release } = setup();

    const pendingA = controller.loadMask();
    state.file = "B.h5";
    const pendingB = controller.loadMask();

    release["B.h5"]();
    await pendingB;
    expect(state.maskRaw[0]).toBe(222);

    release["A.h5"]();
    await pendingA;

    // The stale response must not overwrite the mask that belongs to B, and
    // must not leave B's cache key describing A's data.
    expect(state.maskRaw[0]).toBe(222);
    expect(state.maskPath).toBe("B.h5:/mask");
  });

  it("does not leave a stale mask cached against the new file", async () => {
    const { state, controller, release, fetchMock } = setup();

    const pendingA = controller.loadMask();
    state.file = "B.h5";
    const pendingB = controller.loadMask();
    release["B.h5"]();
    await pendingB;
    release["A.h5"]();
    await pendingA;

    // Reloading must not serve A's mask from B's cache entry, which is how the
    // wrong mask used to survive for the rest of the session.
    await controller.loadMask();
    expect(state.maskRaw[0]).toBe(222);
    expect(fetchMock.mock.calls.length).toBe(2);
  });

  it("drops a mask still in flight when the session is cleared", async () => {
    const { state, controller, release } = setup();

    const pendingA = controller.loadMask();
    controller.clearMaskState();
    release["A.h5"]();
    await pendingA;

    expect(state.maskRaw).toBeNull();
    expect(state.maskAvailable).toBe(false);
  });
});
