import { afterEach, describe, expect, it, vi } from "vitest";

import {
  createRenderEngineController,
  getWebglRenderCompatibility,
} from "../modules/render_engine_controller.js";

function buildPalette() {
  const palette = new Uint8Array(256 * 4);
  for (let i = 0; i < 256; i += 1) {
    const base = i * 4;
    palette[base] = i;
    palette[base + 1] = i;
    palette[base + 2] = i;
    palette[base + 3] = 255;
  }
  return palette;
}

function createFakeGl() {
  const gl = {
    VERTEX_SHADER: 0x8b31,
    FRAGMENT_SHADER: 0x8b30,
    COMPILE_STATUS: 0x8b81,
    LINK_STATUS: 0x8b82,
    ARRAY_BUFFER: 0x8892,
    STATIC_DRAW: 0x88e4,
    FLOAT: 0x1406,
    TEXTURE_2D: 0x0de1,
    TEXTURE_MIN_FILTER: 0x2801,
    TEXTURE_MAG_FILTER: 0x2800,
    TEXTURE_WRAP_S: 0x2802,
    TEXTURE_WRAP_T: 0x2803,
    NEAREST: 0x2600,
    CLAMP_TO_EDGE: 0x812f,
    R8: 0x8229,
    R16UI: 0x8234,
    RED: 0x1903,
    RED_INTEGER: 0x8d94,
    RGBA: 0x1908,
    UNSIGNED_BYTE: 0x1401,
    UNSIGNED_SHORT: 0x1403,
    TRIANGLE_STRIP: 0x0005,
    MAX_TEXTURE_SIZE: 0x0d33,
    TEXTURE0: 0x84c0,
    TEXTURE1: 0x84c1,
    TEXTURE2: 0x84c2,
    UNPACK_ALIGNMENT: 0x0cf5,
    UNPACK_FLIP_Y_WEBGL: 0x9240,
    R32F: 0x822e,
    NO_ERROR: 0,
    createShader: vi.fn(() => ({})),
    shaderSource: vi.fn(),
    compileShader: vi.fn(),
    getShaderParameter: vi.fn(() => true),
    getShaderInfoLog: vi.fn(() => ""),
    deleteShader: vi.fn(),
    createProgram: vi.fn(() => ({})),
    attachShader: vi.fn(),
    linkProgram: vi.fn(),
    getProgramParameter: vi.fn(() => true),
    getProgramInfoLog: vi.fn(() => ""),
    deleteProgram: vi.fn(),
    createVertexArray: vi.fn(() => ({})),
    bindVertexArray: vi.fn(),
    createBuffer: vi.fn(() => ({})),
    bindBuffer: vi.fn(),
    bufferData: vi.fn(),
    enableVertexAttribArray: vi.fn(),
    vertexAttribPointer: vi.fn(),
    createTexture: vi.fn(() => ({})),
    bindTexture: vi.fn(),
    texParameteri: vi.fn(),
    texImage2D: vi.fn(),
    getUniformLocation: vi.fn(() => ({})),
    getParameter: vi.fn((name) => (name === 0x0d33 ? 4096 : 0)),
    viewport: vi.fn(),
    useProgram: vi.fn(),
    activeTexture: vi.fn(),
    pixelStorei: vi.fn(),
    uniform1i: vi.fn(),
    uniform1f: vi.fn(),
    drawArrays: vi.fn(),
    getError: vi.fn(() => 0),
  };
  return gl;
}

function buildController(userAgent) {
  const gl = createFakeGl();
  const canvas = {
    width: 0,
    height: 0,
    getContext: vi.fn((kind) => (kind === "webgl2" ? gl : null)),
  };
  const metaRenderer = { textContent: "" };
  const toFloat32 = vi.fn((data) => new Float32Array(data));
  let renderer = null;
  const controller = createRenderEngineController({
    state: {
      dataRaw: new Uint16Array([5]),
      dataFloat: null,
      dtype: "uint16",
      width: 1,
      height: 1,
      min: 0,
      max: 10,
      colormap: "gray",
      invert: false,
      maskEnabled: false,
      maskAvailable: false,
      maskRaw: null,
      maskShape: null,
      maskSaturatedEnabled: false,
    },
    elements: {
      canvas,
      metaRenderer,
    },
    callbacks: {
      setStatus: vi.fn(),
      toFloat32,
      isSaturatedValue: vi.fn(() => false),
      getWebglUnsignedDtypeKey: vi.fn((dtype) => (dtype === "uint16" ? "u16" : null)),
      isWebglUnsignedRawCandidate: vi.fn((dtype, data) => dtype === "uint16" && data instanceof Uint16Array),
      getWebglUnsignedUploadInfo: vi.fn((innerGl, key) => (
        key === "u16"
          ? { internalFormat: innerGl.R16UI, format: innerGl.RED_INTEGER, type: innerGl.UNSIGNED_SHORT }
          : null
      )),
      getPaletteColorCount: vi.fn((palette) => Math.floor(palette.length / 4)),
      mapValueToNorm: vi.fn((value) => value / 10),
      buildPalette: vi.fn(() => buildPalette()),
      getActiveSaturationMax: vi.fn(() => null),
      scheduleOverview: vi.fn(),
      schedulePixelOverlay: vi.fn(),
      schedulePeakOverlay: vi.fn(),
      getUserAgent: vi.fn(() => userAgent),
      getRenderer: () => renderer,
      setRenderer: (nextRenderer) => {
        renderer = nextRenderer;
      },
    },
  });
  return {
    controller,
    getRenderer: () => renderer,
    metaRenderer,
    toFloat32,
  };
}

describe("render_engine_controller", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("disables unsigned integer WebGL textures on Firefox", () => {
    expect(getWebglRenderCompatibility("Mozilla/5.0 Firefox/149.0").disableUnsignedIntegerTextures).toBe(true);
    expect(getWebglRenderCompatibility("Mozilla/5.0 Chrome/136.0.0.0").disableUnsignedIntegerTextures).toBe(false);
  });

  it("uses float uploads for unsigned detector frames on Firefox", () => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const {
      controller,
      getRenderer,
      metaRenderer,
      toFloat32,
    } = buildController("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:149.0) Gecko/20100101 Firefox/149.0");

    controller.initRenderer();
    expect(metaRenderer.textContent).toBe("WebGL2");
    expect(getRenderer()?.supportsUnsignedTextures).toBe(false);

    controller.redraw();
    expect(toFloat32).toHaveBeenCalledTimes(1);
  });

  it("keeps unsigned integer uploads enabled outside Firefox", () => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const {
      controller,
      getRenderer,
      toFloat32,
    } = buildController("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/136.0.0.0 Safari/537.36");

    controller.initRenderer();
    expect(getRenderer()?.supportsUnsignedTextures).toBe(true);

    controller.redraw();
    expect(toFloat32).not.toHaveBeenCalled();
  });
});
