/**
 * Base frame rendering orchestration (WebGL2 primary, CPU fallback).
 */

import { t } from "./i18n.js";
import { SATURATED_PIXEL_RGBA } from "./viewer_overlay_colors.js";

export function getWebglRenderCompatibility(userAgent = "") {
  const ua = String(
    userAgent || (typeof navigator !== "undefined" ? navigator.userAgent || "" : ""),
  ).toLowerCase();
  const isFirefox = ua.includes("firefox/");
  return {
    disableUnsignedIntegerTextures: isFirefox,
  };
}

export function createRenderEngineController({
  state,
  elements,
  callbacks,
}) {
  const {
    canvas,
    metaRenderer,
  } = elements;

  const {
    setStatus,
    toFloat32,
    isSaturatedValue,
    getWebglUnsignedDtypeKey,
    isWebglUnsignedRawCandidate,
    getWebglUnsignedUploadInfo,
    getPaletteColorCount,
    mapValueToNorm,
    buildPalette,
    getActiveSaturationMax,
    scheduleOverview,
    schedulePixelOverlay,
    schedulePeakOverlay,
    getUserAgent,
    getRenderer,
    setRenderer,
  } = callbacks;

  function createShader(gl, type, source) {
    const shader = gl.createShader(type);
    gl.shaderSource(shader, source);
    gl.compileShader(shader);
    if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {
      const info = gl.getShaderInfoLog(shader);
      gl.deleteShader(shader);
      throw new Error(info || "Shader compile failed");
    }
    return shader;
  }

  function createProgram(gl, vertexSource, fragmentSource) {
    const vs = createShader(gl, gl.VERTEX_SHADER, vertexSource);
    const fs = createShader(gl, gl.FRAGMENT_SHADER, fragmentSource);
    const program = gl.createProgram();
    gl.attachShader(program, vs);
    gl.attachShader(program, fs);
    gl.linkProgram(program);
    gl.deleteShader(vs);
    gl.deleteShader(fs);
    if (!gl.getProgramParameter(program, gl.LINK_STATUS)) {
      const info = gl.getProgramInfoLog(program);
      gl.deleteProgram(program);
      throw new Error(info || "Program link failed");
    }
    return program;
  }

  function createWebGLRenderer() {
    // WebGL2 renderer is the primary path for large image performance.
    // It handles contrast mapping and masking directly in the fragment shader.
    const gl = canvas.getContext("webgl2", {
      antialias: false,
      preserveDrawingBuffer: true,
    });
    if (!gl) {
      return null;
    }
    const renderCompatibility = getWebglRenderCompatibility(getUserAgent?.());

    const vertexSource = `#version 300 es
      layout(location = 0) in vec2 a_position;
      out vec2 v_tex;
      void main() {
        v_tex = a_position * 0.5 + 0.5;
        gl_Position = vec4(a_position, 0.0, 1.0);
      }
    `;

    const buildFragmentSource = (dataDecl, dataReadExpr) => `#version 300 es
      precision highp float;
      precision highp int;
      ${dataDecl}
      uniform sampler2D u_lut;
      uniform sampler2D u_mask;
      uniform float u_mask_enabled;
      uniform float u_mask_saturated_enabled;
      uniform float u_sat_max;
      uniform float u_min;
      uniform float u_max;
      uniform float u_invert;
      uniform float u_hdr;
      uniform float u_lut_size;
      in vec2 v_tex;
      out vec4 outColor;
      void main() {
        float value = ${dataReadExpr};
        if (u_mask_enabled > 0.5) {
          float maskClass = texture(u_mask, v_tex).r;
          if (maskClass > 0.75) {
            outColor = vec4(0.0, 0.0, 0.0, 1.0);
            return;
          } else if (maskClass > 0.25) {
            outColor = vec4(0.1, 0.2, 0.47, 1.0);
            return;
          }
        }
        if (u_mask_saturated_enabled > 0.5 && abs(value - u_sat_max) <= max(1e-9, abs(u_sat_max) * 1e-6)) {
          outColor = vec4(${SATURATED_PIXEL_RGBA.float.join(", ")});
          return;
        }
        float norm = 0.0;
        if (u_hdr > 0.5) {
          const float linSize = 256.0;
          const float logSize = 768.0;
          float bg = u_min;
          float fg = u_max;
          float lfg = fg * 10000.0;
          float idx = 0.0;
          if (value <= bg) {
            idx = 0.0;
          } else if (value >= lfg) {
            idx = linSize + logSize - 1.0;
          } else if (value < fg && fg > bg) {
            float linSlope = linSize / (fg - bg);
            idx = floor((value - bg) * linSlope);
            idx = clamp(idx, 0.0, linSize - 1.0);
          } else if (fg > bg && lfg > fg && value > bg) {
            float denom = log((lfg - bg) / (fg - bg));
            if (denom > 0.0) {
              float logSlope = (logSize - 1.0) / denom;
              float logOffset = -log(max(fg - bg, 1e-12)) * logSlope;
              float x = log(max(value - bg, 1e-12)) * logSlope + logOffset;
              idx = linSize + floor(x);
              idx = clamp(idx, linSize, linSize + logSize - 1.0);
            } else {
              idx = linSize;
            }
          }
          norm = idx / (linSize + logSize - 1.0);
        } else {
          float denom = max(u_max - u_min, 1.0);
          float t = (value - u_min) / denom;
          norm = clamp(t, 0.0, 1.0);
        }
        if (u_invert > 0.5) {
          norm = 1.0 - norm;
        }
        float lutSize = max(u_lut_size, 2.0);
        float lutIndex = floor(norm * (lutSize - 1.0));
        float lutU = (lutIndex + 0.5) / lutSize;
        outColor = texture(u_lut, vec2(lutU, 0.5));
      }
    `;

    const floatFragmentSource = buildFragmentSource("uniform sampler2D u_data;", "texture(u_data, v_tex).r");
    const uintFragmentSource = buildFragmentSource(
      "uniform highp usampler2D u_data;",
      "float(texture(u_data, v_tex).r)",
    );

    let floatProgram;
    try {
      floatProgram = createProgram(gl, vertexSource, floatFragmentSource);
    } catch (err) {
      console.error(err);
      setStatus(t("status.render.webgl_shader_error"));
      return {
        type: "webgl",
        render: () => {},
      };
    }

    let uintProgram = null;
    if (!renderCompatibility.disableUnsignedIntegerTextures) {
      try {
        uintProgram = createProgram(gl, vertexSource, uintFragmentSource);
      } catch (err) {
        console.warn("WebGL integer texture path unavailable; using float upload fallback.", err);
      }
    } else {
      console.warn("Firefox WebGL compatibility mode enabled; using float texture uploads.");
    }

    const vao = gl.createVertexArray();
    gl.bindVertexArray(vao);
    const buffer = gl.createBuffer();
    gl.bindBuffer(gl.ARRAY_BUFFER, buffer);
    gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([-1, -1, 1, -1, -1, 1, 1, 1]), gl.STATIC_DRAW);
    gl.enableVertexAttribArray(0);
    gl.vertexAttribPointer(0, 2, gl.FLOAT, false, 0, 0);
    gl.bindVertexArray(null);

    const configureTexture = (texture) => {
      gl.bindTexture(gl.TEXTURE_2D, texture);
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.NEAREST);
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.NEAREST);
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
    };

    const dataTexFloat = gl.createTexture();
    configureTexture(dataTexFloat);
    const dataTexUint = gl.createTexture();
    configureTexture(dataTexUint);

    const lutTex = gl.createTexture();
    configureTexture(lutTex);

    const maskTex = gl.createTexture();
    configureTexture(maskTex);
    gl.texImage2D(
      gl.TEXTURE_2D,
      0,
      gl.R8,
      1,
      1,
      0,
      gl.RED,
      gl.UNSIGNED_BYTE,
      new Uint8Array([0]),
    );

    const collectUniforms = (program) => ({
      data: gl.getUniformLocation(program, "u_data"),
      lut: gl.getUniformLocation(program, "u_lut"),
      mask: gl.getUniformLocation(program, "u_mask"),
      maskEnabled: gl.getUniformLocation(program, "u_mask_enabled"),
      maskSaturatedEnabled: gl.getUniformLocation(program, "u_mask_saturated_enabled"),
      satMax: gl.getUniformLocation(program, "u_sat_max"),
      min: gl.getUniformLocation(program, "u_min"),
      max: gl.getUniformLocation(program, "u_max"),
      invert: gl.getUniformLocation(program, "u_invert"),
      hdr: gl.getUniformLocation(program, "u_hdr"),
      lutSize: gl.getUniformLocation(program, "u_lut_size"),
    });

    const floatUniforms = collectUniforms(floatProgram);
    const uintUniforms = uintProgram ? collectUniforms(uintProgram) : null;

    const maxTextureSize = gl.getParameter(gl.MAX_TEXTURE_SIZE);
    let maskTexWidth = 1;
    let maskTexHeight = 1;
    let lastMaskData = null;
    let lastMaskEnabled = false;
    let maskClassData = null;

    return {
      type: "webgl",
      maxTextureSize,
      supportsUnsignedTextures: Boolean(uintProgram),
      render({
        floatData,
        rawData,
        dtype,
        width,
        height,
        min,
        max,
        palette,
        invert,
        mask,
        maskWidth,
        maskHeight,
        maskEnabled,
        maskSaturatedEnabled,
        satMax,
        colormap,
      }) {
        const dtypeKey = uintProgram ? getWebglUnsignedDtypeKey(dtype) : null;
        const useUintPath = Boolean(dtypeKey && isWebglUnsignedRawCandidate(dtype, rawData));
        const frameData = useUintPath ? rawData : floatData || (rawData ? toFloat32(rawData) : null);
        if (!frameData) return;
        if (width > maxTextureSize || height > maxTextureSize) {
          setStatus(t("status.render.texture_too_large", { maxTextureSize }));
          return;
        }
        if (canvas.width !== width || canvas.height !== height) {
          canvas.width = width;
          canvas.height = height;
        }

        const program = useUintPath ? uintProgram : floatProgram;
        const uniforms = useUintPath ? uintUniforms : floatUniforms;
        if (!program || !uniforms) return;

        gl.viewport(0, 0, width, height);
        gl.useProgram(program);
        gl.bindVertexArray(vao);

        gl.activeTexture(gl.TEXTURE0);
        gl.bindTexture(gl.TEXTURE_2D, useUintPath ? dataTexUint : dataTexFloat);
        gl.pixelStorei(gl.UNPACK_ALIGNMENT, 1);
        gl.pixelStorei(gl.UNPACK_FLIP_Y_WEBGL, true);
        if (useUintPath) {
          const upload = getWebglUnsignedUploadInfo(gl, dtypeKey);
          if (!upload) return;
          gl.texImage2D(
            gl.TEXTURE_2D,
            0,
            upload.internalFormat,
            width,
            height,
            0,
            upload.format,
            upload.type,
            frameData,
          );
        } else {
          gl.texImage2D(gl.TEXTURE_2D, 0, gl.R32F, width, height, 0, gl.RED, gl.FLOAT, frameData);
        }
        gl.uniform1i(uniforms.data, 0);

        gl.activeTexture(gl.TEXTURE1);
        gl.bindTexture(gl.TEXTURE_2D, lutTex);
        const lutSize = getPaletteColorCount(palette);
        gl.texImage2D(
          gl.TEXTURE_2D,
          0,
          gl.RGBA,
          lutSize,
          1,
          0,
          gl.RGBA,
          gl.UNSIGNED_BYTE,
          palette,
        );
        gl.uniform1i(uniforms.lut, 1);

        const validMask = Boolean(mask && maskWidth === width && maskHeight === height && mask.length === width * height);
        const useMask = Boolean(maskEnabled && validMask);
        const useSaturationMask = Boolean(maskSaturatedEnabled && Number.isFinite(satMax));
        gl.activeTexture(gl.TEXTURE2);
        gl.bindTexture(gl.TEXTURE_2D, maskTex);
        gl.uniform1i(uniforms.mask, 2);
        gl.uniform1f(uniforms.maskEnabled, useMask ? 1.0 : 0.0);
        gl.uniform1f(uniforms.maskSaturatedEnabled, useSaturationMask ? 1.0 : 0.0);
        gl.uniform1f(uniforms.satMax, useSaturationMask ? satMax : 0.0);
        const shouldUploadMask =
          useMask &&
          (mask !== lastMaskData ||
            maskTexWidth !== width ||
            maskTexHeight !== height ||
            maskEnabled !== lastMaskEnabled);
        if (shouldUploadMask) {
          if (!maskClassData || maskClassData.length !== width * height) {
            maskClassData = new Uint8Array(width * height);
          }
          for (let i = 0; i < width * height; i += 1) {
            const bits = mask[i];
            if (bits & 1) {
              maskClassData[i] = 255;
            } else if (bits & 0x1e) {
              maskClassData[i] = 128;
            } else {
              maskClassData[i] = 0;
            }
          }
          gl.pixelStorei(gl.UNPACK_ALIGNMENT, 1);
          gl.texImage2D(
            gl.TEXTURE_2D,
            0,
            gl.R8,
            width,
            height,
            0,
            gl.RED,
            gl.UNSIGNED_BYTE,
            maskClassData,
          );
          maskTexWidth = width;
          maskTexHeight = height;
          lastMaskData = mask;
          lastMaskEnabled = Boolean(maskEnabled);
        }

        gl.uniform1f(uniforms.min, min);
        gl.uniform1f(uniforms.max, max);
        gl.uniform1f(uniforms.invert, invert ? 1.0 : 0.0);
        gl.uniform1f(uniforms.hdr, colormap === "albulaHdr" ? 1.0 : 0.0);
        gl.uniform1f(uniforms.lutSize, lutSize);
        gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
      },
    };
  }

  function createCpuRenderer() {
    const ctx = canvas.getContext("2d");
    return {
      type: "cpu",
      render({ data, width, height, palette, mask, maskEnabled, maskSaturatedEnabled, satMax }) {
        if (!data) return;
        if (canvas.width !== width || canvas.height !== height) {
          canvas.width = width;
          canvas.height = height;
        }
        const imageData = ctx.createImageData(width, height);
        const out = imageData.data;
        const maxIdx = getPaletteColorCount(palette) - 1;
        for (let i = 0; i < data.length; i += 1) {
          const v = data[i];
          if (maskEnabled && mask && mask.length === data.length) {
            const maskValue = mask[i];
            if (maskValue & 1) {
              const j = i * 4;
              out[j] = 0;
              out[j + 1] = 0;
              out[j + 2] = 0;
              out[j + 3] = 255;
              continue;
            } else if (maskValue & 0x1e) {
              const j = i * 4;
              out[j] = 25;
              out[j + 1] = 50;
              out[j + 2] = 120;
              out[j + 3] = 255;
              continue;
            }
          }
          if (maskSaturatedEnabled && Number.isFinite(satMax) && isSaturatedValue(v, satMax)) {
            const j = i * 4;
            out[j] = SATURATED_PIXEL_RGBA.byte[0];
            out[j + 1] = SATURATED_PIXEL_RGBA.byte[1];
            out[j + 2] = SATURATED_PIXEL_RGBA.byte[2];
            out[j + 3] = SATURATED_PIXEL_RGBA.byte[3];
            continue;
          }
          const norm = mapValueToNorm(v);
          const idx = Math.floor(norm * maxIdx) * 4;
          const j = i * 4;
          out[j] = palette[idx];
          out[j + 1] = palette[idx + 1];
          out[j + 2] = palette[idx + 2];
          out[j + 3] = 255;
        }
        ctx.putImageData(imageData, 0, 0);
      },
    };
  }

  function initRenderer() {
    let renderer = createWebGLRenderer();
    if (!renderer) {
      renderer = createCpuRenderer();
    }
    setRenderer(renderer);
    metaRenderer.textContent = renderer.type === "webgl" ? "WebGL2" : "CPU";
  }

  function redraw() {
    if (!state.dataRaw) return;
    const renderer = getRenderer();
    if (!renderer) return;
    const palette = buildPalette(state.colormap);
    const maskReady =
      state.maskEnabled &&
      state.maskAvailable &&
      state.maskRaw &&
      state.maskShape &&
      state.maskShape[0] === state.height &&
      state.maskShape[1] === state.width;
    const maskData = maskReady ? state.maskRaw : null;
    const maskWidth = maskReady ? state.maskShape[1] : 0;
    const maskHeight = maskReady ? state.maskShape[0] : 0;
    const satMax = getActiveSaturationMax();
    const maskSaturatedEnabled = Boolean(state.maskSaturatedEnabled && Number.isFinite(satMax));
    if (renderer.type === "webgl") {
      renderer.render({
        floatData: state.dataFloat,
        rawData: state.dataRaw,
        dtype: state.dtype,
        width: state.width,
        height: state.height,
        min: state.min,
        max: state.max,
        palette,
        invert: state.invert,
        colormap: state.colormap,
        mask: maskData,
        maskWidth,
        maskHeight,
        maskEnabled: maskReady,
        maskSaturatedEnabled,
        satMax,
      });
    } else {
      renderer.render({
        data: state.dataRaw,
        width: state.width,
        height: state.height,
        min: state.min,
        max: state.max,
        palette,
        mask: maskData,
        maskEnabled: maskReady,
        maskSaturatedEnabled,
        satMax,
      });
    }
    scheduleOverview();
    schedulePixelOverlay();
    schedulePeakOverlay();
  }

  return {
    initRenderer,
    redraw,
  };
}
