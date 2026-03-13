/**
 * Viewer export utilities and splash rendering.
 */

import { t } from "./i18n.js";

const SPLASH_STATUS_TERMINAL_KEYS = new Set([
  "backend.splash.ready",
  "splash.status.dataset_scan_failed",
  "splash.status.initialization_failed",
  "splash.status.no_image_datasets",
  "splash.status.no_image_files_found",
  "splash.status.ready_open_file",
]);

export function createExportSplashController({
  state,
  elements,
  callbacks,
}) {
  const {
    canvasWrap,
    splash,
    splashCanvas,
    splashCtx,
    splashActions,
    splashOpenFileBtn,
    splashStatus,
  } = elements;

  const {
    buildPalette,
    getPaletteColorCount,
    mapValueToNorm,
    getEffectiveScrollLeft,
    getEffectiveScrollTop,
    setStatus,
  } = callbacks;

  let html2canvasLoadPromise = null;

  function downloadCanvasImage(sourceCanvas, filename) {
    sourceCanvas.toBlob((blob) => {
      if (!blob) return;
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = filename;
      a.click();
      URL.revokeObjectURL(a.href);
    });
  }

  function renderRegionToCanvas(region) {
    if (!state.dataRaw || !region) return null;
    const { x, y, width, height } = region;
    if (width <= 0 || height <= 0) return null;
    const outCanvas = document.createElement("canvas");
    outCanvas.width = width;
    outCanvas.height = height;
    const ctx = outCanvas.getContext("2d");
    if (!ctx) return null;

    const imageData = ctx.createImageData(width, height);
    const out = imageData.data;
    const palette = buildPalette(state.colormap);
    const maxIdx = getPaletteColorCount(palette) - 1;
    const maskReady =
      state.maskEnabled &&
      state.maskAvailable &&
      state.maskRaw &&
      state.maskShape &&
      state.maskShape[0] === state.height &&
      state.maskShape[1] === state.width;
    const maskData = maskReady ? state.maskRaw : null;

    for (let row = 0; row < height; row += 1) {
      const imgY = y + row;
      const rowOffset = imgY * state.width;
      const outOffset = row * width * 4;
      for (let col = 0; col < width; col += 1) {
        const imgX = x + col;
        const idx = rowOffset + imgX;
        const v = state.dataRaw[idx];
        if (maskReady && maskData) {
          const maskValue = maskData[idx];
          if (maskValue & 1) {
            const j = outOffset + col * 4;
            out[j] = 0;
            out[j + 1] = 0;
            out[j + 2] = 0;
            out[j + 3] = 255;
            continue;
          } else if (maskValue & 0x1e) {
            const j = outOffset + col * 4;
            out[j] = 25;
            out[j + 1] = 50;
            out[j + 2] = 120;
            out[j + 3] = 255;
            continue;
          }
        }
        const norm = mapValueToNorm(v);
        const p = Math.floor(norm * maxIdx) * 4;
        const j = outOffset + col * 4;
        out[j] = palette[p];
        out[j + 1] = palette[p + 1];
        out[j + 2] = palette[p + 2];
        out[j + 3] = 255;
      }
    }
    ctx.putImageData(imageData, 0, 0);
    return outCanvas;
  }

  function getVisibleRegion() {
    if (!canvasWrap || !state.width || !state.height) return null;
    const zoom = state.zoom || 1;
    const viewX = getEffectiveScrollLeft() / zoom;
    const viewY = getEffectiveScrollTop() / zoom;
    const viewW = canvasWrap.clientWidth / zoom;
    const viewH = canvasWrap.clientHeight / zoom;
    let startX = Math.floor(viewX);
    let startY = Math.floor(viewY);
    let endX = Math.ceil(viewX + viewW);
    let endY = Math.ceil(viewY + viewH);
    startX = Math.max(0, startX);
    startY = Math.max(0, startY);
    endX = Math.min(state.width, endX);
    endY = Math.min(state.height, endY);
    const width = Math.max(0, endX - startX);
    const height = Math.max(0, endY - startY);
    return { x: startX, y: startY, width, height };
  }

  function exportFullImage(filenameOverride) {
    if (!state.dataRaw) return;
    const full = renderRegionToCanvas({ x: 0, y: 0, width: state.width, height: state.height });
    if (!full) return;
    const name = filenameOverride || `frame_${state.frameIndex}_full.png`;
    downloadCanvasImage(full, name);
  }

  function exportVisibleArea(filenameOverride) {
    const region = getVisibleRegion();
    if (!region) return;
    const image = renderRegionToCanvas(region);
    if (!image) return;
    const name = filenameOverride || `frame_${state.frameIndex}_view.png`;
    downloadCanvasImage(image, name);
  }

  function ensureHtml2Canvas() {
    if (typeof window.html2canvas === "function") {
      return Promise.resolve(window.html2canvas);
    }
    if (html2canvasLoadPromise) {
      return html2canvasLoadPromise;
    }
    html2canvasLoadPromise = new Promise((resolve, reject) => {
      const script = document.createElement("script");
      script.src = "vendor/html2canvas.min.js";
      script.async = true;
      script.onload = () => {
        if (typeof window.html2canvas === "function") {
          resolve(window.html2canvas);
        } else {
          reject(new Error("html2canvas loaded without global export"));
        }
      };
      script.onerror = () => {
        reject(new Error("Failed to load html2canvas"));
      };
      document.head.appendChild(script);
    }).catch((err) => {
      html2canvasLoadPromise = null;
      throw err;
    });
    return html2canvasLoadPromise;
  }

  async function exportViewerWindow(filenameOverride) {
    let html2canvasFn;
    try {
      html2canvasFn = await ensureHtml2Canvas();
    } catch (err) {
      console.error(err);
      setStatus(t("status.export.viewer_unavailable"));
      return;
    }
    const target = document.querySelector(".page");
    if (!target) return;
    try {
      const shot = await html2canvasFn(target, {
        backgroundColor: null,
        scale: window.devicePixelRatio || 1,
        useCORS: true,
      });
      const name = filenameOverride || `albis_view_${state.frameIndex + 1}.png`;
      downloadCanvasImage(shot, name);
    } catch (err) {
      console.error(err);
      setStatus(t("status.export.viewer_failed"));
    }
  }

  function drawGlowDot(ctx, x, y, core, glow, rgb = "255,255,255") {
    const grad = ctx.createRadialGradient(x, y, 0, x, y, glow);
    grad.addColorStop(0, `rgba(${rgb},0.95)`);
    grad.addColorStop(1, `rgba(${rgb},0)`);
    ctx.fillStyle = grad;
    ctx.beginPath();
    ctx.arc(x, y, glow, 0, Math.PI * 2);
    ctx.fill();
    ctx.fillStyle = `rgba(${rgb},0.98)`;
    ctx.beginPath();
    ctx.arc(x, y, core, 0, Math.PI * 2);
    ctx.fill();
  }

  function seededRandom(seed) {
    let value = seed % 2147483647;
    if (value <= 0) value += 2147483646;
    return () => {
      value = (value * 16807) % 2147483647;
      return (value - 1) / 2147483646;
    };
  }

  function drawStarfield(ctx, width, height) {
    const rand = seededRandom(Math.floor(width * 13 + height * 29));
    const total = Math.round(
      Math.min(420, Math.max(180, (width * height) / 7000)),
    );

    for (let i = 0; i < total; i += 1) {
      const x = rand() * width;
      const y = rand() * height;
      const intensity = rand();
      const size = 0.4 + Math.pow(intensity, 2.1) * 1.6;
      const glow = size * (intensity > 0.82 ? 6.5 : 3.2);
      const tint = intensity > 0.75 ? "190,220,255" : "255,255,255";
      drawGlowDot(ctx, x, y, size, glow, tint);
    }

    for (let i = 0; i < 6; i += 1) {
      const cx = width * (0.15 + rand() * 0.7);
      const cy = height * (0.15 + rand() * 0.7);
      const radius = Math.min(width, height) * (0.2 + rand() * 0.25);
      const haze = ctx.createRadialGradient(cx, cy, 0, cx, cy, radius);
      haze.addColorStop(0, "rgba(80,130,200,0.12)");
      haze.addColorStop(1, "rgba(0,0,0,0)");
      ctx.fillStyle = haze;
      ctx.beginPath();
      ctx.arc(cx, cy, radius, 0, Math.PI * 2);
      ctx.fill();
    }
  }

  function drawSnowflake(ctx, centerX, centerY, radius) {
    const segments = [];
    const addSegment = (x1, y1, x2, y2) => {
      segments.push({ x1, y1, x2, y2 });
    };

    const branchSteps = [
      { t: 0.16, len: 0.28 },
      { t: 0.3, len: 0.26 },
      { t: 0.44, len: 0.24 },
      { t: 0.58, len: 0.22 },
      { t: 0.7, len: 0.2 },
      { t: 0.82, len: 0.16 },
      { t: 0.92, len: 0.12 },
    ];

    for (let arm = 0; arm < 6; arm += 1) {
      const angle = (arm * Math.PI * 2) / 6;
      const dx = Math.cos(angle);
      const dy = Math.sin(angle);
      addSegment(centerX, centerY, centerX + dx * radius, centerY + dy * radius);

      branchSteps.forEach((step) => {
        const baseX = centerX + dx * radius * step.t;
        const baseY = centerY + dy * radius * step.t;
        const branchLen = radius * step.len;

        [1, -1].forEach((sign) => {
          const branchAngle = angle + sign * (Math.PI / 6);
          const bx = baseX + Math.cos(branchAngle) * branchLen;
          const by = baseY + Math.sin(branchAngle) * branchLen;
          addSegment(baseX, baseY, bx, by);

          const twigBaseX =
            baseX + Math.cos(branchAngle) * branchLen * 0.62;
          const twigBaseY =
            baseY + Math.sin(branchAngle) * branchLen * 0.62;
          const twigAngle = branchAngle + sign * (Math.PI / 10);
          const twigLen = branchLen * 0.42;
          addSegment(
            twigBaseX,
            twigBaseY,
            twigBaseX + Math.cos(twigAngle) * twigLen,
            twigBaseY + Math.sin(twigAngle) * twigLen,
          );
        });
      });

      const tipX = centerX + dx * radius;
      const tipY = centerY + dy * radius;
      const tipLen = radius * 0.12;
      [1, -1].forEach((sign) => {
        const tipAngle = angle + sign * (Math.PI / 9);
        addSegment(
          tipX - dx * tipLen * 0.25,
          tipY - dy * tipLen * 0.25,
          tipX + Math.cos(tipAngle) * tipLen,
          tipY + Math.sin(tipAngle) * tipLen,
        );
      });
    }

    const strokeSegments = (width, color, blur = 0, shadow = "") => {
      ctx.save();
      ctx.lineCap = "round";
      ctx.lineJoin = "round";
      ctx.strokeStyle = color;
      ctx.lineWidth = width;
      ctx.shadowBlur = blur;
      ctx.shadowColor = shadow;
      ctx.beginPath();
      segments.forEach((seg) => {
        ctx.moveTo(seg.x1, seg.y1);
        ctx.lineTo(seg.x2, seg.y2);
      });
      ctx.stroke();
      ctx.restore();
    };

    ctx.save();
    ctx.globalCompositeOperation = "lighter";
    strokeSegments(
      Math.max(2.6, radius * 0.02),
      "rgba(120,190,255,0.2)",
      Math.max(18, radius * 0.12),
      "rgba(120,190,255,0.8)",
    );
    strokeSegments(
      Math.max(1.6, radius * 0.012),
      "rgba(200,235,255,0.85)",
    );
    strokeSegments(Math.max(0.9, radius * 0.006), "rgba(255,255,255,0.98)");

    const coreGlow = ctx.createRadialGradient(
      centerX,
      centerY,
      0,
      centerX,
      centerY,
      radius * 0.18,
    );
    coreGlow.addColorStop(0, "rgba(230,245,255,0.9)");
    coreGlow.addColorStop(1, "rgba(120,190,255,0)");
    ctx.fillStyle = coreGlow;
    ctx.beginPath();
    ctx.arc(centerX, centerY, radius * 0.18, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();
  }

  function drawSplash() {
    if (!splash || !splashCanvas || !splashCtx) return;
    const width = Math.max(1, Math.floor(splash.clientWidth));
    const height = Math.max(1, Math.floor(splash.clientHeight));
    const dpr = window.devicePixelRatio || 1;
    splashCanvas.width = width * dpr;
    splashCanvas.height = height * dpr;
    splashCanvas.style.width = `${width}px`;
    splashCanvas.style.height = `${height}px`;
    splashCtx.setTransform(dpr, 0, 0, dpr, 0, 0);
    splashCtx.clearRect(0, 0, width, height);
    splashCtx.fillStyle = "#000";
    splashCtx.fillRect(0, 0, width, height);

    const centerX = width / 2;
    const centerY = height / 2 - Math.min(24, height * 0.03);
    const radius = Math.min(width, height) * 0.22;

    drawStarfield(splashCtx, width, height);

    const halo = splashCtx.createRadialGradient(
      centerX,
      centerY,
      0,
      centerX,
      centerY,
      radius * 3.2,
    );
    halo.addColorStop(0, "rgba(20,40,70,0.45)");
    halo.addColorStop(1, "rgba(0,0,0,0)");
    splashCtx.fillStyle = halo;
    splashCtx.beginPath();
    splashCtx.arc(centerX, centerY, radius * 3.2, 0, Math.PI * 2);
    splashCtx.fill();

    drawSnowflake(splashCtx, centerX, centerY, radius);

    const vignette = splashCtx.createRadialGradient(
      centerX,
      centerY,
      radius * 0.6,
      centerX,
      centerY,
      Math.max(width, height) * 0.7,
    );
    vignette.addColorStop(0, "rgba(0,0,0,0)");
    vignette.addColorStop(1, "rgba(0,0,0,0.65)");
    splashCtx.fillStyle = vignette;
    splashCtx.fillRect(0, 0, width, height);
  }

  function updateSplashCallToAction() {
    if (!splashOpenFileBtn || !splash) return;
    const splashVisible = !splash.classList.contains("is-hidden");
    const ready = Boolean(state.backendAlive) && !state.isLoading;
    const show = splashVisible && ready && !state.hasFrame;
    if (splashActions) {
      splashActions.classList.toggle("is-hidden", !show);
    }
    splashOpenFileBtn.classList.toggle("is-hidden", !show);
    splashOpenFileBtn.disabled = !show;
  }

  function showSplash() {
    splash?.classList.remove("is-hidden");
    updateSplashCallToAction();
  }

  function isI18nKey(value) {
    const normalized = String(value || "").trim();
    return normalized.includes(".") && /^[a-z0-9_.-]+$/i.test(normalized);
  }

  function setSplashStatus(status, vars = {}) {
    if (!splashStatus) return;
    const normalized = String(status || "").trim();
    const normalizedVars = vars && typeof vars === "object" ? vars : {};
    const useI18nKey = isI18nKey(normalized);
    const text = useI18nKey ? t(normalized, normalizedVars) : normalized;
    if (useI18nKey) {
      splashStatus.dataset.i18n = normalized;
      if (Object.keys(normalizedVars).length > 0) {
        splashStatus.dataset.i18nVars = JSON.stringify(normalizedVars);
      } else {
        delete splashStatus.dataset.i18nVars;
      }
    } else {
      delete splashStatus.dataset.i18n;
      delete splashStatus.dataset.i18nVars;
    }
    splashStatus.textContent = text;
    if (!splash) return;
    const lower = text.toLowerCase();
    const busy = useI18nKey
      ? !SPLASH_STATUS_TERMINAL_KEYS.has(normalized)
      : Boolean(text) && !/\b(ready|failed|error|done|complete)\b/.test(lower);
    splash.classList.toggle("is-busy", busy);
    updateSplashCallToAction();
  }

  function hideSplash() {
    splash?.classList.add("is-hidden");
    updateSplashCallToAction();
  }

  return {
    exportFullImage,
    exportVisibleArea,
    exportViewerWindow,
    drawSplash,
    updateSplashCallToAction,
    showSplash,
    setSplashStatus,
    hideSplash,
  };
}
