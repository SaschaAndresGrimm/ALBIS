/**
 * Histogram and colorbar rendering.
 */

export function createHistogramRenderController({
  state,
  elements,
  callbacks,
  constants,
}) {
  const {
    histCanvas,
    histCtx,
    histColorbar,
    histColorCtx,
  } = elements;

  const {
    formatValue,
    buildPalette,
    getPaletteColorCount,
    mapValueToNorm,
  } = callbacks;

  const {
    PLOT_THEME,
  } = constants;

  function histogramValueToX(value, width) {
    const minVal = state.stats?.min ?? 0;
    const maxVal = state.stats?.max ?? 1;
    if (!Number.isFinite(value) || !Number.isFinite(minVal) || !Number.isFinite(maxVal)) {
      return 0;
    }
    const range = maxVal - minVal || 1;
    if (!state.histLogX) {
      return ((value - minVal) / range) * width;
    }
    const symlog = (v) => Math.sign(v) * Math.log10(1 + Math.abs(v));
    const minMap = symlog(minVal);
    const maxMap = symlog(maxVal);
    const mapRange = maxMap - minMap || 1;
    const mapped = (symlog(value) - minMap) / mapRange;
    return Math.min(width, Math.max(0, mapped * width));
  }

  function histogramXToValue(x, width) {
    const minVal = state.stats?.min ?? 0;
    const maxVal = state.stats?.max ?? 1;
    const clampedX = Math.min(width, Math.max(0, x));
    const t = width ? clampedX / width : 0;
    if (!state.histLogX) {
      return minVal + t * (maxVal - minVal);
    }
    const symlog = (v) => Math.sign(v) * Math.log10(1 + Math.abs(v));
    const invSymlog = (v) => Math.sign(v) * (10 ** Math.abs(v) - 1);
    const minMap = symlog(minVal);
    const maxMap = symlog(maxVal);
    const mapRange = maxMap - minMap || 1;
    const mapped = minMap + t * mapRange;
    return invSymlog(mapped);
  }

  function drawHistogram(hist) {
    const width = histCanvas.clientWidth;
    const height = histCanvas.clientHeight;
    if (width < 4 || height < 4) {
      return;
    }
    histCanvas.width = width * window.devicePixelRatio;
    histCanvas.height = height * window.devicePixelRatio;
    histCtx.setTransform(window.devicePixelRatio, 0, 0, window.devicePixelRatio, 0, 0);
    histCtx.clearRect(0, 0, width, height);
    histCtx.fillStyle = PLOT_THEME.bg;
    histCtx.fillRect(0, 0, width, height);
    if (!hist || hist.length === 0) {
      histCtx.strokeStyle = PLOT_THEME.frame;
      histCtx.strokeRect(0.5, 0.5, width - 1, height - 1);
      return;
    }
    const maxCount = Math.max(...hist);
    const bins = hist.length;
    const pad = 10;
    const drawableHeight = Math.max(4, height - pad);
    const logY = state.histLogY;
    const yDenom = logY ? Math.log10(1 + maxCount) : maxCount;

    const barWidth = width / bins;
    histCtx.fillStyle = PLOT_THEME.bar;
    for (let i = 0; i < bins; i += 1) {
      const count = hist[i];
      const norm = yDenom ? (logY ? Math.log10(1 + count) / yDenom : count / yDenom) : 0;
      const h = norm * drawableHeight;
      histCtx.fillRect(i * barWidth, height - h, Math.max(1, barWidth), h);
    }

    const minVal = state.min;
    const maxVal = state.max;
    if (Number.isFinite(minVal) && Number.isFinite(maxVal)) {
      const minX = histogramValueToX(minVal, width);
      const maxX = histogramValueToX(maxVal, width);
      const markerTop = 2;
      const markerBottom = height - 2;

      const drawMarker = (x, color, label, options = {}) => {
        const preferRight = options.preferRight !== false;
        const labelY = Number.isFinite(options.labelY) ? options.labelY : markerTop + 10;
        histCtx.strokeStyle = color;
        histCtx.lineWidth = 1.5;
        histCtx.beginPath();
        histCtx.moveTo(x, markerTop);
        histCtx.lineTo(x, markerBottom);
        histCtx.stroke();

        histCtx.fillStyle = color;
        histCtx.fillRect(x - 3, markerTop, 6, 8);
        histCtx.strokeStyle = PLOT_THEME.markerOutline;
        histCtx.strokeRect(x - 3, markerTop, 6, 8);

        if (label) {
          histCtx.font = '600 10px "Avenir Next", "Segoe UI", "Helvetica Neue", Arial, sans-serif';
          histCtx.textBaseline = "top";
          histCtx.fillStyle = PLOT_THEME.text;
          const metrics = histCtx.measureText(label);
          let textX;
          if (preferRight) {
            textX = Math.min(width - metrics.width - 4, Math.max(4, x + 6));
          } else {
            textX = Math.max(4, Math.min(width - metrics.width - 4, x - metrics.width - 6));
          }
          histCtx.fillText(label, textX, labelY);
        }
      };

      const labelsClose = Math.abs(maxX - minX) < 120;
      const labelTop = markerTop + 10;
      const labelBottom = markerTop + 24;
      drawMarker(minX, "#6eb5ff", `BG ${formatValue(minVal)}`, {
        preferRight: true,
        labelY: labelTop,
      });
      drawMarker(maxX, "#ffd166", `FG ${formatValue(maxVal)}`, {
        preferRight: false,
        labelY: labelsClose ? labelBottom : labelTop,
      });
    }

    histCtx.strokeStyle = PLOT_THEME.frame;
    histCtx.strokeRect(0.5, 0.5, width - 1, height - 1);
    drawColorbar();
  }

  function clearHistogram() {
    const width = histCanvas.clientWidth || 1;
    const height = histCanvas.clientHeight || 1;
    histCanvas.width = width * window.devicePixelRatio;
    histCanvas.height = height * window.devicePixelRatio;
    histCtx.setTransform(window.devicePixelRatio, 0, 0, window.devicePixelRatio, 0, 0);
    histCtx.fillStyle = PLOT_THEME.bg;
    histCtx.fillRect(0, 0, width, height);
    histCtx.strokeStyle = PLOT_THEME.frame;
    histCtx.strokeRect(0.5, 0.5, width - 1, height - 1);
    drawColorbar();
  }

  function drawColorbar() {
    if (!histColorbar || !histColorCtx) return;
    const width = histColorbar.clientWidth || 1;
    const height = histColorbar.clientHeight || 1;
    const dpr = window.devicePixelRatio || 1;
    histColorbar.width = Math.max(1, Math.floor(width * dpr));
    histColorbar.height = Math.max(1, Math.floor(height * dpr));
    histColorCtx.setTransform(dpr, 0, 0, dpr, 0, 0);
    histColorCtx.clearRect(0, 0, width, height);

    const palette = buildPalette(state.colormap);
    const statsMin = Number.isFinite(state.stats?.min) ? state.stats.min : state.min;
    const statsMax = Number.isFinite(state.stats?.max) ? state.stats.max : state.max;
    const statsRange = statsMax - statsMin || 1;
    const useLogX = Boolean(state.histLogX);
    const symlog = (v) => Math.sign(v) * Math.log10(1 + Math.abs(v));
    const invSymlog = (v) => Math.sign(v) * (10 ** Math.abs(v) - 1);
    const minMap = useLogX ? symlog(statsMin) : 0;
    const maxMap = useLogX ? symlog(statsMax) : 0;
    const mapRange = useLogX ? maxMap - minMap || 1 : 1;
    const imageData = histColorCtx.createImageData(width, height);
    const data = imageData.data;
    const maxIdx = getPaletteColorCount(palette) - 1;
    for (let x = 0; x < width; x += 1) {
      const t = width > 1 ? x / (width - 1) : 0;
      const value = useLogX
        ? invSymlog(minMap + t * mapRange)
        : statsMin + t * statsRange;
      const norm = mapValueToNorm(value);
      const idx = Math.min(maxIdx, Math.max(0, Math.round(norm * maxIdx)));
      const p = idx * 4;
      const r = palette[p];
      const g = palette[p + 1];
      const b = palette[p + 2];
      for (let y = 0; y < height; y += 1) {
        const i = (y * width + x) * 4;
        data[i] = r;
        data[i + 1] = g;
        data[i + 2] = b;
        data[i + 3] = 255;
      }
    }
    histColorCtx.putImageData(imageData, 0, 0);
    histColorCtx.strokeStyle = PLOT_THEME.frame;
    histColorCtx.strokeRect(0.5, 0.5, width - 1, height - 1);
  }

  return {
    drawHistogram,
    clearHistogram,
    drawColorbar,
    histogramValueToX,
    histogramXToValue,
  };
}
