/**
 * Data -> Pixel Mask -> Edit on image (beta): the toolbar over the image, the
 * strokes and shapes drawn with it, and the overlay that shows what was
 * changed by hand.
 *
 * While it is open, a left-button drag on the image edits the mask instead of
 * panning: the pixel brush paints, a box or circle is dragged out, a polygon is
 * clicked corner by corner. Holding Alt unmasks instead; holding Space pans as
 * usual. Shift-drag (contrast) and right-drag (ROI) are left to the viewer.
 *
 * Each stroke or shape is one undo step. The edits go into the mask the Pixel
 * Mask section holds (pixel_mask_controller.js), which applies them to the
 * image when a step ends; during a brush stroke the overlay shows them at once.
 * The overlay marks every pixel changed by hand -- magenta where masked, green
 * where unmasked -- for as long as the toolbar is open.
 */

import { t } from "./i18n.js";
import { colorDistance } from "./mask_colors.js";
import {
  createChange,
  createHistory,
  forEachInBox,
  forEachInBrush,
  forEachInCircle,
  forEachInPolygon,
  linePoints,
} from "./pixel_mask_edit.js";

const TOOLS = ["brush", "box", "polygon", "circle"];
const TOOL_KEYS = { b: "brush", r: "box", p: "polygon", c: "circle" };
// The tints for pixels masked and unmasked by hand, each the first of its list
// that stands clear of the mask colours on screen: the auto mask colours can
// themselves be magenta or green, and a tint must not pass for a mask class.
const MASKED_TINTS = [[255, 0, 255], [255, 140, 0], [0, 220, 255], [255, 105, 180]];
const UNMASKED_TINTS = [[0, 220, 120], [180, 255, 0], [0, 200, 255], [255, 220, 0]];
const CLEAR_OF_MASK_COLORS = 40;
// Find bad pixels' preview: a ring around each pixel found, so a single pixel
// shows at any zoom. Hot ones in the masking tint, dead and cold in another.
const COLD_TINTS = [[0, 200, 255], [255, 220, 0], [180, 255, 0]];
const MARKER_LIMIT = 20000;

export function pickTint(candidates, avoid = []) {
  return candidates.find((rgb) => avoid.every((other) => colorDistance(rgb, other) >= CLEAR_OF_MASK_COLORS)) || candidates[0];
}
// A click this close (screen px) to a polygon's first corner closes it.
const CLOSE_RADIUS_PX = 8;

function isTypingTarget(target) {
  const tag = String(target?.tagName || "").toLowerCase();
  return tag === "input" || tag === "textarea" || tag === "select" || Boolean(target?.isContentEditable);
}

export function createPixelMaskEditController({ state, elements = {}, callbacks = {}, isMac = false }) {
  const {
    canvasWrap,
    overlay,
    bar,
    toolButtons = [],
    modeButtons = [],
    brushSize,
    undoBtn,
    redoBtn,
    doneBtn,
    countEl,
    hintEl,
  } = elements;
  const {
    getTarget = () => null,
    onCommit,
    onClose,
    getImagePoint,
    getView,
    syncOverlayCanvas,
    describeEdits = () => "",
    // { gap, defective, saturated } as [r, g, b]: what the tints must not be.
    getMaskColors = () => null,
  } = callbacks;
  const ctx = overlay?.getContext?.("2d") || null;

  let active = false;
  let tool = "brush";
  let mode = "mask";
  let spaceHeld = false;
  let drag = null; // { kind, pointerId, mode, change?, last?, start?, end? }
  let polygon = null; // { points: [...], mode }
  let hover = null;
  let drawScheduled = false;
  let imageData = null;
  let markers = null; // { width, groups: [{ kind: "hot" | "cold", indices }] }
  let tints = { masked: MASKED_TINTS[0], unmasked: UNMASKED_TINTS[0] };

  function updateTints() {
    const colors = getMaskColors();
    const avoid = colors ? Object.values(colors).filter(Array.isArray) : [];
    const masked = pickTint(MASKED_TINTS, avoid);
    const unmasked = pickTint(UNMASKED_TINTS, [...avoid, masked]);
    const cold = pickTint(COLD_TINTS, [...avoid, masked]);
    tints = { masked, unmasked, cold };
  }

  const rgba = (rgb, alpha) => `rgba(${rgb[0]}, ${rgb[1]}, ${rgb[2]}, ${alpha})`;
  const history = createHistory();
  // Indices where the mask differs from what editing started from.
  let changed = new Set();

  function target() {
    const next = getTarget();
    if (!next || next.width !== state.width || next.height !== state.height) return null;
    return next;
  }

  function trackChanged(data, base, indices) {
    for (const index of indices) {
      if (data[index] !== base[index]) changed.add(index);
      else changed.delete(index);
    }
  }

  function rebuildChanged() {
    changed = new Set();
    const current = target();
    if (!current) return;
    const { data, base } = current;
    for (let i = 0; i < data.length; i += 1) if (data[i] !== base[i]) changed.add(i);
  }

  // ---- applying edits -------------------------------------------------------

  function effectiveMode(event) {
    const flipped = Boolean(event?.altKey);
    if (!flipped) return mode;
    return mode === "mask" ? "unmask" : "mask";
  }

  function runShape(shapeMode, cover) {
    const current = target();
    if (!current) return;
    const change = createChange();
    cover(current.width, current.height, (index) => change.apply(current.data, index, shapeMode));
    commit(change, current);
  }

  function commit(change, current) {
    const record = change.finish(current.data);
    if (!record) {
      scheduleDraw();
      return;
    }
    history.push(record);
    trackChanged(current.data, current.base, record.idx);
    onCommit?.();
    renderBar();
    scheduleDraw();
  }

  function undo() {
    const current = target();
    if (!current) return;
    const record = history.undo(current.data);
    if (!record) return;
    trackChanged(current.data, current.base, record.idx);
    onCommit?.();
    renderBar();
    scheduleDraw();
  }

  function redo() {
    const current = target();
    if (!current) return;
    const record = history.redo(current.data);
    if (!record) return;
    trackChanged(current.data, current.base, record.idx);
    onCommit?.();
    renderBar();
    scheduleDraw();
  }

  function paintBrush(from, to, current, change) {
    const size = Number(brushSize?.value) || 1;
    const touched = [];
    for (const point of linePoints(from, to)) {
      forEachInBrush(point, size, current.width, current.height, (index) => {
        if (change.apply(current.data, index, drag.mode)) touched.push(index);
      });
    }
    trackChanged(current.data, current.base, touched);
  }

  function closePolygon() {
    if (!polygon || polygon.points.length < 3) {
      polygon = null;
      scheduleDraw();
      return;
    }
    const { points, mode: shapeMode } = polygon;
    polygon = null;
    runShape(shapeMode, (w, h, visit) => forEachInPolygon(points, w, h, visit));
  }

  // ---- pointer --------------------------------------------------------------

  function imagePoint(event, outside = false) {
    return getImagePoint?.(event, outside ? { allowOutside: true, allowOutsideViewport: true } : {}) || null;
  }

  function screenOf(point) {
    const view = getView();
    return { x: (point.x + 0.5) * view.scaleX + view.offsetX, y: (point.y + 0.5) * view.scaleY + view.offsetY };
  }

  function onPointerDown(event) {
    if (!active || event.pointerType === "touch") return;
    // Space pans, Shift windows the contrast, right/Ctrl-click draws an ROI.
    if (spaceHeld || event.button !== 0 || event.shiftKey || event.ctrlKey || event.metaKey) return;
    const current = target();
    if (!current) return;
    event.preventDefault();
    event.stopImmediatePropagation();
    const shapeMode = effectiveMode(event);
    if (tool === "polygon") {
      const point = imagePoint(event, true);
      if (!point) return;
      if (polygon && polygon.points.length >= 3) {
        const first = screenOf(polygon.points[0]);
        const here = screenOf(point);
        if (Math.hypot(first.x - here.x, first.y - here.y) <= CLOSE_RADIUS_PX) {
          closePolygon();
          return;
        }
      }
      if (!polygon) polygon = { points: [], mode: shapeMode };
      const last = polygon.points[polygon.points.length - 1];
      if (!last || last.x !== point.x || last.y !== point.y) polygon.points.push(point);
      scheduleDraw();
      return;
    }
    const point = imagePoint(event, tool !== "brush");
    if (!point) return;
    canvasWrap?.setPointerCapture?.(event.pointerId);
    if (tool === "brush") {
      drag = { kind: "brush", pointerId: event.pointerId, mode: shapeMode, change: createChange(), last: point };
      paintBrush(point, point, current, drag.change);
    } else {
      drag = { kind: tool, pointerId: event.pointerId, mode: shapeMode, start: point, end: point };
    }
    scheduleDraw();
  }

  function onPointerMove(event) {
    if (!active) return;
    const point = imagePoint(event, true);
    hover = point;
    if (drag && event.pointerId === drag.pointerId && point) {
      if (drag.kind === "brush") {
        const current = target();
        if (current) {
          const clamped = {
            x: Math.max(0, Math.min(current.width - 1, point.x)),
            y: Math.max(0, Math.min(current.height - 1, point.y)),
          };
          paintBrush(drag.last, clamped, current, drag.change);
          drag.last = clamped;
        }
      } else {
        drag.end = point;
      }
    }
    scheduleDraw();
  }

  function onPointerUp(event) {
    if (!active || !drag || event.pointerId !== drag.pointerId) return;
    const finished = drag;
    drag = null;
    canvasWrap?.releasePointerCapture?.(event.pointerId);
    if (finished.kind === "brush") {
      const current = target();
      if (current) commit(finished.change, current);
      return;
    }
    if (finished.kind === "box") {
      runShape(finished.mode, (w, h, visit) => forEachInBox(finished.start, finished.end, w, h, visit));
    } else if (finished.kind === "circle") {
      const radius = Math.hypot(finished.end.x - finished.start.x, finished.end.y - finished.start.y);
      runShape(finished.mode, (w, h, visit) => forEachInCircle(finished.start, radius, w, h, visit));
    }
  }

  // The viewer zooms on a double-click; while editing, one ends a polygon and
  // otherwise does nothing, so a quick second click never jumps the view.
  function onDoubleClick(event) {
    if (!active || spaceHeld) return;
    event.preventDefault();
    event.stopImmediatePropagation();
    if (tool === "polygon" && polygon) closePolygon();
  }

  function onPointerLeave() {
    hover = null;
    scheduleDraw();
  }

  function cancelShape() {
    if (!drag && !polygon) return false;
    if (drag?.kind === "brush") {
      // A stroke under way is kept: it is already on the mask.
      const current = target();
      if (current) commit(drag.change, current);
    }
    drag = null;
    polygon = null;
    scheduleDraw();
    return true;
  }

  // ---- keys -----------------------------------------------------------------

  function onKeyDown(event) {
    if (!active || isTypingTarget(event.target)) return;
    const key = String(event.key || "");
    const lower = key.toLowerCase();
    const command = isMac ? event.metaKey : event.ctrlKey;
    let handled = true;
    if (key === " ") {
      if (!spaceHeld) {
        spaceHeld = true;
        canvasWrap?.classList.add("is-mask-pan");
      }
    } else if (key === "Escape") {
      if (!cancelShape()) close();
    } else if (key === "Enter" && polygon) {
      closePolygon();
    } else if (command && lower === "z") {
      if (event.shiftKey) redo();
      else undo();
    } else if (command && lower === "y" && !isMac) {
      redo();
    } else if (!command && !event.altKey && TOOL_KEYS[lower]) {
      setTool(TOOL_KEYS[lower]);
    } else if (!command && !event.altKey && lower === "x") {
      setMode(mode === "mask" ? "unmask" : "mask");
    } else {
      handled = false;
    }
    if (key === "Alt") {
      bar?.classList.add("is-flipped");
      handled = false;
    }
    if (handled) {
      event.preventDefault();
      event.stopImmediatePropagation();
    }
  }

  function onKeyUp(event) {
    if (event.key === " ") {
      spaceHeld = false;
      canvasWrap?.classList.remove("is-mask-pan");
    }
    if (event.key === "Alt") bar?.classList.remove("is-flipped");
  }

  function onBlur() {
    spaceHeld = false;
    canvasWrap?.classList.remove("is-mask-pan");
    bar?.classList.remove("is-flipped");
  }

  // ---- drawing --------------------------------------------------------------

  function scheduleDraw() {
    if (!overlay || !ctx || drawScheduled) return;
    drawScheduled = true;
    window.requestAnimationFrame(() => {
      drawScheduled = false;
      draw();
    });
  }

  function paintChanged(current, view, dpr) {
    const w = overlay.width;
    const h = overlay.height;
    if (!imageData || imageData.width !== w || imageData.height !== h) imageData = ctx.createImageData(w, h);
    const buf = imageData.data;
    buf.fill(0);
    const { data, width } = current;
    for (const index of changed) {
      const x = index % width;
      const y = (index - x) / width;
      const sx0 = Math.floor((x * view.scaleX + view.offsetX) * dpr);
      const sy0 = Math.floor((y * view.scaleY + view.offsetY) * dpr);
      const sx1 = Math.max(sx0 + 1, Math.floor(((x + 1) * view.scaleX + view.offsetX) * dpr));
      const sy1 = Math.max(sy0 + 1, Math.floor(((y + 1) * view.scaleY + view.offsetY) * dpr));
      if (sx1 <= 0 || sy1 <= 0 || sx0 >= w || sy0 >= h) continue;
      const [r, g, b] = data[index] & ~1 ? tints.masked : tints.unmasked;
      const a = 150;
      for (let py = Math.max(0, sy0); py < Math.min(h, sy1); py += 1) {
        let o = (py * w + Math.max(0, sx0)) * 4;
        for (let px = Math.max(0, sx0); px < Math.min(w, sx1); px += 1) {
          buf[o] = r;
          buf[o + 1] = g;
          buf[o + 2] = b;
          buf[o + 3] = a;
          o += 4;
        }
      }
    }
    ctx.putImageData(imageData, 0, 0);
  }

  function pixelRect(a, b, view) {
    const x0 = Math.min(a.x, b.x);
    const y0 = Math.min(a.y, b.y);
    const x1 = Math.max(a.x, b.x) + 1;
    const y1 = Math.max(a.y, b.y) + 1;
    return [
      x0 * view.scaleX + view.offsetX,
      y0 * view.scaleY + view.offsetY,
      (x1 - x0) * view.scaleX,
      (y1 - y0) * view.scaleY,
    ];
  }

  function drawPreview(view) {
    const strokeFor = (shapeMode) => rgba(shapeMode === "unmask" ? tints.unmasked : tints.masked, 0.95);
    ctx.lineWidth = 1.5;
    if (drag && drag.kind === "box") {
      ctx.strokeStyle = strokeFor(drag.mode);
      ctx.strokeRect(...pixelRect(drag.start, drag.end, view));
    } else if (drag && drag.kind === "circle") {
      const radius = Math.hypot(drag.end.x - drag.start.x, drag.end.y - drag.start.y);
      const c = screenOf(drag.start);
      ctx.strokeStyle = strokeFor(drag.mode);
      ctx.beginPath();
      ctx.ellipse(c.x, c.y, Math.max(0.5, radius * view.scaleX), Math.max(0.5, radius * view.scaleY), 0, 0, Math.PI * 2);
      ctx.stroke();
    }
    if (polygon) {
      ctx.strokeStyle = strokeFor(polygon.mode);
      ctx.fillStyle = strokeFor(polygon.mode);
      ctx.beginPath();
      polygon.points.forEach((point, i) => {
        const s = screenOf(point);
        if (i === 0) ctx.moveTo(s.x, s.y);
        else ctx.lineTo(s.x, s.y);
      });
      if (hover) {
        const s = screenOf(hover);
        ctx.lineTo(s.x, s.y);
      }
      ctx.stroke();
      for (const point of polygon.points) {
        const s = screenOf(point);
        ctx.fillRect(s.x - 2.5, s.y - 2.5, 5, 5);
      }
    }
    if (hover && tool === "brush" && !spaceHeld) {
      const size = Math.max(1, Number(brushSize?.value) || 1);
      const lo = Math.floor((size - 1) / 2);
      const a = { x: hover.x - lo, y: hover.y - lo };
      const b = { x: a.x + size - 1, y: a.y + size - 1 };
      ctx.strokeStyle = strokeFor(drag?.mode || mode);
      ctx.strokeRect(...pixelRect(a, b, view));
    }
  }

  function drawMarkers(view, width, height) {
    if (!markers) return;
    const radius = Math.max(6, view.scaleX * 0.5 + 3);
    let drawn = 0;
    ctx.lineWidth = 1.5;
    for (const group of markers.groups) {
      ctx.strokeStyle = rgba(group.kind === "hot" ? tints.masked : tints.cold, 0.95);
      ctx.beginPath();
      for (const index of group.indices) {
        if (drawn >= MARKER_LIMIT) break;
        const x = index % markers.width;
        const y = (index - x) / markers.width;
        const sx = (x + 0.5) * view.scaleX + view.offsetX;
        const sy = (y + 0.5) * view.scaleY + view.offsetY;
        if (sx < -radius || sy < -radius || sx > width + radius || sy > height + radius) continue;
        ctx.moveTo(sx + radius, sy);
        ctx.arc(sx, sy, radius, 0, Math.PI * 2);
        drawn += 1;
      }
      ctx.stroke();
    }
  }

  function draw() {
    if (!overlay || !ctx) return;
    const metrics = syncOverlayCanvas?.(overlay, ctx);
    if (!metrics) return;
    ctx.clearRect(0, 0, metrics.width, metrics.height);
    if (!active && !markers) return;
    updateTints();
    const view = getView();
    if (active) {
      const current = target();
      if (current && changed.size) paintChanged(current, view, metrics.dpr || 1);
    }
    drawMarkers(view, metrics.width, metrics.height);
    if (active) drawPreview(view);
  }

  // ---- toolbar --------------------------------------------------------------

  function setTool(next) {
    if (!TOOLS.includes(next)) return;
    if (tool !== next) cancelShape();
    tool = next;
    renderBar();
    scheduleDraw();
  }

  function setMode(next) {
    mode = next === "unmask" ? "unmask" : "mask";
    renderBar();
    scheduleDraw();
  }

  function renderBar() {
    if (!bar) return;
    bar.hidden = !active;
    toolButtons.forEach((button) => {
      const on = button.dataset.maskTool === tool;
      button.classList.toggle("is-active", on);
      button.setAttribute("aria-pressed", on ? "true" : "false");
    });
    modeButtons.forEach((button) => {
      const on = button.dataset.maskMode === mode;
      button.classList.toggle("is-active", on);
      button.setAttribute("aria-pressed", on ? "true" : "false");
    });
    if (brushSize) brushSize.disabled = tool !== "brush";
    if (undoBtn) undoBtn.disabled = !history.canUndo;
    if (redoBtn) redoBtn.disabled = !history.canRedo;
    if (countEl) countEl.textContent = describeEdits();
    if (hintEl) hintEl.textContent = t(`pixel_mask.edit.hint.${tool}`, { alt: isMac ? "⌥" : "Alt" });
  }

  /** Re-label the toolbar (language change): titles carry the shortcut keys. */
  function localize() {
    const keys = {
      alt: isMac ? "⌥" : "Alt",
      undo: isMac ? "⌘Z" : "Ctrl+Z",
      redo: isMac ? "⇧⌘Z" : "Ctrl+Y",
    };
    toolButtons.forEach((button) => {
      const label = t(`pixel_mask.edit.tool.${button.dataset.maskTool}`);
      button.title = label;
      button.setAttribute("aria-label", label);
    });
    modeButtons.forEach((button) => {
      button.title = t(`pixel_mask.edit.mode_title.${button.dataset.maskMode}`, keys);
    });
    if (undoBtn) {
      undoBtn.title = t("pixel_mask.edit.undo", keys);
      undoBtn.setAttribute("aria-label", undoBtn.title);
    }
    if (redoBtn) {
      redoBtn.title = t("pixel_mask.edit.redo", keys);
      redoBtn.setAttribute("aria-label", redoBtn.title);
    }
    renderBar();
  }

  // ---- open and close -------------------------------------------------------

  function open() {
    if (active) return true;
    if (!target()) return false;
    active = true;
    history.clear();
    rebuildChanged();
    canvasWrap?.classList.add("is-mask-editing");
    canvasWrap?.addEventListener("pointerdown", onPointerDown, true);
    canvasWrap?.addEventListener("pointermove", onPointerMove);
    canvasWrap?.addEventListener("pointerup", onPointerUp);
    canvasWrap?.addEventListener("pointercancel", onPointerUp);
    canvasWrap?.addEventListener("pointerleave", onPointerLeave);
    canvasWrap?.addEventListener("dblclick", onDoubleClick, true);
    window.addEventListener("keydown", onKeyDown, true);
    window.addEventListener("keyup", onKeyUp, true);
    window.addEventListener("blur", onBlur);
    localize();
    scheduleDraw();
    return true;
  }

  /** Leave edit mode; `options` go to onClose (`keep`: the mask stays pinned). */
  function close(options = {}) {
    if (!active) return;
    cancelShape();
    active = false;
    onBlur();
    canvasWrap?.classList.remove("is-mask-editing");
    canvasWrap?.removeEventListener("pointerdown", onPointerDown, true);
    canvasWrap?.removeEventListener("pointermove", onPointerMove);
    canvasWrap?.removeEventListener("pointerup", onPointerUp);
    canvasWrap?.removeEventListener("pointercancel", onPointerUp);
    canvasWrap?.removeEventListener("pointerleave", onPointerLeave);
    canvasWrap?.removeEventListener("dblclick", onDoubleClick, true);
    window.removeEventListener("keydown", onKeyDown, true);
    window.removeEventListener("keyup", onKeyUp, true);
    window.removeEventListener("blur", onBlur);
    history.clear();
    changed = new Set();
    hover = null;
    renderBar();
    draw();
    onClose?.(options);
  }

  toolButtons.forEach((button) => button.addEventListener("click", () => setTool(button.dataset.maskTool)));
  modeButtons.forEach((button) => button.addEventListener("click", () => setMode(button.dataset.maskMode)));
  brushSize?.addEventListener("change", () => scheduleDraw());
  undoBtn?.addEventListener("click", () => undo());
  redoBtn?.addEventListener("click", () => redo());
  doneBtn?.addEventListener("click", () => close());
  renderBar();

  return {
    open,
    close,
    undo,
    redo,
    setTool,
    setMode,
    localize,
    /** The target changed outside the editor (discarded, replaced): start over. */
    reset() {
      if (!active) return;
      history.clear();
      rebuildChanged();
      renderBar();
      scheduleDraw();
    },
    refresh() {
      renderBar();
      scheduleDraw();
    },
    scheduleDraw,
    /** Circle pixels on the image (Find bad pixels), or null to stop. */
    setMarkers(next) {
      markers = next && next.groups?.some((group) => group.indices.length) ? next : null;
      scheduleDraw();
    },
    get active() {
      return active;
    },
    get tool() {
      return tool;
    },
    get mode() {
      return mode;
    },
  };
}
