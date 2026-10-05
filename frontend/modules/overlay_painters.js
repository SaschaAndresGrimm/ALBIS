/**
 * Overlay painting shared by the on-screen canvases and the GIF exporter.
 *
 * The two callers differ only in how an image pixel maps to an output pixel and
 * in how big the decorations should be, so both are parameters here:
 *
 *   view      an affine map, outX = imageX * scaleX + offsetX
 *   uiScale   a multiplier on line widths, fonts and marker sizes
 *
 * On screen the map is the viewport transform (zoom, aspect stretch, scroll,
 * centring offset) and uiScale is 1, so these functions draw exactly what they
 * drew when they lived in overlay_render_controller.js. For an export the map
 * is the crop region scaled to the output size, and uiScale grows the
 * decorations so a ring is still visible in a 4000 px wide GIF.
 *
 * Anisotropic pixels are handled once, here: the screen stretches the image by
 * `pixelAspect` so a resolution ring is a true circle, while an exported frame
 * is unstretched and the same ring is an ellipse. Both fall out of one formula
 * -- radiusY = radiusPx / pixelAspect * scaleY -- which reduces to radiusX when
 * scaleY already carries the aspect stretch.
 */

import {
  applyGeometryOverrides,
  braggTwoTheta,
  buildGeometryRingSegments,
  pickGeometryRingLabelPoint,
  wavelengthFromEnergy,
} from "./ring_geometry_utils.js";
import { canvasFont } from "./canvas_fonts.js";

// Translucent on screen: halos are meant to darken or lighten whatever is
// underneath rather than replace it.
export const SCREEN_OVERLAY_COLORS = Object.freeze({
  ringHalo: "rgba(255, 255, 255, 0.45)",
  ring: "rgba(20, 80, 170, 0.95)",
  ringActive: "rgba(90, 160, 255, 1)",
  labelBox: "rgba(10, 20, 40, 0.55)",
  labelHalo: "rgba(0, 0, 0, 0.7)",
  labelText: "rgba(230, 240, 255, 0.98)",
  centerGlowFill: "rgba(255, 65, 65, 0.16)",
  centerGlowLine: "rgba(255, 65, 65, 0.85)",
  centerHalo: "rgba(0, 0, 0, 0.72)",
  centerCross: "rgba(255, 65, 65, 0.96)",
  peakHalo: "rgba(8, 10, 14, 0.72)",
  peakRing: "rgba(255, 196, 110, 0.98)",
  selectedHalo: "rgba(18, 18, 18, 0.92)",
  selectedCrossHalo: "rgba(0, 0, 0, 0.8)",
  selectedRing: "rgba(72, 255, 105, 0.98)",
  externalHalo: "rgba(10, 10, 10, 0.68)",
  externalDefault: "#4aa3ff",
});

// A GIF frame has no alpha channel and a 256-entry table, so an exported
// overlay is drawn opaque: every colour below is one palette entry, and an
// anti-aliased edge either reaches a pixel or does not. The alternative --
// blending each halo with the frame underneath -- would need a palette entry
// per (overlay colour, image colour) pair, which does not fit.
export const OPAQUE_OVERLAY_COLORS = Object.freeze({
  ringHalo: "rgb(255, 255, 255)",
  ring: "rgb(20, 80, 170)",
  ringActive: "rgb(20, 80, 170)",
  labelBox: "rgb(0, 0, 0)",
  labelHalo: "rgb(0, 0, 0)",
  labelText: "rgb(255, 255, 255)",
  centerGlowFill: "rgb(255, 65, 65)",
  centerGlowLine: "rgb(255, 65, 65)",
  centerHalo: "rgb(0, 0, 0)",
  centerCross: "rgb(255, 65, 65)",
  peakHalo: "rgb(0, 0, 0)",
  peakRing: "rgb(255, 196, 110)",
  selectedHalo: "rgb(0, 0, 0)",
  selectedCrossHalo: "rgb(0, 0, 0)",
  selectedRing: "rgb(72, 255, 105)",
  externalHalo: "rgb(0, 0, 0)",
  externalDefault: "rgb(74, 163, 255)",
});

// The distinct colours OPAQUE_OVERLAY_COLORS can put on a pixel, in the order
// they are appended to an exported GIF's colour table. Keep in step with the
// values above: a colour drawn but missing here is quantised to the nearest
// entry that is present. Deliberately no near-duplicates -- two whites 25/255
// apart would spend a slot on a difference nobody can see and would make an
// anti-aliased edge land on either of them at random.
export const OPAQUE_OVERLAY_RGB = Object.freeze([
  [255, 255, 255],
  [20, 80, 170],
  [0, 0, 0],
  [255, 65, 65],
  [255, 196, 110],
  [72, 255, 105],
  [74, 163, 255],
]);

/** Nearest OPAQUE_OVERLAY_RGB entry to an (r,g,b), by squared distance. */
export function nearestOverlayColorIndex(r, g, b) {
  let best = 0;
  let bestDist = Infinity;
  for (let i = 0; i < OPAQUE_OVERLAY_RGB.length; i += 1) {
    const c = OPAQUE_OVERLAY_RGB[i];
    const dr = r - c[0];
    const dg = g - c[1];
    const db = b - c[2];
    const dist = dr * dr + dg * dg + db * db;
    if (dist < bestDist) {
      bestDist = dist;
      best = i;
    }
  }
  return best;
}

/** outX = imageX * view.scaleX + view.offsetX. */
export function viewX(view, imageX) {
  return imageX * view.scaleX + view.offsetX;
}

export function viewY(view, imageY) {
  return imageY * view.scaleY + view.offsetY;
}

/**
 * The viewport's image->screen map, in the form the painters take.
 * `scrollX`/`scrollY` are in screen pixels, as the scroll container reports.
 */
export function screenView({ zoom, zoomY, scrollX, scrollY, offsetX, offsetY }) {
  return {
    scaleX: zoom,
    scaleY: zoomY,
    offsetX: offsetX - scrollX,
    offsetY: offsetY - scrollY,
  };
}

// Overlay line widths and label text are sized for a viewport, so they scale
// with an export's output; without this a ring in a 4000 px wide image is a
// hairline. Clamped so a thumbnail keeps legible decorations and a huge
// export does not end up all label.
const OVERLAY_UI_REFERENCE_PX = 900;
const OVERLAY_UI_SCALE_MIN = 0.75;
const OVERLAY_UI_SCALE_MAX = 4;

/** Line width and label scale for an export of ow x oh pixels. */
export function overlayUiScale(ow, oh) {
  const raw = Math.min(ow, oh) / OVERLAY_UI_REFERENCE_PX;
  return Math.max(OVERLAY_UI_SCALE_MIN, Math.min(OVERLAY_UI_SCALE_MAX, raw));
}

/**
 * The ROI outline, as the viewer draws it, into an export.
 *
 * Same conventions as roi_interaction_controller's drawRoiOverlay: a line runs
 * between pixel centres, a box covers its pixels inclusively, and a circle or
 * annulus is centred on `start` with its radius in X-pixel units, so it stays
 * round when the pixels are not square.
 */
export function paintRoiOutline(ctx, { roi, view, outerRadius = 0, uiScale = 1 } = {}) {
  if (!ctx || !roi || !roi.start || !roi.end || !view) return false;
  const x0 = viewX(view, roi.start.x);
  const y0 = viewY(view, roi.start.y);
  const x1 = viewX(view, roi.end.x);
  const y1 = viewY(view, roi.end.y);
  ctx.save();
  ctx.setLineDash([6 * uiScale, 4 * uiScale]);
  ctx.lineJoin = "round";
  ctx.lineCap = "round";
  const strokeWithHalo = () => {
    ctx.lineWidth = 4 * uiScale;
    ctx.strokeStyle = "rgba(0, 0, 0, 0.7)";
    ctx.stroke();
    ctx.lineWidth = 2 * uiScale;
    ctx.strokeStyle = "rgba(255, 255, 255, 0.95)";
    ctx.stroke();
  };
  const fillFaint = (path) => {
    ctx.save();
    ctx.setLineDash([]);
    ctx.fillStyle = "rgba(160, 160, 160, 0.08)";
    path();
    ctx.restore();
  };
  if (roi.mode === "line") {
    ctx.beginPath();
    ctx.moveTo(x0 + view.scaleX * 0.5, y0 + view.scaleY * 0.5);
    ctx.lineTo(x1 + view.scaleX * 0.5, y1 + view.scaleY * 0.5);
    strokeWithHalo();
  } else if (roi.mode === "box") {
    const left = Math.min(x0, x1);
    const top = Math.min(y0, y1);
    const width = Math.max(x0, x1) + view.scaleX - left;
    const height = Math.max(y0, y1) + view.scaleY - top;
    fillFaint(() => ctx.fillRect(left, top, width, height));
    ctx.beginPath();
    ctx.rect(left, top, width, height);
    strokeWithHalo();
  } else if (roi.mode === "circle" || roi.mode === "annulus") {
    const radius = outerRadius * view.scaleX;
    const inner = (roi.mode === "annulus" ? Number(roi.innerRadius) || 0 : 0) * view.scaleX;
    if (!(radius > 0)) {
      ctx.restore();
      return false;
    }
    fillFaint(() => {
      ctx.beginPath();
      ctx.arc(x0, y0, radius, 0, Math.PI * 2);
      if (inner > 0) {
        ctx.moveTo(x0 + inner, y0);
        ctx.arc(x0, y0, inner, 0, Math.PI * 2);
        ctx.fill("evenodd");
      } else {
        ctx.fill();
      }
    });
    ctx.beginPath();
    ctx.arc(x0, y0, radius, 0, Math.PI * 2);
    strokeWithHalo();
    if (inner > 0) {
      ctx.beginPath();
      ctx.arc(x0, y0, inner, 0, Math.PI * 2);
      strokeWithHalo();
    }
  }
  ctx.restore();
  return true;
}

/** The exporter's image->output map for a crop region drawn at ow x oh. */
export function regionView(region, ow, oh) {
  const scaleX = ow / Math.max(1, region.width);
  const scaleY = oh / Math.max(1, region.height);
  return {
    scaleX,
    scaleY,
    offsetX: -region.x * scaleX,
    offsetY: -region.y * scaleY,
  };
}

// Peak markers scale with zoom so they track a spot's image footprint instead
// of staying a fixed (tiny) screen size when zoomed into individual pixels.
// footprintPx is the marker's radius in image pixels; clamped to output bounds
// so markers stay visible when zoomed out and don't get absurd when zoomed in.
export function peakMarkerRadius(zoom, footprintPx, minPx, maxPx) {
  const z = Number.isFinite(zoom) && zoom > 0 ? zoom : 1;
  return Math.max(minPx, Math.min(maxPx, footprintPx * z));
}

// Marker INK thins as markers multiply; marker RADIUS deliberately does not.
// The radius is the only thing on screen saying how big the spot is, so it has
// to keep tracking the footprint. What has to give at a hundred markers is the
// weight of the stroke: each one carries a dark halo so it reads on a light or
// dark frame, and at that density the halos merge into a wash over the very
// pattern they are annotating.
//
// Full weight to 25 markers, then a taper that reaches its floor around 138.
// Note the *shipped* default is 100 (index.html's peaks-count input, which
// overrides the 25 in state.js), so a default session renders at about 0.70 --
// that is the density this exists for, not an edge case.
const MARKER_INK_FULL_UPTO = 25;
const MARKER_INK_FLOOR = 0.55;
const MARKER_INK_TAPER = 0.004;
// Thinning is a SCREEN affordance and is switched off for an export. A GIF has
// no partial alpha: the exporter keeps a pixel only where the overlay's
// coverage clears a hard threshold, so a thinned stroke stops depositing its
// own colour reliably and the marker quantises to whatever palette entry is
// nearest. And the reason to thin does not apply there anyway -- merging halos
// spoil a view you are actively scrutinising and panning; a GIF is a fixed
// artefact where fidelity matters more than legibility under the cursor.
export function markerInkScale(count) {
  const n = Number(count);
  if (!Number.isFinite(n) || n <= MARKER_INK_FULL_UPTO) return 1;
  return Math.max(MARKER_INK_FLOOR, 1 - (n - MARKER_INK_FULL_UPTO) * MARKER_INK_TAPER);
}

/** Ring d-spacings, resolved to screen-space segments once per geometry. */
export function buildGeometryRingCache(params) {
  const cache = new Map();
  if (!params || params.mode !== "geometry" || !params.geometry) return cache;
  if (!Array.isArray(params.rings)) return cache;
  const adjustedGeometry = applyGeometryOverrides(params.geometry, {
    centerX: params.centerX,
    centerY: params.centerY,
    distanceMm: params.distanceMm,
  });
  params.rings.forEach((dSpacing) => {
    const segments = buildGeometryRingSegments({
      geometry: adjustedGeometry,
      energyEv: params.energyEv,
      dSpacing,
    });
    cache.set(dSpacing, { segments, labelPoint: pickGeometryRingLabelPoint(segments) });
  });
  return cache;
}

function ringLabelsOverlap(a, b) {
  return a.x < b.x + b.w && a.x + a.w > b.x && a.y < b.y + b.h && a.y + a.h > b.y;
}

/**
 * Places ring labels without letting them stack on the shared radial ray.
 * One placer per overlay pass; it accumulates the boxes already taken.
 */
function createRingLabelPlacer(ctx, colors, uiScale) {
  // HARD boxes veto a position outright: sibling labels, and the beam-centre
  // marker. Two labels on top of each other are both unreadable, so there is
  // nothing to gain by drawing anyway.
  const hard = [];
  // SOFT boxes are a preference. A label with nowhere clear of them is still
  // drawn, because an unlabelled ring is worse than an overlapping label: it
  // sits next to labelled ones and invites reading the wrong d-spacing off the
  // wrong ring, and that number ends up in a figure caption. The ROI is soft
  // for exactly this reason -- it is routinely concentric with the rings (there
  // is a one-click "snap to beam centre" for circular ROIs), and a concentric
  // ROI encloses whole rings, so a hard veto silently deleted every label
  // inside it however far the label tried to slide.
  const soft = [];
  const fontSize = 14 * uiScale;
  const padX = 6 * uiScale;
  const padY = 3 * uiScale;

  function reserve(box, { soft: isSoft = false } = {}) {
    (isSoft ? soft : hard).push(box);
  }

  function draw(screenPoint, label, direction) {
    if (!screenPoint || !label) return;
    const textWidth = ctx.measureText(label).width;
    const boxW = textWidth + padX * 2;
    const boxH = fontSize + padY * 2;
    // When a label collides, nudge it *tangentially* (along its own ring) rather
    // than radially: sliding sideways keeps it at the ring's radius, whereas a
    // radial nudge would fling a middle ring's label out past the outer ring and
    // leave it looking detached. Bias the slide upward (negative screen y) to
    // stay clear of the horizontal beamstop.
    let rx = direction?.x ?? 1;
    let ry = direction?.y ?? 0;
    const mag = Math.hypot(rx, ry) || 1;
    rx /= mag;
    ry /= mag;
    let tx = -ry;
    let ty = rx;
    if (ty > 0) {
      tx = -tx;
      ty = -ty;
    }
    const originX = screenPoint.x + 8 * uiScale;
    const originY = screenPoint.y;
    const step = boxH + 4 * uiScale;
    const maxSteps = 6;
    const boxAt = (bx, by) => ({ x: bx - padX, y: by - fontSize / 2 - padY, w: boxW, h: boxH });
    const hits = (box, list) => list.some((other) => ringLabelsOverlap(box, other));

    // Slide along the ring looking for a slot clear of everything; remember the
    // first slot that at least clears the hard boxes, to fall back on.
    let best = null;
    let fallback = null;
    for (let attempt = 0; attempt <= maxSteps; attempt += 1) {
      const bx = originX + tx * step * attempt;
      const by = originY + ty * step * attempt;
      const candidate = boxAt(bx, by);
      if (hits(candidate, hard)) continue;
      if (!fallback) fallback = { textX: bx, textY: by, box: candidate };
      if (!hits(candidate, soft)) {
        best = { textX: bx, textY: by, box: candidate };
        break;
      }
    }
    const chosen = best || fallback;
    if (!chosen) return; // every slot collides with another label or the centre
    const { textX, textY, box } = chosen;
    hard.push(box);
    ctx.fillStyle = colors.labelBox;
    ctx.fillRect(box.x, box.y, box.w, box.h);
    ctx.lineWidth = 3 * uiScale;
    ctx.strokeStyle = colors.labelHalo;
    ctx.strokeText(label, textX, textY);
    ctx.fillStyle = colors.labelText;
    ctx.fillText(label, textX, textY);
  }

  return { draw, reserve, fontSize };
}

function strokeRingPath(ctx, points, colors, uiScale) {
  if (!Array.isArray(points) || points.length < 2) return;
  ctx.beginPath();
  ctx.moveTo(points[0].x, points[0].y);
  for (let index = 1; index < points.length; index += 1) {
    ctx.lineTo(points[index].x, points[index].y);
  }
  ctx.lineWidth = 3.5 * uiScale;
  ctx.strokeStyle = colors.ringHalo;
  ctx.stroke();
  ctx.lineWidth = 2 * uiScale;
  ctx.strokeStyle = colors.ring;
  ctx.stroke();
}

function drawBeamCenterMarker(ctx, centerX, centerY, zoom, colors, uiScale, highlight) {
  if (!Number.isFinite(centerX) || !Number.isFinite(centerY)) return;
  const arm = Math.max(10, Math.min(22, 10 + Math.log2(Math.max(1, zoom)) * 4)) * uiScale;
  ctx.setLineDash([]);
  if (highlight) {
    ctx.beginPath();
    ctx.arc(centerX, centerY, arm + 4 * uiScale, 0, Math.PI * 2);
    ctx.fillStyle = colors.centerGlowFill;
    ctx.fill();
    ctx.lineWidth = 1.5 * uiScale;
    ctx.strokeStyle = colors.centerGlowLine;
    ctx.stroke();
  }
  const cross = () => {
    ctx.beginPath();
    ctx.moveTo(centerX - arm, centerY);
    ctx.lineTo(centerX + arm, centerY);
    ctx.moveTo(centerX, centerY - arm);
    ctx.lineTo(centerX, centerY + arm);
  };
  cross();
  ctx.lineWidth = 4 * uiScale;
  ctx.strokeStyle = colors.centerHalo;
  ctx.stroke();
  cross();
  ctx.lineWidth = 2.2 * uiScale;
  ctx.strokeStyle = colors.centerCross;
  ctx.stroke();
}

/**
 * Resolution rings, their d-spacing labels and the beam centre.
 *
 * Returns false when nothing was drawn (geometry incomplete), so a caller can
 * tell "no rings configured" from "rings drawn off-screen".
 */
export function paintResolutionRings(
  ctx,
  {
    params,
    view,
    geometryCache,
    pixelAspect = 1,
    uiScale = 1,
    colors = SCREEN_OVERLAY_COLORS,
    activeHandle = null,
    avoidImageRects = [],
  } = {}
) {
  if (!ctx || !params) return false;
  if (!params.energyEv) return false;
  if (params.mode !== "geometry" && (!params.distanceMm || !params.pixelSizeUm)) return false;
  if (!wavelengthFromEnergy(params.energyEv)) return false;

  // A ring list can be empty -- the beam-centre marker is still drawn, which
  // is how the viewer behaves when every ring field has been cleared.
  const rings = Array.isArray(params.rings) ? params.rings : [];
  const aspect = Number.isFinite(pixelAspect) && pixelAspect > 0 ? pixelAspect : 1;
  const centerX = viewX(view, params.centerX);
  const centerY = viewY(view, params.centerY);
  const centerActive = activeHandle?.type === "center";
  const isRingActive = (d) =>
    activeHandle?.type === "ring" &&
    Number.isFinite(activeHandle.d) &&
    Math.abs(activeHandle.d - d) <= Math.max(0.01, d * 0.005);

  ctx.save();
  ctx.setLineDash([6 * uiScale, 6 * uiScale]);
  ctx.lineJoin = "round";
  ctx.lineCap = "round";
  const labels = createRingLabelPlacer(ctx, colors, uiScale);
  ctx.font = canvasFont(labels.fontSize);
  ctx.textBaseline = "middle";
  // Reserve the beam-centre area so labels never land on top of the marker.
  if (Number.isFinite(centerX) && Number.isFinite(centerY)) {
    const reserve = 22 * uiScale;
    labels.reserve({ x: centerX - reserve, y: centerY - reserve, w: reserve * 2, h: reserve * 2 });
  }
  // Anything else on screen a label must keep off, given in image pixels so
  // the caller need not know the transform -- today the ROI, which is drawn on
  // its own canvas but shares the view. Inflated once transformed: a label
  // should not sit flush against the ROI's 4px stroke, and a horizontal line
  // ROI has zero height in image space and would otherwise reserve nothing.
  for (const rect of avoidImageRects) {
    if (!rect) continue;
    const width = Number(rect.width) || 0;
    const height = Number(rect.height) || 0;
    const ax = viewX(view, Number(rect.x));
    const ay = viewY(view, Number(rect.y));
    const bx = viewX(view, Number(rect.x) + width);
    const by = viewY(view, Number(rect.y) + height);
    if (![ax, ay, bx, by].every(Number.isFinite)) continue;
    const pad = 6 * uiScale;
    labels.reserve(
      {
        x: Math.min(ax, bx) - pad,
        y: Math.min(ay, by) - pad,
        w: Math.abs(bx - ax) + pad * 2,
        h: Math.abs(by - ay) + pad * 2,
      },
      { soft: true }
    );
  }

  if (params.mode === "geometry" && params.geometry) {
    const cache = geometryCache || buildGeometryRingCache(params);
    rings.forEach((d) => {
      const entry = cache.get(d);
      if (!entry || !Array.isArray(entry.segments) || !entry.segments.length) return;
      entry.segments.forEach((segment) => {
        strokeRingPath(
          ctx,
          segment.map((point) => ({ x: viewX(view, point.x), y: viewY(view, point.y) })),
          colors,
          uiScale
        );
      });
      const labelPoint = entry.labelPoint
        ? { x: viewX(view, entry.labelPoint.x), y: viewY(view, entry.labelPoint.y) }
        : null;
      const label = formatRingLabel(d);
      const labelDir = labelPoint
        ? { x: labelPoint.x - centerX, y: labelPoint.y - centerY }
        : null;
      labels.draw(labelPoint, label, labelDir);
    });
    if (params.centerKnown) {
      drawBeamCenterMarker(ctx, centerX, centerY, view.scaleX, colors, uiScale, centerActive);
    }
    ctx.restore();
    return true;
  }

  // params.pixelSizeUm is the X (reference) pixel size; the Y size is that
  // times pixelAspect, which is why the vertical radius divides by the aspect.
  const pixelSizeMm = params.pixelSizeUm / 1000;
  if (!Number.isFinite(pixelSizeMm) || pixelSizeMm <= 0) {
    ctx.restore();
    return false;
  }
  const labelAngle = -Math.PI / 6;
  rings.forEach((d) => {
    const twoTheta = braggTwoTheta(d, params.energyEv);
    if (twoTheta === null) return;
    const radiusPx = (params.distanceMm * Math.tan(twoTheta)) / pixelSizeMm;
    if (!Number.isFinite(radiusPx) || radiusPx <= 0) return;
    const radiusX = radiusPx * view.scaleX;
    const radiusY = (radiusPx / aspect) * view.scaleY;
    if (Math.max(radiusX, radiusY) < 5) return;
    const active = isRingActive(d);
    const ellipse = () => {
      ctx.beginPath();
      ctx.ellipse(centerX, centerY, radiusX, radiusY, 0, 0, Math.PI * 2);
    };
    ellipse();
    ctx.lineWidth = (active ? 4.5 : 3.5) * uiScale;
    ctx.strokeStyle = colors.ringHalo;
    ctx.stroke();
    ellipse();
    ctx.lineWidth = (active ? 3 : 2) * uiScale;
    ctx.strokeStyle = active ? colors.ringActive : colors.ring;
    ctx.stroke();

    labels.draw(
      { x: centerX + Math.cos(labelAngle) * radiusX, y: centerY + Math.sin(labelAngle) * radiusY },
      formatRingLabel(d),
      { x: Math.cos(labelAngle), y: Math.sin(labelAngle) }
    );
  });

  if (params.centerKnown) {
    drawBeamCenterMarker(ctx, centerX, centerY, view.scaleX, colors, uiScale, centerActive);
  }
  ctx.restore();
  return true;
}

function formatRingLabel(d) {
  return Number.isFinite(d) ? `${d.toFixed(2).replace(/\.00$/, "")} Å` : "Å";
}

/**
 * Spot-finder markers, plus any externally supplied peak sets (jfjoch).
 * `selectedPeaks` is a viewer-only notion; an exporter passes none.
 */
export function paintPeakMarkers(
  ctx,
  {
    peaks = [],
    selectedPeaks = [],
    externalPeakSets = [],
    view,
    width,
    height,
    uiScale = 1,
    colors = SCREEN_OVERLAY_COLORS,
    thinWithDensity = true,
  } = {}
) {
  if (!ctx) return;
  const zoom = view.scaleX;
  // Markers are culled generously: a marker whose centre is just outside the
  // output still has a visible arc inside it.
  const margin = 20 * uiScale;
  const offCanvas = (sx, sy) =>
    sx < -margin || sy < -margin || sx > width + margin || sy > height + margin;

  externalPeakSets.forEach((set) => {
    const color = typeof set?.color === "string" && set.color ? set.color : colors.externalDefault;
    const style = typeof set?.style === "string" ? set.style : "";
    const jfjochSet = style === "jfjoch-indexed" || style === "jfjoch-unindexed";
    const points = Array.isArray(set?.points) ? set.points : [];
    // Each set thins on its own count -- a jfjoch frame can carry hundreds.
    // The TOTAL count, deliberately, not the number currently on canvas: a
    // cull-dependent width would make every marker breathe as you pan.
    const ink = thinWithDensity ? markerInkScale(points.length) : 1;
    const radius = jfjochSet
      ? peakMarkerRadius(zoom, 3.0, 8 * uiScale, 80 * uiScale)
      : peakMarkerRadius(zoom, 2.6, 7 * uiScale, 70 * uiScale);
    points.forEach((peak) => {
      const px = Number(peak?.x);
      const py = Number(peak?.y);
      if (!Number.isFinite(px) || !Number.isFinite(py)) return;
      const sx = viewX(view, px + 0.5);
      const sy = viewY(view, py + 0.5);
      if (offCanvas(sx, sy)) return;

      if (jfjochSet) {
        ctx.setLineDash([]);
        ctx.beginPath();
        ctx.arc(sx, sy, radius, 0, Math.PI * 2);
        ctx.lineWidth = 2.8 * uiScale * ink;
        ctx.strokeStyle = colors.externalHalo;
        ctx.stroke();

        ctx.beginPath();
        ctx.arc(sx, sy, Math.max(3 * uiScale, radius - 2 * uiScale), 0, Math.PI * 2);
        ctx.lineWidth = 1.8 * uiScale * ink;
        ctx.strokeStyle = color;
        ctx.stroke();

        const cross = radius + 3 * uiScale;
        ctx.beginPath();
        ctx.moveTo(sx - cross, sy);
        ctx.lineTo(sx + cross, sy);
        ctx.moveTo(sx, sy - cross);
        ctx.lineTo(sx, sy + cross);
        ctx.lineWidth = 1.6 * uiScale * ink;
        ctx.strokeStyle = color;
        ctx.stroke();
      } else {
        ctx.setLineDash([4 * uiScale, 3 * uiScale]);
        ctx.beginPath();
        ctx.arc(sx, sy, radius, 0, Math.PI * 2);
        ctx.lineWidth = 2.4 * uiScale * ink;
        ctx.strokeStyle = colors.externalHalo;
        ctx.stroke();

        ctx.beginPath();
        ctx.arc(sx, sy, Math.max(3 * uiScale, radius - 1.5 * uiScale), 0, Math.PI * 2);
        ctx.lineWidth = 1.35 * uiScale * ink;
        ctx.strokeStyle = color;
        ctx.stroke();
      }
    });
  });

  if (!Array.isArray(peaks) || !peaks.length) {
    ctx.setLineDash([]);
    return;
  }

  // Selected markers keep full weight: there are a handful at most, and the
  // point of the selection is that it stands out from everything around it.
  // Keyed on the total count rather than the visible one, so zooming into a
  // corner does not re-weight every marker as they leave the viewport.
  const ink = thinWithDensity ? markerInkScale(peaks.length) : 1;

  peaks.forEach((peak, index) => {
    const sx = viewX(view, peak.x + 0.5);
    const sy = viewY(view, peak.y + 0.5);
    if (offCanvas(sx, sy)) return;
    const selected = selectedPeaks.includes(index);
    const radius = selected
      ? peakMarkerRadius(zoom, 4.5, 16 * uiScale, 120 * uiScale)
      : peakMarkerRadius(zoom, 3.0, 9 * uiScale, 90 * uiScale);

    ctx.setLineDash([]);
    if (selected) {
      ctx.beginPath();
      ctx.arc(sx, sy, radius, 0, Math.PI * 2);
      ctx.lineWidth = 3.8 * uiScale;
      ctx.strokeStyle = colors.selectedHalo;
      ctx.stroke();

      ctx.beginPath();
      ctx.arc(sx, sy, radius - 1.5 * uiScale, 0, Math.PI * 2);
      ctx.lineWidth = 2.6 * uiScale;
      ctx.strokeStyle = colors.selectedRing;
      ctx.stroke();

      const cross = radius + 5 * uiScale;
      const arms = () => {
        ctx.beginPath();
        ctx.moveTo(sx - cross, sy);
        ctx.lineTo(sx + cross, sy);
        ctx.moveTo(sx, sy - cross);
        ctx.lineTo(sx, sy + cross);
      };
      arms();
      ctx.lineWidth = 5.2 * uiScale;
      ctx.strokeStyle = colors.selectedCrossHalo;
      ctx.stroke();
      arms();
      ctx.lineWidth = 2.8 * uiScale;
      ctx.strokeStyle = colors.selectedRing;
      ctx.stroke();
    } else {
      // Dark halo first so the marker stays readable on light or dark frames.
      // Half of it is absolute: thinning the halo in step with the ring would
      // spend the very contrast margin the halo exists to provide, so the
      // proportional part shrinks and a fixed fringe always remains. Identical
      // to the old 3.4 at full ink.
      ctx.beginPath();
      ctx.arc(sx, sy, radius, 0, Math.PI * 2);
      ctx.lineWidth = 1.7 * uiScale * ink + 1.7 * uiScale;
      ctx.strokeStyle = colors.peakHalo;
      ctx.stroke();

      // Amber ring, matching the ROI peak accent (PLOT_THEME.peak).
      ctx.beginPath();
      ctx.arc(sx, sy, radius, 0, Math.PI * 2);
      ctx.lineWidth = 1.7 * uiScale * ink;
      ctx.strokeStyle = colors.peakRing;
      ctx.stroke();
    }
  });
  ctx.setLineDash([]);
}
