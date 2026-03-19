const TWO_PI = Math.PI * 2;
const EPSILON = 1e-9;
const MAX_SEGMENT_GAP_PX = 48;
const INCIDENT_BEAM = [0, 0, 1];

function dot(a, b) {
  return a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
}

function cross(a, b) {
  return [
    a[1] * b[2] - a[2] * b[1],
    a[2] * b[0] - a[0] * b[2],
    a[0] * b[1] - a[1] * b[0],
  ];
}

function norm(v) {
  return Math.hypot(v[0], v[1], v[2]);
}

function normalize(v) {
  const length = norm(v);
  if (!Number.isFinite(length) || length <= EPSILON) return null;
  return [v[0] / length, v[1] / length, v[2] / length];
}

function scale(v, factor) {
  return [v[0] * factor, v[1] * factor, v[2] * factor];
}

function add(a, b) {
  return [a[0] + b[0], a[1] + b[1], a[2] + b[2]];
}

function sub(a, b) {
  return [a[0] - b[0], a[1] - b[1], a[2] - b[2]];
}

function toFiniteVector(value, size) {
  if (!Array.isArray(value) || value.length !== size) return null;
  const out = value.map((item) => Number(item));
  return out.every((item) => Number.isFinite(item)) ? out : null;
}

function toPositiveSize(value) {
  const out = toFiniteVector(value, 2);
  if (!out) return null;
  return out.every((item) => item > 0) ? out : null;
}

function toIntVector(value) {
  const out = toFiniteVector(value, 2);
  if (!out) return null;
  return out.map((item) => Math.round(item));
}

function preparePanel(panel) {
  if (!panel || typeof panel !== "object") return null;
  const origin = toFiniteVector(panel.origin_mm, 3);
  const fast = normalize(toFiniteVector(panel.fast_axis, 3) || []);
  const slow = normalize(toFiniteVector(panel.slow_axis, 3) || []);
  const pixelSize = toPositiveSize(panel.pixel_size_mm);
  const imageSize = toIntVector(panel.image_size_px);
  const rawOffset = toIntVector(panel.raw_offset_px);
  if (!origin || !fast || !slow || !pixelSize || !imageSize || !rawOffset) return null;
  const normal = normalize(cross(fast, slow));
  if (!normal) return null;
  return {
    name: String(panel.name || ""),
    origin_mm: origin,
    fast_axis: fast,
    slow_axis: slow,
    normal,
    pixel_size_mm: pixelSize,
    image_size_px: imageSize,
    raw_offset_px: rawOffset,
  };
}

export function prepareRingGeometry(payload) {
  if (!payload || typeof payload !== "object") return null;
  if (String(payload.mode || "") !== "geometry") return null;
  const panels = Array.isArray(payload.panels)
    ? payload.panels.map((panel) => preparePanel(panel)).filter(Boolean)
    : [];
  if (!panels.length) return null;
  const prepared = {
    mode: "geometry",
    detector: String(payload.detector || ""),
    source: String(payload.source || ""),
    panels,
  };
  const reference = getGeometryReferencePose(prepared);
  return {
    ...prepared,
    reference_center_x_px: reference?.centerX ?? null,
    reference_center_y_px: reference?.centerY ?? null,
    reference_distance_mm: reference?.distanceMm ?? null,
  };
}

function wavelengthFromEnergy(energyEv) {
  const value = Number(energyEv);
  if (!Number.isFinite(value) || value <= 0) return null;
  return 12398.4193 / value;
}

function twoThetaFromDSpacing(dSpacing, energyEv) {
  const dValue = Number(dSpacing);
  if (!Number.isFinite(dValue) || dValue <= 0) return null;
  const lambda = wavelengthFromEnergy(energyEv);
  if (!lambda) return null;
  const sinArg = lambda / (2 * dValue);
  if (!Number.isFinite(sinArg) || sinArg <= 0 || sinArg >= 1) return null;
  return 2 * Math.asin(sinArg);
}

function ringRay(twoTheta, azimuth) {
  const sinTheta = Math.sin(twoTheta);
  return [
    sinTheta * Math.cos(azimuth),
    sinTheta * Math.sin(azimuth),
    Math.cos(twoTheta),
  ];
}

function intersectRayWithPanel(ray, panel) {
  const denom = dot(ray, panel.normal);
  if (!Number.isFinite(denom) || Math.abs(denom) <= EPSILON) return null;
  const t = dot(panel.origin_mm, panel.normal) / denom;
  if (!Number.isFinite(t) || t <= 0) return null;
  const point = scale(ray, t);
  const rel = sub(point, panel.origin_mm);
  const localFastMm = dot(rel, panel.fast_axis);
  const localSlowMm = dot(rel, panel.slow_axis);
  const imageX = localFastMm / panel.pixel_size_mm[0] - 0.5;
  const imageY = localSlowMm / panel.pixel_size_mm[1] - 0.5;
  if (
    imageX < -0.5 - EPSILON ||
    imageY < -0.5 - EPSILON ||
    imageX > panel.image_size_px[0] - 0.5 + EPSILON ||
    imageY > panel.image_size_px[1] - 0.5 + EPSILON
  ) {
    return null;
  }
  return {
    panelName: panel.name,
    distance: t,
    imageX: panel.raw_offset_px[0] + imageX,
    imageY: panel.raw_offset_px[1] + imageY,
  };
}

function findPanelRayHit(ray, geometry) {
  if (!geometry || !Array.isArray(geometry.panels)) return null;
  let best = null;
  geometry.panels.forEach((panel) => {
    const hit = intersectRayWithPanel(ray, panel);
    if (!hit) return;
    if (!best || hit.distance < best.distance) {
      best = hit;
    }
  });
  return best;
}

export function getGeometryReferencePose(geometry) {
  if (!geometry || !Array.isArray(geometry.panels) || !geometry.panels.length) return null;
  if (
    Number.isFinite(geometry.reference_center_x_px) &&
    Number.isFinite(geometry.reference_center_y_px) &&
    Number.isFinite(geometry.reference_distance_mm)
  ) {
    return {
      centerX: geometry.reference_center_x_px,
      centerY: geometry.reference_center_y_px,
      distanceMm: geometry.reference_distance_mm,
    };
  }
  const hit = findPanelRayHit(INCIDENT_BEAM, geometry);
  if (!hit) return null;
  return {
    centerX: hit.imageX,
    centerY: hit.imageY,
    distanceMm: hit.distance,
  };
}

export function applyGeometryOverrides(geometry, overrides = {}) {
  if (!geometry || !Array.isArray(geometry.panels) || !geometry.panels.length) return geometry;
  const reference = getGeometryReferencePose(geometry);
  if (!reference) return geometry;

  const targetCenterX = Number(overrides.centerX);
  const targetCenterY = Number(overrides.centerY);
  const targetDistanceMm = Number(overrides.distanceMm);
  const shiftX = Number.isFinite(targetCenterX) ? targetCenterX - reference.centerX : 0;
  const shiftY = Number.isFinite(targetCenterY) ? targetCenterY - reference.centerY : 0;
  const shiftDistanceMm = Number.isFinite(targetDistanceMm) && targetDistanceMm > 0
    ? targetDistanceMm - reference.distanceMm
    : 0;

  if (
    Math.abs(shiftX) <= EPSILON &&
    Math.abs(shiftY) <= EPSILON &&
    Math.abs(shiftDistanceMm) <= EPSILON
  ) {
    return geometry;
  }

  return {
    ...geometry,
    panels: geometry.panels.map((panel) => ({
      ...panel,
      origin_mm: [
        panel.origin_mm[0],
        panel.origin_mm[1],
        panel.origin_mm[2] + shiftDistanceMm,
      ],
      raw_offset_px: [
        panel.raw_offset_px[0] + shiftX,
        panel.raw_offset_px[1] + shiftY,
      ],
    })),
    reference_center_x_px: Number.isFinite(targetCenterX) ? targetCenterX : reference.centerX,
    reference_center_y_px: Number.isFinite(targetCenterY) ? targetCenterY : reference.centerY,
    reference_distance_mm: Number.isFinite(targetDistanceMm) && targetDistanceMm > 0
      ? targetDistanceMm
      : reference.distanceMm,
  };
}

function flushSegment(target, points) {
  if (Array.isArray(points) && points.length >= 2) {
    target.push(points.slice());
  }
  return [];
}

export function buildGeometryRingSegments({
  geometry,
  energyEv,
  dSpacing,
  sampleCount = 720,
}) {
  const twoTheta = twoThetaFromDSpacing(dSpacing, energyEv);
  if (!twoTheta || !geometry || !Array.isArray(geometry.panels) || !geometry.panels.length) {
    return [];
  }
  const steps = Math.max(90, Math.round(Number(sampleCount) || 720));
  const segments = [];
  let current = [];
  let currentPanel = "";
  let previousPoint = null;

  for (let index = 0; index <= steps; index += 1) {
    const azimuth = (index / steps) * TWO_PI;
    const hit = findPanelRayHit(ringRay(twoTheta, azimuth), geometry);
    if (!hit) {
      current = flushSegment(segments, current);
      currentPanel = "";
      previousPoint = null;
      continue;
    }
    const point = { x: hit.imageX, y: hit.imageY };
    const jump = previousPoint ? Math.hypot(point.x - previousPoint.x, point.y - previousPoint.y) : 0;
    const shouldSplit = currentPanel && (currentPanel !== hit.panelName || jump > MAX_SEGMENT_GAP_PX);
    if (shouldSplit) {
      current = flushSegment(segments, current);
    }
    if (!current.length) {
      currentPanel = hit.panelName;
    }
    current.push(point);
    previousPoint = point;
  }

  flushSegment(segments, current);
  return segments;
}

function polylineLength(segment) {
  if (!Array.isArray(segment) || segment.length < 2) return 0;
  let total = 0;
  for (let index = 1; index < segment.length; index += 1) {
    const prev = segment[index - 1];
    const next = segment[index];
    total += Math.hypot(next.x - prev.x, next.y - prev.y);
  }
  return total;
}

export function pickGeometryRingLabelPoint(segments) {
  if (!Array.isArray(segments) || !segments.length) return null;
  let best = null;
  let bestLength = 0;
  segments.forEach((segment) => {
    const length = polylineLength(segment);
    if (length > bestLength) {
      best = segment;
      bestLength = length;
    }
  });
  if (!best || !best.length) return null;
  return best[Math.floor(best.length / 2)];
}

export function getGeometryResolutionAtPixel(ix, iy, geometry, energyEv) {
  if (!Number.isFinite(ix) || !Number.isFinite(iy) || !geometry || !Array.isArray(geometry.panels)) {
    return null;
  }
  const lambda = wavelengthFromEnergy(energyEv);
  if (!lambda) return null;
  const panel = geometry.panels.find((item) => (
    ix >= item.raw_offset_px[0] &&
    iy >= item.raw_offset_px[1] &&
    ix < item.raw_offset_px[0] + item.image_size_px[0] &&
    iy < item.raw_offset_px[1] + item.image_size_px[1]
  ));
  if (!panel) return null;
  const localFast = ix - panel.raw_offset_px[0] + 0.5;
  const localSlow = iy - panel.raw_offset_px[1] + 0.5;
  const point = add(
    add(
      panel.origin_mm,
      scale(panel.fast_axis, localFast * panel.pixel_size_mm[0]),
    ),
    scale(panel.slow_axis, localSlow * panel.pixel_size_mm[1]),
  );
  const distance = norm(point);
  if (!Number.isFinite(distance) || distance <= EPSILON) return null;
  const cosTwoTheta = Math.max(-1, Math.min(1, point[2] / distance));
  const twoTheta = Math.acos(cosTwoTheta);
  const sinArg = Math.sin(twoTheta / 2);
  if (!Number.isFinite(sinArg) || sinArg <= EPSILON) return null;
  const dSpacing = lambda / (2 * sinArg);
  return Number.isFinite(dSpacing) && dSpacing > 0 ? dSpacing : null;
}
