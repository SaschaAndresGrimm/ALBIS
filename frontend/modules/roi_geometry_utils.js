const EPSILON = 1e-9;
const DEFAULT_DIRECTION = { x: 1, y: 0 };

function normalizeDirection(direction) {
  const dx = Number(direction?.x);
  const dy = Number(direction?.y);
  const length = Math.hypot(dx, dy);
  if (!Number.isFinite(length) || length <= EPSILON) {
    return { ...DEFAULT_DIRECTION };
  }
  return { x: dx / length, y: dy / length };
}

function isPointInRect(point, rect) {
  if (!point || !rect) return false;
  return (
    point.x >= rect.left - EPSILON &&
    point.x <= rect.right + EPSILON &&
    point.y >= rect.top - EPSILON &&
    point.y <= rect.bottom + EPSILON
  );
}

export function getCircularRoiDirection(start, end) {
  const dx = Number(end?.x) - Number(start?.x);
  const dy = Number(end?.y) - Number(start?.y);
  return normalizeDirection({ x: dx, y: dy });
}

export function getCircularRoiOuterRadius(roiState) {
  const explicit = Number(roiState?.outerRadius);
  if (Number.isFinite(explicit) && explicit >= 0) {
    return Math.max(0, Math.round(explicit));
  }
  if (!roiState?.start || !roiState?.end) return 0;
  return Math.max(
    0,
    Math.round(Math.hypot(
      Number(roiState.end.x) - Number(roiState.start.x),
      Number(roiState.end.y) - Number(roiState.start.y),
    )),
  );
}

export function clampCircularRoiCenterDelta(dx, dy) {
  return { dx, dy };
}

export function clampCircularRoiInnerRadius(innerRadius, outerRadius) {
  const nextInner = Math.max(0, Math.round(Number(innerRadius) || 0));
  const nextOuter = Math.max(0, Math.round(Number(outerRadius) || 0));
  return Math.min(nextInner, nextOuter);
}

export function applyCircularRoiGeometry(roiState, center, outerRadius, direction = null) {
  if (!roiState || !center) return null;
  const nextCenter = {
    x: Math.round(Number(center.x) || 0),
    y: Math.round(Number(center.y) || 0),
  };
  const radius = Math.max(0, Math.round(Number(outerRadius) || 0));
  const unit = direction
    ? normalizeDirection(direction)
    : getCircularRoiDirection(roiState.start, roiState.end);
  roiState.start = nextCenter;
  roiState.end = {
    x: nextCenter.x + unit.x * radius,
    y: nextCenter.y + unit.y * radius,
  };
  roiState.outerRadius = radius;
  return {
    center: nextCenter,
    end: { ...roiState.end },
    outerRadius: radius,
    direction: unit,
  };
}

export function getVisibleCircularHandlePoint(center, radius, direction, visibleRect) {
  if (!center || !visibleRect) return null;
  const nextRadius = Math.max(0, Number(radius) || 0);
  if (!(nextRadius > EPSILON)) return null;
  const unit = direction ? normalizeDirection(direction) : { ...DEFAULT_DIRECTION };
  const preferred = {
    x: center.x + unit.x * nextRadius,
    y: center.y + unit.y * nextRadius,
  };
  if (isPointInRect(preferred, visibleRect)) {
    return preferred;
  }

  const candidates = [];
  const seen = new Set();
  const addCandidate = (x, y) => {
    if (!Number.isFinite(x) || !Number.isFinite(y)) return;
    const point = { x, y };
    if (!isPointInRect(point, visibleRect)) return;
    const key = `${Math.round(x * 1000)}:${Math.round(y * 1000)}`;
    if (seen.has(key)) return;
    seen.add(key);
    candidates.push(point);
  };

  const testVerticalEdge = (edgeX) => {
    const dx = edgeX - center.x;
    const rem = nextRadius * nextRadius - dx * dx;
    if (rem < -EPSILON) return;
    const dy = Math.sqrt(Math.max(0, rem));
    addCandidate(edgeX, center.y + dy);
    addCandidate(edgeX, center.y - dy);
  };

  const testHorizontalEdge = (edgeY) => {
    const dy = edgeY - center.y;
    const rem = nextRadius * nextRadius - dy * dy;
    if (rem < -EPSILON) return;
    const dx = Math.sqrt(Math.max(0, rem));
    addCandidate(center.x + dx, edgeY);
    addCandidate(center.x - dx, edgeY);
  };

  testVerticalEdge(visibleRect.left);
  testVerticalEdge(visibleRect.right);
  testHorizontalEdge(visibleRect.top);
  testHorizontalEdge(visibleRect.bottom);

  if (!candidates.length) return null;

  let best = candidates[0];
  let bestScore = Number.NEGATIVE_INFINITY;
  candidates.forEach((candidate) => {
    const score = (candidate.x - center.x) * unit.x + (candidate.y - center.y) * unit.y;
    if (score > bestScore) {
      best = candidate;
      bestScore = score;
    }
  });
  return best;
}
