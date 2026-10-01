/**
 * The detector geometry ALBIS calculates with: what the data says, and what
 * the user says instead.
 *
 * Distance, pixel size, photon energy and beam centre drive the resolution
 * rings, the cursor's d readout, the d column of the peak table and the
 * geometry a series sum embeds. They come from three places, combined here and
 * nowhere else:
 *
 *   source     what the open file or live stream states, replaced per file and
 *              per frame (a live frame missing a field keeps the last one from
 *              the same stream);
 *   reference  the pose of a detector geometry (`.expt` or built-in), used for
 *              distance and beam centre where the source has none -- or always,
 *              when the geometry is a file the user chose, whose pose is the
 *              calibrated one;
 *   override   values the user entered, applied to every image until switched
 *              off and kept across sessions, for metadata that is missing or
 *              wrong at the source and cannot be fixed there.
 *
 * Before this, a typed value behaved three different ways depending on the
 * source, and on plain files it was silently replaced by the next frame's
 * header.
 */

export const GEOMETRY_KEYS = Object.freeze([
  "distanceMm",
  "pixelSizeXUm",
  "pixelSizeYUm",
  "energyEv",
  "centerX",
  "centerY",
]);

/** Must be > 0 to mean anything; a beam centre may be anywhere, even negative. */
const POSITIVE_KEYS = new Set(["distanceMm", "pixelSizeXUm", "pixelSizeYUm", "energyEv"]);
/** What a detector geometry's reference pose supplies. */
const POSE_KEYS = new Set(["distanceMm", "centerX", "centerY"]);

export const GEOMETRY_OVERRIDE_STORAGE_KEY = "albis.geometryOverride";
const STORAGE_VERSION = 1;

export function emptyGeometryValues() {
  return Object.fromEntries(GEOMETRY_KEYS.map((key) => [key, null]));
}

export function createGeometryOverride() {
  return { enabled: false, values: emptyGeometryValues(), geometryFile: "" };
}

/** A usable value for `key`, or null. null and "" stay null rather than becoming 0. */
export function geometryValueOrNull(key, value) {
  if (value === null || value === undefined || value === "" || typeof value === "boolean") return null;
  const number = Number(value);
  if (!Number.isFinite(number)) return null;
  if (POSITIVE_KEYS.has(key) && number <= 0) return null;
  return number;
}

export function sanitizeGeometryValues(raw) {
  const source = raw && typeof raw === "object" ? raw : {};
  return Object.fromEntries(GEOMETRY_KEYS.map((key) => [key, geometryValueOrNull(key, source[key])]));
}

export function sanitizeGeometryOverride(raw) {
  const source = raw && typeof raw === "object" ? raw : {};
  return {
    enabled: Boolean(source.enabled),
    values: sanitizeGeometryValues(source.values),
    geometryFile: typeof source.geometryFile === "string" ? source.geometryFile.trim() : "",
  };
}

/**
 * Combine source, reference pose and override into the values in effect.
 *
 * Returns, per key, the value in effect, where it came from ("override", the
 * source's own origin, "geometry", or "missing") and what the source alone
 * would have given -- shown next to an overridden field.
 */
export function resolveGeometryParams({
  source = {},
  sourceOrigin = "",
  reference = null,
  poseFromGeometry = false,
  override = null,
} = {}) {
  const values = {};
  const origins = {};
  const sourceValues = {};
  const sourceOrigins = {};
  for (const key of GEOMETRY_KEYS) {
    let value = geometryValueOrNull(key, source?.[key]);
    let origin = value !== null ? sourceOrigin || "source" : "missing";
    if (POSE_KEYS.has(key) && reference) {
      const pose = geometryValueOrNull(key, reference[key]);
      if (pose !== null && (poseFromGeometry || value === null)) {
        value = pose;
        origin = "geometry";
      }
    }
    sourceValues[key] = value;
    sourceOrigins[key] = origin;
  }

  const manual = override?.enabled ? sanitizeGeometryValues(override.values) : emptyGeometryValues();
  for (const key of GEOMETRY_KEYS) {
    if (manual[key] !== null) {
      values[key] = manual[key];
      origins[key] = "override";
    } else {
      values[key] = sourceValues[key];
      origins[key] = sourceOrigins[key];
    }
  }

  // One pixel size typed for a square-pixel source means both axes. Leaving Y
  // at the file's value would turn an X correction into a stretched image.
  // A source that really is non-square keeps its own Y.
  if (origins.pixelSizeXUm === "override" && origins.pixelSizeYUm !== "override") {
    const sourceX = sourceValues.pixelSizeXUm;
    const sourceY = sourceValues.pixelSizeYUm;
    const sourceIsSquare = sourceX === null || sourceY === null || sourceX === sourceY;
    if (sourceIsSquare) {
      values.pixelSizeYUm = values.pixelSizeXUm;
      origins.pixelSizeYUm = "override";
    }
  }

  return { values, origins, sourceValues, sourceOrigins };
}

/** Display aspect (Y/X) from the pixel sizes in effect; square when unknown. */
export function pixelAspectFrom(values) {
  const x = geometryValueOrNull("pixelSizeXUm", values?.pixelSizeXUm);
  const y = geometryValueOrNull("pixelSizeYUm", values?.pixelSizeYUm);
  return x !== null && y !== null ? y / x : 1;
}

function defaultStorage() {
  try {
    return globalThis.localStorage || null;
  } catch {
    return null;
  }
}

/** The override kept from an earlier session, or a fresh one. Never throws. */
export function loadGeometryOverride(storage = defaultStorage()) {
  try {
    const raw = storage?.getItem(GEOMETRY_OVERRIDE_STORAGE_KEY);
    if (!raw) return createGeometryOverride();
    const parsed = JSON.parse(raw);
    if (!parsed || parsed.v !== STORAGE_VERSION) return createGeometryOverride();
    return sanitizeGeometryOverride(parsed);
  } catch {
    return createGeometryOverride();
  }
}

/** Keep the override for later sessions and other windows. Never throws. */
export function saveGeometryOverride(override, storage = defaultStorage()) {
  try {
    storage?.setItem(
      GEOMETRY_OVERRIDE_STORAGE_KEY,
      JSON.stringify({ v: STORAGE_VERSION, ...sanitizeGeometryOverride(override) }),
    );
  } catch {
    // Private windows and full or blocked storage: the override still works
    // for this session, it just will not be remembered.
  }
}
