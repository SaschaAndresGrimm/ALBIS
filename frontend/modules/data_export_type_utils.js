/**
 * The data types of Convert Dataset: what a source holds, and what an
 * "Automatic" export writes from it. No DOM here.
 */

const DTYPE_OF_ARRAY = new Map([
  [Uint8Array, "<u1"],
  [Uint16Array, "<u2"],
  [Uint32Array, "<u4"],
  [Int8Array, "<i1"],
  [Int16Array, "<i2"],
  [Int32Array, "<i4"],
  [Float32Array, "<f4"],
  [Float64Array, "<f8"],
]);

// A live frame's dtype: as declared, else read off its typed array.
export function liveFrameDtype(entry) {
  const declared = String(entry?.dtype || "");
  if (declared) return declared;
  return DTYPE_OF_ARRAY.get(entry?.data?.constructor) || "";
}

// Bits per value of a dtype string: "<u4" -> 32, "<f8" -> 64.
export function dtypeBits(dtype) {
  const match = /^[<>|=]?[uif](\d+)$/.exec(String(dtype || ""));
  return match ? Number(match[1]) * 8 : null;
}

// What "Automatic" writes, as the backend's auto_float_type decides it: a
// CBF and an integer source signed 32-bit, a flatfielded TIFF float32, a
// floating-point source its own float type, never rounded.
export function autoExportType(dtype, flatfielded, format) {
  if (String(format || "tiff").toLowerCase() === "cbf") return "signed32";
  if (flatfielded) return "float32";
  if (!/^[<>|=]?f\d+$/.test(String(dtype || ""))) return "signed32";
  return dtypeBits(dtype) > 32 ? "float64" : "float32";
}
