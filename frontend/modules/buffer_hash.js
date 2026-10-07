/**
 * A hash of every byte of a buffer, for telling live frames apart.
 *
 * FNV-1a's xor-and-multiply step, applied to each 32-bit word: the step is
 * invertible, so two buffers that differ in any one word always hash
 * differently, and for wider differences a collision is a 1-in-2^32 event.
 * An 18 MB EIGER2 4M frame takes about 6 ms, a 72 MB 16M about 22 ms -- small
 * next to fetching it.
 */
export function hashBuffer(buffer) {
  if (!buffer) return "";
  const bytes = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
  const len = bytes.length;
  const words = len >>> 2;
  let hash = 2166136261;
  if (words) {
    // A Uint32 view needs 4-byte alignment; an ArrayBuffer always has it.
    const view =
      bytes.byteOffset % 4 === 0
        ? new Uint32Array(bytes.buffer, bytes.byteOffset, words)
        : new Uint32Array(bytes.slice(0, words * 4).buffer);
    for (let i = 0; i < words; i += 1) {
      hash = Math.imul(hash ^ view[i], 16777619);
    }
  }
  for (let i = words * 4; i < len; i += 1) {
    hash = Math.imul(hash ^ bytes[i], 16777619);
  }
  return `${len}-${hash >>> 0}`;
}
