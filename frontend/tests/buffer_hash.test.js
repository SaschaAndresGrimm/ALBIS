import { describe, expect, it } from "vitest";

import { hashBuffer } from "../modules/buffer_hash.js";

describe("hashBuffer", () => {
  it("is the same for the same bytes", () => {
    const a = new Uint32Array(1000).fill(7).buffer;
    const b = new Uint32Array(1000).fill(7).buffer;
    expect(hashBuffer(a)).toBe(hashBuffer(b));
  });

  it("changes when any one pixel changes, wherever it is", () => {
    // A sparse 4 MB frame: the sampled hash this replaces read one byte in
    // 2048 and missed a photon anywhere else.
    const base = new Uint32Array(1 << 20);
    const reference = hashBuffer(base.buffer);
    for (const index of [0, 1, 777, 2047, 524287, (1 << 20) - 1]) {
      const frame = base.slice();
      frame[index] = 1;
      expect(hashBuffer(frame.buffer)).not.toBe(reference);
    }
  });

  it("covers bytes past the last whole word, and unaligned views", () => {
    const bytes = new Uint8Array(11);
    const reference = hashBuffer(bytes);
    const tail = bytes.slice();
    tail[10] = 1;
    expect(hashBuffer(tail)).not.toBe(reference);
    const backing = new Uint8Array(12);
    backing.set(bytes, 1);
    expect(hashBuffer(backing.subarray(1))).toBe(reference);
  });
});
