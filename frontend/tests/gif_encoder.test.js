import { describe, expect, it } from "vitest";

import { GifWriter } from "../modules/gif_encoder.js";

// Independent GIF parser + LZW decoder, used purely to verify the encoder's
// output round-trips back to the original palette indices.
function decodeGif(u8) {
  let p = 6; // skip "GIF89a"
  const width = u8[p] | (u8[p + 1] << 8);
  p += 2;
  const height = u8[p] | (u8[p + 1] << 8);
  p += 2;
  const packed = u8[p++];
  const gctSize = 1 << ((packed & 0x07) + 1);
  p += 2; // background colour + aspect ratio
  const palette = [];
  for (let i = 0; i < gctSize; i += 1) {
    palette.push([u8[p], u8[p + 1], u8[p + 2]]);
    p += 3;
  }
  const frames = [];
  let loop = -1;
  while (p < u8.length) {
    const block = u8[p++];
    if (block === 0x3b) break; // trailer
    if (block === 0x21) {
      const label = u8[p++];
      let size = u8[p++];
      if (label === 0xff) {
        const app = String.fromCharCode(...u8.slice(p, p + size));
        p += size;
        let sub = u8[p++];
        while (sub !== 0) {
          if (app === "NETSCAPE2.0" && sub === 3) loop = u8[p + 1] | (u8[p + 2] << 8);
          p += sub;
          sub = u8[p++];
        }
      } else {
        while (size !== 0) {
          p += size;
          size = u8[p++];
        }
      }
      continue;
    }
    if (block === 0x2c) {
      p += 8; // left, top, width, height
      const imagePacked = u8[p++];
      expect(imagePacked & 0x80).toBe(0); // no local colour table expected
      const minCode = u8[p++];
      const data = [];
      let size = u8[p++];
      while (size !== 0) {
        for (let i = 0; i < size; i += 1) data.push(u8[p++]);
        size = u8[p++];
      }
      frames.push(lzwDecode(minCode, Uint8Array.from(data), width * height));
    }
  }
  return { width, height, palette, frames, loop };
}

function lzwDecode(minCode, data, pixelCount) {
  const clear = 1 << minCode;
  const eoi = clear + 1;
  let codeSize = minCode + 1;
  let dict = [];
  const reset = () => {
    dict = [];
    for (let i = 0; i < clear; i += 1) dict.push([i]);
    dict.push(null, null);
    codeSize = minCode + 1;
  };
  reset();
  const out = [];
  let bitPos = 0;
  const readCode = () => {
    let code = 0;
    for (let i = 0; i < codeSize; i += 1) {
      const byteIdx = bitPos >> 3;
      const bit = bitPos & 7;
      if (byteIdx >= data.length) return eoi;
      if (data[byteIdx] & (1 << bit)) code |= 1 << i;
      bitPos += 1;
    }
    return code;
  };
  let prev = null;
  for (;;) {
    const code = readCode();
    if (code === eoi) break;
    if (code === clear) {
      reset();
      prev = null;
      continue;
    }
    let entry;
    if (code < dict.length && dict[code]) entry = dict[code];
    else if (code === dict.length && prev) entry = [...prev, prev[0]];
    else throw new Error(`bad LZW code ${code}`);
    for (const value of entry) out.push(value);
    if (prev) {
      dict.push([...prev, entry[0]]);
      if (dict.length === 1 << codeSize && codeSize < 12) codeSize += 1;
    }
    prev = entry;
    if (out.length >= pixelCount) break;
  }
  return out;
}

describe("GifWriter", () => {
  it("produces a valid GIF89a container with a looping block", () => {
    const palette = new Uint8Array([0, 0, 0, 255, 0, 0, 0, 255, 0]);
    const gif = new GifWriter({ width: 2, height: 2, palette, loop: 0 });
    gif.addFrame(Uint8Array.from([0, 1, 2, 0]), 10);
    const bytes = gif.finish();

    expect(String.fromCharCode(...bytes.slice(0, 6))).toBe("GIF89a");
    expect(bytes[bytes.length - 1]).toBe(0x3b);
    const decoded = decodeGif(bytes);
    expect(decoded.loop).toBe(0);
  });

  it("round-trips multi-frame palette indices exactly", () => {
    const width = 7;
    const height = 5;
    const palette = new Uint8Array([0, 0, 0, 255, 0, 0, 0, 255, 0, 0, 0, 255, 255, 255, 0]);
    const frame = (shift) =>
      Uint8Array.from({ length: width * height }, (_, i) => (i + shift) % 5);
    const frames = [frame(0), frame(2), new Uint8Array(width * height).fill(3)];

    const gif = new GifWriter({ width, height, palette, loop: 0 });
    frames.forEach((f) => gif.addFrame(f, 8));
    const decoded = decodeGif(gif.finish());

    expect(decoded.width).toBe(width);
    expect(decoded.height).toBe(height);
    expect(decoded.frames).toHaveLength(frames.length);
    frames.forEach((f, idx) => {
      expect(decoded.frames[idx]).toEqual(Array.from(f));
    });
  });

  it("handles 256 colours with dictionary growth and clears (high entropy)", () => {
    const width = 120;
    const height = 80; // 9600 px > 4096 forces 12-bit codes and a table clear
    const palette = new Uint8Array(256 * 3);
    for (let i = 0; i < 256; i += 1) {
      palette[i * 3] = i;
      palette[i * 3 + 1] = (i * 5) & 0xff;
      palette[i * 3 + 2] = (255 - i) & 0xff;
    }
    const pixels = new Uint8Array(width * height);
    let seed = 99;
    for (let i = 0; i < pixels.length; i += 1) {
      seed = (seed * 1103515245 + 12345) & 0x7fffffff;
      pixels[i] = seed & 0xff;
    }
    const gif = new GifWriter({ width, height, palette, loop: -1 });
    gif.addFrame(pixels, 4);
    const decoded = decodeGif(gif.finish());

    expect(decoded.loop).toBe(-1); // no Netscape block written
    expect(decoded.frames[0]).toEqual(Array.from(pixels));
  });

  it("omits the looping block when loop is negative", () => {
    const palette = new Uint8Array([0, 0, 0, 1, 1, 1]);
    const gif = new GifWriter({ width: 1, height: 1, palette, loop: -1 });
    gif.addFrame(Uint8Array.from([1]), 5);
    const decoded = decodeGif(gif.finish());
    expect(decoded.loop).toBe(-1);
  });
});
