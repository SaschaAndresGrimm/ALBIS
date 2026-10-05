import zlib from "node:zlib";

import { describe, expect, it } from "vitest";

import {
  MAX_EXPORT_SIDE_PX,
  defaultExportScale,
  exportScaleOptions,
  exportSize,
  printSizeCm,
  readPngDpi,
  setPngDpi,
} from "../modules/image_export.js";

function chunk(type, data) {
  const out = Buffer.alloc(12 + data.length);
  out.writeUInt32BE(data.length, 0);
  out.write(type, 4, "ascii");
  Buffer.from(data).copy(out, 8);
  out.writeUInt32BE(zlib.crc32(out.subarray(4, 8 + data.length)), 8 + data.length);
  return out;
}

// A real 2 x 1 RGBA PNG, built the way a browser's encoder lays it out.
function tinyPng(extraChunks = []) {
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(2, 0);
  ihdr.writeUInt32BE(1, 4);
  ihdr[8] = 8;
  ihdr[9] = 6;
  const raw = Buffer.from([0, 255, 0, 0, 255, 0, 0, 255, 255]);
  return new Uint8Array(
    Buffer.concat([
      Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]),
      chunk("IHDR", ihdr),
      ...extraChunks,
      chunk("IDAT", zlib.deflateSync(raw)),
      chunk("IEND", Buffer.alloc(0)),
    ]),
  );
}

function chunks(png) {
  const buf = Buffer.from(png);
  const out = [];
  for (let at = 8; at < buf.length; ) {
    const length = buf.readUInt32BE(at);
    const type = buf.toString("ascii", at + 4, at + 8);
    const crc = buf.readUInt32BE(at + 8 + length);
    out.push({ type, crcOk: crc === zlib.crc32(buf.subarray(at + 4, at + 8 + length)) });
    at += 12 + length;
  }
  return out;
}

describe("export sizes", () => {
  const pollux = { x: 0, y: 0, width: 1544, height: 96 };

  it("enlarges by whole factors and applies the pixel aspect", () => {
    expect(exportSize(pollux, 2)).toEqual({ width: 3088, height: 192 });
    expect(exportSize({ x: 0, y: 0, width: 100, height: 50 }, 2, 3)).toEqual({ width: 200, height: 300 });
  });

  it("defaults to the smallest size that fills a slide", () => {
    // 1x is 1544 wide; 2x is the first at least 2000 px wide.
    expect(defaultExportScale(pollux)).toBe(2);
    expect(defaultExportScale({ x: 0, y: 0, width: 4148, height: 4362 })).toBe(1);
  });

  it("offers what a browser can make, and greys out what it cannot", () => {
    const eiger16m = exportScaleOptions({ x: 0, y: 0, width: 4148, height: 4362 });

    expect(eiger16m.map((option) => option.allowed)).toEqual([true, true, false, false]);
    eiger16m
      .filter((option) => option.allowed)
      .forEach((option) => expect(Math.max(option.width, option.height)).toBeLessThanOrEqual(MAX_EXPORT_SIDE_PX));
  });

  it("states the printed size at a resolution", () => {
    const size = printSizeCm(2000, 192, 300);
    expect(size.width).toBeCloseTo(16.93, 2);
    expect(printSizeCm(2000, 192, 0)).toBeNull();
  });
});

describe("the PNG's resolution", () => {
  it("is written as a valid pHYs chunk right after IHDR", () => {
    const out = setPngDpi(tinyPng(), 300);

    expect(chunks(out).map((c) => c.type)).toEqual(["IHDR", "pHYs", "IDAT", "IEND"]);
    expect(chunks(out).every((c) => c.crcOk)).toBe(true);
    expect(readPngDpi(out)).toBe(300);
  });

  it("replaces a resolution already there, and can remove it", () => {
    const stamped = setPngDpi(tinyPng(), 150);

    const restamped = setPngDpi(stamped, 600);
    expect(chunks(restamped).filter((c) => c.type === "pHYs")).toHaveLength(1);
    expect(readPngDpi(restamped)).toBe(600);

    const cleared = setPngDpi(stamped, 0);
    expect(chunks(cleared).map((c) => c.type)).toEqual(["IHDR", "IDAT", "IEND"]);
    expect(readPngDpi(cleared)).toBeNull();
  });

  it("leaves the image data untouched", () => {
    const before = tinyPng();
    const after = setPngDpi(before, 300);
    const idat = (png) => Buffer.from(png).subarray(Buffer.from(png).indexOf("IDAT") - 4);

    expect(Buffer.compare(idat(before), idat(after))).toBe(0);
  });

  it("returns bytes that are not a PNG unchanged", () => {
    const notPng = new Uint8Array([1, 2, 3, 4, 5]);
    expect(setPngDpi(notPng, 300)).toBe(notPng);
  });
});
