/**
 * Comparing two runs of `ui:snapshots`: which screenshots changed, by how much,
 * and where.
 *
 * Looking at every before/after pair by eye is the check that catches what
 * tests cannot, and also the one most easily skimmed -- by a person after the
 * tenth pair, or by an agent that opened the one window it changed. This
 * points at the pairs that differ, and at the pixels, so the looking goes
 * where it is needed. It does not decide whether a change is right.
 *
 * Self-contained, like the rest of ALBIS: PNG reading and writing on Node's
 * zlib, for the 8-bit, non-interlaced RGB(A) images Chrome writes.
 */

import { deflateSync, inflateSync } from "node:zlib";

const SIGNATURE = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
const CHANNELS = { 0: 1, 2: 3, 4: 2, 6: 4 }; // grey, RGB, grey+alpha, RGBA

const CRC_TABLE = Array.from({ length: 256 }, (_, n) => {
  let c = n;
  for (let k = 0; k < 8; k += 1) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
  return c >>> 0;
});

function crc32(bytes) {
  let c = 0xffffffff;
  for (const byte of bytes) c = CRC_TABLE[(c ^ byte) & 0xff] ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}

function paeth(a, b, c) {
  const p = a + b - c;
  const pa = Math.abs(p - a);
  const pb = Math.abs(p - b);
  const pc = Math.abs(p - c);
  if (pa <= pb && pa <= pc) return a;
  return pb <= pc ? b : c;
}

/** A PNG as `{ width, height, data }`, data RGBA with 4 bytes per pixel. */
export function decodePng(buffer) {
  if (!buffer.subarray(0, 8).equals(SIGNATURE)) throw new Error("not a PNG");
  let offset = 8;
  let header;
  const idat = [];
  while (offset < buffer.length) {
    const length = buffer.readUInt32BE(offset);
    const type = buffer.toString("latin1", offset + 4, offset + 8);
    const body = buffer.subarray(offset + 8, offset + 8 + length);
    if (type === "IHDR") {
      header = {
        width: body.readUInt32BE(0),
        height: body.readUInt32BE(4),
        bitDepth: body[8],
        colourType: body[9],
        interlace: body[12],
      };
    } else if (type === "IDAT") {
      idat.push(body);
    } else if (type === "IEND") {
      break;
    }
    offset += 12 + length;
  }
  if (!header) throw new Error("PNG without IHDR");
  const channels = CHANNELS[header.colourType];
  if (header.bitDepth !== 8 || !channels || header.interlace !== 0) {
    throw new Error(
      `unsupported PNG (bit depth ${header.bitDepth}, colour type ${header.colourType}, interlace ${header.interlace})`,
    );
  }

  const { width, height } = header;
  const raw = inflateSync(Buffer.concat(idat));
  const stride = width * channels;
  const pixels = Buffer.alloc(stride * height);
  for (let y = 0; y < height; y += 1) {
    const filter = raw[y * (stride + 1)];
    const line = raw.subarray(y * (stride + 1) + 1, (y + 1) * (stride + 1));
    const out = y * stride;
    for (let x = 0; x < stride; x += 1) {
      const left = x >= channels ? pixels[out + x - channels] : 0;
      const up = y > 0 ? pixels[out - stride + x] : 0;
      const upLeft = y > 0 && x >= channels ? pixels[out - stride + x - channels] : 0;
      const predictor = [0, left, up, (left + up) >> 1, paeth(left, up, upLeft)][filter];
      if (predictor === undefined) throw new Error(`bad PNG filter ${filter}`);
      pixels[out + x] = (line[x] + predictor) & 0xff;
    }
  }

  const data = Buffer.alloc(width * height * 4);
  for (let i = 0; i < width * height; i += 1) {
    const src = i * channels;
    const grey = channels <= 2;
    data[i * 4] = pixels[src];
    data[i * 4 + 1] = grey ? pixels[src] : pixels[src + 1];
    data[i * 4 + 2] = grey ? pixels[src] : pixels[src + 2];
    data[i * 4 + 3] = channels === 4 ? pixels[src + 3] : channels === 2 ? pixels[src + 1] : 255;
  }
  return { width, height, data };
}

function chunk(type, body) {
  const head = Buffer.alloc(8);
  head.writeUInt32BE(body.length, 0);
  head.write(type, 4, "latin1");
  const crc = Buffer.alloc(4);
  crc.writeUInt32BE(crc32(Buffer.concat([head.subarray(4), body])), 0);
  return Buffer.concat([head, body, crc]);
}

/** An RGBA image `{ width, height, data }` as PNG bytes. */
export function encodePng({ width, height, data }) {
  const header = Buffer.alloc(13);
  header.writeUInt32BE(width, 0);
  header.writeUInt32BE(height, 4);
  header[8] = 8;
  header[9] = 6;
  const raw = Buffer.alloc((width * 4 + 1) * height);
  for (let y = 0; y < height; y += 1) {
    data.copy(raw, y * (width * 4 + 1) + 1, y * width * 4, (y + 1) * width * 4);
  }
  return Buffer.concat([
    SIGNATURE,
    chunk("IHDR", header),
    chunk("IDAT", deflateSync(raw)),
    chunk("IEND", Buffer.alloc(0)),
  ]);
}

/**
 * How two images differ. A pixel counts as changed when any channel moves by
 * more than `tolerance`, so antialiasing jitter between runs does not.
 * `image` is the after picture dimmed, with changed pixels in magenta and the
 * box around them outlined; null when nothing changed. Images of different
 * sizes are compared over their common area and reported as resized.
 */
export function diffImages(before, after, { tolerance = 16 } = {}) {
  const width = Math.min(before.width, after.width);
  const height = Math.min(before.height, after.height);
  const resized = before.width !== after.width || before.height !== after.height;
  const out = Buffer.alloc(after.width * after.height * 4);
  let changed = 0;
  let box = null;

  for (let y = 0; y < after.height; y += 1) {
    for (let x = 0; x < after.width; x += 1) {
      const a = (y * after.width + x) * 4;
      let differs = x >= width || y >= height;
      if (!differs) {
        const b = (y * before.width + x) * 4;
        for (let c = 0; c < 4 && !differs; c += 1) {
          differs = Math.abs(before.data[b + c] - after.data[a + c]) > tolerance;
        }
      }
      if (differs) {
        changed += 1;
        box = box
          ? {
              left: Math.min(box.left, x),
              top: Math.min(box.top, y),
              right: Math.max(box.right, x),
              bottom: Math.max(box.bottom, y),
            }
          : { left: x, top: y, right: x, bottom: y };
        out.set([255, 0, 255, 255], a);
      } else {
        // The unchanged picture, faded towards white so the changes stand out.
        for (let c = 0; c < 3; c += 1) out[a + c] = 255 - ((255 - after.data[a + c]) >> 2);
        out[a + 3] = 255;
      }
    }
  }

  if (box) {
    for (let x = box.left; x <= box.right; x += 1) {
      for (const y of [box.top, box.bottom]) out.set([255, 0, 255, 255], (y * after.width + x) * 4);
    }
    for (let y = box.top; y <= box.bottom; y += 1) {
      for (const x of [box.left, box.right]) out.set([255, 0, 255, 255], (y * after.width + x) * 4);
    }
  }

  const total = after.width * after.height;
  return {
    changed,
    share: total ? changed / total : 0,
    resized,
    box,
    image: changed ? { width: after.width, height: after.height, data: out } : null,
  };
}

/**
 * Pairs two folders' file lists by name: in both, only before (a window that
 * no longer opens, or a renumbered shot), only after.
 */
export function pairSnapshots(beforeNames, afterNames) {
  const isPng = (name) => name.toLowerCase().endsWith(".png");
  const before = new Set(beforeNames.filter(isPng));
  const after = new Set(afterNames.filter(isPng));
  return {
    both: [...after].filter((name) => before.has(name)).sort(),
    onlyBefore: [...before].filter((name) => !after.has(name)).sort(),
    onlyAfter: [...after].filter((name) => !before.has(name)).sort(),
  };
}
