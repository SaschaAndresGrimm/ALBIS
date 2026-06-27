/**
 * Minimal dependency-free animated GIF encoder.
 *
 * Frames are supplied as palette-indexed pixel buffers that share a single
 * global colour table (<= 256 entries). This sidesteps colour quantisation:
 * ALBIS already maps each pixel to a colormap palette entry, so the colormap
 * palette becomes the GIF colour table and each pixel is simply its index.
 *
 * The LZW compressor below is the public-domain GIF flavour of the UNIX
 * `compress` routine (Kevin Weiner's LZWEncoder lineage), reproduced faithfully
 * because correctness of the variable-width bit packing is what makes or breaks
 * a GIF. The container assembly (header, screen descriptor, per-frame graphic
 * control + image descriptors, Netscape looping block, trailer) is written
 * incrementally so only one frame's pixels are held in memory at a time.
 */

class ByteBuffer {
  constructor() {
    this.chunks = [];
    this.length = 0;
  }

  writeByte(value) {
    this.chunks.push(value & 0xff);
    this.length += 1;
  }

  writeBytes(array, offset = 0, count = array.length) {
    for (let i = 0; i < count; i += 1) {
      this.chunks.push(array[offset + i] & 0xff);
    }
    this.length += count;
  }

  writeUint16(value) {
    this.writeByte(value & 0xff);
    this.writeByte((value >> 8) & 0xff);
  }

  writeString(text) {
    for (let i = 0; i < text.length; i += 1) {
      this.writeByte(text.charCodeAt(i));
    }
  }

  toUint8Array() {
    return Uint8Array.from(this.chunks);
  }
}

const LZW_MASKS = [
  0x0000, 0x0001, 0x0003, 0x0007, 0x000f, 0x001f, 0x003f, 0x007f, 0x00ff,
  0x01ff, 0x03ff, 0x07ff, 0x0fff, 0x1fff, 0x3fff, 0x7fff, 0xffff,
];

// Adapted from the public-domain LZWEncoder (Kevin Weiner / UNIX compress).
function lzwEncode(out, pixels, colorDepth) {
  const initCodeSize = Math.max(2, colorDepth);
  const BITS = 12;
  const HSIZE = 5003;
  const maxbits = BITS;
  const maxmaxcode = 1 << BITS;

  const htab = new Int32Array(HSIZE);
  const codetab = new Int32Array(HSIZE);
  const accum = new Uint8Array(256);

  let nBits;
  let maxcode;
  let freeEnt = 0;
  let clearFlag = false;
  let curAccum = 0;
  let curBits = 0;
  let aCount = 0;
  let remaining = pixels.length;
  let curPixel = 0;

  const MAXCODE = (n) => (1 << n) - 1;

  function flushChar() {
    if (aCount > 0) {
      out.writeByte(aCount);
      out.writeBytes(accum, 0, aCount);
      aCount = 0;
    }
  }

  function charOut(c) {
    accum[aCount++] = c;
    if (aCount >= 254) flushChar();
  }

  function clHash(hsize) {
    for (let i = 0; i < hsize; i += 1) htab[i] = -1;
  }

  function clBlock() {
    clHash(HSIZE);
    freeEnt = clearCode + 2;
    clearFlag = true;
    output(clearCode);
  }

  function output(code) {
    curAccum &= LZW_MASKS[curBits];
    if (curBits > 0) curAccum |= code << curBits;
    else curAccum = code;
    curBits += nBits;
    while (curBits >= 8) {
      charOut(curAccum & 0xff);
      curAccum >>= 8;
      curBits -= 8;
    }
    if (freeEnt > maxcode || clearFlag) {
      if (clearFlag) {
        nBits = gInitBits;
        maxcode = MAXCODE(nBits);
        clearFlag = false;
      } else {
        nBits += 1;
        maxcode = nBits === maxbits ? maxmaxcode : MAXCODE(nBits);
      }
    }
    if (code === eofCode) {
      while (curBits > 0) {
        charOut(curAccum & 0xff);
        curAccum >>= 8;
        curBits -= 8;
      }
      flushChar();
    }
  }

  function nextPixel() {
    if (remaining === 0) return -1;
    remaining -= 1;
    return pixels[curPixel++] & 0xff;
  }

  // GIF requires the LZW minimum code size byte before the data sub-blocks.
  out.writeByte(initCodeSize);

  const initBits = initCodeSize + 1;
  const gInitBits = initBits;
  const clearCode = 1 << (initBits - 1);
  const eofCode = clearCode + 1;
  clearFlag = false;
  nBits = gInitBits;
  maxcode = MAXCODE(nBits);
  freeEnt = clearCode + 2;
  aCount = 0;

  let ent = nextPixel();
  let hshift = 0;
  for (let fcode = HSIZE; fcode < 65536; fcode *= 2) hshift += 1;
  hshift = 8 - hshift;
  clHash(HSIZE);
  output(clearCode);

  let c;
  outer: while ((c = nextPixel()) !== -1) {
    const fcode = (c << maxbits) + ent;
    let i = (c << hshift) ^ ent;
    if (htab[i] === fcode) {
      ent = codetab[i];
      continue;
    } else if (htab[i] >= 0) {
      let disp = HSIZE - i;
      if (i === 0) disp = 1;
      do {
        i -= disp;
        if (i < 0) i += HSIZE;
        if (htab[i] === fcode) {
          ent = codetab[i];
          continue outer;
        }
      } while (htab[i] >= 0);
    }
    output(ent);
    ent = c;
    if (freeEnt < maxmaxcode) {
      codetab[i] = freeEnt++;
      htab[i] = fcode;
    } else {
      clBlock();
    }
  }
  output(ent);
  output(eofCode);

  // Block terminator for the image data section.
  out.writeByte(0);
}

/**
 * Streaming GIF writer.
 *
 * @param {object} options
 * @param {number} options.width   Logical screen / frame width in pixels.
 * @param {number} options.height  Logical screen / frame height in pixels.
 * @param {Uint8Array} options.palette  Flat RGB triplets (length = colorCount * 3).
 * @param {number} options.loop    Loop count (0 = forever, -1 = no loop block).
 */
export class GifWriter {
  constructor({ width, height, palette, loop = 0 }) {
    this.width = width;
    this.height = height;
    this.buffer = new ByteBuffer();
    this.finished = false;

    const colorCount = Math.max(2, Math.floor(palette.length / 3));
    // GIF colour tables must be a power-of-two size; derive the bit depth.
    let depth = 1;
    while (1 << depth < colorCount) depth += 1;
    this.colorDepth = Math.max(2, depth);
    const tableSize = 1 << this.colorDepth;

    this.buffer.writeString("GIF89a");
    this.buffer.writeUint16(width);
    this.buffer.writeUint16(height);
    // Packed: global colour table flag, colour resolution, sort flag, GCT size.
    this.buffer.writeByte(0x80 | ((this.colorDepth - 1) << 4) | (this.colorDepth - 1));
    this.buffer.writeByte(0); // background colour index
    this.buffer.writeByte(0); // pixel aspect ratio

    // Global colour table padded to the power-of-two table size.
    for (let i = 0; i < tableSize; i += 1) {
      const base = i * 3;
      this.buffer.writeByte(palette[base] || 0);
      this.buffer.writeByte(palette[base + 1] || 0);
      this.buffer.writeByte(palette[base + 2] || 0);
    }

    if (loop >= 0) {
      // Netscape 2.0 application extension enabling animation looping.
      this.buffer.writeByte(0x21);
      this.buffer.writeByte(0xff);
      this.buffer.writeByte(0x0b);
      this.buffer.writeString("NETSCAPE2.0");
      this.buffer.writeByte(0x03);
      this.buffer.writeByte(0x01);
      this.buffer.writeUint16(loop);
      this.buffer.writeByte(0x00);
    }
  }

  /**
   * Append one frame.
   *
   * @param {Uint8Array} indices  Palette indices, length = width * height.
   * @param {number} delayCs      Display duration in centiseconds (1/100 s).
   */
  addFrame(indices, delayCs) {
    if (this.finished) throw new Error("GifWriter already finished");
    const delay = Math.max(2, Math.round(delayCs));

    // Graphic control extension (per-frame delay, no transparency).
    this.buffer.writeByte(0x21);
    this.buffer.writeByte(0xf9);
    this.buffer.writeByte(0x04);
    this.buffer.writeByte(0x04); // disposal method = do not dispose
    this.buffer.writeUint16(delay);
    this.buffer.writeByte(0x00); // transparent colour index (unused)
    this.buffer.writeByte(0x00);

    // Image descriptor (full-frame, no local colour table).
    this.buffer.writeByte(0x2c);
    this.buffer.writeUint16(0);
    this.buffer.writeUint16(0);
    this.buffer.writeUint16(this.width);
    this.buffer.writeUint16(this.height);
    this.buffer.writeByte(0x00);

    lzwEncode(this.buffer, indices, this.colorDepth);
  }

  finish() {
    if (!this.finished) {
      this.buffer.writeByte(0x3b); // trailer
      this.finished = true;
    }
    return this.buffer.toUint8Array();
  }
}
