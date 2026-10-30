import { describe, expect, it } from "vitest";
import fs from "node:fs";
import path from "node:path";

import { decodePng, diffImages, encodePng, pairSnapshots } from "../../scripts/ui_snapshot_diff_lib.mjs";

function solid(width, height, rgba) {
  const data = Buffer.alloc(width * height * 4);
  for (let i = 0; i < width * height; i += 1) data.set(rgba, i * 4);
  return { width, height, data };
}

describe("ui snapshot diff: PNG codec", () => {
  it("reads back what it writes", () => {
    const image = solid(3, 2, [10, 20, 30, 255]);
    image.data.set([200, 100, 50, 128], 4 * 4);
    expect(decodePng(encodePng(image))).toEqual(image);
  });

  it("reads a real RGB PNG with filtered rows, as Chrome and other tools write them", () => {
    // The README picture: 8-bit RGB, rows filtered with every filter type.
    const image = decodePng(fs.readFileSync(path.join(process.cwd(), "frontend", "resources", "albis.png")));
    expect([image.width, image.height]).toEqual([1600, 900]);
    expect(image.data.length).toBe(1600 * 900 * 4);
    expect(image.data[3]).toBe(255);
  });

  it("refuses what it cannot read rather than misreading it", () => {
    expect(() => decodePng(Buffer.from("not a png"))).toThrow("not a PNG");
  });
});

describe("ui snapshot diff: comparing", () => {
  it("finds nothing between identical pictures", () => {
    const result = diffImages(solid(4, 4, [1, 2, 3, 255]), solid(4, 4, [1, 2, 3, 255]));
    expect(result).toMatchObject({ changed: 0, share: 0, box: null, image: null, resized: false });
  });

  it("ignores jitter within the tolerance, which antialiasing causes between runs", () => {
    const result = diffImages(solid(2, 2, [100, 100, 100, 255]), solid(2, 2, [110, 100, 100, 255]), { tolerance: 16 });
    expect(result.changed).toBe(0);
  });

  it("counts the changed pixels and boxes them", () => {
    const before = solid(10, 10, [255, 255, 255, 255]);
    const after = solid(10, 10, [255, 255, 255, 255]);
    after.data.set([0, 0, 0, 255], (2 * 10 + 3) * 4);
    after.data.set([0, 0, 0, 255], (5 * 10 + 7) * 4);
    const result = diffImages(before, after);
    expect(result.changed).toBe(2);
    expect(result.share).toBeCloseTo(0.02);
    expect(result.box).toEqual({ left: 3, top: 2, right: 7, bottom: 5 });
    expect(Array.from(result.image.data.subarray((2 * 10 + 3) * 4, (2 * 10 + 3) * 4 + 4))).toEqual([255, 0, 255, 255]);
  });

  it("counts a grown picture's new area as changed and says it was resized", () => {
    const result = diffImages(solid(2, 2, [0, 0, 0, 255]), solid(2, 3, [0, 0, 0, 255]));
    expect(result).toMatchObject({ changed: 2, resized: true });
  });
});

describe("ui snapshot diff: pairing the runs", () => {
  it("pairs by name and lists the shots only one run has", () => {
    expect(pairSnapshots(["01-main.png", "02-about.png", "notes.txt"], ["01-main.png", "03-log.png"])).toEqual({
      both: ["01-main.png"],
      onlyBefore: ["02-about.png"],
      onlyAfter: ["03-log.png"],
    });
  });
});
