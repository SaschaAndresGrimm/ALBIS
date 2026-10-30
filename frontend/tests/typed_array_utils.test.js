import { describe, expect, it } from "vitest";

import { computeMedian, parseShape, quickSelect, toFloat32, typedArrayFrom } from "../modules/typed_array_utils.js";

describe("typed_array_utils", () => {
  it("reads the shape header as numbers", () => {
    expect(parseShape("1062,1028")).toEqual([1062, 1028]);
    expect(parseShape("")).toEqual([]);
    expect(parseShape(null)).toEqual([]);
  });

  it("puts the bytes into the array type the dtype names", () => {
    const bytes = new Uint8Array([1, 0, 2, 0]).buffer;
    expect(typedArrayFrom(bytes, "<u2")).toBeInstanceOf(Uint16Array);
    expect(Array.from(typedArrayFrom(bytes, "<u2"))).toEqual([1, 2]);
    expect(typedArrayFrom(bytes, "|u1")).toBeInstanceOf(Uint8Array);
    expect(typedArrayFrom(bytes, "<i2")).toBeInstanceOf(Int16Array);
    expect(typedArrayFrom(new Float32Array([1.5]).buffer, "<f4")[0]).toBe(1.5);
    // An unknown dtype reads as the detectors' usual 32-bit counts.
    expect(typedArrayFrom(new Uint32Array([7]).buffer, "?")).toBeInstanceOf(Uint32Array);
  });

  it("widens 64-bit integers to float64, which WebGL and the statistics can use", () => {
    const out = typedArrayFrom(new BigUint64Array([5n, 2n ** 40n]).buffer, "<u8");
    expect(out).toBeInstanceOf(Float64Array);
    expect(Array.from(out)).toEqual([5, 2 ** 40]);
    expect(Array.from(typedArrayFrom(new BigInt64Array([-3n]).buffer, "<i8"))).toEqual([-3]);
  });

  it("copies to float32 only when the data is not float32 already", () => {
    const f = new Float32Array([1, 2]);
    expect(toFloat32(f)).toBe(f);
    expect(Array.from(toFloat32(new Uint16Array([3, 4])))).toEqual([3, 4]);
  });

  it("finds the median without changing the array it was given", () => {
    const values = new Float32Array([9, 1, 5, 3, 7]);
    expect(computeMedian(values)).toBe(5);
    expect(Array.from(values)).toEqual([9, 1, 5, 3, 7]);
    expect(computeMedian(new Float32Array([4, 1, 3, 2]))).toBe(2.5);
    expect(computeMedian(new Float32Array([]))).toBeNaN();
  });

  it("selects the k-th smallest value in place", () => {
    const values = [5, 3, 9, 1, 7];
    expect(quickSelect(values, 0)).toBe(1);
    expect(quickSelect([5, 3, 9, 1, 7], 4)).toBe(9);
  });
});
