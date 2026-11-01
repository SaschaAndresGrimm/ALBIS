import { describe, expect, it } from "vitest";
import {
  aggregateSeriesLocally,
  buildBreadcrumbSegments,
  compareNatural,
  deriveParentPath,
  fileTypeLabelKey,
  formatFileSize,
  inferFileExt,
  joinBrowsePath,
  matchesBrowseQuery,
  normalizeFileItems,
  normalizeRequestedExts,
  serializeExts,
  sortFileItems,
  splitHdfSeries,
  splitSeriesName,
} from "../modules/file_browser_utils.js";

describe("file_browser_utils", () => {
  it("normalizes requested extensions and adds .cbf.gz to .cbf", () => {
    expect(serializeExts(normalizeRequestedExts("CBF, tif"))).toBe(".cbf,.cbf.gz,.tif");
    expect(normalizeRequestedExts("").has(".h5")).toBe(true);
  });

  it("sorts names naturally", () => {
    expect(compareNatural("img_2", "img_10")).toBeLessThan(0);
    expect(compareNatural("img_10", "img_2")).toBeGreaterThan(0);
    expect(compareNatural("a", "a")).toBe(0);
  });

  it("joins and splits paths in POSIX and Windows style", () => {
    expect(joinBrowsePath("/data/", "run1")).toBe("/data/run1");
    expect(joinBrowsePath("C:\\data", "run1")).toBe("C:\\data\\run1");
    expect(joinBrowsePath("", "run1")).toBe("run1");
    expect(deriveParentPath("/data/run1")).toBe("/data");
    expect(deriveParentPath("/data")).toBe("/");
    expect(deriveParentPath("C:\\data\\run1")).toBe("C:\\data");
    expect(deriveParentPath("rel/sub")).toBe("rel");
  });

  it("builds breadcrumb segments", () => {
    expect(buildBreadcrumbSegments("/a/b")).toEqual([
      { label: "a", path: "/a" },
      { label: "b", path: "/a/b" },
    ]);
    expect(buildBreadcrumbSegments("C:\\x\\y")).toEqual([
      { label: "C:", path: "C:" },
      { label: "x", path: "C:\\x" },
      { label: "y", path: "C:\\x\\y" },
    ]);
  });

  it("infers extensions and type labels", () => {
    expect(inferFileExt("frame.CBF.GZ")).toBe(".cbf.gz");
    expect(inferFileExt("noext")).toBe("");
    expect(fileTypeLabelKey(".h5")).toBe("browse.filter.option.hdf5");
    expect(fileTypeLabelKey(".expt")).toBe("browse.filter.option.geometry");
    expect(fileTypeLabelKey(".xyz")).toBe("");
    expect(matchesBrowseQuery("Scan_01.h5", "scan")).toBe(true);
    expect(matchesBrowseQuery("other", "scan")).toBe(false);
  });

  it("splits image and HDF5 series names", () => {
    expect(splitSeriesName("img_0007.tif")).toEqual({ prefix: "img_", digits: "0007", suffix: "" });
    expect(splitHdfSeries("Run_master.h5")).toEqual({ kind: "master", prefix: "run", index: 0 });
    expect(splitHdfSeries("run_data_000002.h5")).toEqual({ kind: "data", prefix: "run", index: 2 });
    expect(splitHdfSeries("plain.h5")).toBeNull();
  });

  it("groups series locally under their lowest-numbered lead", () => {
    const items = [
      { name: "img_0002.tif", path: "/d/img_0002.tif", mtime: 5 },
      { name: "img_0001.tif", path: "/d/img_0001.tif", mtime: 3 },
      { name: "run_master.h5", path: "/d/run_master.h5", mtime: 1 },
      { name: "run_data_000001.h5", path: "/d/run_data_000001.h5", mtime: 9 },
      { name: "notes.txt", path: "/d/notes.txt" },
    ];
    const grouped = aggregateSeriesLocally(items);
    const tif = grouped.find((item) => item.ext === ".tif");
    expect(tif).toMatchObject({ name: "img_0001.tif", seriesCount: 2, isSeriesLead: true, mtime: 5 });
    const hdf = grouped.find((item) => item.name === "run_master.h5");
    expect(hdf).toMatchObject({ seriesCount: 1, isSeriesLead: false, mtime: 9 });
    expect(grouped.some((item) => item.name === "notes.txt")).toBe(true);
  });

  it("sorts file items by the chosen mode", () => {
    const items = [
      { name: "b", ext: ".tif", mtime: 1, sizeBytes: 30 },
      { name: "a", ext: ".h5", mtime: 2, sizeBytes: 10 },
    ];
    expect(sortFileItems(items, "name_asc").map((item) => item.name)).toEqual(["a", "b"]);
    expect(sortFileItems(items, "mtime_desc").map((item) => item.name)).toEqual(["a", "b"]);
    expect(sortFileItems(items, "size_desc").map((item) => item.name)).toEqual(["b", "a"]);
    expect(sortFileItems(items, "type_asc").map((item) => item.name)).toEqual(["a", "b"]);
  });

  it("formats file sizes", () => {
    expect(formatFileSize(0)).toBe("—");
    expect(formatFileSize(512)).toBe("512 B");
    expect(formatFileSize(1536)).toBe("1.5 KB");
    expect(formatFileSize(20 * 1024 * 1024)).toBe("20 MB");
  });

  it("normalizes browse responses with and without rich metadata", () => {
    expect(normalizeFileItems({ files: ["x.h5"] }, "/d")).toEqual([
      { name: "x.h5", path: "/d/x.h5", ext: ".h5", mtime: 0, sizeBytes: 0, isSeriesLead: false, seriesCount: 1 },
    ]);
    expect(normalizeFileItems({ fileItems: [{ name: "y.tif", seriesCount: 3, isSeriesLead: true }] }, "/d")).toEqual([
      { name: "y.tif", path: "/d/y.tif", ext: ".tif", mtime: 0, sizeBytes: 0, isSeriesLead: true, seriesCount: 3 },
    ]);
    expect(normalizeFileItems({}, "/d")).toEqual([]);
  });
});
