import { beforeEach, describe, expect, it, vi } from "vitest";

import { createRecentFiles } from "../modules/recent_files.js";

describe("recent files store", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it("starts empty", () => {
    expect(createRecentFiles().list()).toEqual([]);
  });

  it("puts the most recently opened file first", () => {
    const recent = createRecentFiles();

    recent.record("/data/a.h5");
    recent.record("/data/b.h5");

    expect(recent.list()).toEqual(["/data/b.h5", "/data/a.h5"]);
  });

  it("moves a file that is opened again to the front instead of duplicating it", () => {
    const recent = createRecentFiles();

    recent.record("/data/a.h5");
    recent.record("/data/b.h5");
    recent.record("/data/a.h5");

    expect(recent.list()).toEqual(["/data/a.h5", "/data/b.h5"]);
  });

  it("keeps only the newest entries", () => {
    const recent = createRecentFiles({ limit: 3 });

    ["a", "b", "c", "d"].forEach((name) => recent.record(`/data/${name}.h5`));

    expect(recent.list()).toEqual(["/data/d.h5", "/data/c.h5", "/data/b.h5"]);
  });

  it("ignores an empty path", () => {
    const recent = createRecentFiles();

    recent.record("");
    recent.record("   ");
    recent.record(null);

    expect(recent.list()).toEqual([]);
  });

  it("survives a reload, which is the entire point", () => {
    createRecentFiles().record("/data/a.h5");

    expect(createRecentFiles().list()).toEqual(["/data/a.h5"]);
  });

  it("removes a single entry", () => {
    const recent = createRecentFiles();
    recent.record("/data/a.h5");
    recent.record("/data/b.h5");

    recent.remove("/data/a.h5");

    expect(recent.list()).toEqual(["/data/b.h5"]);
  });

  it("clears everything", () => {
    const recent = createRecentFiles();
    recent.record("/data/a.h5");

    recent.clear();

    expect(recent.list()).toEqual([]);
  });

  it("treats an unreadable stored value as an empty list", () => {
    localStorage.setItem("albis.recentFiles", "{not json");

    expect(createRecentFiles().list()).toEqual([]);
  });

  it("ignores stored entries that are not paths", () => {
    localStorage.setItem(
      "albis.recentFiles",
      JSON.stringify(["/data/a.h5", "", null, 42, { path: "/data/b.h5" }]),
    );

    // A bare number has neither a path nor a usable string form, so it is
    // dropped; an object with a `path` is accepted, since that is the shape a
    // future entry with metadata would take.
    expect(createRecentFiles().list()).toEqual(["/data/a.h5", "/data/b.h5"]);
  });

  it("works when storage is unavailable", () => {
    // A private window throws on access rather than returning null; a viewer
    // must still open files, just without remembering them.
    const getItem = vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
      throw new Error("denied");
    });
    const setItem = vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new Error("denied");
    });

    const recent = createRecentFiles();
    expect(() => recent.record("/data/a.h5")).not.toThrow();
    expect(recent.list()).toEqual([]);

    getItem.mockRestore();
    setItem.mockRestore();
  });
});
