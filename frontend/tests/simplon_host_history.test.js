import { describe, expect, it } from "vitest";

import {
  MAX_RECENT_SIMPLON_HOSTS,
  addRecentSimplonHost,
  recordSimplonHost,
  renderSimplonHostOptions,
  sanitizeRecentSimplonHosts,
} from "../modules/simplon_host_history.js";

describe("sanitizeRecentSimplonHosts", () => {
  it("keeps clean string entries in order", () => {
    expect(sanitizeRecentSimplonHosts(["http://a", "http://b"])).toEqual(["http://a", "http://b"]);
  });

  it("drops junk, blanks and duplicates from persisted data", () => {
    expect(
      sanitizeRecentSimplonHosts(["http://a", "", "  ", "http://a", null, 42, { x: 1 }, "http://b"]),
    ).toEqual(["http://a", "http://b"]);
  });

  it("returns an empty list for a non-array", () => {
    expect(sanitizeRecentSimplonHosts(undefined)).toEqual([]);
    expect(sanitizeRecentSimplonHosts("http://a")).toEqual([]);
    expect(sanitizeRecentSimplonHosts(null)).toEqual([]);
  });

  it("caps the stored length", () => {
    const many = Array.from({ length: 30 }, (_, i) => `http://det${i}`);
    expect(sanitizeRecentSimplonHosts(many)).toHaveLength(MAX_RECENT_SIMPLON_HOSTS);
  });
});

describe("addRecentSimplonHost", () => {
  it("puts the newest address first", () => {
    expect(addRecentSimplonHost(["http://a"], "http://b")).toEqual(["http://b", "http://a"]);
  });

  it("moves a repeat visit to the front instead of duplicating it", () => {
    expect(addRecentSimplonHost(["http://a", "http://b"], "http://b")).toEqual([
      "http://b",
      "http://a",
    ]);
  });

  it("ignores a blank address", () => {
    expect(addRecentSimplonHost(["http://a"], "  ")).toEqual(["http://a"]);
    expect(addRecentSimplonHost(["http://a"], null)).toEqual(["http://a"]);
  });

  it("evicts the oldest entry past the cap", () => {
    const full = Array.from({ length: MAX_RECENT_SIMPLON_HOSTS }, (_, i) => `http://det${i}`);
    const result = addRecentSimplonHost(full, "http://new");
    expect(result).toHaveLength(MAX_RECENT_SIMPLON_HOSTS);
    expect(result[0]).toBe("http://new");
    expect(result).not.toContain(`http://det${MAX_RECENT_SIMPLON_HOSTS - 1}`);
  });

  it("does not mutate the input", () => {
    const input = ["http://a"];
    addRecentSimplonHost(input, "http://b");
    expect(input).toEqual(["http://a"]);
  });
});

describe("recordSimplonHost", () => {
  it("records a new address and reports the change", () => {
    const state = { autoload: { simplonRecentHosts: [] } };
    expect(recordSimplonHost(state, "http://det.local")).toBe(true);
    expect(state.autoload.simplonRecentHosts).toEqual(["http://det.local"]);
  });

  it("reports no change when the address is already newest", () => {
    const state = { autoload: { simplonRecentHosts: ["http://det.local"] } };
    expect(recordSimplonHost(state, "http://det.local")).toBe(false);
    expect(state.autoload.simplonRecentHosts).toEqual(["http://det.local"]);
  });

  it("reports a change when an older address is promoted", () => {
    const state = { autoload: { simplonRecentHosts: ["http://a", "http://b"] } };
    expect(recordSimplonHost(state, "http://b")).toBe(true);
    expect(state.autoload.simplonRecentHosts).toEqual(["http://b", "http://a"]);
  });

  it("tolerates missing state and blank input", () => {
    expect(recordSimplonHost(null, "http://a")).toBe(false);
    expect(recordSimplonHost({}, "http://a")).toBe(false);
    expect(recordSimplonHost({ autoload: {} }, "")).toBe(false);
  });
});

describe("renderSimplonHostOptions", () => {
  it("fills the datalist with one option per address", () => {
    const list = document.createElement("datalist");
    renderSimplonHostOptions(list, ["http://a", "http://b"]);
    expect([...list.querySelectorAll("option")].map((o) => o.value)).toEqual([
      "http://a",
      "http://b",
    ]);
  });

  it("replaces previous options rather than appending", () => {
    const list = document.createElement("datalist");
    renderSimplonHostOptions(list, ["http://a"]);
    renderSimplonHostOptions(list, ["http://b"]);
    expect([...list.querySelectorAll("option")].map((o) => o.value)).toEqual(["http://b"]);
  });

  it("tolerates a missing element", () => {
    expect(() => renderSimplonHostOptions(null, ["http://a"])).not.toThrow();
  });
});
