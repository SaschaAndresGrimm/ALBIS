import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  MAX_RECENT_DETECTORS,
  arrangeDetectors,
  createKnownDetectors,
  detectorLabel,
  forgetDetector,
  recordDetector,
  sanitizeDetectors,
  shortAddress,
  updateDetector,
} from "../modules/known_detectors.js";

const P4 = { address: "192.168.20.191", model: "PILATUS4 2M", serial: "P4-0042" };
const E2 = { address: "192.168.20.88", model: "EIGER2 X 9M", serial: "E-08-0123" };

describe("recording a detector", () => {
  it("remembers the address with what answered there", () => {
    const list = recordDetector([], P4, 10);
    expect(list).toEqual([
      { address: "http://192.168.20.191", name: "", model: "PILATUS4 2M", serial: "P4-0042", kept: false, lastUsed: 10 },
    ]);
  });

  it("is one entry per address, whatever form it was typed in", () => {
    let list = recordDetector([], { address: "192.168.20.191" }, 1);
    list = recordDetector(list, { address: "http://192.168.20.191/" }, 2);
    expect(list).toHaveLength(1);
    expect(list[0].lastUsed).toBe(2);
  });

  it("keeps the model and serial when an address-only test confirms it", () => {
    let list = recordDetector([], P4, 1);
    list = recordDetector(list, { address: P4.address }, 2);
    expect(list[0]).toMatchObject({ model: "PILATUS4 2M", serial: "P4-0042", lastUsed: 2 });
  });

  it("follows a detector to a new address, with its name and star", () => {
    let list = recordDetector([], P4, 1);
    list = updateDetector(list, P4.address, { name: "Lab 2", kept: true });
    list = recordDetector(list, { ...P4, address: "10.0.0.5" }, 2);
    expect(list).toEqual([
      expect.objectContaining({ address: "http://10.0.0.5", name: "Lab 2", kept: true, serial: "P4-0042" }),
    ]);
  });

  it("drops the old name when another detector answers at a known address", () => {
    let list = recordDetector([], P4, 1);
    list = updateDetector(list, P4.address, { name: "Lab 2", kept: true });
    list = recordDetector(list, { ...E2, address: P4.address }, 2);
    expect(list).toEqual([
      expect.objectContaining({ address: "http://192.168.20.191", name: "", model: "EIGER2 X 9M", kept: true }),
    ]);
  });

  it("keeps every starred detector and only the last few others", () => {
    let list = recordDetector([], P4, 0);
    list = updateDetector(list, P4.address, { kept: true });
    for (let i = 1; i <= MAX_RECENT_DETECTORS + 3; i += 1) {
      list = recordDetector(list, { address: `10.0.0.${i}` }, i);
    }
    expect(list.filter((entry) => entry.kept)).toHaveLength(1);
    expect(list.filter((entry) => !entry.kept)).toHaveLength(MAX_RECENT_DETECTORS);
    expect(list.some((entry) => entry.address === "http://10.0.0.1")).toBe(false);
  });

  it("ignores an empty address", () => {
    expect(recordDetector([], { address: "  " })).toEqual([]);
  });
});

describe("the stored list", () => {
  it("drops what is not a detector entry", () => {
    expect(sanitizeDetectors([null, "http://a", { address: "" }, { address: "det" }, { address: "det" }])).toEqual([
      { address: "http://det", name: "", model: "", serial: "", kept: false, lastUsed: 0 },
    ]);
    expect(sanitizeDetectors("nope")).toEqual([]);
  });

  it("forgets an entry by address", () => {
    const list = recordDetector(recordDetector([], P4, 1), E2, 2);
    expect(forgetDetector(list, "192.168.20.88").map((entry) => entry.address)).toEqual(["http://192.168.20.191"]);
  });
});

describe("showing the list", () => {
  it("labels a detector by its name, else its model, else its address", () => {
    expect(detectorLabel({ name: "Lab 2", model: "PILATUS4 2M", address: "http://x" })).toBe("Lab 2");
    expect(detectorLabel({ name: "", model: "PILATUS4 2M", address: "http://x" })).toBe("PILATUS4 2M");
    expect(detectorLabel({ name: "", model: "", address: "http://10.0.0.1:8080/" })).toBe("10.0.0.1:8080");
    expect(shortAddress("https://det.local")).toBe("https://det.local");
  });

  it("puts starred ones first by name, then the others by last use", () => {
    let list = recordDetector([], { address: "10.0.0.1", model: "Zeta" }, 1);
    list = recordDetector(list, { address: "10.0.0.2", model: "Alpha" }, 2);
    list = recordDetector(list, { address: "10.0.0.3" }, 3);
    list = recordDetector(list, { address: "10.0.0.4" }, 4);
    list = updateDetector(list, "10.0.0.1", { kept: true });
    list = updateDetector(list, "10.0.0.2", { kept: true });
    const { kept, recent } = arrangeDetectors(list);
    expect(kept.map(detectorLabel)).toEqual(["Alpha", "Zeta"]);
    expect(recent.map(detectorLabel)).toEqual(["10.0.0.4", "10.0.0.3"]);
  });

  it("narrows by any word of name, model, serial or address", () => {
    let list = recordDetector([], P4, 1);
    list = recordDetector(list, E2, 2);
    list = updateDetector(list, P4.address, { name: "Lab 2" });
    const all = (query) => {
      const { kept, recent } = arrangeDetectors(list, query);
      return [...kept, ...recent].map(detectorLabel);
    };
    expect(all("lab")).toEqual(["Lab 2"]);
    expect(all("eiger 9m")).toEqual(["EIGER2 X 9M"]);
    expect(all("P4-00")).toEqual(["Lab 2"]);
    expect(all("192.168.20.8")).toEqual(["EIGER2 X 9M"]);
    expect(all("nothing")).toEqual([]);
  });
});

// The server's side, on the same rules: what /api/known-detectors answers.
function fakeServer({ fail = false } = {}) {
  let entries = [];
  const calls = [];
  global.fetch = vi.fn(async (url, init = {}) => {
    const method = init.method || "GET";
    const body = init.body ? JSON.parse(init.body) : null;
    calls.push(`${method} ${url}`);
    if (fail && method !== "GET") return { ok: false, status: 500, json: async () => ({ detail: "disk full" }) };
    if (method === "POST" && url.endsWith("/seen")) entries = recordDetector(entries, body);
    if (method === "POST" && url.endsWith("/import")) {
      for (const item of body.detectors) entries = updateDetector(recordDetector(entries, item, item.lastUsed), item.address, { kept: item.kept, ...(item.name ? { name: item.name } : {}) });
    }
    if (method === "PATCH") {
      const { address, ...changes } = body;
      entries = updateDetector(entries, address, changes);
    }
    if (method === "DELETE") entries = forgetDetector(entries, decodeURIComponent(url.split("address=")[1]));
    return { ok: true, status: 200, json: async () => ({ detectors: entries }) };
  });
  return { calls, set: (next) => (entries = next) };
}

describe("the shared store", () => {
  beforeEach(() => localStorage.clear());
  afterEach(() => {
    delete global.fetch;
  });

  it("shows a change at once, then the server's list, and tells its subscribers", async () => {
    const server = fakeServer();
    const store = createKnownDetectors();
    const seen = [];
    store.subscribe((list) => seen.push(list.length));
    const pending = store.record(P4);
    expect(store.find("192.168.20.191")).toMatchObject({ model: "PILATUS4 2M" });
    await pending;
    await store.setKept(P4.address, true);
    await store.rename(P4.address, " Lab 2 ");
    expect(store.find(P4.address)).toMatchObject({ name: "Lab 2", kept: true });
    expect(server.calls).toEqual([
      "POST /api/known-detectors/seen",
      "PATCH /api/known-detectors",
      "PATCH /api/known-detectors",
    ]);
    expect(seen.length).toBeGreaterThanOrEqual(3);
    await store.forget(P4.address);
    expect(server.calls.at(-1)).toBe("DELETE /api/known-detectors?address=192.168.20.191");
    expect(store.list()).toEqual([]);
  });

  it("takes a change back when the server cannot save it, and says so", async () => {
    fakeServer({ fail: true });
    const store = createKnownDetectors();
    await store.record(P4);
    expect(store.list()).toEqual([]);
    // The server's own reason, inside http.js's translated message.
    expect(store.lastError).toContain("disk full");
  });

  it("reads changes made by others", async () => {
    const server = fakeServer();
    const store = createKnownDetectors();
    server.set(recordDetector([], E2, 5));
    await store.refresh();
    expect(store.list().map(detectorLabel)).toEqual(["EIGER2 X 9M"]);
  });

  it("hands the lists earlier versions kept in this browser to the server, once", async () => {
    const server = fakeServer();
    localStorage.setItem("albis.knownDetectors", JSON.stringify(updateDetector(recordDetector([], P4, 5), P4.address, { name: "Lab 2", kept: true })));
    localStorage.setItem("albis.autoload", JSON.stringify({ mode: "simplon", simplonRecentHosts: ["http://new", "http://old"] }));
    const store = createKnownDetectors();
    await store.start();
    expect(server.calls.slice(0, 2)).toEqual(["POST /api/known-detectors/import", "GET /api/known-detectors"]);
    expect(store.find(P4.address)).toMatchObject({ name: "Lab 2", kept: true });
    expect(arrangeDetectors(store.list()).recent.map((entry) => entry.address)).toEqual(["http://new", "http://old"]);
    expect(localStorage.getItem("albis.knownDetectors")).toBeNull();
    expect(JSON.parse(localStorage.getItem("albis.autoload"))).toEqual({ mode: "simplon" });
  });

  it("keeps the browser's list when the server cannot take it", async () => {
    fakeServer({ fail: true });
    localStorage.setItem("albis.knownDetectors", JSON.stringify(recordDetector([], P4, 5)));
    const store = createKnownDetectors();
    expect(await store.adoptBrowserLists()).toBe(false);
    expect(localStorage.getItem("albis.knownDetectors")).not.toBeNull();
  });
});
