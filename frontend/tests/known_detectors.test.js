import { beforeEach, describe, expect, it } from "vitest";
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

describe("the shared store", () => {
  beforeEach(() => localStorage.clear());

  it("persists and tells its subscribers", () => {
    const store = createKnownDetectors({ storageKey: "test.known" });
    const seen = [];
    store.subscribe((list) => seen.push(list.length));
    store.record(P4);
    store.setKept(P4.address, true);
    store.rename(P4.address, " Lab 2 ");
    expect(seen).toEqual([1, 1, 1]);
    const again = createKnownDetectors({ storageKey: "test.known" });
    expect(again.find("192.168.20.191")).toMatchObject({ name: "Lab 2", kept: true });
    again.forget(P4.address);
    expect(again.list()).toEqual([]);
  });

  it("takes over the old address history once, most recent first", () => {
    const store = createKnownDetectors({ storageKey: "test.known" });
    store.adoptLegacy(["http://new", "http://old"]);
    expect(arrangeDetectors(store.list()).recent.map((entry) => entry.address)).toEqual(["http://new", "http://old"]);
    store.forget("http://new");
    store.adoptLegacy(["http://new"]);
    expect(store.list().map((entry) => entry.address)).toEqual(["http://old"]);
  });

  it("reads an unreadable store as empty", () => {
    localStorage.setItem("test.known", "{not json");
    expect(createKnownDetectors({ storageKey: "test.known" }).list()).toEqual([]);
  });
});
