import { describe, expect, it } from "vitest";
import fs from "node:fs";
import path from "node:path";

/**
 * Guards the wiring rather than the logic: post_file_picker_bindings.js takes
 * the elements apart by name and hands bindViewerControls a list of its own.
 * An element bindViewerControls uses but that list leaves out arrives as
 * undefined, and its control does nothing in the real app while the unit
 * tests, which call bindViewerControls directly, still pass. That is how the
 * 1:1 zoom button shipped without a click listener.
 */
function readModule(name) {
  return fs.readFileSync(path.join(process.cwd(), "frontend", "modules", name), "utf8");
}

function names(block) {
  return block
    .split(",")
    .map((part) => part.replace(/\/\/.*$/gm, "").trim().split(/[\s:]/)[0])
    .filter((name) => /^[A-Za-z_$][\w$]*$/.test(name));
}

function usedElements() {
  const source = readModule("viewer_controls_bindings.js");
  const match = source.match(/const \{([^}]*)\} = elements;/);
  return names(match[1]);
}

function passedElements() {
  const source = readModule("post_file_picker_bindings.js");
  const call = source.indexOf("bindViewerControls({");
  const match = source.slice(call).match(/elements: \{([^}]*)\}/);
  return names(match[1]);
}

function receivedElements() {
  const source = readModule("post_file_picker_bindings.js");
  const match = source.match(/const \{([^}]*)\} = elements;/);
  return names(match[1]);
}

describe("viewer controls wiring", () => {
  it("finds the element lists it compares", () => {
    expect(usedElements()).toContain("zoomRange");
    expect(passedElements()).toContain("zoomRange");
    expect(receivedElements()).toContain("zoomRange");
  });

  it("passes bindViewerControls every element it binds", () => {
    const passed = new Set(passedElements());
    expect(usedElements().filter((name) => !passed.has(name))).toEqual([]);
  });

  it("takes every element it passes on from its own elements", () => {
    const received = new Set(receivedElements());
    expect(passedElements().filter((name) => !received.has(name))).toEqual([]);
  });
});
