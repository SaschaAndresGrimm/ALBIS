/**
 * Without these moves the inspector tree could not be used from the keyboard:
 * only a group's toggle button took focus, so datasets and links were out of
 * reach.
 */

import { describe, expect, it } from "vitest";

import { treeKeyAction } from "../modules/inspector_tree_nav_utils.js";

// entry (open)
//   data (closed group)
//   instrument (open)
//     detector (leaf)
//   definition (leaf)
const ITEMS = [
  { level: 1, expandable: true, expanded: true },
  { level: 2, expandable: true, expanded: false },
  { level: 2, expandable: true, expanded: true },
  { level: 3, expandable: false, expanded: false },
  { level: 2, expandable: false, expanded: false },
];

describe("treeKeyAction", () => {
  it("moves up and down through the visible items and stops at the ends", () => {
    expect(treeKeyAction(ITEMS, 0, "ArrowDown")).toEqual({ action: "focus", index: 1 });
    expect(treeKeyAction(ITEMS, 3, "ArrowUp")).toEqual({ action: "focus", index: 2 });
    expect(treeKeyAction(ITEMS, 4, "ArrowDown")).toBeNull();
    expect(treeKeyAction(ITEMS, 0, "ArrowUp")).toBeNull();
    expect(treeKeyAction(ITEMS, 3, "Home")).toEqual({ action: "focus", index: 0 });
    expect(treeKeyAction(ITEMS, 1, "End")).toEqual({ action: "focus", index: 4 });
  });

  it("opens a closed group on the right, then steps into it", () => {
    expect(treeKeyAction(ITEMS, 1, "ArrowRight")).toEqual({ action: "expand", index: 1 });
    expect(treeKeyAction(ITEMS, 2, "ArrowRight")).toEqual({ action: "focus", index: 3 });
    expect(treeKeyAction(ITEMS, 4, "ArrowRight")).toBeNull();
  });

  it("does not step into the next sibling when an open group shows no children yet", () => {
    const items = [
      { level: 1, expandable: true, expanded: true },
      { level: 1, expandable: false, expanded: false },
    ];
    expect(treeKeyAction(items, 0, "ArrowRight")).toBeNull();
  });

  it("closes an open group on the left, else goes to the parent", () => {
    expect(treeKeyAction(ITEMS, 2, "ArrowLeft")).toEqual({ action: "collapse", index: 2 });
    expect(treeKeyAction(ITEMS, 3, "ArrowLeft")).toEqual({ action: "focus", index: 2 });
    expect(treeKeyAction(ITEMS, 4, "ArrowLeft")).toEqual({ action: "focus", index: 0 });
    expect(treeKeyAction(ITEMS.slice(1), 0, "ArrowLeft")).toBeNull();
  });

  it("activates on Enter and Space and ignores other keys", () => {
    expect(treeKeyAction(ITEMS, 3, "Enter")).toEqual({ action: "activate", index: 3 });
    expect(treeKeyAction(ITEMS, 3, " ")).toEqual({ action: "activate", index: 3 });
    expect(treeKeyAction(ITEMS, 3, "a")).toBeNull();
    expect(treeKeyAction([], 0, "ArrowDown")).toBeNull();
  });
});
