import { describe, expect, it } from "vitest";
import fs from "node:fs";

import { createRoiState } from "../modules/state.js";

// A new session starts with a box ROI. The default lives in two places -- the
// state and the option index.html marks selected, which the bootstrap reads
// -- and the two have to agree, or the panel shows one mode while the state
// draws another until the dropdown is touched.
describe("default ROI mode", () => {
  it("is the box, in the state and in the dropdown", () => {
    expect(createRoiState().mode).toBe("box");
    document.body.innerHTML = fs.readFileSync("frontend/index.html", "utf8");
    expect(document.getElementById("roi-mode").value).toBe("box");
  });
});
