import { describe, expect, it } from "vitest";

import { HANDOFF_ENDPOINT, handoffKeys } from "../modules/update_handoff_utils.js";

describe("update handoff keys", () => {
  it("names the step for the installs that cannot update while ALBIS runs", () => {
    expect(handoffKeys("macos_app").action).toBe("update_check.action.handoff.macos_app");
    expect(handoffKeys("windows_portable").steps).toBe(
      "update_check.handoff.steps.windows_portable",
    );
    expect(handoffKeys("macos_app").stopped).toBe("update_check.handoff.stopped.macos_app");
    expect(HANDOFF_ENDPOINT).toBe("update-download/open-and-quit");
  });

  it("has nothing for the rest, nor for an inherited property name", () => {
    // The installer closes ALBIS itself; an AppImage is replaced while it runs.
    expect(handoffKeys("windows_installer")).toBeNull();
    expect(handoffKeys("appimage")).toBeNull();
    expect(handoffKeys("toString")).toBeNull();
    expect(handoffKeys("")).toBeNull();
  });
});
