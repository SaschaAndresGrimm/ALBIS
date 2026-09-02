import fs from "node:fs";
import path from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

// The real English catalogue, so a renamed or deleted status key fails here
// instead of silently degrading to a raw key in the UI.
const EN = JSON.parse(
  fs.readFileSync(path.join(process.cwd(), "frontend", "locales", "en.json"), "utf8")
);

function makeState(overrides = {}) {
  return {
    file: "/data/single_frame.cbf",
    dataset: "",
    seriesFiles: [],
    frameCount: 1,
    fps: 5,
    width: 64,
    height: 64,
    animationExport: { running: false, cancelling: false },
    ...overrides,
  };
}

async function createController(state) {
  vi.resetModules();
  global.fetch = vi.fn(async () => ({ ok: true, json: async () => EN }));
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createAnimationExportController } = await import(
    "../modules/animation_export_controller.js"
  );
  const setStatus = vi.fn();
  const openModal = vi.fn();
  const controller = createAnimationExportController({
    apiBase: "/api",
    state,
    elements: {},
    callbacks: { setStatus, openModal, closeModal: vi.fn() },
  });
  return { controller, setStatus, openModal };
}

describe("animation_export_controller", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("reports a single frame as not exportable", async () => {
    const { controller } = await createController(makeState());
    expect(controller.isReady()).toBe(false);
  });

  it("reports a multi-frame dataset as exportable", async () => {
    const { controller } = await createController(
      makeState({ file: "/data/stack.h5", dataset: "/entry/data/data", frameCount: 12 })
    );
    expect(controller.isReady()).toBe(true);
  });

  it("refuses a single frame with a toast rather than silently", async () => {
    const { controller, setStatus, openModal } = await createController(makeState());
    controller.openDialog();
    expect(openModal).not.toHaveBeenCalled();
    expect(setStatus).toHaveBeenCalledWith(EN["status.animation_export.not_series"], {
      tone: "warning",
    });
  });

  it("refuses with no file open with a toast rather than silently", async () => {
    const { controller, setStatus, openModal } = await createController(makeState({ file: "" }));
    controller.openDialog();
    expect(openModal).not.toHaveBeenCalled();
    expect(setStatus).toHaveBeenCalledWith(EN["status.animation_export.no_file"], {
      tone: "warning",
    });
  });

  it("opens the dialog for an exportable series without a warning", async () => {
    const { controller, setStatus, openModal } = await createController(
      makeState({ file: "/data/series_0001.cbf", seriesFiles: ["a.cbf", "b.cbf"], frameCount: 2 })
    );
    controller.openDialog();
    expect(openModal).toHaveBeenCalledTimes(1);
    expect(setStatus).not.toHaveBeenCalled();
  });
});
