import fs from "node:fs";
import path from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

// The real English catalogue, so a renamed status key fails here instead of
// quietly degrading to a raw key in the UI.
const EN = JSON.parse(
  fs.readFileSync(path.join(process.cwd(), "frontend", "locales", "en.json"), "utf8")
);

const isHdfFile = (file) => /\.(h5|hdf5)$/i.test(String(file || ""));

function makeState(overrides = {}) {
  return {
    file: "",
    dataset: "",
    frameCount: 1,
    thresholdCount: 1,
    dataExport: {
      running: false,
      cancelling: false,
      jobId: "",
      progress: 0,
      message: "",
      outputs: [],
      openTarget: "",
      sourceKey: "",
      autoOutputDir: "",
      autoOutputPrefix: "",
    },
    ...overrides,
  };
}

async function build(state) {
  vi.resetModules();
  global.fetch = vi.fn(async () => ({ ok: true, json: async () => EN }));
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createDataExportController } = await import("../modules/data_export_controller.js");
  const setStatus = vi.fn();
  const openModal = vi.fn();
  const controller = createDataExportController({
    apiBase: "/api",
    state,
    elements: {},
    callbacks: { isHdfFile, openModal, closeModal: vi.fn(), setStatus },
  });
  return { controller, setStatus, openModal };
}

describe("data_export_controller", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("refuses with no file open with a toast rather than silently", async () => {
    const { controller, setStatus, openModal } = await build(makeState());
    controller.openDialog();
    expect(openModal).not.toHaveBeenCalled();
    expect(setStatus).toHaveBeenCalledWith(EN["status.data_export.no_file"], { tone: "warning" });
  });

  it("refuses an HDF5 file with no dataset with a toast rather than silently", async () => {
    const { controller, setStatus, openModal } = await build(
      makeState({ file: "/data/stack.h5" })
    );
    controller.openDialog();
    expect(openModal).not.toHaveBeenCalled();
    expect(setStatus).toHaveBeenCalledWith(EN["status.data_export.no_dataset"], {
      tone: "warning",
    });
  });

  it("opens the dialog for an exportable source without a warning", async () => {
    const { controller, setStatus, openModal } = await build(
      makeState({ file: "/data/frame.cbf" })
    );
    controller.openDialog();
    expect(openModal).toHaveBeenCalledTimes(1);
    expect(setStatus).not.toHaveBeenCalled();
  });
});
