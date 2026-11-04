import { afterEach, describe, expect, it, vi } from "vitest";
import fs from "node:fs";

// The update dialog's "Don't check for updates at startup". It is offered only
// when the dialog opened by itself, never when the environment decides the
// setting, and it acts when the dialog closes, so a tick can be taken back.

const UPDATE = {
  status: "update_available",
  current_version: "0.9.2",
  latest_version: "0.9.3",
  release_url: "https://example.invalid/releases/v0.9.3",
  message: "",
};

async function setup({ locked = false, disable = vi.fn(async () => "saved") } = {}) {
  vi.resetModules();
  const notifySuccess = vi.fn();
  const notifyError = vi.fn();
  vi.doMock("../modules/toast.js", () => ({ notifySuccess, notifyError }));
  document.body.innerHTML = `
    <div id="modal" class="modal">
      <div class="modal-backdrop"></div>
      <div id="message"></div>
      <button id="action" type="button" hidden></button>
      <button id="close" type="button">Close</button>
      <label id="skip-row" hidden><input type="checkbox" id="skip" /></label>
    </div>`;
  const catalogue = JSON.parse(fs.readFileSync("frontend/locales/en.json", "utf8"));
  global.fetch = vi.fn(async (url) => ({
    ok: true,
    json: async () => (String(url).endsWith("/api/update-check") ? UPDATE : catalogue),
  }));
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createUpdateCheckController } = await import("../modules/update_check_controller.js");
  const byId = (id) => document.getElementById(id);
  const settings = { isEnvOverridden: vi.fn((key) => locked && key === "ui.auto_check_updates"), disableStartupUpdateCheck: disable };
  const controller = createUpdateCheckController({
    core: { settings },
    apiBase: "/api",
    state: { backendVersion: "0.9.2" },
    elements: {
      updateCheckModal: byId("modal"),
      updateCheckMessage: byId("message"),
      updateCheckAction: byId("action"),
      updateCheckClose: byId("close"),
      updateCheckSkipStartupRow: byId("skip-row"),
      updateCheckSkipStartup: byId("skip"),
    },
    callbacks: { openModal: vi.fn(() => true), closeModal: vi.fn(() => true) },
  });
  const tickAndClose = async () => {
    byId("skip").checked = true;
    byId("close").click();
    await Promise.resolve();
    await Promise.resolve();
  };
  return { controller, settings, byId, notifySuccess, notifyError, tickAndClose };
}

afterEach(() => {
  vi.doUnmock("../modules/toast.js");
  delete global.fetch;
});

describe("update dialog: don't check for updates at startup", () => {
  it("is offered when the dialog opened by itself at startup", async () => {
    const { controller, byId } = await setup();
    await controller.checkOnStartup();
    expect(byId("skip-row").hidden).toBe(false);
  });

  it("is not offered when the dialog was opened from Help", async () => {
    const { controller, byId } = await setup();
    await controller.openAndCheck();
    expect(byId("skip-row").hidden).toBe(true);
  });

  it("is not offered when the environment sets the update check", async () => {
    const { controller, byId } = await setup({ locked: true });
    await controller.checkOnStartup();
    expect(byId("skip-row").hidden).toBe(true);
  });

  it("switches the startup check off when the dialog closes ticked, and says how to undo it", async () => {
    const { controller, settings, notifySuccess, tickAndClose, byId } = await setup();
    await controller.checkOnStartup();
    await tickAndClose();
    expect(settings.disableStartupUpdateCheck).toHaveBeenCalledOnce();
    expect(notifySuccess).toHaveBeenCalledWith(expect.stringContaining("Settings → Connection"));
    expect(byId("skip").checked).toBe(false);
    expect(byId("skip-row").hidden).toBe(true);
  });

  it("changes nothing when the dialog closes unticked", async () => {
    const { controller, settings, byId } = await setup();
    await controller.checkOnStartup();
    byId("close").click();
    expect(settings.disableStartupUpdateCheck).not.toHaveBeenCalled();
  });

  it("says so when saving fails", async () => {
    const disable = vi.fn(async () => {
      throw Object.assign(new Error("x"), { detail: "Failed to save config" });
    });
    const { controller, notifyError, tickAndClose } = await setup({ disable });
    await controller.checkOnStartup();
    await tickAndClose();
    expect(notifyError).toHaveBeenCalledWith(expect.stringContaining("Failed to save config"));
  });
});
