import { beforeEach, describe, expect, it, vi } from "vitest";
import fs from "node:fs";
import path from "node:path";

function readLocale(language) {
  const filePath = path.join(process.cwd(), "frontend", "locales", `${language}.json`);
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function buildFetchMock() {
  return vi.fn(async (url) => {
    const match = String(url).match(/locales\/([^/]+)\.json/);
    if (match) {
      return { ok: true, json: async () => readLocale(decodeURIComponent(match[1])) };
    }
    throw new Error(`Unexpected fetch: ${url}`);
  });
}

async function setup({ entries = [], openPath = vi.fn(async () => {}) } = {}) {
  vi.resetModules();
  localStorage.clear();
  document.body.innerHTML = `
    <div class="dropdown-item dropdown-submenu-parent" id="recent-files-parent">
      <div class="dropdown-submenu" id="recent-files-submenu"></div>
    </div>
  `;
  global.fetch = buildFetchMock();

  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });

  const { createRecentFiles } = await import("../modules/recent_files.js");
  const { createRecentFilesController } = await import("../modules/recent_files_controller.js");

  const recentFiles = createRecentFiles();
  entries.forEach((entry) => recentFiles.record(entry));

  const closeMenu = vi.fn();
  const setStatus = vi.fn();
  const controller = createRecentFilesController({
    recentFiles,
    elements: {
      submenu: document.getElementById("recent-files-submenu"),
      parent: document.getElementById("recent-files-parent"),
    },
    callbacks: { openPath, closeMenu, setStatus },
  });

  return { controller, recentFiles, openPath, closeMenu, setStatus, locale: readLocale("en") };
}

function items() {
  return Array.from(document.querySelectorAll("#recent-files-submenu .dropdown-item"));
}

describe("recent files submenu", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it("says so when there is nothing to reopen", async () => {
    const { controller, locale } = await setup();

    controller.render();

    const entries = items();
    expect(entries).toHaveLength(1);
    expect(entries[0].textContent).toBe(locale["menu.file.no_recent_files"]);
    expect(entries[0].classList.contains("is-disabled")).toBe(true);
  });

  it("lists the most recent file first, by name", async () => {
    const { controller } = await setup({
      entries: ["/data/run_01/first.h5", "/gpfs/beamline/second.cbf"],
    });

    controller.render();

    const names = items()
      .filter((item) => item.querySelector(".recent-file-name"))
      .map((item) => item.textContent);
    expect(names).toEqual(["second.cbf", "first.h5"]);
  });

  it("keeps the full path reachable, since two runs share a file name", async () => {
    const { controller } = await setup({ entries: ["/gpfs/visit_a/frame_0001.cbf"] });

    controller.render();

    expect(items()[0].title).toBe("/gpfs/visit_a/frame_0001.cbf");
  });

  it("opens the file that was clicked and closes the menu", async () => {
    const { controller, openPath, closeMenu } = await setup({ entries: ["/data/a.h5"] });
    controller.render();

    items()[0].click();
    await vi.waitFor(() => expect(openPath).toHaveBeenCalledWith("/data/a.h5"));

    expect(closeMenu).toHaveBeenCalled();
  });

  it("drops an entry that can no longer be opened, and says why", async () => {
    // A scratch directory gets cleared and a mount gets unplugged; a dead entry
    // that keeps failing is worse than one that removes itself.
    const openPath = vi.fn(async () => {
      throw new Error("File not found");
    });
    const { controller, recentFiles, setStatus } = await setup({
      entries: ["/data/gone.h5", "/data/still_here.h5"],
      openPath,
    });
    controller.render();

    const gone = items().find((item) => item.title === "/data/gone.h5");
    gone.click();
    await vi.waitFor(() => expect(setStatus).toHaveBeenCalled());

    expect(recentFiles.list()).toEqual(["/data/still_here.h5"]);
    expect(setStatus.mock.calls[0][0]).toContain("gone.h5");
    expect(items().some((item) => item.title === "/data/gone.h5")).toBe(false);
  });

  it("offers a way to forget the list, and forgets it", async () => {
    const { controller, recentFiles, locale } = await setup({ entries: ["/data/a.h5"] });
    controller.render();

    const clear = items().find((item) => item.textContent === locale["menu.file.clear_recent"]);
    expect(clear).toBeTruthy();
    clear.click();

    expect(recentFiles.list()).toEqual([]);
    expect(items()[0].textContent).toBe(locale["menu.file.no_recent_files"]);
  });

  it("records an opened file and shows it on the next render", async () => {
    const { controller } = await setup();

    controller.recordOpened("/data/opened.h5");
    controller.render();

    expect(items()[0].title).toBe("/data/opened.h5");
  });

  it("renders again without stacking duplicate items", async () => {
    const { controller } = await setup({ entries: ["/data/a.h5"] });

    controller.render();
    controller.render();

    expect(items().filter((item) => item.title === "/data/a.h5")).toHaveLength(1);
  });
});
