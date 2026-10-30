import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { chooseNativePath } from "../modules/native_picker.js";

/**
 * Seven controls used to read /api/choose-file and /api/choose-folder each in
 * their own way. These pin the one reading they now share: a path, "" for a
 * cancel, and the translated error (status 409: no native dialog) otherwise.
 */

describe("chooseNativePath", () => {
  beforeEach(() => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete globalThis.fetch;
  });

  it("asks the folder chooser with its purpose and the interface language", async () => {
    globalThis.fetch = vi.fn(async () => ({ ok: true, status: 200, json: async () => ({ path: "/data/run7" }) }));
    await expect(chooseNativePath("/api", { kind: "folder", purpose: "autoload" })).resolves.toBe("/data/run7");
    expect(globalThis.fetch.mock.calls[0][0]).toMatch(/^\/api\/choose-folder\?purpose=autoload&lang=[\w-]+$/);
  });

  it("limits a file chooser to the given extensions, encoded", async () => {
    globalThis.fetch = vi.fn(async () => ({ ok: true, status: 200, json: async () => ({ path: "/f.tif" }) }));
    await chooseNativePath("/api", { exts: ".tif,.tiff,.cbf" });
    expect(globalThis.fetch.mock.calls[0][0]).toMatch(/^\/api\/choose-file\?exts=\.tif%2C\.tiff%2C\.cbf&lang=/);
  });

  it("reads a cancelled dialog (204) as no path, not a failure", async () => {
    const json = vi.fn();
    globalThis.fetch = vi.fn(async () => ({ ok: true, status: 204, json }));
    await expect(chooseNativePath("/api", { kind: "folder" })).resolves.toBe("");
    expect(json).not.toHaveBeenCalled();
  });

  it("reads an answer without a path as no path", async () => {
    globalThis.fetch = vi.fn(async () => ({ ok: true, status: 200, json: async () => ({}) }));
    await expect(chooseNativePath("/api")).resolves.toBe("");
  });

  it("rejects with status 409 when this machine has no native dialog", async () => {
    globalThis.fetch = vi.fn(async () => ({ ok: false, status: 409, json: async () => ({ detail: "no display" }) }));
    await expect(chooseNativePath("/api")).rejects.toMatchObject({ status: 409 });
  });
});
