import { afterEach, describe, expect, it, vi } from "vitest";
import { createUploadFlowController } from "../modules/upload_flow.js";

// A minimal XMLHttpRequest that answers every upload with the given response.
function installXhr(status, response) {
  const sent = [];
  class FakeXhr {
    constructor() {
      this.listeners = {};
      this.upload = { addEventListener: () => {} };
    }
    open(method, url) {
      this.url = url;
    }
    addEventListener(name, fn) {
      this.listeners[name] = fn;
    }
    send() {
      sent.push(this.url);
      this.status = status;
      this.response = response;
      queueMicrotask(() => this.listeners.load());
    }
  }
  global.XMLHttpRequest = FakeXhr;
  return sent;
}

function buildController(fetchJSON) {
  const statuses = [];
  const opened = [];
  const controller = createUploadFlowController({
    apiBase: "/api",
    state: { autoload: { dir: "/data/run" } },
    elements: {},
    callbacks: {
      ensureFileMode: async () => {},
      setLoading: () => {},
      setStatus: (text) => statuses.push(text),
      loadFiles: async () => {},
      openPathInViewer: async (path) => opened.push(path),
      fileLabel: (path) => path,
      fetchJSON,
    },
  });
  return { controller, statuses, opened };
}

const file = new File([new Uint8Array(10)], "scan_data_000001.h5");

describe("upload flow", () => {
  afterEach(() => {
    delete global.XMLHttpRequest;
  });

  it("reopens a copy already on the server only by name and size", async () => {
    const fetchJSON = vi.fn(async () => ({ path: "run/scan_data_000001.h5" }));
    const sent = installXhr(200, {});
    const { controller, opened } = buildController(fetchJSON);

    await controller.uploadAndOpenSelectedFiles([file]);

    const url = new URL(fetchJSON.mock.calls[0][0], "http://host");
    expect(url.pathname).toBe("/api/upload/existing");
    expect(url.searchParams.get("name")).toBe("scan_data_000001.h5");
    expect(url.searchParams.get("size")).toBe("10");
    expect(url.searchParams.get("folder")).toBe("/data/run");
    expect(sent).toEqual([]);
    expect(opened).toEqual(["run/scan_data_000001.h5"]);
  });

  it("uploads with the file's size and shows why the server refused it", async () => {
    const fetchJSON = vi.fn(async () => ({ path: null }));
    const sent = installXhr(422, { detail: "Upload of scan_data_000001.h5 is incomplete" });
    const { controller, statuses, opened } = buildController(fetchJSON);
    vi.spyOn(console, "error").mockImplementation(() => {});

    await controller.uploadAndOpenSelectedFiles([file]);

    const url = new URL(sent[0], "http://host");
    expect(url.pathname).toBe("/api/upload");
    expect(url.searchParams.get("size")).toBe("10");
    expect(opened).toEqual([]);
    expect(statuses.at(-1)).toContain("Upload of scan_data_000001.h5 is incomplete");
  });
});
