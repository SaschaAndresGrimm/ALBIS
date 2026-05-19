import { afterEach, describe, expect, it, vi } from "vitest";

import { bindFileIngress } from "../modules/file_ingress_bindings.js";

const cleanups = [];

function makeFile(name = "frame_0001.cbf") {
  return new File(["test"], name, { type: "application/octet-stream" });
}

function makeFileTransferEvent(type, files = [makeFile()]) {
  const event = new Event(type, { bubbles: true, cancelable: true });
  const dataTransfer = {
    types: ["Files"],
    files,
    dropEffect: "copy",
  };
  Object.defineProperty(event, "dataTransfer", {
    configurable: true,
    value: dataTransfer,
  });
  return { event, dataTransfer };
}

function setupIngress(options = {}) {
  document.body.innerHTML = `
    <input id="file-input" type="file" />
    <div id="canvas-shell"></div>
  `;
  const fileInput = document.getElementById("file-input");
  const canvasShell = document.getElementById("canvas-shell");
  const onFilesSelected = vi.fn(async () => {});
  const onDocumentDropDisabled = vi.fn();
  cleanups.push(bindFileIngress({
    fileInput,
    canvasShell,
    onFilesSelected,
    allowDocumentDrop: options.allowDocumentDrop,
    onDocumentDropDisabled,
  }));
  return {
    fileInput,
    canvasShell,
    onFilesSelected,
    onDocumentDropDisabled,
  };
}

afterEach(() => {
  while (cleanups.length) {
    cleanups.pop()?.();
  }
  document.body.innerHTML = "";
  vi.restoreAllMocks();
});

describe("file_ingress_bindings", () => {
  it("disables document drops without invoking uploads when local drops are disabled", () => {
    const { canvasShell, onFilesSelected, onDocumentDropDisabled } = setupIngress({
      allowDocumentDrop: false,
    });

    const drag = makeFileTransferEvent("dragover");
    document.dispatchEvent(drag.event);

    expect(drag.event.defaultPrevented).toBe(true);
    expect(drag.dataTransfer.dropEffect).toBe("none");
    expect(canvasShell.classList.contains("is-file-drop-target")).toBe(false);

    const drop = makeFileTransferEvent("drop");
    document.dispatchEvent(drop.event);

    expect(drop.event.defaultPrevented).toBe(true);
    expect(drop.dataTransfer.dropEffect).toBe("none");
    expect(onFilesSelected).not.toHaveBeenCalled();
    expect(onDocumentDropDisabled).toHaveBeenCalledTimes(1);
  });

  it("keeps document drag-and-drop upload behavior when enabled", async () => {
    const { canvasShell, onFilesSelected, onDocumentDropDisabled } = setupIngress({
      allowDocumentDrop: true,
    });
    const files = [makeFile("scan_0001.tiff")];

    const drag = makeFileTransferEvent("dragover", files);
    document.dispatchEvent(drag.event);

    expect(drag.event.defaultPrevented).toBe(true);
    expect(drag.dataTransfer.dropEffect).toBe("copy");
    expect(canvasShell.classList.contains("is-file-drop-target")).toBe(true);

    const drop = makeFileTransferEvent("drop", files);
    document.dispatchEvent(drop.event);
    await Promise.resolve();

    expect(canvasShell.classList.contains("is-file-drop-target")).toBe(false);
    expect(onFilesSelected).toHaveBeenCalledWith(files);
    expect(onDocumentDropDisabled).not.toHaveBeenCalled();
  });

  it("leaves hidden file input selection enabled when document drops are disabled", async () => {
    const { fileInput, onFilesSelected } = setupIngress({
      allowDocumentDrop: false,
    });
    const files = [makeFile("picked_0001.h5")];
    Object.defineProperty(fileInput, "files", {
      configurable: true,
      value: files,
    });

    fileInput.dispatchEvent(new Event("change", { bubbles: true }));
    await Promise.resolve();

    expect(onFilesSelected).toHaveBeenCalledWith(files);
  });
});
