import { beforeEach, describe, expect, it, vi } from "vitest";

// Choosing a live source in the Data Source field: it starts, and when it
// still needs an address the cursor goes into that field.
describe("autoload_controls: choosing a source", () => {
  let bindAutoloadControls;

  beforeEach(async () => {
    ({ bindAutoloadControls } = await import("../modules/autoload_controls.js"));
    document.body.innerHTML = `
      <select id="autoload-mode">
        <option value="file">File</option>
        <option value="simplon">SIMPLON</option>
        <option value="remote">Remote</option>
        <option value="jungfraujoch">JUNGFRAUJOCH</option>
      </select>
      <input id="simplon-url" />
      <input id="jfjoch-preview-endpoint" />`;
  });

  function bind(autoload) {
    const state = { autoload: { mode: "file", running: false, watchEnabled: false, ...autoload } };
    const startAutoload = vi.fn(() => Promise.resolve());
    const updateAutoloadUI = vi.fn();
    const elements = {
      autoloadMode: document.getElementById("autoload-mode"),
      simplonUrl: document.getElementById("simplon-url"),
      jfjochEndpointInput: document.getElementById("jfjoch-preview-endpoint"),
    };
    bindAutoloadControls({
      apiBase: "/api",
      state,
      analysisState: { externalPeakSets: [] },
      backendIsLocal: () => true,
      elements,
      callbacks: {
        updateAutoloadUI,
        persistAutoloadSettings: vi.fn(),
        updateRemoteMetaUI: vi.fn(),
        openFileBrowser: vi.fn(),
        handleLocalFileSelection: vi.fn(),
        logClient: vi.fn(),
      },
      core: {
        autoloadOrchestration: { startAutoload, stopAutoload: vi.fn(() => Promise.resolve()) },
        frameMetadata: { loadFiles: vi.fn(() => Promise.resolve()) },
      },
    });
    return { state, elements, startAutoload, updateAutoloadUI };
  }

  async function choose(select, value) {
    select.value = value;
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await Promise.resolve();
    await Promise.resolve();
  }

  it("starts SIMPLON and puts the cursor in its empty address", async () => {
    const { state, elements, startAutoload } = bind({ simplonUrl: "" });
    await choose(elements.autoloadMode, "simplon");
    expect(state.autoload.mode).toBe("simplon");
    expect(startAutoload).toHaveBeenCalled();
    expect(document.activeElement).toBe(elements.simplonUrl);
  });

  it("puts the cursor in JUNGFRAUJOCH's empty preview endpoint", async () => {
    const { elements, startAutoload } = bind({ jfjochEndpoint: "" });
    await choose(elements.autoloadMode, "jungfraujoch");
    expect(startAutoload).toHaveBeenCalled();
    expect(document.activeElement).toBe(elements.jfjochEndpointInput);
  });

  // The header kept saying "Enter the detector's address" after it had been
  // typed and tested, because nothing redrew it when the address changed.
  it("redraws the section once an address is entered", async () => {
    const { state, elements, updateAutoloadUI } = bind({ mode: "simplon", simplonUrl: "" });
    updateAutoloadUI.mockClear();
    elements.simplonUrl.value = "10.0.0.1";
    elements.simplonUrl.dispatchEvent(new Event("change"));
    expect(state.autoload.simplonUrl).not.toBe("");
    expect(updateAutoloadUI).toHaveBeenCalled();

    updateAutoloadUI.mockClear();
    elements.jfjochEndpointInput.value = "host:5400";
    elements.jfjochEndpointInput.dispatchEvent(new Event("change"));
    expect(state.autoload.jfjochEndpoint).not.toBe("");
    expect(updateAutoloadUI).toHaveBeenCalled();
  });

  it("leaves the focus alone when the source has its address, or needs none", async () => {
    const { elements, startAutoload } = bind({ simplonUrl: "http://10.0.0.1:80" });
    elements.autoloadMode.focus();
    await choose(elements.autoloadMode, "simplon");
    expect(document.activeElement).toBe(elements.autoloadMode);
    await choose(elements.autoloadMode, "remote");
    expect(document.activeElement).toBe(elements.autoloadMode);
    expect(startAutoload).toHaveBeenCalledTimes(2);
  });
});
