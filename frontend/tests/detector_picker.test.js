import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { attachDetectorPicker } from "../modules/detector_picker.js";
import { createKnownDetectors } from "../modules/known_detectors.js";

function setup() {
  document.body.innerHTML = `
    <label class="field">
      <span>Base URL</span>
      <div class="inline"><input id="url" list="old-list" /><button id="go">Connect</button></div>
    </label>
    <button id="elsewhere">Elsewhere</button>`;
  const input = document.getElementById("url");
  const store = createKnownDetectors({ storageKey: "test.picker" });
  store.record({ address: "192.168.20.191", model: "PILATUS4 2M", serial: "P4-0042" });
  store.record({ address: "192.168.20.88", model: "EIGER2 X 9M", serial: "E-08-0123" });
  const onPick = vi.fn();
  const picker = attachDetectorPicker({ input, store, onPick });
  const list = document.querySelector(".detector-picker-list");
  const names = () => [...list.querySelectorAll(".detector-picker-name")].map((node) => node.textContent);
  const key = (name) => input.dispatchEvent(new KeyboardEvent("keydown", { key: name, bubbles: true, cancelable: true }));
  return { input, store, onPick, picker, list, names, key };
}

describe("known detectors under an address field", () => {
  beforeEach(() => localStorage.clear());
  afterEach(() => {
    document.body.innerHTML = "";
  });

  it("replaces the browser's suggestions with its own list, under the field and its buttons", () => {
    const { input, list } = setup();
    expect(input.hasAttribute("list")).toBe(false);
    expect(input.getAttribute("role")).toBe("combobox");
    expect(input.parentElement.classList.contains("detector-picker-field")).toBe(true);
    expect(input.closest(".inline").nextElementSibling).toBe(list);
    expect(list.hidden).toBe(true);
  });

  it("opens on focus, most recent first, and names each detector", () => {
    const { input, list, names } = setup();
    input.dispatchEvent(new FocusEvent("focus"));
    expect(list.hidden).toBe(false);
    expect(input.getAttribute("aria-expanded")).toBe("true");
    expect(names()).toEqual(["EIGER2 X 9M", "PILATUS4 2M"]);
    expect(list.querySelector(".detector-picker-meta").textContent).toBe("192.168.20.88 · E-08-0123");
  });

  it("narrows while typing and hides when nothing matches", () => {
    const { input, list, names } = setup();
    input.value = "pil";
    input.dispatchEvent(new Event("input"));
    expect(names()).toEqual(["PILATUS4 2M"]);
    input.value = "10.0.";
    input.dispatchEvent(new Event("input"));
    expect(list.hidden).toBe(true);
  });

  it("picks with the keys, and leaves Enter alone when nothing is highlighted", () => {
    const { input, onPick, key } = setup();
    input.dispatchEvent(new FocusEvent("focus"));
    expect(key("Enter")).toBe(true);
    expect(onPick).not.toHaveBeenCalled();
    key("ArrowDown");
    key("ArrowDown");
    expect(key("Enter")).toBe(false);
    expect(input.value).toBe("http://192.168.20.191");
    expect(onPick).toHaveBeenCalledWith(expect.objectContaining({ model: "PILATUS4 2M" }));
  });

  it("picks with a click", () => {
    const { input, list, onPick } = setup();
    input.dispatchEvent(new FocusEvent("focus"));
    list.querySelector("[role=option]").click();
    expect(input.value).toBe("http://192.168.20.88");
    expect(onPick).toHaveBeenCalledTimes(1);
    expect(list.hidden).toBe(true);
  });

  it("stars, renames and forgets in place", () => {
    const { input, list, store, names, onPick } = setup();
    input.dispatchEvent(new FocusEvent("focus"));
    list.querySelector("[role=option]:last-child .detector-picker-icon").click();
    expect(store.find("192.168.20.191").kept).toBe(true);
    // Starred ones on top, under their own heading.
    expect([...list.querySelectorAll(".detector-picker-head")].length).toBe(2);
    expect(names()).toEqual(["PILATUS4 2M", "EIGER2 X 9M"]);

    // ☆, the name, ✎, ×: the third child renames.
    list.querySelector("[role=option]").children[2].click();
    const field = list.querySelector(".detector-picker-rename");
    field.value = "Lab 2";
    field.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
    expect(store.find("192.168.20.191").name).toBe("Lab 2");

    const rows = list.querySelectorAll("[role=option]");
    rows[1].querySelector(".detector-picker-icon:last-child").click();
    expect(store.find("192.168.20.88")).toBe(null);
    expect(names()).toEqual(["Lab 2"]);
    expect(onPick).not.toHaveBeenCalled();
  });

  it("closes with Escape and when the focus leaves", () => {
    const { input, list, key } = setup();
    input.dispatchEvent(new FocusEvent("focus"));
    key("Escape");
    expect(list.hidden).toBe(true);
    key("ArrowDown");
    expect(list.hidden).toBe(false);
    input.dispatchEvent(new FocusEvent("focusout", { bubbles: true, relatedTarget: document.getElementById("elsewhere") }));
    expect(list.hidden).toBe(true);
  });

  it("has no ▾ while there is nothing to offer", () => {
    document.body.innerHTML = `<div class="inline"><input id="url" /></div>`;
    const store = createKnownDetectors({ storageKey: "test.empty" });
    attachDetectorPicker({ input: document.getElementById("url"), store });
    expect(document.querySelector(".detector-picker-toggle").hidden).toBe(true);
    store.record({ address: "det.local" });
    expect(document.querySelector(".detector-picker-toggle").hidden).toBe(false);
  });
});
