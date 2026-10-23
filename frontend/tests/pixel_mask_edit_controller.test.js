import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../modules/i18n.js", () => ({
  t: (key, vars = {}) => (Object.keys(vars).length ? `${key} ${JSON.stringify(vars)}` : key),
}));

import { USER_MASK_BIT } from "../modules/pixel_mask_edit.js";
import { createPixelMaskEditController, pickTint } from "../modules/pixel_mask_edit_controller.js";

function pointer(type, { x, y, button = 0, altKey = false, shiftKey = false, pointerId = 1 } = {}) {
  const event = new MouseEvent(type, { bubbles: true, cancelable: true, button, altKey, shiftKey, clientX: x, clientY: y });
  Object.defineProperty(event, "pointerId", { value: pointerId });
  Object.defineProperty(event, "pointerType", { value: "mouse" });
  return event;
}

function key(type, k, extra = {}) {
  return new KeyboardEvent(type, { key: k, bubbles: true, cancelable: true, ...extra });
}

const opened = [];

function setup({ width = 8, height = 6 } = {}) {
  document.body.innerHTML = "";
  const canvasWrap = document.createElement("div");
  const bar = document.createElement("div");
  bar.hidden = true;
  const mk = (tag, attrs = {}) => {
    const el = document.createElement(tag);
    Object.entries(attrs).forEach(([name, value]) => el.setAttribute(name, value));
    bar.appendChild(el);
    return el;
  };
  const toolButtons = ["brush", "box", "polygon", "circle"].map((tool) => mk("button", { "data-mask-tool": tool }));
  const modeButtons = ["mask", "unmask"].map((mode) => mk("button", { "data-mask-mode": mode }));
  const brushSize = mk("select");
  ["1", "3"].forEach((value) => {
    const option = document.createElement("option");
    option.value = value;
    brushSize.appendChild(option);
  });
  const undoBtn = mk("button");
  const redoBtn = mk("button");
  const doneBtn = mk("button");
  const countEl = mk("span");
  document.body.append(canvasWrap, bar);

  const state = { width, height };
  const target = {
    data: new Uint32Array(width * height),
    base: new Uint32Array(width * height),
    width,
    height,
  };
  // The viewer's own handler, which must not see what the editor takes.
  const viewerDown = vi.fn();
  canvasWrap.addEventListener("pointerdown", viewerDown);
  const callbacks = {
    getTarget: vi.fn(() => target),
    onCommit: vi.fn(() => {
      target.data = target.data.slice();
    }),
    onClose: vi.fn(),
    getImagePoint: (event) => ({ x: event.clientX, y: event.clientY }),
    getView: () => ({ scaleX: 10, scaleY: 10, offsetX: 0, offsetY: 0 }),
    syncOverlayCanvas: () => null,
    describeEdits: () => "count",
  };
  const editor = createPixelMaskEditController({
    state,
    elements: { canvasWrap, bar, toolButtons, modeButtons, brushSize, undoBtn, redoBtn, doneBtn, countEl },
    callbacks,
    isMac: true,
  });
  opened.push(editor);
  const at = (x, y) => target.data[y * width + x];
  return { editor, canvasWrap, bar, target, callbacks, viewerDown, brushSize, undoBtn, at };
}

beforeEach(() => {
  // One editor at a time, as in the app: an open one listens on window.
  opened.splice(0).forEach((editor) => editor.close());
  vi.restoreAllMocks();
});

describe("pixel mask editor", () => {
  it("leaves the image alone until opened", () => {
    const { canvasWrap, viewerDown, bar } = setup();
    canvasWrap.dispatchEvent(pointer("pointerdown", { x: 1, y: 1 }));
    expect(viewerDown).toHaveBeenCalledTimes(1);
    expect(bar.hidden).toBe(true);
  });

  it("paints with the brush, joining a fast drag, as one undo step", () => {
    const { editor, canvasWrap, viewerDown, at, callbacks, undoBtn, bar } = setup();
    expect(editor.open()).toBe(true);
    expect(bar.hidden).toBe(false);
    canvasWrap.dispatchEvent(pointer("pointerdown", { x: 0, y: 0 }));
    canvasWrap.dispatchEvent(pointer("pointermove", { x: 3, y: 0 }));
    canvasWrap.dispatchEvent(pointer("pointerup", { x: 3, y: 0 }));
    expect(viewerDown).not.toHaveBeenCalled();
    expect([at(0, 0), at(1, 0), at(2, 0), at(3, 0), at(4, 0)]).toEqual([
      USER_MASK_BIT, USER_MASK_BIT, USER_MASK_BIT, USER_MASK_BIT, 0,
    ]);
    expect(callbacks.onCommit).toHaveBeenCalledTimes(1);
    expect(undoBtn.disabled).toBe(false);

    undoBtn.click();
    expect([at(0, 0), at(3, 0)]).toEqual([0, 0]);
    expect(callbacks.onCommit).toHaveBeenCalledTimes(2);
  });

  it("masks a dragged box, and unmasks with Alt held", () => {
    const { editor, canvasWrap, target, at } = setup();
    editor.open();
    editor.setTool("box");
    canvasWrap.dispatchEvent(pointer("pointerdown", { x: 1, y: 1 }));
    canvasWrap.dispatchEvent(pointer("pointermove", { x: 3, y: 2 }));
    canvasWrap.dispatchEvent(pointer("pointerup", { x: 3, y: 2 }));
    expect(Array.from(target.data).filter(Boolean)).toHaveLength(6);

    canvasWrap.dispatchEvent(pointer("pointerdown", { x: 2, y: 1, altKey: true }));
    canvasWrap.dispatchEvent(pointer("pointerup", { x: 2, y: 1, altKey: true }));
    expect(at(2, 1)).toBe(0);
    expect(at(1, 1)).toBe(USER_MASK_BIT);
  });

  it("closes a polygon on its first corner", () => {
    const { editor, canvasWrap, target, callbacks } = setup();
    editor.open();
    editor.setTool("polygon");
    for (const [x, y] of [[1, 1], [4, 1], [4, 4], [1, 1]]) {
      canvasWrap.dispatchEvent(pointer("pointerdown", { x, y }));
      canvasWrap.dispatchEvent(pointer("pointerup", { x, y }));
    }
    expect(callbacks.onCommit).toHaveBeenCalledTimes(1);
    const masked = Array.from(target.data).filter(Boolean).length;
    expect(masked).toBeGreaterThan(6);
  });

  it("lets Space pan, Shift set the contrast and the right button draw an ROI", () => {
    const { editor, canvasWrap, viewerDown, target } = setup();
    editor.open();
    canvasWrap.dispatchEvent(pointer("pointerdown", { x: 1, y: 1, shiftKey: true }));
    canvasWrap.dispatchEvent(pointer("pointerdown", { x: 1, y: 1, button: 2 }));
    window.dispatchEvent(key("keydown", " "));
    canvasWrap.dispatchEvent(pointer("pointerdown", { x: 1, y: 1 }));
    window.dispatchEvent(key("keyup", " "));
    expect(viewerDown).toHaveBeenCalledTimes(3);
    expect(Array.from(target.data).every((v) => v === 0)).toBe(true);
  });

  it("switches tools by key and undoes with the platform's shortcut", () => {
    const { editor, canvasWrap, at } = setup();
    editor.open();
    window.dispatchEvent(key("keydown", "r"));
    expect(editor.tool).toBe("box");
    window.dispatchEvent(key("keydown", "x"));
    expect(editor.mode).toBe("unmask");
    window.dispatchEvent(key("keydown", "x"));
    canvasWrap.dispatchEvent(pointer("pointerdown", { x: 0, y: 0 }));
    canvasWrap.dispatchEvent(pointer("pointerup", { x: 0, y: 0 }));
    expect(at(0, 0)).toBe(USER_MASK_BIT);
    window.dispatchEvent(key("keydown", "z", { metaKey: true }));
    expect(at(0, 0)).toBe(0);
    window.dispatchEvent(key("keydown", "z", { metaKey: true, shiftKey: true }));
    expect(at(0, 0)).toBe(USER_MASK_BIT);
  });

  it("does not take keys typed into a field", () => {
    const { editor } = setup();
    editor.open();
    const input = document.createElement("input");
    document.body.appendChild(input);
    input.dispatchEvent(key("keydown", "r"));
    expect(editor.tool).toBe("brush");
  });

  it("cancels a polygon with Escape, then closes with a second one", () => {
    const { editor, canvasWrap, callbacks, bar } = setup();
    editor.open();
    editor.setTool("polygon");
    canvasWrap.dispatchEvent(pointer("pointerdown", { x: 1, y: 1 }));
    window.dispatchEvent(key("keydown", "Escape"));
    expect(editor.active).toBe(true);
    window.dispatchEvent(key("keydown", "Escape"));
    expect(editor.active).toBe(false);
    expect(bar.hidden).toBe(true);
    expect(callbacks.onClose).toHaveBeenCalledTimes(1);
    expect(callbacks.onCommit).not.toHaveBeenCalled();
  });

  it("does not open on a mask that does not fit the image", () => {
    const { editor, callbacks } = setup();
    callbacks.getTarget.mockReturnValue({ data: new Uint32Array(4), base: new Uint32Array(4), width: 2, height: 2 });
    expect(editor.open()).toBe(false);
    expect(editor.active).toBe(false);
  });
});

describe("edit tints", () => {
  it("steer clear of a mask colour that is already magenta", () => {
    const candidates = [[255, 0, 255], [255, 140, 0]];
    expect(pickTint(candidates, [[0, 0, 0], [25, 50, 120], [88, 183, 198]])).toEqual([255, 0, 255]);
    expect(pickTint(candidates, [[0, 0, 0], [25, 50, 120], [255, 0, 255]])).toEqual([255, 140, 0]);
  });
});
