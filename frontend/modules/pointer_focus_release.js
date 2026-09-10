/**
 * Return focus to the document after a toolbar button is clicked with a
 * pointer, so the viewer keeps its keyboard shortcuts.
 *
 * `Tab` toggles playback, for parity with ALBULA, but only when focus is not
 * already on a control -- otherwise it would swallow the one key that moves
 * keyboard focus, which is what it used to do across the whole window. A
 * clicked button keeps focus in every engine, so without this the shortcut
 * stopped working the moment the user pressed the toolbar's own play button,
 * which is the most likely way to reach for it.
 *
 * Only pointer activation releases focus. A button activated from the
 * keyboard fires `click` with `detail === 0`, and a keyboard user must keep
 * focus where they put it -- moving it would be the accessibility bug this
 * exists to avoid, in the opposite direction.
 */

const TOOLBAR_BUTTON = ".tool-btn";

export function createPointerFocusRelease({ root = document, selector = TOOLBAR_BUTTON } = {}) {
  function handleClick(event) {
    // detail is the click count: 0 for a synthetic click from Enter or Space,
    // 1+ for a real pointer press. `pointerType === ""` on some engines is not
    // reliable enough to use instead.
    if (!event.detail) return;
    const target = event.target;
    if (!target || typeof target.closest !== "function") return;
    const button = target.closest(selector);
    if (!button || typeof button.blur !== "function") return;
    // Only if the click actually left focus on the button; a handler that
    // moved focus somewhere deliberate must not be undone.
    if (root.activeElement !== button) return;
    button.blur();
  }

  function attach() {
    root.addEventListener("click", handleClick);
  }

  function detach() {
    root.removeEventListener("click", handleClick);
  }

  return { attach, detach, handleClick };
}
