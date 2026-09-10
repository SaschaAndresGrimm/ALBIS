/**
 * Keyboard shortcut handlers.
 */

function isFormElement(target) {
  if (!target) return false;
  if (target.isContentEditable) return true;
  const tag = target.tagName ? target.tagName.toLowerCase() : "";
  if (["input", "textarea", "select", "option"].includes(tag)) return true;
  return Boolean(target.closest?.("input, textarea, select, [contenteditable='true']"));
}

// Anything the browser would move focus to with Tab. Wider than isFormElement,
// which covers only text-entry controls: the window is mostly buttons, and
// they are exactly what Tab has to be able to reach.
const FOCUSABLE_SELECTOR = [
  "a[href]",
  "button:not([disabled])",
  "input:not([disabled])",
  "select:not([disabled])",
  "textarea:not([disabled])",
  // A <summary> is a tab stop with no tabindex and no disabled state, so it
  // matched none of the rules above. index.html has two in visible panel
  // sections ("Advanced view controls", "Advanced source settings"), and the
  // trap this guard exists to close was still open on both of them.
  "summary",
  "audio[controls]",
  "video[controls]",
  "iframe",
  '[tabindex]:not([tabindex="-1"])',
  '[contenteditable=""]',
  '[contenteditable="true"]',
  '[contenteditable="plaintext-only"]',
].join(", ");

function isFocusableChrome(target) {
  if (!target || typeof target.closest !== "function") return false;
  return Boolean(target.closest(FOCUSABLE_SELECTOR));
}

export function createShortcutHandlers({
  state,
  elements,
  callbacks,
}) {
  const {
    thresholdSelect,
    toolbarThresholdSelect,
    toolbarMoreThreshold,
  } = elements;

  const {
    handleMenuAction,
    isCommandPaletteOpen,
    closeCommandPalette,
    openCommandPalette,
    openFileModal,
    closeCurrentFile,
    stopPlayback,
    startPlayback,
    setThresholdIndex,
    getThresholdIndexAtOffset,
    toggleFullscreen,
    togglePanel,
    requestFrame,
  } = callbacks;

  function handleShortcut(event) {
    const isMod = event.metaKey || event.ctrlKey;
    if (!isMod) return;
    const key = event.key.toLowerCase();
    const isShift = event.shiftKey;
    const isAlt = event.altKey;
    if (
      ["o", "s", "n", "w", ",", "k", "g"].includes(key) ||
      (key === "x" && isShift && !isAlt)
    ) {
      event.preventDefault();
    }
    switch (key) {
      case "o":
        openFileModal();
        break;
      case "g":
        if (!isShift && !isAlt) {
          handleMenuAction("export-animation");
        }
        break;
      case "w":
        closeCurrentFile();
        break;
      case "s":
        if (isAlt) {
          handleMenuAction("save-window");
        } else if (isShift) {
          handleMenuAction("save-visible");
        } else {
          handleMenuAction("save-full");
        }
        break;
      case "x":
        if (isShift && !isAlt) {
          handleMenuAction("export-data");
        }
        break;
      case "n":
        handleMenuAction("new-window");
        break;
      case ",":
        handleMenuAction("settings-open");
        break;
      case "k":
        if (isCommandPaletteOpen()) {
          closeCommandPalette();
        } else {
          openCommandPalette();
        }
        break;
      default:
        break;
    }
  }

  function handleNavShortcut(event) {
    if (event.metaKey || event.ctrlKey || event.altKey) return false;
    if (event.key === "Tab" || event.keyCode === 9) {
      // Tab toggles playback for ALBULA parity (README.md), but Tab is also the
      // only key that moves keyboard focus. This branch used to sit above the
      // isFormElement guard and preventDefault() unconditionally, so focus
      // could not reach any of the window's ~265 controls and Shift+Tab did
      // nothing either -- a WCAG 2.1.1 failure across the whole shell.
      //
      // Claim the key only when focus is somewhere that is not itself a
      // stop on the tab ring, which is the canvas or the body in practice.
      // Shift+Tab is never a playback gesture.
      if (event.shiftKey) return false;
      if (isFocusableChrome(event.target)) return false;
      event.preventDefault();
      if (state.playing) {
        stopPlayback();
      } else {
        startPlayback();
      }
      return true;
    }

    const hasThresholds = state.thresholdCount > 1 && state.autoload.mode !== "simplon";
    const isThresholdTarget =
      event.target === thresholdSelect || event.target === toolbarThresholdSelect || event.target === toolbarMoreThreshold;

    if (hasThresholds && (event.key === "ArrowUp" || event.key === "ArrowDown")) {
      if (!isThresholdTarget && isFormElement(event.target)) return false;
      event.preventDefault();
      stopPlayback();
      const delta = event.key === "ArrowUp" ? -1 : 1;
      void setThresholdIndex(getThresholdIndexAtOffset(delta));
      return true;
    }

    if (isFormElement(event.target)) return false;

    if (!event.metaKey && !event.ctrlKey && !event.altKey && event.key.toLowerCase() === "f") {
      event.preventDefault();
      void toggleFullscreen();
      return true;
    }

    switch (event.key) {
      case "m":
      case "M":
        event.preventDefault();
        togglePanel();
        return true;
      case "ArrowLeft":
        event.preventDefault();
        stopPlayback();
        requestFrame(state.frameIndex - 1);
        return true;
      case "ArrowRight":
        event.preventDefault();
        stopPlayback();
        requestFrame(state.frameIndex + 1);
        return true;
      case "ArrowUp": {
        event.preventDefault();
        stopPlayback();
        const step = Math.max(1, state.step || 1);
        requestFrame(state.frameIndex - step);
        return true;
      }
      case "ArrowDown": {
        event.preventDefault();
        stopPlayback();
        const step = Math.max(1, state.step || 1);
        requestFrame(state.frameIndex + step);
        return true;
      }
      default:
        return false;
    }
  }

  return {
    handleShortcut,
    handleNavShortcut,
  };
}
