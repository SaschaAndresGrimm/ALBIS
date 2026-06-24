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
    if (["o", "s", "n", "w", ",", "k"].includes(key) || (key === "x" && isShift && !isAlt)) {
      event.preventDefault();
    }
    switch (key) {
      case "o":
        openFileModal();
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
