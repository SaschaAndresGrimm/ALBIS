/**
 * Command palette controller.
 */

import { t } from "./i18n.js";

export function createCommandPaletteController({
  elements,
  callbacks,
}) {
  const {
    commandModal,
    commandInput,
    commandList,
  } = elements;

  const {
    getCommands,
    closeMenu,
    closeToolbarPlaybackPopover,
    closeToolbarMorePopover,
    focusModal,
    openModal,
    closeModal,
  } = callbacks;

  let commandPaletteItems = [];
  let commandPaletteIndex = 0;

  function isOpen() {
    return Boolean(commandModal?.classList.contains("is-open"));
  }

  function filterCommands(query) {
    const commands = getCommands();
    const tokens = String(query || "")
      .toLowerCase()
      .split(/\s+/)
      .filter(Boolean);
    if (!tokens.length) return commands;
    return commands.filter((command) => {
      const haystack = `${command.id || ""} ${command.label} ${command.search || ""} ${command.shortcut || ""}`
        .toLowerCase();
      return tokens.every((token) => haystack.includes(token));
    });
  }

  function setIndex(nextIndex) {
    if (!Number.isFinite(nextIndex)) return;
    commandPaletteIndex = Math.max(0, Math.floor(nextIndex));
  }

  function close(options = {}) {
    const { restoreFocus = true } = options;
    if (!closeModal(commandModal, { restoreFocus })) return false;
    commandPaletteItems = [];
    commandPaletteIndex = 0;
    return true;
  }

  function execute(index = commandPaletteIndex) {
    const command = commandPaletteItems[index];
    if (!command) return;
    close({ restoreFocus: false });
    try {
      const maybePromise = command.run?.();
      if (maybePromise && typeof maybePromise.catch === "function") {
        maybePromise.catch((err) => console.error(err));
      }
    } catch (err) {
      console.error(err);
    }
  }

  function render() {
    if (!commandList) return;
    commandPaletteItems = filterCommands(commandInput?.value || "");
    commandList.innerHTML = "";
    if (!commandPaletteItems.length) {
      const empty = document.createElement("div");
      empty.className = "command-empty";
      empty.textContent = t("command.empty");
      commandList.appendChild(empty);
      return;
    }

    commandPaletteIndex = Math.max(0, Math.min(commandPaletteIndex, commandPaletteItems.length - 1));
    commandPaletteItems.forEach((command, idx) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "command-item";
      if (idx === commandPaletteIndex) {
        button.classList.add("is-active");
      }
      button.dataset.commandIndex = String(idx);

      const label = document.createElement("span");
      label.className = "command-label";
      label.textContent = command.label;
      button.appendChild(label);

      if (command.shortcut) {
        const shortcut = document.createElement("span");
        shortcut.className = "command-shortcut";
        shortcut.textContent = command.shortcut;
        button.appendChild(shortcut);
      }

      button.addEventListener("mouseenter", () => {
        if (idx === commandPaletteIndex) return;
        commandPaletteIndex = idx;
        render();
      });

      button.addEventListener("click", () => {
        execute(idx);
      });

      commandList.appendChild(button);
    });

    commandList.querySelector(".command-item.is-active")?.scrollIntoView({ block: "nearest" });
  }

  function open(prefill = "") {
    if (!commandModal || !commandInput) return;
    closeMenu();
    closeToolbarPlaybackPopover();
    closeToolbarMorePopover();
    const nextValue = typeof prefill === "string" ? prefill : "";

    if (isOpen()) {
      if (typeof prefill === "string") {
        commandInput.value = nextValue;
        commandPaletteIndex = 0;
        render();
      }
      focusModal(commandModal, commandInput);
      commandInput.setSelectionRange(commandInput.value.length, commandInput.value.length);
      return;
    }

    commandInput.value = nextValue;
    commandPaletteIndex = 0;
    openModal(commandModal, { focusTarget: commandInput });
    render();
    window.requestAnimationFrame(() => {
      if (!isOpen()) return;
      commandInput.setSelectionRange(commandInput.value.length, commandInput.value.length);
    });
  }

  function handleKeydown(event) {
    if (!isOpen()) return false;
    if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
      event.preventDefault();
      close();
      return true;
    }
    if (event.key === "Escape") {
      event.preventDefault();
      close();
      return true;
    }
    if (event.key === "ArrowDown") {
      event.preventDefault();
      if (commandPaletteItems.length) {
        commandPaletteIndex = (commandPaletteIndex + 1) % commandPaletteItems.length;
        render();
      }
      return true;
    }
    if (event.key === "ArrowUp") {
      event.preventDefault();
      if (commandPaletteItems.length) {
        commandPaletteIndex = (commandPaletteIndex - 1 + commandPaletteItems.length) % commandPaletteItems.length;
        render();
      }
      return true;
    }
    if (event.key === "Enter") {
      event.preventDefault();
      execute(commandPaletteIndex);
      return true;
    }
    return false;
  }

  return {
    isOpen,
    setIndex,
    render,
    open,
    close,
    handleKeydown,
  };
}
