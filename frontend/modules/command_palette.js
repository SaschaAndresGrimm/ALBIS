/**
 * Command palette controller.
 */

import {
  groupCommands,
  parseRecentCommandIds,
  rankCommands,
  rememberRecentCommand,
} from "./command_palette_utils.js";
import { t } from "./i18n.js";

const RECENT_COMMANDS_STORAGE_KEY = "albis.commandPalette.recent";

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

  function readRecentIds() {
    try {
      return parseRecentCommandIds(localStorage.getItem(RECENT_COMMANDS_STORAGE_KEY));
    } catch {
      return [];
    }
  }

  function rememberRecent(id) {
    try {
      const next = rememberRecentCommand(readRecentIds(), id);
      localStorage.setItem(RECENT_COMMANDS_STORAGE_KEY, JSON.stringify(next));
    } catch {
      // Storage may be full or blocked; the palette works without recents.
    }
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
    commandInput?.setAttribute("aria-expanded", "false");
    commandInput?.removeAttribute("aria-activedescendant");
    return true;
  }

  function execute(index = commandPaletteIndex) {
    const command = commandPaletteItems[index];
    if (!command) return;
    close({ restoreFocus: false });
    rememberRecent(command.id);
    try {
      const maybePromise = command.run?.();
      if (maybePromise && typeof maybePromise.catch === "function") {
        maybePromise.catch((err) => console.error(err));
      }
    } catch (err) {
      console.error(err);
    }
  }

  // Moves the highlight without rebuilding the list, so hovering does not
  // reset the scroll position.
  function updateActive({ scroll = true } = {}) {
    if (!commandList) return;
    commandList.querySelectorAll(".command-item").forEach((button) => {
      const isActive = Number(button.dataset.commandIndex) === commandPaletteIndex;
      button.classList.toggle("is-active", isActive);
      button.setAttribute("aria-selected", String(isActive));
    });
    if (!commandPaletteItems.length) {
      commandInput?.removeAttribute("aria-activedescendant");
      return;
    }
    commandInput?.setAttribute("aria-activedescendant", `command-option-${commandPaletteIndex}`);
    if (scroll) {
      commandList.querySelector(".command-item.is-active")?.scrollIntoView({ block: "nearest" });
    }
  }

  function createItem(command, text, idx) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "command-item";
    button.id = `command-option-${idx}`;
    button.setAttribute("role", "option");
    button.dataset.commandIndex = String(idx);
    button.dataset.commandId = command.id || "";

    const label = document.createElement("span");
    label.className = "command-label";
    label.textContent = text;
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
      updateActive({ scroll: false });
    });
    button.addEventListener("click", () => {
      execute(idx);
    });
    return button;
  }

  function renderGroups(commands) {
    const groups = groupCommands(commands, readRecentIds());
    commandPaletteItems = [];
    groups.forEach((group, groupIndex) => {
      const wrapper = document.createElement("div");
      wrapper.className = "command-group";
      wrapper.setAttribute("role", "group");
      const headingId = `command-group-${groupIndex}`;
      wrapper.setAttribute("aria-labelledby", headingId);
      wrapper.dataset.group = group.id;

      const heading = document.createElement("div");
      heading.className = "command-group-heading";
      heading.id = headingId;
      heading.setAttribute("role", "presentation");
      heading.textContent = group.id === "recent" ? t("command.group.recent") : group.label;
      wrapper.appendChild(heading);

      group.items.forEach(({ command, text }) => {
        const idx = commandPaletteItems.length;
        commandPaletteItems.push(command);
        wrapper.appendChild(createItem(command, text, idx));
      });
      commandList.appendChild(wrapper);
    });
  }

  function render() {
    if (!commandList) return;
    const query = commandInput?.value || "";
    const commands = getCommands();
    commandList.innerHTML = "";
    if (query.trim()) {
      commandPaletteItems = rankCommands(commands, query);
      commandPaletteItems.forEach((command, idx) => {
        commandList.appendChild(createItem(command, command.label, idx));
      });
    } else {
      renderGroups(commands);
    }

    if (!commandPaletteItems.length) {
      const empty = document.createElement("div");
      empty.className = "command-empty";
      empty.setAttribute("role", "presentation");
      empty.textContent = t("command.empty");
      commandList.appendChild(empty);
      commandInput?.removeAttribute("aria-activedescendant");
      return;
    }

    commandPaletteIndex = Math.max(0, Math.min(commandPaletteIndex, commandPaletteItems.length - 1));
    updateActive();
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
    commandInput.setAttribute("aria-expanded", "true");
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
        updateActive();
      }
      return true;
    }
    if (event.key === "ArrowUp") {
      event.preventDefault();
      if (commandPaletteItems.length) {
        commandPaletteIndex = (commandPaletteIndex - 1 + commandPaletteItems.length) % commandPaletteItems.length;
        updateActive();
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
