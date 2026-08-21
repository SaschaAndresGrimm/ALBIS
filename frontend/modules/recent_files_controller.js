/**
 * The "Open Recent" submenu: render the list, open what was clicked.
 *
 * Rebuilt every time the File menu opens rather than kept in sync with the
 * store, because the list is short and the alternative is two sources of truth
 * for what is on screen.
 */

import { t } from "./i18n.js";

function fileName(path) {
  const normalized = String(path || "").replace(/\\/g, "/");
  const parts = normalized.split("/").filter(Boolean);
  return parts.length ? parts[parts.length - 1] : normalized;
}

export function createRecentFilesController({
  recentFiles,
  elements,
  callbacks,
}) {
  const { submenu, parent } = elements;
  const { openPath, closeMenu, setStatus } = callbacks;

  function disabledItem(labelKey) {
    const item = document.createElement("div");
    item.className = "dropdown-item is-disabled";
    item.setAttribute("role", "menuitem");
    item.setAttribute("aria-disabled", "true");
    const label = document.createElement("span");
    label.dataset.i18n = labelKey;
    label.textContent = t(labelKey);
    item.append(label);
    return item;
  }

  function entryItem(path) {
    const item = document.createElement("button");
    item.className = "dropdown-item";
    item.type = "button";
    item.setAttribute("role", "menuitem");
    // The name is what a person recognises; the full path is what disambiguates
    // two runs with the same file name, so it stays reachable on hover.
    item.title = path;
    const label = document.createElement("span");
    label.className = "recent-file-name";
    label.textContent = fileName(path);
    item.append(label);
    item.addEventListener("click", async () => {
      closeMenu?.();
      try {
        await openPath(path);
      } catch (err) {
        console.error(err);
        // A recent file can be gone: a scratch directory cleared, a mount
        // unplugged. Say so and drop it rather than leaving a dead entry.
        recentFiles.remove(path);
        setStatus?.(t("status.recent_files.open_failed", { file: fileName(path) }), {
          tone: "error",
        });
        render();
      }
    });
    return item;
  }

  function clearItem() {
    const item = document.createElement("button");
    item.className = "dropdown-item";
    item.type = "button";
    item.setAttribute("role", "menuitem");
    const label = document.createElement("span");
    label.dataset.i18n = "menu.file.clear_recent";
    label.textContent = t("menu.file.clear_recent");
    item.append(label);
    item.addEventListener("click", () => {
      recentFiles.clear();
      render();
    });
    return item;
  }

  function render() {
    if (!submenu) return;
    const entries = recentFiles.list();
    submenu.textContent = "";
    if (!entries.length) {
      submenu.append(disabledItem("menu.file.no_recent_files"));
      parent?.classList.add("is-empty");
      return;
    }
    parent?.classList.remove("is-empty");
    entries.forEach((path) => submenu.append(entryItem(path)));
    const separator = document.createElement("div");
    separator.className = "dropdown-separator";
    submenu.append(separator, clearItem());
  }

  return { render, recordOpened: (path) => recentFiles.record(path) };
}
