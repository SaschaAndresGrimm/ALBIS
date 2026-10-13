/**
 * The known-detectors list under a SIMPLON address field.
 *
 * Opens when the field is focused, from its ▾ button or with ↓; typing
 * narrows it by name, address, model or serial. Picking a row puts its
 * address in the field and hands the entry to `onPick` (the Detector tab
 * connects, the Live source tests). Each row can be kept (☆/★), renamed (✎)
 * or removed (×) in place. The list sits in the page flow, under the field
 * and its buttons, so a short panel scrolls rather than clipping it.
 *
 * Names come from the user and models from the detector: both are put on the
 * page as text, never as markup.
 */

import { onLanguageChange, t } from "./i18n.js";
import { arrangeDetectors, detectorLabel, knownDetectors, shortAddress } from "./known_detectors.js";

let pickerCount = 0;

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined && text !== null) node.textContent = String(text);
  return node;
}

export function attachDetectorPicker({ input, store = knownDetectors, onPick } = {}) {
  if (!input) return null;
  const id = `detector-picker-${++pickerCount}`;

  // The field and its ▾ share a box, so the button sits inside the field.
  const box = el("span", "detector-picker-field");
  input.before(box);
  box.append(input);
  const toggle = el("button", "detector-picker-toggle", "▾");
  toggle.type = "button";
  toggle.tabIndex = -1;
  box.append(toggle);

  const list = el("ul", "detector-picker-list");
  list.id = id;
  list.setAttribute("role", "listbox");
  list.hidden = true;
  (input.closest(".inline") || box).after(list);

  input.removeAttribute("list");
  input.setAttribute("role", "combobox");
  input.setAttribute("aria-autocomplete", "list");
  input.setAttribute("aria-controls", id);
  input.setAttribute("aria-expanded", "false");

  let open = false;
  let query = "";
  let active = -1;
  let editing = null;

  function labelToggle() {
    toggle.title = t("known_detectors.show");
    toggle.setAttribute("aria-label", t("known_detectors.show"));
    toggle.hidden = store.list().length === 0;
  }

  function options() {
    return [...list.querySelectorAll("[role=option]")];
  }

  function setActive(index) {
    const rows = options();
    active = rows.length ? Math.max(-1, Math.min(index, rows.length - 1)) : -1;
    rows.forEach((row, i) => row.classList.toggle("is-active", i === active));
    if (active >= 0) {
      input.setAttribute("aria-activedescendant", rows[active].id);
      rows[active].scrollIntoView?.({ block: "nearest" });
    } else {
      input.removeAttribute("aria-activedescendant");
    }
  }

  function iconButton(className, label, title, onClick) {
    const button = el("button", `detector-picker-icon ${className}`, label);
    button.type = "button";
    button.tabIndex = -1;
    button.title = title;
    button.setAttribute("aria-label", title);
    // Keeps the focus where it is, and the list open.
    button.addEventListener("mousedown", (event) => event.preventDefault());
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      onClick();
    });
    return button;
  }

  function nameEditor(entry) {
    const field = el("input", "detector-picker-rename");
    field.type = "text";
    field.value = entry.name || detectorLabel(entry);
    field.placeholder = entry.model || shortAddress(entry.address);
    field.setAttribute("aria-label", t("known_detectors.rename"));
    let done = false;
    const finish = (save) => {
      if (done) return;
      done = true;
      editing = null;
      // An unchanged default is not a name: the model stays the label, and
      // follows the detector.
      const value = field.value.trim();
      if (save && value !== detectorLabel(entry)) store.rename(entry.address, value);
      else render();
    };
    field.addEventListener("keydown", (event) => {
      event.stopPropagation();
      if (event.key === "Enter" || event.key === "Escape") {
        event.preventDefault();
        finish(event.key === "Enter");
        input.focus();
      }
    });
    field.addEventListener("blur", () => finish(true));
    field.addEventListener("click", (event) => event.stopPropagation());
    return field;
  }

  function row(entry, index) {
    const item = el("li", "detector-picker-row");
    item.id = `${id}-${index}`;
    item.setAttribute("role", "option");
    item.setAttribute("aria-selected", "false");
    const star = iconButton(
      entry.kept ? "is-kept" : "",
      entry.kept ? "★" : "☆",
      entry.kept ? t("known_detectors.unkeep") : t("known_detectors.keep"),
      () => store.setKept(entry.address, !entry.kept),
    );
    star.setAttribute("aria-pressed", entry.kept ? "true" : "false");
    const words = el("span", "detector-picker-text");
    const label = detectorLabel(entry);
    // Each fact once: an entry known only by its address is labelled with it.
    const meta = [];
    if (shortAddress(entry.address) !== label) meta.push(shortAddress(entry.address));
    if (entry.model && entry.model !== label) meta.push(entry.model);
    if (entry.serial) meta.push(entry.serial);
    words.append(
      editing === entry.address ? nameEditor(entry) : el("span", "detector-picker-name", label),
      el("span", "detector-picker-meta", meta.join(" · ")),
    );
    const rename = iconButton("", "✎", t("known_detectors.rename"), () => {
      editing = entry.address;
      render();
      const field = list.querySelector(".detector-picker-rename");
      field?.focus();
      field?.select();
    });
    const forget = iconButton("", "×", t("known_detectors.forget"), () => store.forget(entry.address));
    item.append(star, words, rename, forget);
    item.addEventListener("mousedown", (event) => {
      if (!event.target.closest?.(".detector-picker-rename")) event.preventDefault();
    });
    item.addEventListener("click", () => pick(entry));
    return item;
  }

  function render() {
    labelToggle();
    if (!open) return;
    const { kept, recent } = arrangeDetectors(store.list(), query);
    if (!kept.length && !recent.length) {
      // Nothing matches what is typed: out of the way, not an empty box.
      list.hidden = true;
      input.setAttribute("aria-expanded", "false");
      return;
    }
    list.replaceChildren();
    let index = 0;
    const group = (title, entries) => {
      if (!entries.length) return;
      // A heading only where there are two groups to tell apart.
      if (kept.length && recent.length) {
        const head = el("li", "detector-picker-head", title);
        head.setAttribute("role", "presentation");
        head.addEventListener("mousedown", (event) => event.preventDefault());
        list.append(head);
      }
      for (const entry of entries) list.append(row(entry, index++));
    };
    group(t("known_detectors.kept"), kept);
    group(t("known_detectors.recent"), recent);
    // The last change could not be saved, and was taken back: say so.
    if (store.lastError) {
      const note = el("li", "detector-picker-error", t("known_detectors.save_failed"));
      note.setAttribute("role", "presentation");
      list.append(note);
    }
    list.hidden = false;
    input.setAttribute("aria-expanded", "true");
    setActive(Math.min(active, index - 1));
  }

  function show(filter = "") {
    const opening = !open;
    open = true;
    query = filter;
    active = -1;
    render();
    // Changes others made since: they come in while the list is open.
    if (opening) void store.refresh?.();
  }

  function close() {
    open = false;
    editing = null;
    active = -1;
    list.hidden = true;
    input.setAttribute("aria-expanded", "false");
    input.removeAttribute("aria-activedescendant");
  }

  function pick(entry) {
    close();
    input.value = entry.address;
    onPick?.(entry);
  }

  input.addEventListener("focus", () => {
    if (!open && store.list().length) show();
  });
  input.addEventListener("input", () => show(input.value));
  input.addEventListener("keydown", (event) => {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      if (!open || list.hidden) show();
      else setActive(active + 1);
    } else if (event.key === "ArrowUp" && open) {
      event.preventDefault();
      setActive(active - 1);
    } else if (event.key === "Enter" && open && active >= 0) {
      // A highlighted row is the choice; otherwise Enter connects to what
      // is typed, as before (the field's own handler).
      event.preventDefault();
      const entries = arrangeDetectors(store.list(), query);
      const entry = [...entries.kept, ...entries.recent][active];
      if (entry) pick(entry);
    } else if (event.key === "Escape" && open && !list.hidden) {
      event.preventDefault();
      event.stopPropagation();
      close();
    }
  });
  toggle.addEventListener("mousedown", (event) => event.preventDefault());
  toggle.addEventListener("click", () => {
    if (open && !list.hidden) {
      close();
    } else {
      show();
      input.focus();
    }
  });
  // Closed when the focus leaves the field and its list, not when it moves
  // between them (renaming a row).
  const inside = (node) => Boolean(node) && (box.contains(node) || list.contains(node));
  const onFocusOut = (event) => {
    if (!inside(event.relatedTarget)) close();
  };
  box.addEventListener("focusout", onFocusOut);
  list.addEventListener("focusout", onFocusOut);

  const unsubscribe = store.subscribe(() => render());
  const unlisten = onLanguageChange(() => render());
  labelToggle();

  return {
    open: () => show(),
    close,
    get isOpen() {
      return open && !list.hidden;
    },
    destroy() {
      unsubscribe();
      unlisten?.();
      close();
    },
  };
}
