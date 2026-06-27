import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

import { bindMenuAndGlobalInteractions } from "../modules/menu_bindings.js";

// bindMenuAndGlobalInteractions attaches document-level listeners with no
// teardown, so the menu is rendered and bound exactly once for the whole file
// and per-test state is reset instead of re-binding (which would leak handlers
// pointing at detached DOM and cross-contaminate later tests).
function renderMenu() {
  document.body.innerHTML = `
    <div class="chrome">
      <div class="menu-bar">
        <button class="menu-item" data-menu="file">File</button>
        <button class="menu-item" data-menu="help">Help</button>
      </div>
      <div class="menu-dropdown" id="menu-dropdown" aria-hidden="true">
        <div class="dropdown-panel" data-menu="file">
          <button class="dropdown-item" id="it-open" data-action="open">Open</button>
          <div class="dropdown-item dropdown-submenu-parent" id="it-saveas" tabindex="0"
               aria-haspopup="true" aria-expanded="false">
            <span>Save As</span>
            <div class="dropdown-submenu">
              <button class="dropdown-item" id="it-full" data-action="save-full">Full</button>
              <button class="dropdown-item" id="it-visible" data-action="save-visible">Visible</button>
            </div>
          </div>
          <button class="dropdown-item" id="it-export" data-action="export">Export</button>
        </div>
        <div class="dropdown-panel" data-menu="help">
          <button class="dropdown-item" id="it-about" data-action="help-about">About</button>
        </div>
      </div>
    </div>
  `;
}

const handleMenuAction = vi.fn();
const handleNavShortcut = vi.fn(() => false);
let ctx;

function wire() {
  const dropdown = document.getElementById("menu-dropdown");
  const menuButtons = document.querySelectorAll(".menu-item[data-menu]");
  const dropdownPanels = document.querySelectorAll(".dropdown-panel");
  const submenuParents = document.querySelectorAll(".dropdown-submenu-parent");
  const menuActions = document.querySelectorAll(".dropdown-item[data-action]");

  let activeMenu = null;
  const setActive = (menu) => {
    activeMenu = menu;
    menuButtons.forEach((b) => b.classList.toggle("is-active", b.dataset.menu === menu));
    dropdownPanels.forEach((p) => p.classList.toggle("is-active", p.dataset.menu === menu));
  };
  const closeSubmenus = () =>
    submenuParents.forEach((p) => {
      p.classList.remove("is-open");
      p.setAttribute("aria-expanded", "false");
    });

  bindMenuAndGlobalInteractions({
    menuButtons,
    submenuParents,
    dropdown,
    menuActions,
    callbacks: {
      cancelClose: () => {},
      scheduleClose: () => {},
      isCoarsePointerDevice: () => false,
      openMenu: (menu) => {
        dropdown.classList.add("is-open");
        dropdown.setAttribute("aria-hidden", "false");
        closeSubmenus();
        setActive(menu);
      },
      closeMenu: () => {
        dropdown.classList.remove("is-open");
        dropdown.setAttribute("aria-hidden", "true");
        closeSubmenus();
      },
      closeSubmenus,
      closeToolbarPlaybackPopover: () => {},
      closeToolbarMorePopover: () => {},
      closeFooterVersionPopover: () => {},
      registerChromeActivity: () => {},
      isMenuActive: (menu) => activeMenu === menu,
      trapModalFocus: () => false,
      isCommandPaletteOpen: () => false,
      handleCommandPaletteKeydown: () => false,
      closeTopModal: () => false,
      getTopOpenModal: () => null,
      handleNavShortcut,
      handleMenuAction,
      handleShortcut: () => {},
    },
  });

  const reset = () => {
    dropdown.classList.remove("is-open");
    dropdown.setAttribute("aria-hidden", "true");
    menuButtons.forEach((b) => b.classList.remove("is-active"));
    dropdownPanels.forEach((p) => p.classList.remove("is-active"));
    closeSubmenus();
    activeMenu = null;
    if (document.activeElement && document.activeElement !== document.body) {
      document.activeElement.blur();
    }
  };

  return { dropdown, reset };
}

function press(key) {
  document.dispatchEvent(new KeyboardEvent("keydown", { key, bubbles: true, cancelable: true }));
}

const id = (x) => document.getElementById(x);
const fileButton = () => document.querySelector('.menu-item[data-menu="file"]');

describe("menu keyboard navigation", () => {
  beforeAll(() => {
    renderMenu();
    ctx = wire();
  });

  beforeEach(() => {
    ctx.reset();
    handleMenuAction.mockClear();
    handleNavShortcut.mockClear();
  });

  it("opens a menu and focuses the first item when ArrowDown is pressed on its button", () => {
    fileButton().focus();
    press("ArrowDown");
    expect(ctx.dropdown.classList.contains("is-open")).toBe(true);
    expect(document.activeElement).toBe(id("it-open"));
  });

  it("moves focus through items with ArrowDown/ArrowUp and wraps around", () => {
    fileButton().focus();
    press("ArrowDown"); // it-open
    press("ArrowDown"); // it-saveas (submenu parent)
    expect(document.activeElement).toBe(id("it-saveas"));
    press("ArrowUp"); // back to it-open
    expect(document.activeElement).toBe(id("it-open"));
    press("ArrowUp"); // wraps to last item it-export
    expect(document.activeElement).toBe(id("it-export"));
  });

  it("opens a submenu with ArrowRight and returns to the parent with ArrowLeft", () => {
    fileButton().focus();
    press("ArrowDown");
    press("ArrowDown"); // focus submenu parent
    press("ArrowRight");
    expect(id("it-saveas").getAttribute("aria-expanded")).toBe("true");
    expect(document.activeElement).toBe(id("it-full"));
    press("ArrowLeft");
    expect(id("it-saveas").classList.contains("is-open")).toBe(false);
    expect(document.activeElement).toBe(id("it-saveas"));
  });

  it("activates a div submenu parent with Enter (no native click)", () => {
    fileButton().focus();
    press("ArrowDown");
    press("ArrowDown"); // submenu parent
    press("Enter");
    expect(id("it-saveas").getAttribute("aria-expanded")).toBe("true");
    expect(document.activeElement).toBe(id("it-full"));
  });

  it("switches between top-level menus with ArrowRight while open", () => {
    fileButton().focus();
    press("ArrowDown"); // open file menu, focus it-open
    press("ArrowRight"); // non-submenu item -> next top-level menu (help)
    expect(document.activeElement).toBe(id("it-about"));
  });

  it("closes the menu on Escape and restores focus to the menu button", () => {
    const fileBtn = fileButton();
    fileBtn.focus();
    press("ArrowDown");
    expect(ctx.dropdown.classList.contains("is-open")).toBe(true);
    press("Escape");
    expect(ctx.dropdown.classList.contains("is-open")).toBe(false);
    expect(document.activeElement).toBe(fileBtn);
  });

  it("does not claim arrow keys for the menu when no menu is open", () => {
    document.body.focus();
    press("ArrowRight");
    // The menu handler falls through so frame navigation still runs.
    expect(handleNavShortcut).toHaveBeenCalled();
  });
});
