/**
 * Menu + global interaction bindings.
 */

export function bindMenuAndGlobalInteractions({
  menuButtons,
  submenuParents,
  dropdown,
  menuActions,
  callbacks,
}) {
  const {
    cancelClose,
    scheduleClose,
    isCoarsePointerDevice,
    openMenu,
    closeMenu,
    closeSubmenus,
    closeToolbarPlaybackPopover,
    closeToolbarMorePopover,
    closeFooterVersionPopover,
    registerChromeActivity,
    isMenuActive,
    trapModalFocus,
    isCommandPaletteOpen,
    handleCommandPaletteKeydown,
    closeTopModal,
    getTopOpenModal,
    handleNavShortcut,
    handleMenuAction,
    handleShortcut,
  } = callbacks;

  menuButtons.forEach((btn) => {
    btn.addEventListener("mouseenter", () => {
      cancelClose();
      if (!dropdown?.classList.contains("is-open")) return;
      if (isCoarsePointerDevice()) return;
      openMenu(btn.dataset.menu, btn);
    });
    btn.addEventListener("click", () => {
      cancelClose();
      if (dropdown.classList.contains("is-open") && isMenuActive(btn.dataset.menu)) {
        closeMenu();
      } else {
        openMenu(btn.dataset.menu, btn);
      }
    });
  });

  submenuParents.forEach((parent) => {
    parent.addEventListener("click", (event) => {
      if (!isCoarsePointerDevice()) return;
      if (event.target.closest(".dropdown-submenu")) return;
      const alreadyOpen = parent.classList.contains("is-open");
      closeSubmenus();
      if (!alreadyOpen) {
        parent.classList.add("is-open");
        parent.setAttribute("aria-expanded", "true");
      }
      event.preventDefault();
      event.stopPropagation();
    });
  });

  dropdown?.addEventListener("mouseenter", cancelClose);
  dropdown?.addEventListener("mouseleave", scheduleClose);

  document.addEventListener("click", (event) => {
    const withinMenu = event.target.closest(".menu-bar") || event.target.closest(".menu-dropdown");
    const withinPlayback = event.target.closest("#toolbar-playback-wrap");
    const withinMore = event.target.closest("#toolbar-more-wrap");
    const withinFooterVersions =
      event.target.closest("#footer-version-toggle") || event.target.closest("#footer-version-popover");
    if (!withinPlayback) {
      closeToolbarPlaybackPopover();
    }
    if (!withinMore) {
      closeToolbarMorePopover();
    }
    if (!withinFooterVersions) {
      closeFooterVersionPopover();
    }
    if (dropdown && !withinMenu) {
      closeMenu();
    }
    registerChromeActivity();
  });

  const isMenuOpen = () => Boolean(dropdown?.classList.contains("is-open"));

  function getActivePanel() {
    return dropdown?.querySelector(".dropdown-panel.is-active") || null;
  }

  // Visible, enabled items in the active panel, with an open submenu's items
  // spliced in right after their parent so Up/Down walk a single flat list.
  function getMenuItems(panel) {
    if (!panel) return [];
    const items = [];
    panel.querySelectorAll(":scope > .dropdown-item").forEach((el) => {
      if (el.classList.contains("is-disabled")) return;
      items.push(el);
      if (el.classList.contains("dropdown-submenu-parent") && el.classList.contains("is-open")) {
        el.querySelectorAll(".dropdown-submenu > .dropdown-item:not(.is-disabled)").forEach((sub) => {
          items.push(sub);
        });
      }
    });
    return items;
  }

  function focusItemAt(items, index) {
    if (!items.length) return;
    const wrapped = ((index % items.length) + items.length) % items.length;
    items[wrapped].focus();
  }

  function openMenuByOffset(offset) {
    const buttons = Array.from(menuButtons);
    if (!buttons.length) return null;
    let base = buttons.findIndex((btn) => isMenuActive(btn.dataset.menu));
    if (base < 0) base = 0;
    const wrapped = (((base + offset) % buttons.length) + buttons.length) % buttons.length;
    const btn = buttons[wrapped];
    openMenu(btn.dataset.menu, btn);
    return btn;
  }

  function openSubmenu(parent) {
    closeSubmenus();
    parent.classList.add("is-open");
    parent.setAttribute("aria-expanded", "true");
    const firstSub = parent.querySelector(".dropdown-submenu > .dropdown-item:not(.is-disabled)");
    if (firstSub) firstSub.focus();
  }

  // Keyboard navigation for the menu bar and its dropdowns. Returns true when it
  // consumes the event. Arrow keys are claimed only while a menu is open (or a
  // menu-bar button is focused) so they stay free for frame navigation otherwise.
  function handleMenuKeydown(event) {
    const onMenuButton = Boolean(document.activeElement?.classList?.contains("menu-item"));

    if (!isMenuOpen()) {
      if (!onMenuButton) return false;
      const buttons = Array.from(menuButtons);
      const idx = buttons.indexOf(document.activeElement);
      if (event.key === "ArrowRight" || event.key === "ArrowLeft") {
        focusItemAt(buttons, idx + (event.key === "ArrowRight" ? 1 : -1));
        event.preventDefault();
        return true;
      }
      if (event.key === "ArrowDown") {
        openMenu(document.activeElement.dataset.menu, document.activeElement);
        focusItemAt(getMenuItems(getActivePanel()), 0);
        event.preventDefault();
        return true;
      }
      return false;
    }

    // Tab out of an open menu closes it and lets focus move on naturally.
    if (event.key === "Tab") {
      closeMenu();
      return false;
    }

    const panel = getActivePanel();
    const items = getMenuItems(panel);
    const active = document.activeElement;
    const idx = items.indexOf(active);
    const onSubmenuParent = Boolean(active?.classList?.contains("dropdown-submenu-parent"));

    switch (event.key) {
      case "ArrowDown":
        focusItemAt(items, idx < 0 ? 0 : idx + 1);
        event.preventDefault();
        return true;
      case "ArrowUp":
        focusItemAt(items, idx < 0 ? -1 : idx - 1);
        event.preventDefault();
        return true;
      case "Home":
        focusItemAt(items, 0);
        event.preventDefault();
        return true;
      case "End":
        focusItemAt(items, items.length - 1);
        event.preventDefault();
        return true;
      case "ArrowRight":
        if (onSubmenuParent) {
          openSubmenu(active);
        } else {
          openMenuByOffset(1);
          focusItemAt(getMenuItems(getActivePanel()), 0);
        }
        event.preventDefault();
        return true;
      case "ArrowLeft": {
        const openParent = panel?.querySelector(".dropdown-submenu-parent.is-open");
        if (openParent && openParent.contains(active) && active !== openParent) {
          closeSubmenus();
          openParent.focus();
        } else {
          openMenuByOffset(-1);
          focusItemAt(getMenuItems(getActivePanel()), 0);
        }
        event.preventDefault();
        return true;
      }
      case "Enter":
      case " ":
        // The submenu parent is a div with no native click activation.
        if (onSubmenuParent) {
          openSubmenu(active);
          event.preventDefault();
          return true;
        }
        return false;
      default:
        return false;
    }
  }

  document.addEventListener("keydown", (event) => {
    registerChromeActivity();
    if (trapModalFocus(event)) {
      return;
    }
    if (isCommandPaletteOpen()) {
      if (handleCommandPaletteKeydown(event)) {
        return;
      }
      return;
    }
    if (event.key === "Escape") {
      if (closeTopModal()) {
        event.preventDefault();
        return;
      }
      closeToolbarPlaybackPopover();
      closeToolbarMorePopover();
      closeFooterVersionPopover();
      // Return focus to the menu-bar button so keyboard users keep their place.
      const activeMenuBtn = isMenuOpen()
        ? Array.from(menuButtons).find((btn) => btn.classList.contains("is-active"))
        : null;
      closeMenu();
      if (activeMenuBtn) activeMenuBtn.focus();
      registerChromeActivity();
      return;
    }
    if (getTopOpenModal()) {
      return;
    }
    if (handleMenuKeydown(event)) {
      return;
    }
    if (handleNavShortcut(event)) {
      return;
    }
    if (event.key === "F1") {
      event.preventDefault();
      handleMenuAction("help-docs");
      return;
    }
    handleShortcut(event);
  });

  menuActions.forEach((item) => {
    item.addEventListener("click", () => {
      if (item.classList.contains("is-disabled")) return;
      handleMenuAction(item.dataset.action);
      closeMenu();
    });
  });
}
