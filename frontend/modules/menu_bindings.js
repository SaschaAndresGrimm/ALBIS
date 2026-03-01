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
      closeMenu();
      registerChromeActivity();
      return;
    }
    if (getTopOpenModal()) {
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
