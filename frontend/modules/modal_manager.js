/**
 * Modal stack and focus management.
 */

function isVisibleElement(element) {
  return Boolean(element && (element.offsetWidth || element.offsetHeight || element.getClientRects().length));
}

export function createModalManager({
  modalFocusRestore,
  modalStack,
  focusableSelector,
}) {
  function isModalOpen(modalEl) {
    return Boolean(modalEl?.classList.contains("is-open"));
  }

  function trackModalOpen(modalEl) {
    const index = modalStack.indexOf(modalEl);
    if (index >= 0) {
      modalStack.splice(index, 1);
    }
    modalStack.push(modalEl);
  }

  function trackModalClose(modalEl) {
    const index = modalStack.indexOf(modalEl);
    if (index >= 0) {
      modalStack.splice(index, 1);
    }
  }

  function getTopOpenModal() {
    for (let idx = modalStack.length - 1; idx >= 0; idx -= 1) {
      const modalEl = modalStack[idx];
      if (isModalOpen(modalEl)) {
        return modalEl;
      }
      modalStack.splice(idx, 1);
    }
    return null;
  }

  function getModalFocusableElements(modalEl) {
    if (!(modalEl instanceof HTMLElement)) return [];
    return Array.from(modalEl.querySelectorAll(focusableSelector)).filter((element) => {
      if (!(element instanceof HTMLElement)) return false;
      if (!isVisibleElement(element)) return false;
      return !element.closest('[aria-hidden="true"]');
    });
  }

  function focusModal(modalEl, preferredFocus = null) {
    if (!isModalOpen(modalEl)) return;
    if (preferredFocus instanceof HTMLElement && modalEl.contains(preferredFocus) && isVisibleElement(preferredFocus)) {
      preferredFocus.focus({ preventScroll: true });
      return;
    }
    const focusable = getModalFocusableElements(modalEl);
    if (focusable.length) {
      focusable[0].focus({ preventScroll: true });
      return;
    }
    const card = modalEl.querySelector(".modal-card");
    if (card instanceof HTMLElement) {
      card.setAttribute("tabindex", "-1");
      card.focus({ preventScroll: true });
    }
  }

  function openModal(modalEl, { focusTarget = null } = {}) {
    if (!(modalEl instanceof HTMLElement)) return false;
    if (!isModalOpen(modalEl)) {
      const active = document.activeElement;
      if (active instanceof HTMLElement && !modalEl.contains(active)) {
        modalFocusRestore.set(modalEl, active);
      } else {
        modalFocusRestore.delete(modalEl);
      }
    }
    modalEl.classList.add("is-open");
    modalEl.setAttribute("aria-hidden", "false");
    trackModalOpen(modalEl);
    window.requestAnimationFrame(() => {
      focusModal(modalEl, focusTarget);
    });
    return true;
  }

  function closeModal(modalEl, { restoreFocus = true } = {}) {
    if (!(modalEl instanceof HTMLElement) || !isModalOpen(modalEl)) return false;
    modalEl.classList.remove("is-open");
    modalEl.setAttribute("aria-hidden", "true");
    trackModalClose(modalEl);
    const restoreTarget = modalFocusRestore.get(modalEl);
    modalFocusRestore.delete(modalEl);
    if (
      restoreFocus &&
      restoreTarget instanceof HTMLElement &&
      restoreTarget.isConnected &&
      typeof restoreTarget.focus === "function"
    ) {
      window.requestAnimationFrame(() => {
        restoreTarget.focus({ preventScroll: true });
      });
    }
    return true;
  }

  function trapModalFocus(event) {
    if (event.key !== "Tab") return false;
    const modalEl = getTopOpenModal();
    if (!modalEl) return false;
    const focusable = getModalFocusableElements(modalEl);
    if (!focusable.length) {
      event.preventDefault();
      focusModal(modalEl);
      return true;
    }

    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    const active = document.activeElement;
    if (event.shiftKey) {
      if (!(active instanceof HTMLElement) || !modalEl.contains(active) || active === first) {
        event.preventDefault();
        last.focus({ preventScroll: true });
        return true;
      }
      return false;
    }

    if (!(active instanceof HTMLElement) || !modalEl.contains(active) || active === last) {
      event.preventDefault();
      first.focus({ preventScroll: true });
      return true;
    }
    return false;
  }

  return {
    getTopOpenModal,
    focusModal,
    openModal,
    closeModal,
    trapModalFocus,
  };
}
