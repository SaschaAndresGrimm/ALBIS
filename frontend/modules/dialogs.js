/**
 * Promise-based prompt/confirm dialogs that replace the native window.prompt
 * and window.confirm calls.
 *
 * Self-contained (imported directly like i18n.t / toast.js): builds a modal
 * using the existing .modal / .modal-card styling, traps focus, and resolves a
 * value/boolean. Key handling lives on the dialog element in the bubble phase
 * and stops propagation, so the document-level app shortcuts do not fire behind
 * an open dialog while typing in the input still works.
 */

import { t } from "./i18n.js";

const FOCUSABLE_SELECTOR =
  'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

let activeDialog = null;

function getFocusable(card) {
  return Array.from(card.querySelectorAll(FOCUSABLE_SELECTOR)).filter(
    (el) => el instanceof HTMLElement && (el.offsetWidth || el.offsetHeight || el.getClientRects().length),
  );
}

/**
 * @param {{kind: "prompt"|"confirm", title: string, message?: string,
 *   defaultValue?: string, confirmLabel?: string, cancelLabel?: string,
 *   danger?: boolean}} options
 * @returns {Promise<string|null|boolean>}
 */
function openDialog(options) {
  const {
    kind,
    title = "",
    message = "",
    defaultValue = "",
    confirmLabel = t("common.confirm"),
    cancelLabel = t("common.cancel"),
    danger = false,
  } = options;

  // Only one dialog at a time; resolve any existing one as cancelled.
  if (activeDialog) {
    activeDialog.cancel();
  }

  if (typeof document === "undefined") {
    return Promise.resolve(kind === "prompt" ? null : false);
  }

  return new Promise((resolve) => {
    const previousFocus = document.activeElement;

    const overlay = document.createElement("div");
    overlay.className = "modal dialog-modal is-open";

    const backdrop = document.createElement("div");
    backdrop.className = "modal-backdrop";
    overlay.appendChild(backdrop);

    const card = document.createElement("div");
    card.className = "modal-card dialog-card";
    card.setAttribute("role", kind === "confirm" ? "alertdialog" : "dialog");
    card.setAttribute("aria-modal", "true");

    const header = document.createElement("div");
    header.className = "modal-header";
    const titleEl = document.createElement("div");
    titleEl.className = "dialog-title";
    titleEl.textContent = title;
    header.appendChild(titleEl);
    card.appendChild(header);

    const body = document.createElement("div");
    body.className = "modal-body dialog-body";

    if (message) {
      const messageEl = document.createElement("p");
      messageEl.className = "dialog-message";
      messageEl.textContent = message;
      body.appendChild(messageEl);
    }

    let input = null;
    if (kind === "prompt") {
      input = document.createElement("input");
      input.type = "text";
      input.className = "dialog-input";
      input.value = defaultValue;
      input.setAttribute("aria-label", title || cancelLabel);
      body.appendChild(input);
    }

    const actions = document.createElement("div");
    actions.className = "dialog-actions";
    const cancelBtn = document.createElement("button");
    cancelBtn.type = "button";
    cancelBtn.className = "btn btn-secondary dialog-cancel";
    cancelBtn.textContent = cancelLabel;
    const confirmBtn = document.createElement("button");
    confirmBtn.type = "button";
    confirmBtn.className = `btn ${danger ? "btn-danger" : "btn-primary"} dialog-confirm`;
    confirmBtn.textContent = confirmLabel;
    actions.appendChild(cancelBtn);
    actions.appendChild(confirmBtn);
    body.appendChild(actions);

    card.appendChild(body);
    overlay.appendChild(card);
    document.body.appendChild(overlay);

    let settled = false;
    function close(result) {
      if (settled) return;
      settled = true;
      activeDialog = null;
      overlay.remove();
      if (
        previousFocus instanceof HTMLElement &&
        previousFocus.isConnected &&
        typeof previousFocus.focus === "function"
      ) {
        previousFocus.focus({ preventScroll: true });
      }
      resolve(result);
    }

    function accept() {
      close(kind === "prompt" ? input.value : true);
    }
    function cancel() {
      close(kind === "prompt" ? null : false);
    }

    confirmBtn.addEventListener("click", accept);
    cancelBtn.addEventListener("click", cancel);
    backdrop.addEventListener("click", cancel);

    // Keep all key handling inside the dialog so app-level shortcuts do not
    // fire behind it. Typing is handled at the input (target) before this
    // bubble-phase listener stops propagation.
    overlay.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        event.preventDefault();
        cancel();
      } else if (event.key === "Enter" && event.target !== cancelBtn) {
        event.preventDefault();
        accept();
      } else if (event.key === "Tab") {
        const focusable = getFocusable(card);
        if (!focusable.length) return;
        const first = focusable[0];
        const last = focusable[focusable.length - 1];
        const active = document.activeElement;
        if (event.shiftKey && active === first) {
          event.preventDefault();
          last.focus();
        } else if (!event.shiftKey && active === last) {
          event.preventDefault();
          first.focus();
        }
      }
      event.stopPropagation();
    });

    activeDialog = { cancel };

    window.requestAnimationFrame(() => {
      if (input) {
        input.focus();
        input.select();
      } else {
        confirmBtn.focus();
      }
    });
  });
}

/**
 * Prompt for a text value. Resolves the entered string, or null if cancelled.
 * @returns {Promise<string|null>}
 */
export function showPromptDialog(options = {}) {
  return openDialog({ ...options, kind: "prompt" });
}

/**
 * Ask the user to confirm. Resolves true if confirmed, false otherwise.
 * @returns {Promise<boolean>}
 */
export function showConfirmDialog(options = {}) {
  return openDialog({ ...options, kind: "confirm" });
}
