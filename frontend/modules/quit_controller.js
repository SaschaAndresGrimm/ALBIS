/**
 * File -> Quit ALBIS: stops the ALBIS server, which otherwise runs on after
 * the last tab closes -- keeping its memory and, on Windows, its HDF5 files
 * locked. On macOS the Dock's Quit did this already; Windows and Linux had no
 * way short of Task Manager.
 *
 * Offered only when the server says it can be stopped from here
 * (`can_quit` on /api/health): one started by the launcher on this computer.
 * It asks first only when something would be lost -- a conversion, an export
 * or a series sum still running, a live watch -- and otherwise quits at once.
 * Afterwards the page says ALBIS has stopped, rather than failing quietly.
 */

import { showConfirmDialog as defaultConfirm } from "./dialogs.js";
import { t } from "./i18n.js";
import { fetchResponse } from "./http.js";

export function createQuitController({ apiBase, state, elements, callbacks }) {
  const { menuItem } = elements;
  const { collectBusyWork = () => [], showConfirmDialog = defaultConfirm, setStatus } = callbacks;
  let stopping = false;

  function sync() {
    if (menuItem) menuItem.hidden = !state.canQuit;
  }

  function busyText(items) {
    return items.map((item) => t(`quit.busy.${item}`)).join(", ");
  }

  async function confirmBusy(items) {
    return showConfirmDialog({
      title: t("quit.confirm_title"),
      message: t("quit.confirm_busy", { work: busyText(items) }),
      confirmLabel: t("quit.confirm_anyway"),
      danger: true,
    });
  }

  function showStopped() {
    const overlay = document.createElement("div");
    overlay.className = "albis-stopped";
    overlay.setAttribute("role", "alertdialog");
    overlay.setAttribute("aria-live", "assertive");
    const card = document.createElement("div");
    card.className = "albis-stopped-card";
    const title = document.createElement("h2");
    title.textContent = t("quit.stopped_title");
    const text = document.createElement("p");
    text.textContent = t("quit.stopped_text");
    card.append(title, text);
    overlay.append(card);
    document.body.append(overlay);
  }

  async function send(force) {
    return fetchResponse(`${apiBase}/quit`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ force }),
    });
  }

  async function quit() {
    if (stopping || !state.canQuit) return false;
    // What this tab has running, which the server cannot see.
    const local = collectBusyWork().filter(Boolean);
    if (local.length && !(await confirmBusy(local))) return false;
    try {
      try {
        await send(local.length > 0);
      } catch (err) {
        if (err?.status !== 409) throw err;
        // The server's own work (a series sum, a conversion from another tab).
        const busy = Array.isArray(err.detailData?.busy) ? err.detailData.busy : ["unknown"];
        if (!(await confirmBusy(busy))) return false;
        await send(true);
      }
      stopping = true;
      showStopped();
      return true;
    } catch (err) {
      console.error(err);
      setStatus(t("quit.failed"), { tone: "error" });
      return false;
    }
  }

  sync();
  return { quit, sync };
}
