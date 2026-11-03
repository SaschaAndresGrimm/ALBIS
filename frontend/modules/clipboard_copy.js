/**
 * Copy text to the clipboard, with or without the Clipboard API.
 *
 * ALBIS is routinely reached over plain HTTP on a LAN -- that is a documented
 * way to use it -- and `navigator.clipboard` needs a secure context, so its
 * absence is the ordinary case for a remote session, not an edge case. A
 * hidden textarea and `execCommand` still work there, and a button that
 * copies is worth more than a modern API that does nothing.
 *
 * The footer's build info, the update command and the inspector's path each
 * had a copy of this; they share it now.
 */

function copyViaSelection(text) {
  if (typeof document === "undefined" || !document.body) return false;
  const scratch = document.createElement("textarea");
  scratch.value = text;
  // Off-screen rather than hidden: a display:none element cannot be selected.
  scratch.setAttribute("aria-hidden", "true");
  scratch.style.position = "fixed";
  scratch.style.top = "-1000px";
  scratch.style.opacity = "0";
  document.body.appendChild(scratch);
  let copied = false;
  try {
    scratch.select();
    copied = document.execCommand?.("copy") === true;
  } catch {
    copied = false;
  } finally {
    scratch.remove();
  }
  return copied;
}

/** True when the text reached the clipboard by either route. */
export async function copyText(text) {
  try {
    if (typeof navigator !== "undefined" && navigator?.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch {
    // Refused (permissions, focus); fall through to the selection route.
  }
  return copyViaSelection(text);
}
