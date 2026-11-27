/**
 * The words for "Quit and Open Disk Image" / "Quit and Show in Explorer".
 *
 * On macOS and with the Windows portable zip, the update cannot be installed
 * while ALBIS runs: Finder will not replace an open app, and Windows will not
 * overwrite files in use. So the update dialog's last step there shows the
 * download and closes ALBIS (`/api/update-download/open-and-quit`). The steps
 * are said twice: what is about to happen before the click (`steps`), and
 * what to do now on the page left behind (`stopped`), because the dialog
 * that said them first is gone by then.
 */

const HANDOFF_KEYS = {
  macos_app: {
    action: "update_check.action.handoff.macos_app",
    steps: "update_check.handoff.steps.macos_app",
    stopped: "update_check.handoff.stopped.macos_app",
  },
  windows_portable: {
    action: "update_check.action.handoff.windows_portable",
    steps: "update_check.handoff.steps.windows_portable",
    stopped: "update_check.handoff.stopped.windows_portable",
  },
};

export const HANDOFF_ENDPOINT = "update-download/open-and-quit";

/** The translation keys for an install kind, or null where it does not apply. */
export function handoffKeys(installKind) {
  return Object.hasOwn(HANDOFF_KEYS, installKind) ? HANDOFF_KEYS[installKind] : null;
}
