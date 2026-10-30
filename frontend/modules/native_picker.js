/**
 * The system's own file and folder dialogs, which the backend opens when it
 * runs on the user's machine.
 *
 * Seven controls asked /api/choose-file or /api/choose-folder, and each read
 * the answer its own way: 204 is the user cancelling, 409 that this machine
 * has no native dialog, anything else a failure, after which most of them
 * offer ALBIS's own file browser instead. One reading of it, here.
 */

import { fetchResponse } from "./http.js";
import { getLanguage } from "./i18n.js";

/**
 * Opens the native chooser and resolves to the chosen path, or to "" when the
 * user cancelled (or chose nothing). Rejects with fetchResponse's translated
 * error; `err.status` 409 means there is no native dialog on this machine.
 *
 * `kind` is "file" or "folder"; `purpose` titles a folder chooser
 * ("autoload", "export_output", …); `exts` limits a file chooser
 * (".expt", ".tif,.tiff,.cbf").
 */
export async function chooseNativePath(apiBase, { kind = "file", purpose = "", exts = "" } = {}) {
  const query = [
    purpose ? `purpose=${encodeURIComponent(purpose)}` : "",
    exts ? `exts=${encodeURIComponent(exts)}` : "",
    `lang=${encodeURIComponent(getLanguage())}`,
  ].filter(Boolean);
  const res = await fetchResponse(`${apiBase}/choose-${kind}?${query.join("&")}`);
  if (res.status === 204) return "";
  const data = await res.json();
  return String(data?.path || "");
}
