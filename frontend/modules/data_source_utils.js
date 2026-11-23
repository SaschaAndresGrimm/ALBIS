/**
 * What the Data Source section says about the chosen source: the state badge
 * in its header, and whether the source still needs an address before it can
 * show anything. Pure: takes `state.autoload`, returns keys for `t()`.
 */

/**
 * The address a live source cannot start without, or "" when it has one (or
 * needs none). SIMPLON needs the detector's address, JUNGFRAUJOCH the preview
 * endpoint; a file needs no address, and a remote stream is pushed to ALBIS.
 *
 * @param {{ mode?: string, simplonUrl?: string, jfjochEndpoint?: string }} autoload
 * @returns {"" | "simplon_address" | "jfjoch_endpoint"}
 */
export function missingSourceAddress(autoload) {
  const mode = autoload?.mode || "file";
  if (mode === "simplon" && !String(autoload.simplonUrl || "").trim()) return "simplon_address";
  if (mode === "jungfraujoch" && !String(autoload.jfjochEndpoint || "").trim()) return "jfjoch_endpoint";
  return "";
}

/**
 * The Data Source header's state badge for the chosen source.
 *
 * A live source missing its address asks for it, even while it runs: it is
 * selected, but nothing can arrive yet, so "active" would be wrong.
 *
 * @param {object} autoload `state.autoload`
 * @param {boolean} hasFile whether an image file is open
 * @returns {{ tone: "active" | "empty", key: string }}
 */
export function dataSourceSectionState(autoload, hasFile) {
  const mode = autoload?.mode || "file";
  if (mode === "file") {
    if (autoload.watchEnabled && autoload.running) return { tone: "active", key: "data_source.state.watch_active" };
    return hasFile
      ? { tone: "active", key: "data_source.state.file_mode_ready" }
      : { tone: "empty", key: "data_source.state.select_file_to_begin" };
  }
  const missing = missingSourceAddress(autoload);
  if (missing === "simplon_address") return { tone: "empty", key: "data_source.state.simplon_needs_address" };
  if (missing === "jfjoch_endpoint") return { tone: "empty", key: "data_source.state.jfjoch_needs_endpoint" };
  if (autoload.running) {
    if (mode === "simplon") return { tone: "active", key: "data_source.state.simplon_active" };
    if (mode === "jungfraujoch") return { tone: "active", key: "data_source.state.jfjoch_active" };
    return { tone: "active", key: "data_source.state.remote_active" };
  }
  if (mode === "simplon") return { tone: "empty", key: "data_source.state.configure_simplon" };
  if (mode === "jungfraujoch") return { tone: "empty", key: "data_source.state.configure_jfjoch" };
  return { tone: "empty", key: "data_source.state.configure_remote" };
}

/**
 * Whether the header's source chip would only repeat the Data Source field:
 * a file source that is not watching a folder. The chip then shows only while
 * the section is collapsed, where the field is out of sight.
 *
 * @param {{ mode?: string, watchEnabled?: boolean }} autoload
 */
export function sourceChipIsModeOnly(autoload) {
  return (autoload?.mode || "file") === "file" && !autoload?.watchEnabled;
}
