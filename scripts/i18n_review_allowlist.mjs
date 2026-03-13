const EXACT_KEY_ALLOWLIST = new Set([
  "analysis.rings.placeholder.center_x",
  "analysis.rings.placeholder.center_y",
  "cursor.resolution",
  "series.ui.norm_image_placeholder",
  "series.ui.output_placeholder",
  "settings.language.rm",
  "toolbar.playback.fps_option",
  "view.colormap.albula_hdr",
  "view.colormap.cividis",
  "view.colormap.inferno",
  "view.colormap.magma",
  "view.colormap.turbo",
]);

const VALUE_ALLOWLIST = [
  /^path\/to\/[a-z0-9_.-]+$/i,
  /^output\/[a-z0-9_.-]+$/i,
  /^[XY]$/u,
  /^Rin\s+→\s+Rout$/u,
  /^ALBULA HDR$/u,
  /^Cividis$/u,
  /^Inferno$/u,
  /^Magma$/u,
  /^Turbo$/u,
];

export function isApprovedEnglishCarryover(key, value) {
  if (EXACT_KEY_ALLOWLIST.has(String(key))) {
    return true;
  }
  const rawValue = String(value || "").trim();
  return VALUE_ALLOWLIST.some((pattern) => pattern.test(rawValue));
}

export function getAllowlistedKeys() {
  return [...EXACT_KEY_ALLOWLIST].sort();
}
