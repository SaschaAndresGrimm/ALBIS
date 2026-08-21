const EXACT_KEY_ALLOWLIST = new Set([
  "analysis.rings.placeholder.center_x",
  "analysis.rings.placeholder.center_y",
  // Product name plus a placeholder — identical in every locale by design.
  "autoload.status.simplon.error_reason",
  "backend.live.live",
  "backend.server.offline",
  "backend.server.online",
  "cursor.resolution",
  "roi.mode.default",
  "series.ui.norm_image_placeholder",
  "series.ui.output_placeholder",
  "settings.language.mi",
  "settings.language.rm",
  // "Data" is the Swedish and Danish word as well as the English one, so the
  // match is a cognate rather than a missing translation.
  "settings.tab.data",
  "toolbar.playback.fps_option",
  "viewer.footer.badge.live",
  "viewer.footer.badge.server",
  "view.colormap.albula_hdr",
  "view.colormap.cividis",
  "view.colormap.inferno",
  "view.colormap.magma",
  "view.colormap.turbo",
]);

const VALUE_ALLOWLIST = [
  /^path\/to\/[a-z0-9_.-]+$/i,
  // Example host and address shown as a placeholder, like the path examples
  // above: a literal sample value, identical in every language.
  /^albis\.lab,\s*\d{1,3}(?:\.\d{1,3}){3}$/u,
  /^output\/[a-z0-9_.-]+$/i,
  /^[XY]$/u,
  /^Rin\s+→\s+Rout$/u,
  /^ALBULA HDR$/u,
  /^Cividis$/u,
  /^DType$/u,
  /^Inferno$/u,
  /^LIVE$/u,
  /^Magma$/u,
  /^OFFLINE$/u,
  /^ROI$/u,
  /^SERVER$/u,
  /^SIMPLON$/u,
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
