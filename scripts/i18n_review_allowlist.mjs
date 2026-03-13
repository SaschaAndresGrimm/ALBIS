const EXACT_KEY_ALLOWLIST = new Set([
  "analysis.rings.placeholder.center_x",
  "analysis.rings.placeholder.center_y",
  "backend.live.live",
  "backend.server.offline",
  "backend.server.online",
  "cursor.resolution",
  "roi.mode.default",
  "series.ui.norm_image_placeholder",
  "series.ui.output_placeholder",
  "settings.language.mi",
  "settings.language.rm",
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
