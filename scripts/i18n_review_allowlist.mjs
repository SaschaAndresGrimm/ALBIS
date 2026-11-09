const EXACT_KEY_ALLOWLIST = new Set([
  "analysis.rings.placeholder.center_x",
  "analysis.rings.placeholder.center_y",
  // Product name plus a placeholder — identical in every locale by design.
  "autoload.status.simplon.error_reason",
  "backend.live.live",
  "backend.server.offline",
  "backend.server.online",
  // "Export" and "Animation" are the German and Swiss German words as well as
  // the English ones, so the match is a cognate rather than a missing
  // translation.
  "command.label.export_animation",
  "cursor.resolution",
  // "Direction" is the French word as well as the English one: a cognate.
  "geometry.two_theta.direction.none",
  // A symbol, a number and a unit: the same in every language.
  "geometry.inline.two_theta_file",
  "roi.mode.default",
  "series.ui.output_placeholder",
  "settings.language.mi",
  "settings.language.rm",
  // "Data" is the Swedish and Danish word as well as the English one, so the
  // match is a cognate rather than a missing translation.
  "settings.tab.data",
  // German and Italian both use "Frame" for a frame -- the same loanword their
  // own toolbar.frame.next already uses -- so the match is a cognate.
  "toolbar.frame.slider",
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

// Glossary findings (npm run review:i18n, "Glossary") that are correct as
// written: the English word is used in another sense, or the term is there in
// a form the stem matching cannot see. Each entry names the term, the key, the
// locales ("*" for all) and why, so the list stays short and checkable.
const GLOSSARY_EXCEPTIONS = [
  {
    term: "mean",
    key: "settings.data.max_upload_hint",
    locales: "*",
    reason: '"0 means no limit": the verb, not the statistical mean.',
  },
  {
    term: "annulus",
    key: "roi.mode.annulus",
    locales: ["fr", "it", "pt"],
    reason: "ROI annulaire / anulare / anular: the adjective of the glossary noun.",
  },
  {
    term: "annulus",
    key: "hint.roi.mode",
    locales: ["ja", "zh-CN"],
    reason: "A list of shape names (line, box, circle, annulus) where the short 環 / 环 sits next to 円 / 圆.",
  },
  {
    term: "detector_geometry",
    key: "detector.help.adv.geometry",
    locales: ["pt"],
    reason: 'European Portuguese spells it "detetor"; the locale has both varieties, and this is not the place to choose.',
  },
  {
    term: "mask",
    key: "pixel_mask.edit.mode.mask",
    locales: ["rm", "zh-CN"],
    reason: "The edit mode is the verb (mascrar, 掩蔽), paired with Unmask; the noun does not fit.",
  },
  {
    term: "mask",
    key: "pixel_mask.edit.mode_title.mask",
    locales: ["rm", "zh-CN"],
    reason: "The edit mode is the verb (mascrar, 掩蔽), paired with Unmask; the noun does not fit.",
  },
  {
    term: "mask",
    key: "view.mask_saturated",
    locales: ["rm", "zh-CN"],
    reason: "Mask saturated is an action (mascrar, 屏蔽), not the mask noun.",
  },
  {
    term: "sum",
    key: "analysis.roi.stats.sum",
    locales: ["zh-CN"],
    reason: "A statistics column label: the noun 总和; the glossary's 求和 is the summing operation.",
  },
  {
    term: "mean",
    key: "inspector.stats.mean",
    locales: ["zh-CN"],
    reason: "A short statistics label next to min and max, where 平均 alone reads naturally.",
  },
];

export function isGlossaryException(locale, key, termId) {
  return GLOSSARY_EXCEPTIONS.some(
    (entry) =>
      entry.term === termId &&
      entry.key === key &&
      (entry.locales === "*" || entry.locales.includes(locale)),
  );
}

export function getGlossaryExceptions() {
  return GLOSSARY_EXCEPTIONS.map((entry) => ({ ...entry }));
}
