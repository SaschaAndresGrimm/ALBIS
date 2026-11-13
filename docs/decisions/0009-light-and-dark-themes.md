# 0009: Light and dark themes

**Status:** Accepted (0.45.0)

## Context

ALBIS was dark only. In a bright room or on a projector the dark panels were
hard to read, and people asked for a light interface. A theme touches every
colour in the frontend: some 580 literals in the stylesheets, and colours
drawn on canvas by the histogram, the ROI plots and the overview. Several of
the choices below look, to a newcomer, like things to tidy up.

## Decision

- **Tokens, two values.** Every stylesheet colour is a token with its dark
  value in `:root` and its light value in `:root[data-theme="light"]`
  (`style.css` for shared tokens, a pair of blocks at the top of an area's
  stylesheet for its own). The style guard's colour baseline is 0. Canvas
  plots read the `--plot-*` tokens through `modules/plot_theme.js` and are
  drawn again on `albis:themechange`; `js_colour_guard.test.js` keeps new
  literals out of the JavaScript.
- **Dark is the default.** "System" is offered, not assumed: ALBIS looked the
  same for everyone until someone chooses otherwise.
- **The choice belongs to the browser, not to the server config.** It is kept
  in `localStorage` (`albis.theme`), like the side panel's width. One ALBIS
  server is often shared by a beamline; a config key would have set one
  person's theme for everybody. It also lets an inline script in `<head>`
  (index.html and docs.html) apply the theme before the first paint, before
  the config has arrived; otherwise a light window opens dark and flashes.
- **The image area stays dark in both themes**, once a file is open, and so
  does everything drawn over it (cursor readout, magnifier, mask-edit bar,
  loading badge). A bright surround makes the eye adapt to it and faint
  signal in a frame loses contrast, which is why image viewers keep it dark
  or grey in their light themes too. The surround is a neutral grey
  (`--ws-canvas`) so colours in the frame are judged against no tint.
- **Data colours do not change**: colour maps, the mask legend, ring and peak
  overlays mean the same in either theme.
- **The start screen, About and the documentation follow the theme.** With no
  file open the start screen is a home screen, not the image area. The wave
  picture (light lines on black) is shown in light with
  `filter: invert(1) hue-rotate(180deg)`, which keeps its blue, rather than
  shipping and maintaining a second picture.

## Consequences

- A visible change has to be checked in both themes
  (`ui:snapshots --theme light|dark`, the ui-check skill, the PR template).
- A token added without a light value silently keeps its dark value in the
  light theme; only the screenshots catch it.
- A user who works on two machines sets the theme on each.
- Revisit if a facility asks to set a default theme for its users: that would
  be a config key for the default, with the browser's choice still winning.
