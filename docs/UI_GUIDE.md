# UI Guide

How the ALBIS interface is put together, so that a change looks like the rest
of it. For contributors and coding agents alike: read the section for what
you are touching before you start, and the [checklist](#checklist) before you
open a pull request.

ALBIS runs in Chrome, Edge, Firefox and Safari, and should look and behave the
same in each. Most rules below exist because a browser default differed, or
because a one-off value drifted away from its neighbours.

## How this is enforced

- **`frontend/tests/style_guard.test.js`** (part of `npm run test:js`) fails
  when a stylesheet adds a colour, text size or corner radius literal
  outside `:root`, uses a z-index off the [stacking scale](#stacking), removes
  a focus outline without a replacement ring, or uses `backdrop-filter`
  without its `-webkit-` twin. Its failure message names the token to use.
- **`npm run ui:snapshots`** screenshots every window — see
  [Checking a change](#checking-a-change).
- **Locale tests** (`locale_parity`, `locale_integrity`) fail when a string is
  missing from one of the 13 languages. See
  [I18N_REVIEW_GUIDE.md](I18N_REVIEW_GUIDE.md).

## Tokens

All shared values live in `:root` at the top of `frontend/style.css`. Use the
token; if none fits, add one there with a comment saying what it is *for* —
not just its value — and use it.

### Light and dark

ALBIS has two themes: dark (the default) and light, chosen in **Settings →
Viewer → Appearance** and set as `data-theme` on `<html>`
(`modules/theme.js`). Each token has its dark value in `:root` and its light
value in `:root[data-theme="light"]`, so anything coloured through tokens
follows the theme without a rule of its own; a colour written as a literal
stays the same in both and is the one thing that breaks the light theme.

- A token only one area needs goes in a `:root` block at the top of that
  area's stylesheet, with a matching `:root[data-theme="light"]` block. The
  style guard does not count literals in either.
- The image area stays dark in both themes once a file is open, and so does
  everything over it: the cursor readout, the magnifier, the mask-editing bar,
  the loading badge. Their tokens have the same value in both blocks. The
  start screen is the home screen, not the image area, and follows the theme.
- The wave picture (start screen, About, documentation) is light on black;
  the light theme shows it with `filter: invert(1) hue-rotate(180deg)`, which
  keeps its blue, rather than a second image file.
- `docs.html` is a page of its own with its own tokens and a light block; it
  reads the same `albis.theme` from the browser.
- Colours that are data -- colour maps, the mask legend, ring and peak
  overlays -- do not change with the theme.
- Plots drawn on canvas read their colours from the `--plot-*` tokens through
  `modules/plot_theme.js` and are drawn again on `albis:themechange`.
- Check a change in both: `npm run ui:snapshots -- --theme light` and
  `--theme dark`.

## Where a style goes

`frontend/style.css` holds the tokens, the fonts and the base rules. Everything
else is in `frontend/styles/`, one file per area of the interface
(`02-toolbar.css`, `12-export-settings.css`, …), each under 1,000 lines.
`index.html` links them in the order of their numbers, and that order is the
cascade: at equal specificity a rule in a later file wins, as one further down
a single file would. So:

- Add a rule to the file for the area it styles, next to the rules for the
  same component.
- A rule that must come after another, to win over it, goes in the same file
  below it, or in a later one, with a comment saying which rule it overrides.
- A `url()` resolves against the stylesheet's own folder: from `styles/`, an
  image in `frontend/resources/` is `url("../resources/…")`.
  `frontend/tests/stylesheets.test.js` checks that every one exists.
- A new area is a new file with the next number, linked in `index.html`; the
  same test fails until it is linked, in order.
- `19-workspace.css`, last, lays out the window as a workspace (header, docked
  panel, timeline, status strip) over the canvas-first rules in
  `04-panel.css`; see [decision 0008](decisions/0008-workspace-layout.md).
  Its flat surfaces use the `--ws-*` tokens and its icons the `--icon-*`
  masks in `:root`.

### Text

| Token | Use |
| --- | --- |
| `--text` | Body text, values, labels that matter. |
| `--muted-strong` | Labels beside controls. |
| `--muted` | Hints, captions, summaries, secondary information. |
| `--accent` | Links, the lit state of a toggle, the focus ring. Not body text. |

Message tones have tokens: `--tone-error` (with `--tone-error-text` for
text on its tint), `--tone-warning`, `--tone-success`. The toasts use them;
older warning and error styles elsewhere still carry literals, to move onto
these as they are touched. Text keeps at least 4.5:1 contrast on its surface; `--muted`
is 8.3:1 on the panel.

### Text sizes

Five steps: `--font-size-xs` 10px, `-sm` 11px, `-md` 12px (labels, the
default), `-lg` 13px, `-xl` 14px (touch layouts). No half pixels. Icon glyphs
(⚙, ×, ↷) and the splash's display type keep their own sizes.

The interface font is Inter, bundled (`--font-ui`); never a system font
stack, which sets the same panel in three faces on three platforms.

### Surfaces

| Token | Use |
| --- | --- |
| `--panel-*`, `--surface-0..3` | Panel and section backgrounds. |
| `--surface-well` | The dark inset *inside* a panel or dialog: plot, list, overview, histogram, progress bar. |

**Floating surfaces are not wells.** Anything that floats over the image —
menus, popovers, tooltips, the pixel readout, the menu bar, toolbar and
status bar — is nearly opaque (0.86–0.97) and frosted (`backdrop-filter:
blur(10px)` with its `-webkit-` twin), so the image cannot read through it.
Giving a menu the well's 0.92 once made the toolbar legible through it.

What counts is the opacity you see: an element's `opacity` times its
background's alpha. Never fade a floating surface with `opacity`; dull it
with `filter` instead. The idle bars once faded to 0.7 over a 0.88
background, 62% in all, and a bright frame or the pixel mask's tint turned
them that colour, with the pixel values readable through the toolbar.

### Borders

`--border-subtle` (0.22) · `--border-soft` (0.28) · `--border` (0.34) ·
`--border-strong` (0.44) · `--border-bold` (0.6, edges meant to stand out).
All the same blue-grey; pick by how much the edge should show. Hover, active
and focus borders use the accent family.

### Corners

`--radius-sm` 4px (controls, chips, tooltips) · `--radius-md` 7px (menus,
popovers, cards) · `--radius-lg` 10px (floating surfaces: bars, panel,
dialogs, image frame). Pills are `999px`, dots `50%`.

### Motion

`--motion-fast` / `-med` / `-slow` with `--motion-ease-standard`. A
`prefers-reduced-motion` block switches transitions off; new animations need
nothing extra as long as they use `transition`/`animation`.

## Controls

ALBIS draws its own controls instead of the browser's, which differ in every
engine. Use the existing ones; do not reintroduce native styling.

- **Checkboxes** — any `input[type="checkbox"]` is drawn by ALBIS: outlined
  box, blue fill and dark tick when checked, dash when indeterminate, faded
  when disabled. Put it in a `label.checkbox` with its text.
- **Dropdowns** — a plain `<select>` gets the ALBIS chevron
  (`--select-chevron`). Inside a dialog or panel, wrap it in `label.field`.
  The chevron is all a bare one gets: no border, corners, height or text size,
  as Find bad pixels' "exactly / or more / or less" once showed. One in a
  compact row of its own is added to the shared field rule instead; the style
  guard lists the containers that rule covers and fails on a `<select>`
  outside them.
- **Chevrons** — collapse and disclosure arrows are drawn with
  `--chevron-mask` in `currentColor` (see `.section-chevron`), never a ▾/▸
  character, which comes from whatever font has one.
- **Number fields** — every `input[type="number"]` in the page at start-up
  gets the ALBIS stepper (`attachAllNumberSteppers` in
  `modules/number_stepper.js`; the pixel-mask rules and geometry axes opt
  out for room). A field created later by JS calls `attachNumberStepper`.
- **On/off that changes behaviour continuously** (Auto contrast) — a lit
  toggle button: a `label.btn` around a visually hidden checkbox (see
  `.auto-toggle`), not a button plus a checkbox for the same state.
- **Section switches** — `label.section-switch` for a tool's on/off in its
  section header.
- **Buttons** — `.btn` for actions, `.btn-primary` for *the* action of a dialog
  or form (one per dialog, rightmost), `.btn-secondary` for everything else,
  Close and Cancel included.
- **Unavailable options** stay visible, faded (`.checkbox.is-disabled` or
  `disabled`), with the reason in a hint under them — never hidden without
  saying why.

## Dialogs

Use the `.modal` > `.modal-card` > `.modal-header` / `.modal-body` /
`.modal-footer` structure, and:

- **What the dialog acts on** (the file, the source) is a plain muted line at
  the top of the body (`.data-export-source`) — not a box, which reads as a
  field to type in.
- **What it will produce** (size, frame count, status, "Saved") sits in the
  footer on the left (`.data-export-summary`, `.settings-message`), beside
  the button that produces it.
- **Footer buttons** on the right: secondary ones first, the single primary
  last. A dialog that only shows information has no primary button.
- **Tabs** inside a dialog reuse `.panel-tabs` / `.panel-tab`. Hold the body at
  the tallest tab's height (see `.settings-tabpage`) so the dialog does not
  jump when switching.
- **Grids of fields** align their rows at the top (`align-items: start`), so a
  field with a hint under it does not push its neighbour down.
- Esc and the backdrop close it; every dialog has the ✕ in its header.

## Side panel

- Sections are `section.panel-section` with a `button.section-title`
  (`data-section-toggle`) holding the title and a `.section-chevron`. The JS
  adds `aria-expanded`, `aria-controls` and `inert` — do not add them by hand.
- Tabs are `.panel-tab` buttons in `.panel-tabs[role="tablist"]`; the JS adds
  the roles, selection and arrow-key navigation (visible tabs only).
- Group headings inside a section are small uppercase labels (as SERIES,
  TIMING in the Detector tab).

## Stacking

Pick a layer from this scale; a new one goes into both this table and
`Z_SCALE` in the style guard, with what it sits between.

| z-index | Layer |
| --- | --- |
| 1–8 | Within a component (sticky headers, overlays on the canvas). |
| 20–36 | Chrome: panel, menu bar and toolbar, panel resizer, status bar, toolbar popovers. |
| 42–48 | File drop overlay, the Versions popover. |
| 60–82 | In-panel popovers, the collapsed panel and its Open menu button. |
| 90 | Dialogs (`.modal`) — above everything in the window. |
| 1000 | Popovers opened from inside a dialog or panel (mask colours). |
| 1500 | Confirmations (`.dialog-modal`), above the dialog that asked. |
| 2000 | Toasts. |
| 3000–3001 | Tooltips and info bubbles, above everything. |

Over the image (`.canvas-shell`), the 1–8 band in this order. Areas painted
over the image go under the lines drawn on it, and what you read goes over
both. The mask overlay once sat at 6, above the rings and ROI, and when it
painted large areas its tint hid them, the cursor readout and the loading
badge.

| z-index | Layer |
| --- | --- |
| (none) | The image (`.canvas-wrap`). |
| 1 | The pixel mask overlay: pixels changed by hand or found, the shape being drawn (`#mask-edit-overlay`). |
| 2 | Resolution rings. |
| 3 | Peaks. |
| 4 | Pixel values at high zoom. |
| 5 | ROI. |
| 6 | The magnifier lens. |
| 8 | What you read and use: the cursor readout, **Loading frame…**, the mask-edit toolbar. |

## Text and translation

- Every visible string goes through `data-i18n` (text), `data-i18n-title`,
  `data-i18n-aria-label` or `data-i18n-placeholder`, or `t()` in JS, with the
  key added to all 13 files in `frontend/locales/`. See
  [I18N_REVIEW_GUIDE.md](I18N_REVIEW_GUIDE.md) and the glossary.
- Menu items and dialog titles are Title Case, with short prepositions
  lowercase ("Open on Server Host").
- Labels say what something is; hints say what happens or why something is
  unavailable. Put shortcuts in the tooltip: "Magnifier (hold Z)".

## Keyboard and accessibility

- **One focus ring**: `box-shadow: var(--focus-ring)` in a `:focus-visible`
  rule. A global zero-specificity fallback gives every control without its
  own rule the same look; never `outline: none` without a replacement.
- Every control has an accessible name: visible text, a `<label>`, or
  `aria-label` (with `data-i18n-aria-label`). A placeholder is not a name.
- Icon-only buttons get a tooltip and an `aria-label` saying what they do.
- Hidden inputs behind a styled control (switches, the Auto toggle) put the
  focus ring on the visible part (`:has(input:focus-visible)` or
  `input:focus-visible + span`).

## Cross-browser

- Prefer drawn controls and `appearance: none` over restyling native ones.
- `backdrop-filter` always with `-webkit-backdrop-filter` (Safari before 18).
- `:has()` and `:not(a, b)` with complex selectors are fine (all current
  engines).
- Check a visible change in at least Chrome and one of Firefox or Safari; say
  which in the pull request.

## Checking a change

```bash
npm run check              # all CI gates, the style guard and locale tests included

# Screenshots of every window, with an image from testdata/ loaded:
ALBIS_DATA_ROOT=testdata .venv/bin/python -m uvicorn backend.app:app --port 8731 &
npm run ui:snapshots -- --out /tmp/albis-ui
```

`ui:snapshots` writes one PNG per window into `--out`: the main view, each
Settings tab, About, Check for Updates, the backend log, the command palette,
the file browser, the export dialogs, the menus and the toolbar popovers. It
opens the image through the Data tab's file list (`--file` picks one), never
by upload, so the data folder is left as it was. `ALBIS_DATA_ROOT=testdata`
uses the test files tracked in the repository, so it works on a fresh clone. Run it before and after your
change and compare. `--width 1100` shows the layout with the side panel
collapsed.

### The user guide's pictures

The pictures in [USER_GUIDE.md](USER_GUIDE.md) are made, not taken:

```bash
npm run guide:screenshots                     # every picture
npm run guide:screenshots -- --only roi,rings # those whose scene name starts so
```

`scripts/guide_screenshots.mjs` starts its own ALBIS, with a temporary config
and a copy of the insulin series from `testdata/`, and a simulated detector;
no server needs to be running, and your settings are not touched. Each
picture is a scene that sets the interface up and crops to the part a section
is about, written as WebP into `docs/images/user-guide/`. A picture that looks
the same as the one already there is left alone, so only what changed shows
in `git status`; `--force` writes them all.
The README's picture, `frontend/resources/albis.png`, is a scene too
(`readme`): a PNG, because the Linux software centres show it as well
(`packaging/linux/ALBIS.metainfo.xml`) and AppStream expects PNG.

When a change alters something a picture shows, run the scenes for it and
commit the new pictures with the change. A new picture is a new scene;
`tests/test_user_guide_images.py` fails when the guide shows a picture that is
missing or has no alt text, when a picture is shown nowhere, or when no scene
makes it.

## Checklist

- [ ] Colours, sizes, corners and borders come from `:root` tokens; any new
      token has a comment saying what it is for, and a light value.
- [ ] Looked at in both themes (`ui:snapshots --theme light` and `--theme dark`).
- [ ] Floating surfaces are nearly opaque and frosted; wells only inside panels.
- [ ] Existing controls reused: drawn checkbox, dropdown, chevron, stepper,
      toggle; one primary button per dialog, rightmost.
- [ ] Unavailable options are faded with a reason, not hidden.
- [ ] Every string translated in all 13 locales; every control has a name.
- [ ] Keyboard: reachable with Tab, same focus ring, Esc closes.
- [ ] `npm run check` passes; screenshots before/after attached, browser named.
- [ ] The user guide's pictures of what changed are made again
      (`npm run guide:screenshots -- --only …`).
