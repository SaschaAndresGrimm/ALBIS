# 0010: Tooltips name a control; explanations go in a "?"

**Status:** Accepted (Unreleased)

## Context

ALBIS had "tool hints": a hover bubble on some seventy controls, behind a
setting (**Settings → Viewer → Enable tool hints**, `ui.tool_hints`) that was
off by default. Most of them restated the control's label ("Adjust image
zoom level." over the zoom slider), and a field without a hint of its own
showed its label as one, so the Operation dropdown in Combine Frames said
"Operation" over "Sum". The few that carried something real (a shortcut, why
a control was unavailable, what the mouse does on the image) were hidden from
everyone who had not found the setting. Meanwhile the "?" buttons
(`info_tips.js`) had started to explain the options that need it, always on.
The hover bubble also fought the browser's own `title` tooltip, with about a
hundred lines of workarounds.

## Decision

- **No setting.** The hover tooltip is always on, and `ui.tool_hints` is
  gone. A config that still sets it loads; the key is dropped before
  validation.
- **The tooltip names a control; it does not explain it.** It appears for an
  icon-only button (its `aria-label`), a control with a `title`, and above
  all a control that is unavailable (`data-help-reason`). The shortcut comes
  from `aria-keyshortcuts` and is drawn as a keycap. Never over a button with
  words, a labelled field or a checkbox.
- **What an option means is a "?"** beside it, and units or limits a hint
  under the field. The useful old hints moved there (level bands, SIMPLON
  address, mouse controls on the image and on the plots); the rest were
  deleted.
- Half a second the first time, at once while moving along a toolbar,
  anchored under the control rather than following the pointer, on keyboard
  focus but not on a click, never on touch. It replaces the browser's own
  `title` bubble while the pointer is on the control.

## Consequences

- A new control gets no hover text by default. An icon button needs an
  `aria-label` anyway; anything more is a "?" or a hint, which also works on
  a touch screen and can be read without hunting.
- About sixty strings fewer in each of the 13 locales.
- Revisit if users ask for help on controls that are neither icon-only nor
  covered by a "?": the answer is more "?" buttons or clearer labels, not a
  hint layer back.
