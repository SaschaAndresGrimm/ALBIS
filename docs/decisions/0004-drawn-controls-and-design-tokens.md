# 0004 — The interface draws its own controls and takes its values from tokens

**Status:** Accepted, 2026-10-30.

## Context

ALBIS is used in Chrome, Edge, Firefox and Safari and should look and work the
same in each. Native checkboxes, dropdown arrows, number spinners, focus rings
and blur support differ between engines: an unticked dark checkbox all but
disappeared in Safari, a ▾ character came from whichever font had one. In
the stylesheet, one-off values had drifted into near-copies — 42 shades of the
same border blue, eleven text sizes between 9 and 15px, menus that let the
toolbar read through after a background was swapped for an inset's.

## Decision

- ALBIS draws its own checkbox, dropdown chevron, section chevrons, number
  stepper and focus ring.
- Colours, text sizes, corners, borders, surfaces and stacking come from the
  tokens in `:root` of `frontend/style.css`, each documented for what it is
  for in [UI_GUIDE.md](../UI_GUIDE.md).
- `frontend/tests/style_guard.test.js` enforces what can be checked: no new
  literals (ratchets that only go down), the stacking scale, focus rings,
  the Safari blur prefix, no self-referencing tokens.

## Consequences

- The same interface in every engine, and a change in one place moves every
  use.
- A new control is a little more work than dropping in a native one, and must
  reuse the drawn ones.
- Hundreds of older literals remain (backgrounds, gradients, state colours);
  the ratchets keep them from growing while they are removed over time.
- **Revisit if** the browsers' native controls become stylable enough
  (`appearance: base` and similar) to give the same result with less code.
