# 0005 — `app.js` takes its elements from one table; late-created controllers keep stable wrappers

**Status:** Accepted, 2026-10-31.

## Context

`frontend/app.js` is the composition root (see `frontend/AGENTS.md`). By
0.38.1 it was 5,659 lines, and two kinds of line made up much of it:

- **456 element lookups**, `const x = document.getElementById("…")`, spread
  over 3,300 lines in 27 runs, each next to the code that first needed it.
  Finding which element a name refers to meant searching the file, and a
  coding agent reading one region never saw the rest.
- **About 250 stable wrappers**, `function x(...) { return controller?.x(...); }`.
  Controllers are created in an order, and one created early is handed
  callbacks into one created later. A function declaration is hoisted, so the
  wrapper exists from the start and reaches the controller once it does.

## Decision

- **The lookups are one table**, `frontend/modules/app_elements.js`
  (`queryAppElements()`), in their old order and groups, and `app.js` takes
  every name from it in one destructuring where the first lookup used to be.
  The names in `app.js` did not change.
- This is safe because none of these elements is created or replaced while
  ALBIS starts: for every lookup `app.js` made at its top level, the element
  returned was the one present at the first lookup and still the one in the
  document after boot and after opening an image (checked in Chrome by
  instrumenting `getElementById`; a deliberately replaced element was
  reported). Moving a `const` earlier cannot cause a temporal-dead-zone error.
- **The stable wrappers stay.** Removing them means creating controllers in an
  order where nothing is needed before it exists, or passing getters instead
  of functions. Either changes how every controller is wired, at once, in the
  one file that wires them, with little a test can see; the wrappers are short
  and the pattern is documented.

## Consequences

- An element is added in three places: its id in `index.html`, an entry in
  `app_elements.js`, its name in the destructuring in `app.js`.
  `frontend/tests/app_elements.test.js` fails if an id is missing from
  `index.html` or the two lists differ; eslint reports a name taken but unused.
- An element a script creates later does not belong in the table: looked up
  early, it would be `null`. Such lookups stay where the element is made.
- Gathering the lookups showed three ids `index.html` no longer had
  (`export-btn`, `hist-log-x`, `hist-log-y`): always `null`, wired through
  the binding contexts to listeners that never ran. They were removed with
  that code; the table test now fails on any id `index.html` lacks.
- `app.js` lost 379 lines; the binding contexts in `app.js` still list
  element names one by one, since they are `app.js`'s own variables.
- **Revisit if** controllers come to be created from a single declarative list
  (then the wrappers can go), or if the elements object is passed to the
  binding-context factories whole instead of name by name.
