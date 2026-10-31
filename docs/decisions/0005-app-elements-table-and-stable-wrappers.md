# 0005 — `app.js` takes its elements from one table; late-created controllers keep stable wrappers

**Status:** Accepted, 2026-10-31; amended the same day (wrappers that were
not needed are gone, see the end).

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
- **Stable wrappers stay where a function is needed before its controller
  exists.** Removing those means creating controllers in an order where
  nothing is needed early, or passing getters instead of functions. Three
  groups of controllers need each other (live sources and file loading,
  18 controllers; overlays, 3; ROI, 2), so no order removes all of them.
- **Everywhere else, `app.js` takes the method from the controller** right
  after creating it (`const { updateRoiStats, … } = roiStatsController;`).

## Consequences

- An element is added in two places: its id in `index.html` and an entry in
  `app_elements.js`. Controllers receive the whole table (`elements:
  { ...appElements, … }`) and take what they need; `app.js` names an element
  in its own destructuring only if it uses it itself.
  `frontend/tests/app_elements.test.js` fails if an id is missing from
  `index.html` or an entry is read nowhere; eslint reports a name `app.js`
  takes but does not use.
- Passing the whole table was checked per controller before it was done: no
  controller reads an element from the table that it was not given before
  (that would switch on a path that had been off), and nothing in the
  frontend iterates or spreads an elements object.
- An element a script creates later does not belong in the table: looked up
  early, it would be `null`. Such lookups stay where the element is made.
- Gathering the lookups showed three ids `index.html` no longer had
  (`export-btn`, `hist-log-x`, `hist-log-y`): always `null`, wired through
  the binding contexts to listeners that never ran. They were removed with
  that code; the table test now fails on any id `index.html` lacks.
- `app.js` lost 379 lines to the table and 511 more when the controllers'
  element lists gave way to `...appElements`.
- **Revisit if** controllers come to be created from a single declarative list
  (then the wrappers can go).

## Amendment: the wrappers that were not needed

Of 197 forwarding wrappers, 116 went the same day. A wrapper was replaced by
taking the method from its controller only when all of these held:

- its call passes exactly its own parameters, and the method declares no
  more (else a caller's extra argument, such as an event, would now reach it);
- it returns the method's result, or the method returns none (a wrapper
  that dropped a returned promise would otherwise hand it to callers that
  then wait for it);
- nothing reads it while `app.js`'s top level runs before the controller is
  created;
- for wrappers written `controller?.method()`, which did nothing when called
  before the controller existed: no such early call happens. Checked by
  instrumenting them and starting ALBIS five ways (fresh, with saved folder
  watch, remote and SIMPLON modes, and with a launch target); a call planted
  on purpose was reported.

`npm run ui:live` (docs/LIVE_TEST_PLAN.md) passed all 25 checks, with the
same recorded values as `main`, three times. The 81 that remain are async,
take default parameters, drop a result, or are needed before their
controller exists.
