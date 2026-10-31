# 0006 — How the viewer's core controllers reach each other

**Status:** Proposed, 2026-10-31 — for the maintainer to choose an option.

## Context

`frontend/app.js` creates 64 controllers. Decision 0005 removed 116 of the
197 one-line wrappers that let a controller call one created after it; 81
remain, because their controllers need each other in a circle. Counting the
calls `app.js`'s own functions make on the controllers' behalf, 30 controllers
form one cycle (plus two small ones: ROI statistics ↔ ROI interaction, and
the overlays).

The cycle runs through a few hub functions in `app.js` that are called by
nearly every core controller and call many of them back:

| Hub | What it does | Called from (in `app.js`) |
| --- | --- | --- |
| `redraw` | renderer, mask legend, inpainting and flatfield state, magnifier | 12 places |
| `schedulePeakOverlay`, `…ResolutionOverlay`, `…PixelOverlay`, `scheduleOverview`, `scheduleHistogram`, `scheduleRoiOverlay` | one redraw per frame of one layer | 10–14 each |
| `requestFrame`, `stopPlayback`, `updateFrameControls` | frame navigation | 6–10 each |
| `startAutoload`, `stopAutoload`, `ensureFileMode` | which source is live | 3–10 each |
| `setStatus`, `updateToolbar`, `updateLiveBadge` | what the chrome shows | 8–33 each |

No creation order removes the cycle: the file pipeline needs playback,
playback needs the pipeline's load state, the live sources need the file
session, the file session needs the overlays, the overlays need the viewport,
and the viewport requests frames.

## Options

### A. Registry: create first, connect second

Controllers keep calling each other directly, but through one object filled
after all are created, instead of through wrappers:

```js
const core = {};                       // before any controller
const framePlayback = createFramePlaybackController({ state, elements, core });
// ... every controller ...
Object.assign(core, { framePlayback, fileDataPipeline, overlays, ... });
// inside a controller:  core.framePlayback.stopPlayback();
```

- **Gains:** the remaining 81 wrappers go; each controller's module says by
  name which others it uses (`core.framePlayback…`), instead of receiving
  anonymous callbacks; nothing about when or in what order things run
  changes, so the live check can show it is the same.
- **Costs:** the cycle stays, now visible rather than hidden; every core
  controller's signature changes (callbacks into `core`), touching ~30
  modules and their tests; a call made before `Object.assign` fails, as a
  call into a not-yet-created controller does today.
- **Risk:** low — the same functions are called at the same moments. Mechanical,
  checkable per controller like decision 0005's change.

### B. Events and a render scheduler

Break the cycle by changing how the core talks:

- a **render scheduler** (`render_scheduler.js`): layers (image, peaks, rings,
  pixel values, overview, histogram, ROI outline, mask editor) register a draw
  function; anyone calls `invalidate(layer)` or `invalidate("all")`; one
  `requestAnimationFrame` draws what is dirty. Replaces `redraw` and the six
  `schedule…` hubs.
- **viewer events** (`viewer_events.js`): a small emitter with a fixed list —
  `frame-shown`, `source-changed`, `geometry-changed`, `playback-changed`.
  The chrome (toolbar, live badge, status line, metadata panel), ROI
  statistics, the series-sum path and recent files subscribe instead of being
  called by whoever changed something.
- Commands that must act at once (`requestFrame`, `stopPlayback`,
  `ensureFileMode`) stay direct calls, owned by the playback and source
  controllers, which are created before their callers once the chrome no
  longer sits in the cycle.

- **Gains:** a real dependency order; the chrome becomes pure listeners that
  can be tested alone; redraws are coalesced in one place (no layer drawn
  twice per frame); the wrappers go as each part moves.
- **Costs:** five or so PRs; every module that redraws or announces a change
  is touched; the order in which listeners run becomes something to get
  right, where today it is the order of the calls.
- **Risk:** medium to high in exactly the paths that matter at a beamline —
  live sources, playback, switching sources. Event-order mistakes show as a
  stale panel or a frame drawn with the previous contrast, intermittently.
  The live check sees much of this; the session at a detector
  (`docs/LIVE_TEST_PLAN.md`) is needed before the last step merges.

### C. Leave it

The 81 wrappers stay, documented by decision 0005.

## Recommendation

**A now; B's render scheduler later, if wanted.** A removes the wrappers and
names every dependency at little risk, which is most of what a reorder was
meant to buy. The cycle B would remove is real, but it is a design question
about the core — who announces what — that deserves its own discussion, and
B's riskiest part (events between live sources and the chrome) buys the least.
The render scheduler is the part of B with clear value on its own (coalesced
redraws, `redraw`'s fan-out in one place) and could follow A without the
events.

Planned order for A, each a PR checked with `npm run ui:live` against `main`
and `ui:snapshots`:

1. `core` object and the playback/pipeline/session controllers.
2. Live sources and autoload.
3. Overlays, viewport, ROI, chrome.
4. Remove the last wrappers; amend decision 0005.

No step changes behaviour, so none needs the detector session to merge; a
session after step 2 is still worth it, as the live code moved.
