---
name: ui-check
description: Check a visible ALBIS change by eye - start ALBIS on the test data, screenshot every window before and after the change with ui:snapshots, and compare. Use after touching frontend/index.html, style.css, a locale file, or anything else that changes what the interface shows.
argument-hint: "[--file name-in-testdata] [--width 1100]"
---

# Checking a UI change by eye

Tests do not see layout. `npm run check` stays green when a label overflows its
button, a dialog loses its padding or a translation pushes a control off-screen,
so a visible change is not done until the screenshots have been looked at. The
rules being checked are in `docs/UI_GUIDE.md` (its checklist at the end).

Chrome must be installed on the machine; `ui:snapshots` drives it headless. It
is not in the devcontainer, so this runs on the host. `$CHROME` points it at a
non-default binary.

## 1. Before: the screenshots of `main`

Take them before editing if you can. If the change is already made, take them
from a worktree of `main` instead of stashing your work:

```bash
git worktree add /tmp/albis-before main
cd /tmp/albis-before
ALBIS_DATA_ROOT=testdata "$OLDPWD/.venv/bin/python" -m uvicorn backend.app:app --port 8732 &
cd "$OLDPWD"
npm run ui:snapshots -- --url http://127.0.0.1:8732/ --out /tmp/albis-ui/before
```

Then stop that server (`kill %1`, or the PID `$!` printed) and
`git worktree remove /tmp/albis-before`.

## 2. After: the screenshots of your branch

```bash
ALBIS_DATA_ROOT=testdata .venv/bin/python -m uvicorn backend.app:app --port 8731 &
npm run ui:snapshots -- --out /tmp/albis-ui/after
```

- `ALBIS_DATA_ROOT=testdata` serves the files tracked in the repository. Never
  open a file by uploading it through the page: an upload copies it into the
  data folder.
- Pass the same `--file`, `--width` and `--height` to both runs, or the pairs
  do not compare. `--width 1100` shows the layout with the side panel
  collapsed; check that too when the change touches the panel or toolbar.
- Do not try to drive Chrome yourself with `--screenshot` or
  `--virtual-time-budget`: ALBIS polls every second, so headless Chrome never
  goes idle and the run never settles. `ui:snapshots` already handles it.
- Port 8731 already taken means an ALBIS is still running; stop it rather
  than screenshotting a stale build.

## 3. Compare

Open each `after/*.png` and its `before/` counterpart and look at them. Look
at every window, not only the one you changed: tokens and shared controls
carry a change into other windows. For a string change, also check a
long-worded language (`de`, `rm`) by switching the language in Settings and
re-running.

Look for: clipped or wrapping text, misaligned controls, a surface that lost
its frosting or opacity, a focus ring missing, more than one primary button,
a colour that is not from the tokens.

## 4. The user guide's pictures

If the change alters something a picture in `docs/USER_GUIDE.md` shows, make
that picture again and commit it with the change, or the guide shows the old
interface:

```bash
npm run guide:screenshots -- --only roi,histogram   # scene names, by prefix
```

It starts its own ALBIS and needs no server running. The scenes are listed in
`scripts/guide_screenshots.mjs`; `git status docs/images/user-guide` shows
which pictures changed. Look at each one, as in step 3. A new picture in the
guide is a new scene there; see `docs/UI_GUIDE.md`, "The user guide's
pictures".

## 5. Clean up and report

Stop every server you started (`jobs`, then `kill`). Attach the before and
after pairs that show the change to the pull request, and name the browsers
you checked in (`ui:snapshots` covers Chrome; the UI guide asks for Firefox
or Safari as well).
