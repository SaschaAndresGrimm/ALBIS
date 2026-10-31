# Live Test Plan

How to check that ALBIS still follows a running experiment after a change to
how it is wired (`frontend/app.js`, the controllers, the live sources). Two
parts: an automated run that needs no hardware, and a session at a detector
for what only real hardware shows.

## 1. Without a detector: `npm run ui:live`

`scripts/live_check.mjs` starts ALBIS on a scratch copy of `testdata/`, the
simulated detector (`test_scripts/fake_simplon_dcu.py`) and a simulated
JUNGFRAUJOCH publisher (`test_scripts/fake_jfjoch_publisher.py`), then drives
Chrome through:

- a CBF series: open, step, play; an HDF5 stack: open, step;
- folder watch: two files written into the watched folder are opened in turn;
- the Remote Stream API: two pushed frames are shown;
- the SIMPLON monitor of the simulated detector during a series;
- the JUNGFRAUJOCH stream: frames keep coming, with their spots;
- back to a file from a live source;
- a handoff job, a launch target (`#albis-open=`), Duplicate Window;
- no uncaught error in the page throughout.

Run it on `main` first and keep the fingerprint, then on the change:

```bash
git switch main && npm run ui:live -- --out /tmp/live-main.json
git switch <branch> && npm run ui:live -- --out /tmp/live-branch.json --compare /tmp/live-main.json
```

Run it on a quiet machine. The checks wait for what the viewer shows, so
anything else competing for the browser shifts their timing: other ALBIS pages
left open (each polls the server every second), or headless Chrome instances
left over from an interrupted script. With three such pages running, an
unchanged `main` failed four checks; with them closed, it passed every time.

It fails on any failed check and on any recorded value (file, frame, shape,
data range, dataset, series) that differs from the earlier run. Three runs of
an unchanged `main` record the same values.

What it cannot see: a real detector's timing, network and firmware; the
Detector tab's acquisition controls against hardware state; a real
JUNGFRAUJOCH's stream (multi-channel, compressed images, its own series
handling); file-writer output appearing while it is written.

## 2. At a detector

About 30 minutes. Note the detector model, firmware/SIMPLON version, the ALBIS
commit (Help → About) and the browser, then go through the list; for each
item, what you should see. Stop at the first surprise and note what you did
just before it.

### SIMPLON monitor (Data tab → Live source: SIMPLON)

1. Enter the detector's address and press **Test**: the version found is
   shown; a wrong port says so (try one), and a host that does not exist says
   so.
2. With the monitor enabled, start a series from the detector's own control
   software: frames appear, the series and image numbers in the panel count
   up, the live badge shows live.
3. Start a second series: the series number changes and the image follows.
4. Stop the series: the last image stays; the status says no new frame.
5. Switch back to a file in the Data tab: the file opens, polling stops (the
   live badge goes off).

### Detector tab (Settings → Viewer → Beta: detector control)

6. Connect: state, model and serial appear; nothing is written to the
   detector on connect.
7. Initialize (if idle-uninitialized): the state goes through initialize to
   idle; the progress shows.
8. Change count time or frame time and apply: the value read back matches; an
   out-of-range value is refused with the detector's own reason.
9. **Single image**, then **Acquire** a short series (a few images): progress,
   then the result, and the files in the output list; open one from there.
10. **Live**: frames update until stopped; stop returns to idle.
11. Disconnect the network cable (or stop the DCU) during a poll: the tab says
    the detector cannot be reached, and recovers when it is back.

### Pixel mask (Data → Pixel Mask, beta)

12. Read the detector's mask: the counts match what the detector reports.
13. Copy it to a file: the HDF5 file appears in `masks/` and opens.
14. Send an edited mask (one extra masked pixel), confirm: the result is
    verified, and a backup is listed. Restore from the backup: the original
    count is back. A refused write (e.g. while armed) shows the detector's
    reason.

### File writer and folder watch

15. Point folder watch at the file writer's output folder and record a series:
    each new master file opens as it appears, without errors while it is still
    being written.

### JUNGFRAUJOCH (if available)

16. Enter the preview endpoint, **Test**, then select the source: frames
    arrive, spots are drawn, the channel selector picks the channel.

### Report

Pass, or for each failing item: what you did, what you saw, what you expected,
and the backend log (Help → View Log) around that time.
