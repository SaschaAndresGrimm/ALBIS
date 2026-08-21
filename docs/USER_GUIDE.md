# ALBIS User Guide

This guide is organised by what you are trying to do, not by where the buttons
live. Each section is self-contained — jump to the one that matches your task.

If you only want to install ALBIS, see the [README](../README.md). If you are
configuring a server, scripting against the API, or running in Docker, see the
[Power User Guide](POWER_USER_GUIDE.md). Press **F1** inside ALBIS for a short
reference of the same material.

## Contents

- [Open your data](#open-your-data)
- [Move through a series](#move-through-a-series)
- [Make the image readable](#make-the-image-readable)
- [Measure a region](#measure-a-region)
- [Resolution rings and reflections](#resolution-rings-and-reflections)
- [Follow a running experiment](#follow-a-running-experiment)
- [Combine a series into one image](#combine-a-series-into-one-image)
- [Get data back out](#get-data-back-out)
- [Compare two views](#compare-two-views)
- [Work from another machine](#work-from-another-machine)
- [Keyboard shortcuts](#keyboard-shortcuts)
- [What ALBIS sends over the network](#what-albis-sends-over-the-network)
- [When something looks wrong](#when-something-looks-wrong)

---

## Open your data

**File → Open…** (`⌘O` / `Ctrl+O`) opens the file browser. ALBIS reads:

| What you have | What to open |
| --- | --- |
| An HDF5 stack (filewriter1 or filewriter2) | the `_master.h5`, or a `_data_*.h5` directly |
| Single images | `.tif` / `.tiff`, `.cbf`, `.cbf.gz`, `.edf` |
| A numbered image series | any one file — ALBIS finds its siblings |
| A MYTHEN(2) acquisition | the acquisition's `.cfg` file |

A numbered series such as `scan_00001.cbf … scan_00500.cbf` is recognised from
one member: open any of them and the frame slider covers the whole series. In
the file browser, a series is collapsed to a single entry so a folder of
thousands of frames stays readable.

A **MYTHEN(2)** acquisition is a folder holding one `.cfg` descriptor and one
`FrameNNNN.dat` per exposure. Open the `.cfg` and the whole run is assembled
into a single image: channel across, frame down, counts as intensity.

### Open Recent

**File → Open Recent** lists the last ten files you opened, newest first, so
yesterday's dataset is two clicks away instead of a walk back through the file
browser. The list survives closing the browser, and **Clear Recent Files**
empties it. Files loaded by a watched folder or a live stream are not listed —
those are frames that arrived, not files you chose. An entry that can no longer
be opened (a cleared scratch directory, an unplugged mount) says so and removes
itself.

### Choosing the dataset and threshold

An HDF5 file usually contains more than one dataset. The **Data** tab lists the
image-capable ones; pick the one you want. Multi-threshold (multi-channel) data
gains a **Threshold** selector in the toolbar, and `⌘K → Threshold: Next` steps
between channels without leaving the keyboard.

The same tab has the **HDF5 inspector**: browse the file tree, read attributes,
and search for a dataset by name when you know what you are looking for but not
where it lives.

---

## Move through a series

- The **slider** and the frame number field jump anywhere in the series.
- `←` and `→` step one frame.
- **Play** runs through the series; the **Playback** popover sets the rate in
  frames per second.

The chosen rate is a ceiling, not a promise. If frames arrive more slowly than
the rate, playback simply runs slower rather than skipping or stalling.

Frames you have already seen are kept in memory, so stepping back is instant
and costs no transfer. The budget is memory rather than a frame count, because
a frame ranges from about 4 MB on an EIGER 1M to about 18 MB on a 4M — set it in
**Settings → Viewer → Frame cache**, or to `0` to switch it off. Nothing is
cached while a live source is running, since the file may still be growing.

---

## Make the image readable

**Contrast** is the control you will reach for most.

- **Autoscale** (on by default) picks a sensible range for each frame.
- Turn it off and set **min** and **max** by hand to compare frames on a fixed
  scale — essential when judging whether a spot got stronger.
- **Shift + left-drag** on the image adjusts contrast and brightness directly:
  horizontal for contrast, vertical for brightness.

**Colour maps** are in the **View** tab: Grey Scale, Heat, Viridis, Magma,
Inferno, Cividis, Turbo, and **ALBULA HDR** for a high-dynamic-range look
familiar from ALBULA. **Invert Color** flips any of them.

**Zoom and pan**

- Mouse wheel zooms at the cursor.
- Left-drag pans, including while zoomed out.
- Double-click zooms in a step.
- **Fit to Window** in the View tab returns to the whole frame.

**Pixel values** in the View tab prints the number in each cell once you are
zoomed in far enough to read them. How far, how many, and their format are in
**Settings → Viewer**.

**Masks.** When the data carries a pixel mask, **Apply mask** hides module gaps
and defective pixels so they do not distort what you see or the statistics you
measure. **Mask saturated** does the same for pixels at the detector's
saturation value. Both are unavailable — and say so — when the loaded data
provides no mask or no saturation limit.

---

## Measure a region

Everything here is in the **Overlay** tab, under **Statistics and ROI**.

Pick a **Mode** — Line, Box, Circle or Annulus — then **right-drag** on the
image to place the region. **Center on beam** snaps a circle or annulus to the
beam centre, which is usually what you want for powder rings.

For the region you draw, ALBIS reports **min, max, sum, mean, median and
standard deviation**, along with a pixel census: total, gap, defective and
saturated. The census matters — a mean over a region half-covered by a module
gap is not a mean of anything, and the counts tell you when that is happening.

With no ROI drawn, the same statistics describe the whole image.

Three plots accompany the region:

- **Line Profile** — intensity along a line ROI, with the X axis switchable to
  pixels, **d (Å)** or **Q (1/nm)**.
- **Profile along X / Y** — collapsed profiles of a box ROI.
- **ROI Histogram** — the value distribution, with automatic or fixed bins and
  an optional log count axis.

Plot axes autoscale by default; switch to **Manual axes** to pin them while
stepping through frames, so the shape you are watching does not rescale
underneath you. Drag on a plot to pan, wheel over an axis to zoom, double-click
to reset.

**Export CSV** writes the current statistics and profile for use elsewhere.

---

## Resolution rings and reflections

Also in the **Overlay** tab.

**Resolution Rings** needs four numbers: detector distance (mm), pixel size
(µm), photon energy (eV) and beam centre (px). ALBIS fills these from the file's
metadata when they are there. Where they are not — or where they are wrong —
type over them; **Reset to live** restores the values from the data. A DIALS
`.expt` **geometry file** can supply them instead.

Enter the ring positions you want in **Rings (Å)**. Once rings are on, the
cursor readout gains a **d** value, so pointing at a feature tells you its
resolution.

**Peak Finder** detects reflections in the current frame. Set how many to look
for and a minimum signal-to-noise ratio; found peaks are listed with position,
intensity, SNR and resolution, and are drawn over the image. Selecting a row
highlights that peak.

---

## Follow a running experiment

The **Data** tab's source selector switches between file and live modes.

**Watch folder** polls a directory and loads the newest matching file. Choose
which file types to watch and, if you need it, a filename pattern. Use this when
another program is writing frames to disk.

**SIMPLON monitor** reads the live monitor image straight from a DECTRIS
detector. Enter the hostname or IP — `http://` and port 80 are filled in for
you — and press **Test** (or Enter in the address field). On success it names
the detector and serial number, so you can confirm you are pointed at the right
instrument; on failure it tells you which failure it was: unknown host, refused
port, wrong API version, or timeout. Addresses that have answered before are
offered as autocomplete.

**JUNGFRAUJOCH Preview** subscribes to a JUNGFRAUJOCH ZeroMQ preview stream and
draws its indexed and unindexed reflections over the image. Enter `host:port`;
`tcp://` is filled in, but there is no default preview port so the port is
required. **Test** checks the port accepts connections — frames confirm once the
preview starts.

**Remote stream** displays frames pushed to ALBIS by your own script through the
Remote Stream API, including any peak overlays the script supplies. See the
[Power User Guide](POWER_USER_GUIDE.md#remote-stream-api) for the endpoints.

While a live source runs, the toolbar shows **live**, and a badge shows when you
have scrolled away from the newest frame — one click returns you to it. Pausing
lets you examine a frame without losing the stream.

---

## Combine a series into one image

**Data → Series Operations** reduces many frames to one: **Sum**, **Mean** or
**Median**.

Choose what to combine — all frames, chunks of N, every Nth frame, or a start
and end range — and optionally normalise first, by a reference frame, a scalar,
or a flat-field TIFF. **Apply mask** keeps masked pixels out of the result.

Long runs report progress and can be cancelled. The output path is prefilled
and the result opens directly from the panel when it finishes.

---

## Get data back out

| You want | Use |
| --- | --- |
| The frames as TIFF or CBF | **File → Convert Dataset…** (`⇧⌘X`) |
| An animation of a series | **File → Export Animation…** (`⌘G`) |
| A picture of what is on screen | **File → Full Image / Visible Area / Viewer Window** |
| ROI numbers for analysis | **Export CSV** in the Overlay tab |

**Convert Dataset** writes all frames, the current frame, or a range. Exports
are signed integers using the common detector convention: module gaps are `-1`,
bad or saturated pixels are `-2`. TIFF exports carry DECTRIS-style header
metadata; CBF exports carry a miniCBF header with detector, pixel size,
exposure, energy, distance, beam centre and rotation where those are known.

**Export Animation** renders a GIF matching the screen exactly — colour map,
contrast, mask and saturation highlighting all apply. Choose the frame range and
step, the full image or just the visible area, a scale, and the frame rate. A
live summary estimates the file size before you commit; frame count, region and
scale are the levers that control it.

The three **save image** entries differ in what they capture: the whole frame at
full resolution, only the part you are looking at, or the viewer window as it
appears.

---

## Compare two views

**File → New Window** (`⌘N`) opens a second viewer. Use it to put two datasets
side by side, or the same dataset at two thresholds.

The **link** control in the toolbar chooses what the windows share: **Position**
(pan and zoom), **Contrast**, and **ROI**. Link position to keep both views on
the same feature while you navigate; unlink to look at different regions with
the same contrast.

---

## Work from another machine

ALBIS runs a local server, so the browser does not have to be on the machine
holding the data. Point a browser at the ALBIS URL and it works as it does
locally, with two differences worth knowing:

- Frames are compressed on the wire for non-local clients, so a remote session
  moves far less data. Nothing changes for a browser on the same machine, where
  the transfer was already instant.
- The frame cache matters more. Revisiting a frame you have already seen costs
  no transfer at all, so raising **Settings → Viewer → Frame cache** helps most
  over a slow link.

Serving other machines needs **Settings → Connection → Allow external
connections**, and behind a reverse proxy you also need to add the proxy's
hostname to **Allowed hosts**. Both are covered in the
[Power User Guide](POWER_USER_GUIDE.md#reverse-proxies-and-remote-access).

ALBIS has no authentication. Treat a shared instance as visible to everyone who
can reach the port.

---

## Keyboard shortcuts

Shown with the macOS modifier. On Windows and Linux use `Ctrl` where `⌘`
appears and `Alt` where `⌥` appears.

| Shortcut | Action |
| --- | --- |
| `⌘K` | Command palette — reaches everything below by name |
| `⌘O` / `⌘W` / `⌘N` | Open… / Close file / New window |
| `←` `→` | Previous / next frame |
| `⌘S` / `⇧⌘S` / `⌥⌘S` | Save full image / visible area / viewer window |
| `⌘G` / `⇧⌘X` | Export animation… / Convert dataset… |
| `⌘,` | Preferences… |
| `F` / `F1` | Full screen / this documentation |

The command palette is the fastest route to anything you do not have a shortcut
for, including switching panel tabs and stepping thresholds.

---

## What ALBIS sends over the network

Almost nothing, and nothing about you. ALBIS has no telemetry and no analytics.
Your images, file paths and the datasets you browse are read from disk and sent
to your own browser; they are never uploaded anywhere.

There is one exception. When the interface starts, ALBIS asks GitHub whether a
newer release exists, so it can tell you when to update. The request carries the
version you are running and nothing else — no file names, no identifier of you
or your machine. If the machine is offline or firewalled the check fails quietly
and everything else keeps working.

To stop it, uncheck **Settings → Connection → Check for updates on startup**.
ALBIS then makes no outbound request of its own accord at all.

Live sources are the other network traffic, and they only ever go to the address
you typed — the SIMPLON monitor or JUNGFRAUJOCH endpoint you connected to. ALBIS
does not look for detectors by itself.

For the precise details, including what a facility's IT group will want to know,
see [Network Behaviour and Privacy](NETWORK_AND_PRIVACY.md).

---

## When something looks wrong

**The viewer says OFFLINE.** The backend has not finished starting, or has
stopped. Wait a moment, then restart the launcher.

**No image appears.** Check the selected dataset and threshold in the Data tab —
an HDF5 file often holds several datasets and only some are images.

**A file will not open.** ALBIS names the reason. The usual cause is a file the
filewriter has not finished writing; retrying once it is complete normally
works.

**Everything fails with 403 and a message about allowed hosts.** You are
reaching ALBIS under a name it does not answer to, which happens behind a
reverse proxy. Add the proxy's hostname to **Settings → Connection → Allowed
hosts**.

**Your settings appear to have reset.** ALBIS could not read its configuration
file and started on defaults rather than refusing to start. The log says which
file and why; saving from the Settings dialog replaces it.

**Overlays look stale during playback.** Pause and re-enable the overlay tool
once.

**Statistics look wrong near a module gap.** Check the gap and defective pixel
counts in the ROI census, and turn on **Apply mask**.

**ALBIS asks you to reload.** The server was upgraded or restarted on a
different build while this tab stayed open, so the page is running older code
than the server it is talking to. Reload and the warning goes away.

For anything else, **Help → View Backend Log** shows what the server is doing,
and the log can be downloaded from there to attach to an issue. The **Versions**
button in the bottom right names the exact build you are running and copies it
to the clipboard — quote that in a report, because a version number alone cannot
distinguish two builds of the same release.
