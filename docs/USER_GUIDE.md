# ALBIS User Guide

ALBIS is a free, open-source viewer for DECTRIS detector data, in the style of ALBULA. It opens HDF5 stacks, TIFF, CBF, EDF and MYTHEN acquisitions, follows a running experiment live, and runs on your own computer or on the machine that holds the data, viewed from a browser elsewhere.

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
- [Detector geometry](#detector-geometry)
- [Resolution rings and reflections](#resolution-rings-and-reflections)
- [Follow a running experiment](#follow-a-running-experiment)
- [Combine a series into one image](#combine-a-series-into-one-image)
- [Get data back out](#get-data-back-out)
- [Compare two views](#compare-two-views)
- [Test a detector (beta)](#test-a-detector-beta)
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

### Watching a series that is still being written

You can open an HDF5 stack while the detector is still writing it. ALBIS reads
the frames flushed so far, and then keeps up: the frame slider grows as the
acquisition does, and the status line says how many frames have arrived. The
frame you are looking at, the playback position and the mask are left alone
while that happens — following a run does not mean being dragged back to the
first frame every second.

When the filewriter closes the file, ALBIS notices and stops asking. Nothing to
switch on, and nothing to switch off afterwards.

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

**Export CSV** writes the plots you are looking at, for use elsewhere. Every
plot gets its own pair of columns side by side — a line profile its distance
and intensity, a histogram its intensity and count — because they do not share
an x axis and stacking them would stop the file being one table. Shorter series
simply end early. The file opens as a single table in Excel, Numbers and Origin,
and `pandas.read_csv(path, comment="#")` gives you a DataFrame directly.

The leading `#` lines name the ALBIS build, the file and frame, and the ROI the
numbers were measured over. Scalar statistics are not written: every one of them
can be recomputed from the exported columns.

---

## Detector geometry

In the **Data** tab, **Detector Geometry** shows the numbers ALBIS calculates
with: detector distance (mm), pixel size X and Y (µm), photon energy (eV), beam
centre (px), and a DIALS `.expt` **geometry file** if one is in use. They drive
the resolution rings, the cursor's **d** readout, the d column of the peak
table, and the geometry a series sum records. ALBIS reads them from the image
metadata: the file header, the HDF5 master file, or the live stream. The
badge says whether they came from the metadata, are incomplete, or are manual.

When the metadata is missing a value, or states a wrong one that you cannot fix
at the detector, switch on **Override manually** and type the value in. A
changed field is outlined, and the hint below it shows what the metadata says.
Your values replace the metadata **for every image**, from file to file and
from one session to the next, until you switch the override off. Fields you
leave alone keep following the metadata, so the energy still changes with each
frame, for example. Empty a field to hand it back to the metadata. **Reset to
metadata** discards all your values at once, and a geometry file with them.
Dragging the beam centre on the image is the same as typing it, and switches
the override on. What a geometry file has to contain, and how to make one, is
in the [Power User Guide](POWER_USER_GUIDE.md#geometry-files).

Because the override outlasts the image you set it for, the **Resolution Rings**
and **Peak Finder** sections repeat the values in effect in one line, marked
*manual* while the override is on. **Edit** there opens this section.

The **PILATUS 12M at Diamond beamline I23** needs no geometry file. Its 24
module rows sit on a half cylinder around the sample, which a file header cannot
describe, so ALBIS recognises the detector by its serial number (S/N 120-0100)
in CBF and DECTRIS TIFF files and uses the detector's fixed geometry, shown as
*Auto geometry: DLS I23 PILATUS 12M 120-0100*. The distance shows the
sample-to-detector distance along the beam, about 260 mm: the header's
`Detector_distance` (0.010 m at I23) is an offset from the detector's fixed
position, and ALBIS reads it the way DIALS does. An `imported.expt` next to the
data, or one chosen as the geometry file, still takes precedence.

---

## Resolution rings and reflections

Also in the **Overlay** tab.

**Resolution Rings** draws rings at the d-spacings you choose, from the values
in [Detector geometry](#detector-geometry); the line at the top of the section
shows them, and **Edit** goes there.

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
or a flat-field TIFF. **Apply mask** keeps masked pixels out of the result. The
**?** next to Mode, Operation and Normalization explains each choice; hover
over it, or click or tap it to keep the explanation open.

Long runs report progress and can be cancelled. The output path is prefilled
and the result opens directly from the panel when it finishes.

---

## Get data back out

| You want | Use |
| --- | --- |
| The frames as TIFF or CBF | **File → Convert Dataset…** (`⇧⌘X`) |
| A figure for a slide or a paper | **File → Export Image…** (`⇧⌘E`) |
| An animation of a series | **File → Export Animation…** (`⌘G`) |
| A quick picture of what is on screen | **File → Save As → Full Image / Visible Area / Viewer Window** |
| ROI numbers for analysis | **Export CSV** in the Overlay tab |

**Export Image** writes the frame, or the visible area, as a PNG made for
slides and papers. **Size** enlarges it by 1×, 2×, 4× or 8×, turning each
detector pixel into a sharp square; the default is the smallest size at least
2000 pixels wide. Without that, a small image is enlarged by whichever program
shows it, and looks blurry. Sizes too large for a browser to create are greyed
out. **As on screen** uses the viewer's zoom instead: zoom in, choose
**Visible area** and **As on screen**, and the PNG is what you see. Non-square
detector pixels are drawn at their true proportions. **Print resolution** (300
dpi by default) is stored in the file, so a layout program gives it the right
printed size; the dialog shows that size. **Include overlays** draws the
resolution rings, found peaks and the ROI as the viewer shows them. **Pixel
values** draws each pixel's value into it, as the viewer does when zoomed in.
It needs the pixel values switched on in the viewer, and a size at which each
pixel is at least as large as the viewer's minimum for them (18 px unless
changed in Settings) — in practice **As on screen** while zoomed in. An option
that cannot be used right now says why underneath it.

The quick exports under **Save As** need no dialog. **Full Image** writes one
image pixel per detector pixel: the exact data, for analysis or another
program. **Visible Area** writes what you see, without the interface: at the
viewer's zoom and pixel proportions, each detector pixel a sharp block (zoomed
out below 1×, it keeps one pixel per detector pixel). **Viewer Window** is a
screenshot of the whole window, the image in it as sharp as on screen.

**Convert Dataset** writes all frames, the current frame, or a range. Exports
are signed integers using the common detector convention: module gaps are `-1`,
bad or saturated pixels are `-2`.

### What the exported header keeps

TIFF and CBF exports carry the same header text — CBF in its miniCBF header,
TIFF in the standard `ImageDescription` tag, which `tifffile`, PIL and ImageJ
all show. A TIFF additionally keeps the DECTRIS private tag, which holds the
series id, image number, threshold ids and lost-pixel count that the text has
no place for. The header states, where the source states them: detector model, serial and location,
the acquisition timestamp, pixel size, sensor thickness, exposure time and
period, tau, count cutoff, threshold setting, gain setting, wavelength,
incident energy, detector distance, beam centre, start angle and angle
increment. Three further lines record that ALBIS produced the file, which
source and frame it came from, and the pixel substitutions above — an exported
frame is derived data, and a header that did not say so would read to XDS or
DIALS as raw detector output.

Four kinds of line are deliberately **not** carried over from a source CBF:
`N_excluded_pixels`, `Excluded_pixels`, `Flat_field` and `Trim_file` describe
corrections applied to the raw pixel array, which the export has already
altered, so repeating them would describe an array that no longer exists.
`Image_path` is dropped too — the provenance line records the source file's
name rather than its directory, so that a file you send to a collaborator does
not carry your folder layout.

How much survives depends on the source. HDF5 and DECTRIS TIFF carry the most.
A CBF or EDF source gives what its own header states, which for a PILATUS
miniCBF is nearly all of the list above.

A summed or averaged **TIFF** keeps the instrument facts — detector model,
serial and location, pixel size, sensor thickness, tau, threshold setting,
gain, wavelength, incident energy, detector distance and beam centre — and
states what it is made of (`# Combined: sum of 10 frames 1-10`). The
per-exposure fields are deliberately left out: a sum of ten one-second frames
is not a one-second exposure, its values can exceed the per-frame count cutoff,
and it spans a wedge of rotation rather than one step. The header says so
rather than leaving you to wonder whether they were simply unknown.

A summed or averaged **HDF5** output keeps the source's `/entry/instrument`
detector and beam metadata — description, sensor thickness, `count_time`,
`frame_time`, `saturation_value`, incident wavelength — with their units, plus
the per-threshold channel groups. The pixel mask and any bulk array are not
copied: a summed output has its own masking applied, and the mask describes the
source frames. Geometry you have corrected in ALBIS is written afterwards and
wins over the source's own copy of it.

**Export Animation** renders a GIF matching the screen exactly — colour map,
contrast, mask and saturation highlighting all apply, and non-square pixels
keep their proportions. Choose the frame range and step, the full image or just
the visible area, a size, and the frame rate. Sizes above 1× (2×, 4×) enlarge
each detector pixel into a sharp block; **As on screen** uses the viewer's
zoom. The default is the largest size up to 1600 pixels on its longer side —
enough for a slide without an outsized file. A live summary estimates the file
size before you commit; frame count, region and size are the levers that
control it.

Tick **Pixel values** to write each pixel's value into every frame, read from
that frame. It has the same conditions as in Export Image: pixel values shown
in the viewer, and pixels exported at least as large as the viewer draws them,
so typically **Visible area** with **As on screen** while zoomed in. Labels in
a GIF have a heavier dark outline than on screen, so the digits stay readable
with only two colours to draw them in.

Tick **Include overlays** to draw the resolution rings and the spot finder into
the GIF as well. The option is only available when at least one of them is
switched on in the Overlay tab. Two things are worth knowing:

- The spot finder **re-runs on every exported frame**, so the markers belong to
  the frame they sit on rather than to whichever frame was on screen when you
  started. That is the honest result, and it is also the slow one — a long
  series with the spot finder on takes noticeably longer to export.
- Overlays are drawn opaque, not translucent as on screen. A GIF has 256
  colours and no alpha channel, so the halos that soften the rings on screen
  become solid; the rings are a little heavier in the file than in the viewer.

The three **save image** entries differ in what they capture: the whole frame at
full resolution, only the part you are looking at, or the viewer window as it
appears.

---

## Compare two views

**File → New Window** (`⌘N`) opens an empty second viewer. Use it to put two
datasets side by side, or the same dataset at two thresholds.

**File → Duplicate Window** (`⇧⌘N`) opens the *current* image again in a second
window, set up the way this one is: same frame and threshold, colour map,
contrast, zoom and position, mask, resolution rings, spot finder and ROI. It is
the quicker way into a side-by-side comparison — duplicate, then change one
thing in the copy.

The duplicate starts **independent**: it does not follow this window unless you
switch the link control on. That is deliberate, since the usual reason to
duplicate a view is to make the two differ. What is not copied is anything the
new window works out for itself — the decoded frame, and the image's position
within a window that may be a different size.

A duplicate cannot be made of a live source (SIMPLON, JUNGFRAUJOCH, or pushed
frames): those frames arrive over a stream this window alone is subscribed to,
and there is no file for a second window to open. The menu entry greys out and
says so.

The **link** control in the toolbar chooses what the windows share: **Position**
(pan and zoom), **Contrast**, and **ROI**. Link position to keep both views on
the same feature while you navigate; unlink to look at different regions with
the same contrast.

---

## Test a detector (beta)

The **Detector** tab is a quick way to check a DECTRIS detector: connect, take a
test series and watch it live, without writing code. It works with any detector
that speaks SIMPLON 1.6 or later, from an EIGER1 to a PILATUS4; ALBIS reads the
detector's API version when it connects. It is a convenience for first tests, not a
replacement for a beamline control system or scripted acquisition.

**Good for:** a first image after installing or moving a detector, checking that
it responds and what it is set to, demonstrations and training, and learning the
SIMPLON API: every setting's **?** names its key (for example
`detector/config/count_time`), the name to use when you later script it.

**Not meant for:** experiments run by a beamline control system (the tab can be
used alongside one, and flags changes it makes, but does not coordinate with
it), scans or synchronisation with motors, sequences of series, unattended
operation, or an ALBIS shared with others: anyone who can open ALBIS can use the
tab while it is switched on.

It is off by default. Switch it on under **Settings → Viewer → Beta: detector
control**; while it is off, ALBIS cannot drive a detector at all. Then enter the
detector's address and press **Connect**. Every field is built from what the
detector reports, so units, limits and choices are that detector's own:

- **The main button** offers the one sensible next step: **Initialize** after
  power-up (up to two minutes), **Acquire** when ready, **Stop** while a series
  runs. With **Show images while acquiring** on, the viewer shows the series
  live as it is taken: a preview of the newest image, not every frame.
- **Acquisition** holds the series, timing, energy and thresholds, and **Images**
  chooses which images the detector delivers. A value outside the detector's
  range is refused before it is sent.
- **Data output** switches the file writer, stream and monitor, and lists the
  files on the detector for download.
- **Advanced** holds every other documented setting; **Troubleshooting** has
  re-initialize, reset the stream and delete the files on the detector, each
  behind a confirmation that says what it does.

To try it without a detector, `python test_scripts/fake_simplon_dcu.py` runs a
simulated one at `http://127.0.0.1:8100` (`--thresholds 4` for a
PILATUS4-like detector).

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

To look at a file from your own computer, drag it onto the image: ALBIS uploads
a copy into the server's data folder and opens it. On the machine that runs ALBIS, dropping is
switched off, since **File → Open…** reads the file where it is without copying
it.

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
| `⇧⌘E` / `⌘G` / `⇧⌘X` | Export image… / Export animation… / Convert dataset… |
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

When a newer release exists, the dialog names the one file that matches how your
copy of ALBIS was installed, so you do not have to pick it out of the release
page yourself — the AppImage, the Windows installer or portable archive, or the
macOS disk image for your processor. Running in Docker or from a source
checkout, it shows the command to run instead, with a button to copy it.

**Download Update** fetches that file and checks it. ALBIS compares what
arrived against the checksum the release published for it, and tells you the
result: a file that does not match is deleted rather than handed over. When it
does match, the dialog shows the SHA-256, where the file was saved, and a
**Show in Folder** button. Nothing is downloaded until you click, and the whole
step can be switched off in **Settings → Connection**, which leaves the dialog
offering the download link only.

By default you apply the update yourself from that folder. If your installation
has **Settings → Connection → Install updates from ALBIS** switched on — it is
off unless someone turned it on — a verified download also gets an **Install
Update** button. On Linux that
replaces the AppImage you are running; on Windows it runs the installer, which
closes ALBIS and updates it in place. Either way ALBIS closes when it is done
and you start it again; it does not restart itself. It will not install an
update it could not verify, and it will not install one while a live watch,
series sum or export is running — the dialog says so and offers the folder
instead.

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
