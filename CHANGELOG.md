# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- ALBIS can open a file the detector is still writing. A filewriter holds its output open in SWMR mode for the length of a series, and HDF5 refuses a plain read-only open of such a file — it reports `file is already open for write`, the same bare `OSError` it uses for a corrupt file. ALBIS translated that into "not a readable HDF5 file (it may be incomplete or corrupt)", so the live-acquisition workflow the viewer exists for did not work at all: every frame, dataset listing and metadata read on a running series was refused, and the message blamed the file. The read-only open now retries with `swmr=True`, which sees the frames flushed so far, and because each request opens its own handle the next one sees the frames written since — so the frame count grows with the acquisition. Plain open first, so a finished file costs exactly what it did before, and when both attempts fail the *plain* error is the one reported, because for a genuinely truncated file it is the one that names the real problem. External and linked targets (filewriter2 master/data layouts) go through the same path. `tests/test_hdf5_live_writer.py` stages a real writer process, because HDF5 lets a process reopen a file *it* holds open — an in-process writer would have proved nothing.

- The JUNGFRAUJOCH preview never showed a frame. `cbor2` documents its tag hook as taking `(decoder, tag)`, and the C implementation shipped in the pinned 6.1.x calls it as `(tag, immutable)` — so ALBIS read the tag out of the wrong argument and raised on every message. Nothing surfaced: the worker caught it, recorded `CBOR decode failed: error decoding semantic tag 40`, and went back to waiting, so a feature named in the README simply produced nothing. The hook now finds the tag among its arguments whichever order they arrive in, and the multidimensional-array decoder accepts a tuple payload, which is what `cbor2` actually hands over for the contents of a tag. The existing tests could not see any of this because they called the hook directly, the way the code expected to be called; the new ones go through `cbor2.loads`, which is the only caller that matters.

- A `NaN` in a detector metadata field no longer breaks the geometry panel. An uninitialised NeXus field reads as `NaN`, and `coerce_scalar` passed it on: it survived the unit conversion, reached the analysis payload, and was serialized as the literal `NaN`, which is not valid JSON — so one unwritten field in a file turned the whole response into a parse error in the browser. Non-finite values are now reported as absent, which is both true and a case the interface already handles.

- `POST /api/analysis/series-sum/start` rejects a `step` of `0` instead of silently summing in chunks of ten. The value was read as `payload.step or 10` while the request model already defaults it to `10`, so the fallback never handled a missing value — it only rewrote an explicit `0`, which made the `step must be >= 1` rule below it unreachable for the one value a user is most likely to send by mistake.

- The settings dialog's four tab panels carried `role="tabpage"`, which is not an ARIA role. A role that does not exist is worse than none: it replaces what the element would have been announced as with nothing, so assistive technology saw a tablist whose tabs pointed at nothing — in the one dialog where the rest of the accessibility work was careful. They are `tabpanel` now, and the tabs and panels are actually associated (`id`, `aria-controls`, `aria-labelledby`), because a valid role on an unassociated panel is still broken. A test rejects any invented role in the shipped HTML.

- The `Host` header check now applies where the attack it exists to stop actually applies. A wildcard bind switched the check off entirely, on the reasoning that a LAN or container client arrives under a name that cannot be predicted. The effect was that the defence was present on a loopback bind, where DNS rebinding cannot reach anything, and absent on `0.0.0.0` — which is the Docker default and the documented way to expose ALBIS on a network. Any page in the user's browser could point its own domain at that address and read `/api/browse`, `/api/files` and `/api/frame`; the cross-site check only covered writes. What is predictable is narrower than a name: rebinding needs one, because the browser copies the name from the URL into `Host` and only DNS can be made to point it elsewhere. An address cannot be rebound, and a cross-origin response fetched from one is still unreadable to the page that asked for it. A wildcard bind therefore answers to any IP literal and to this machine's own hostnames, and refuses a foreign name. Deployments reached under a name ALBIS cannot derive — a reverse proxy, a container service alias — set `server.allowed_hosts`, which the `403` names, and `["*"]` still turns the check off. A non-loopback bind logs one line at startup saying what it will answer to, so the operator who typed `0.0.0.0` learns this at startup rather than from a colleague's `403`.

- Directory scans are bounded, and say when they were cut short. The file list, the folder list and the newest-matching-file search walked with no limit of any kind: unlimited depth by default, no cap on entries, no time budget. Autoload turned that into a poll — about once a second, uncached, for as long as a folder was watched — so on a beamline data directory ALBIS spent every second walking a filesystem it could not finish, holding a threadpool worker each time, and answered with a partial listing that looked complete. Walks now carry a budget (`data.max_scan_entries`, default 200000; `data.max_scan_seconds`, default 5.0) and report `truncated` when they hit it: as a field on `/api/files`, `/api/folders` and `/api/autoload/latest`, and as `X-Scan-Truncated` on the latter — including on its bodyless `204`, where "no newest file because the walk ran out" would otherwise be indistinguishable from "this folder is empty". The interface says the folder is too large to scan rather than that it is empty. The autoload poll is cached under the existing `scan_cache_sec`, and the cache now single-flights its loader, so concurrent pollers share one walk instead of starting their own. `data.max_scan_depth` stays unlimited by default on purpose: a depth limit hides files that sit deeper, silently and forever, while a budget stops at "enough for now" and says so.

- Exported TIFF and CBF files say that ALBIS made them. An export is not a copy of the detector's output — the dtype is widened, masked gaps become `-1`, bad and saturated pixels become `-2` — while the written mini-CBF header declared `SLS_1.0` and listed detector, wavelength, distance and beam centre with no mention of ALBIS, the source file, the frame, or the substitutions. XDS or DIALS read that as genuine detector output carrying values ALBIS chose. Both writers now add provenance: the build that produced the file, the source name with dataset, frame and threshold, and the convention in effect. TIFFs carry it in the standard `Software` and `ImageDescription` tags. Files written without source metadata — a summed series, for instance — still name the build, because those are derived data too. A source's own header text is preserved and appended to, not replaced. Pixels are untouched by any of this, and a test asserts that.

- The Docker image installs the dependency set that is pinned. It carved `hdf5plugin` out of `backend/requirements.txt` and installed `4.1.3` instead of the pinned `7.0.0`, because `4.1.3` has no `aarch64` wheel and had to be built from source — which is what the surrounding `--no-build-isolation --no-deps` step and its extra build dependencies were for. Two consequences: the published image had different HDF5 filter support from every desktop build, so a dataset that opened on a workstation could fail in the container; and `THIRD_PARTY_LICENSES.md` described a version the image did not contain, which the release checklist explicitly requires it not to do. `7.0.0` ships manylinux wheels for both published architectures, so the carve-out is gone and the install is the plain pinned set. The compiler toolchain stays, because `dectris-compression` publishes no wheels at all. `tests/test_shipped_dependency_parity.py` now fails when the Dockerfile names a version the requirements do not, when a requirement is filtered out of the install, or when the licence table and the pins disagree.

### Added

- Configuration can come from the environment and the command line, not only from a JSON file. For the published container images that was real friction: the conventional `docker run -e ALBIS_DATA_ROOT=/data` did nothing, so changing one value meant baking or mounting a file. Every config key now has a matching `ALBIS_<SECTION>_<KEY>` variable, derived from the key so the two cannot drift; `ALBIS_CONFIG` names the file outright and then *replaces* the search rather than joining it, because quietly reading a different file than the one you named is the worst possible outcome. The launcher accepts `--host`, `--port`, `--allowed-hosts`, `--data-root`, `--log-level`, `--language`, `--no-browser`, `--config` and `--version`, each of which sets the environment variable for the same key — one precedence order to explain and one to test. Arguments ALBIS does not recognise are logged and ignored: a desktop build is started by the operating system, which passes things nobody typed (macOS sends `-psn_0_...`, and a document path when ALBIS is used to open a file), and refusing to start over one of those would be a worse failure than ignoring it.

  A key the environment decides cannot be changed by saving the config file, so **Settings** disables those fields and names them, rather than accepting an edit the next start would ignore. `GET /api/settings` reports them as `env_overrides`, and both the launcher and the backend name them in the log at startup — a value from the environment is invisible in the file, so "the file says 127.0.0.1" and "ALBIS is listening on 0.0.0.0" can both be true and only the log explains it.

- **File → Open Recent**: the last ten files opened, newest first, surviving a browser restart. The interface remembered panel widths, section states, autoload settings and SIMPLON hosts, but not which data anyone had looked at, so reopening yesterday's dataset meant walking the file browser back to it every morning. Only explicit opens are recorded — a watched folder loading a new file every second would otherwise fill the list with frames nobody chose — and an entry that can no longer be opened says so and removes itself rather than failing again tomorrow.

- [Compatibility Policy](docs/COMPATIBILITY.md): what a version number promises. `SECURITY.md` said configuration keys and API details may still change *because* ALBIS is `0.x`, which means `1.0.0` withdraws that sentence and replaces it with a commitment — and no document said what the commitment covers. Meanwhile `API_CONTRACTS.md` called the HTTP surface "the stable contract" while two endpoint families are versioned and the rest are not, leaving a reader unable to tell which was the promise. The policy names the covered surfaces (endpoints, parameters, response fields, status codes, binary headers, classification vocabularies, configuration keys, exported file layouts, shortcuts), names what is deliberately not covered (log format, internal module layout, the frontend's own calls, performance characteristics, translations), and states the deprecation path: announced with its replacement, warned for at least one minor release, removed only in a major one. It is published now, while it can still be argued with.

- The Docker CI job decodes a frame. It checked `/api/health` and a file listing, neither of which touches the HDF5 filter plugins — those libraries are only loaded when chunks are actually pulled in, which is why `scripts/make_compressed_hdf5_fixture.py` and the packaged desktop smoke test exist. The one artifact whose dependency install differed from a plain `pip install` was the one artifact never asked to decode anything. It now reads a frame from the compression-filtered fixture and checks the payload length against `X-Dtype`/`X-Shape` and the pixel values against what the generator wrote, so a filter that silently produced zeros fails the run.

### Changed

- The frame slider follows a series that is still being written. The SWMR fix made a live file *readable* and `/api/metadata` reports the current frame count on every request — but the interface asked once, at open, so a viewer opened during a run showed the count the acquisition happened to have at that moment and the frames written since were unreachable without closing and reopening the file. Half a fix is easy to mistake for a whole one.

  `/api/metadata` now also reports `writer_present`, which comes free: needing SWMR to open the file at all *is* the signal that someone still holds it. While that flag is set the interface re-reads the count about once a second and grows the slider; when the filewriter closes the file the flag drops and the polling stops by itself, so a finished file costs nothing. The re-read deliberately touches the count and nothing else — not the frame on screen, not the playback position, not the mask — because `loadMetadata` resets all three, and on a one-second timer that would drag the viewer back to the first frame for as long as the run lasted. Verified against a real writer process through a real server: 2 → 4 → 5 → 6 frames while the run was live, then settled when it ended.

- `GET /api/browse` is bounded like the other scans. The recursive walks were budgeted first because they looked like the risk, but this is the endpoint the file browser panel actually calls, and it had no cap at all — while a beamline folder is frequently flat and enormous, one directory per run holding a hundred thousand frames. Unbounded that meant a `stat` per entry, a dict per entry, a sort across all of them and the whole listing serialized, for something nobody could read. It now shares one entry budget across its folder and file passes and reports `truncated`.

  Writing the test for it found a bug in the budget itself: "0 means unlimited" was stored as the same value an exhausted counter reaches, so once the allowance ran out every remaining entry was let through — silently, and only in the second pass. Unlimited is now held separately from the count.

- ALBIS is built on **Python 3.13**, not 3.10. 3.10's upstream support ends in October 2026, and the desktop artifacts bundle the interpreter — every `.dmg`, `.exe` and `.AppImage` ships a CPython inside it — so releasing `1.0` on 3.10 would have meant shipping an interpreter that stops receiving security fixes weeks later. The timing is also a compatibility question: `docs/COMPATIBILITY.md` puts the Python a source checkout needs inside the covered surface, so raising that floor after `1.0` would be a major-release change. Now it costs a changelog entry.

  3.13 rather than 3.12 or 3.14 because it needs **no dependency changes at all**: every pinned requirement already has 3.13 wheels at the exact version ALBIS ships, and the one sdist-only dependency (`dectris-compression`, a C extension) builds cleanly. 3.12 would have bought a year less for identical work. 3.14 is blocked only by the pinned `numpy`, whose 3.14 wheels start at 2.3 — hence the `<3.14` upper bound in `requires-python`, which says exactly what is tested and comes off when numpy is bumped.

  Nothing changes for anyone using a packaged build: the interpreter is inside the bundle, and there is nothing to install. Running from source now needs 3.13, which the Power User Guide and the Developer Guide both say.

  Verified rather than assumed: the pinned dependency set installs unmodified on 3.13, the full suite passes on it, and a real PyInstaller build of the app passes the same packaged-binary smoke test CI runs — the one that decodes a compression-filtered HDF5 frame and checks that zstd came along.

- A test now compares every statement of the supported Python version against `.python-version`: `pyproject.toml`, the ruff and black targets, all four workflows, and the Docker base image. The image is checked twice, because the version appears in the tag *and* inside `HDF5_PLUGIN_PATH` — and the second one fails silently. A stale plugin path builds, starts, and answers `/api/health` while quietly losing every HDF5 compression filter, so real detector data stops decoding with nothing to show for it. The container frame-decode smoke test added earlier is the other half of that guard.

- The coverage gate is `77`, not `50`. Actual coverage was 76%, so 26 points of real coverage could be lost without CI noticing; the new tests took it to 79% and the gate now sits just under that, with enough margin for the platform-specific branches in `os_actions`. The lift came from the places that only fail in front of a detector or a beamline filesystem: the series-summing request gate (57% → 93%), the unit conversions all detector geometry is read through (63% → 99%), and the JUNGFRAUJOCH message handling (58% → 83%) — where writing the tests is what found the decode bug above.

- The development tooling is pinned, and the commit hooks now run what CI runs. `pytest`, `pytest-cov`, `httpx`, `ruff` and `pre-commit` floated to whatever was newest, so an unrelated upstream release could turn a commit that changed nothing into a red build — a real prospect for `ruff`, which adds rules. Worse, `.pre-commit-config.yaml` pinned ruff `v0.9.7` while CI installed the latest (`0.16.4` today): seven minor versions apart, checking different rules, so the hook and the gate disagreed by construction.

  The hooks also ran `ruff-format` while CI gates on `black` — two formatters, and not compatible ones: `ruff format` wanted to rewrite three files that `black` considers correctly formatted. Anyone who had installed the hooks was having commits reformatted into a state CI would then reject. `ruff-format` is gone, black owns formatting, and `ruff` lints. Tests now fail when a dev requirement loses its pin, when a hook version stops matching the pinned one, or when a second formatter reappears.

- `ruff` now also checks `albis_launcher.py`. It was outside the lint scope in every workflow, which is how a file with argument parsing, socket handling and platform branches had no linting at all.

- Two configuration keys added for the scan budget: `data.max_scan_entries` and `data.max_scan_seconds`, both documented in the Settings Reference and both in the published schema.

- `backend/services/root_scan_cache.py` becomes `backend/services/scan_cache.py`, keyed by scan rather than by "root files" and "root folders", so the autoload poll can share it. The directory walks move out of `backend/app.py` into `backend/services/directory_scan.py`, where the budget and the truncation reporting are unit-testable without starting the app.

- A cached listing is dropped after an upload. The upload flow asks for the file list to find the file it just wrote, and a root scan taken a moment earlier did not contain it.

### Documentation

- The configuration file ALBIS writes on a first packaged run failed validation against the schema ALBIS ships for validating it: `launcher.startup_health_timeout_sec` was in `DEFAULT_CONFIG`, in no schema and in no document, and `additionalProperties` is `false`. The schema also documented a `startup_timeout_sec` default of `5.0` that the code has never used — it is `10.0`, as were the example config and the guide wrong about it. The check that fails when a `ui` key is undocumented now covers every section, and a second one compares the schema's keys and defaults against the code, so this class of drift cannot ship again.


## [0.12.0] - 2026-08-21

### Added

- ALBIS knows which build it is. The commit was already resolved at packaging time to name artifacts like `ALBIS-linux-x64-v0.12.0-a1b2c3d.tar.gz`, then discarded, so the running program could not say which build it was and neither could a bug report: two builds of one release are indistinguishable by version number. `scripts/stamp_build.py` writes it before packaging, the Docker image takes it as a build argument, a checkout falls back to `git`, and `/api/health` reports it. An unstamped build reports no commit and shows its version alone rather than inventing one.

- The footer reports whether the release is current, instead of the answer existing only inside a dialog that opened once at startup and was then gone. The row becomes a control only when an update is actually pending, in which case it opens the existing update dialog rather than duplicating its release link.

- **Copy build info** puts the version, commit, server state and browser into the clipboard for pasting into an issue. It falls back to a selection-based copy where the Clipboard API is unavailable — that is a plain-HTTP LAN session, which is a documented way to run ALBIS and therefore the ordinary case for a remote user rather than an edge case.

- ALBIS now notices when a tab has fallen behind the server. The caching policy already rules out being *served* a stale frontend: entry documents are `no-store` and modules revalidate against an ETag. What it cannot rule out is a viewer left open across an upgrade — the code in memory then predates the server it is talking to, and only a reload fixes it. The footer says so and offers the reload. Because the check compares version *and* commit, it also catches a rebuild of the same release, which a version comparison misses.

- A test that `VERSION`, `package.json`, `pyproject.toml` and `CITATION.cff` all agree on the version. Nothing compared them before: the release workflow checked the git tag against `VERSION` and a human was asked to eyeball the rest, so two of the four could drift a release apart without any check failing. A second test fails when a `ui` configuration key has no entry in the Power User Guide's Settings Reference.

### Changed

- The **Versions** footer names the build rather than repeating itself. It showed two rows, `Frontend: local` and `Backend: v0.11.0`. The first was a string literal in `app.js` that no build step ever replaced, so it read `local` in every shipped artifact on every platform; the second repeated what **Help → About** already said. Since the frontend and backend ship inside one bundle they cannot disagree on a version, so the popover asked a question that had no answer and answered one nobody asked. It now shows one line — `ALBIS v0.12.0 · a1b2c3d` — naming the commit the build came from.

- The security policy no longer disclaims the version that is actually shipping. It promised triage for `1.x` releases and best-effort for `0.x`, while every user was on `0.x` — so read literally, no released version was supported. Support is now stated against the latest published release regardless of its number, with the `0.x` caveat limited to what semantic versioning actually implies about configuration and API stability. The scope section also says outright that an unauthenticated listener exposed to the internet is a deployment choice rather than a vulnerability.

- The Python packaging metadata called ALBIS `Development Status :: 5 - Production/Stable` while the version was `0.11.0`; it now says `4 - Beta`, matching the version and the security policy.

### Documentation

- ALBIS now states what it sends over a network (`docs/NETWORK_AND_PRIVACY.md`), because it was not saying so anywhere a user or a facility's IT group would look. There is no telemetry and never was — but the interface asks GitHub for the latest release at every startup, and the only trace of that in any document was one line of comment in the example configuration. The new document names the exact URL, what the request contains (the running version, in the `User-Agent`, and nothing else), how often it happens, what happens when it fails, and how to switch it off. It also says plainly where image data, paths and logs stay, and separates the traffic ALBIS starts by itself from the SIMPLON and JUNGFRAUJOCH addresses the user typed. Summarised in the README, the User Guide, and the built-in help (F1).

- The Settings Reference documented six of the eight `ui` configuration keys. The two missing were `language` and `auto_check_updates` — the second being the off switch for the only outbound request ALBIS makes, which meant the setting existed, shipped enabled, and was undocumented. A test now fails when a `ui` key has no entry.

- ALBIS can be cited. `CITATION.cff` gives GitHub a **Cite this repository** button with APA and BibTeX, which for software used to produce published results is the difference between being credited and being a footnote nobody can resolve. The release checklist gained the one-time Zenodo setup for a permanent DOI, and the file's version is now covered by the release version check.

- Troubleshooting covers the two things the footer now surfaces: what the reload prompt means, and that the **Versions** button copies the exact build to quote in a report.

## [0.11.0] - 2026-08-21

### Documentation

- There is a **User Guide** (`docs/USER_GUIDE.md`), organised by task rather than by where the buttons are: opening data, moving through a series, making the image readable, measuring a region, resolution rings and reflections, following a running experiment, combining a series, exporting, comparing two views, and working from another machine. Until now the documentation went from an install page straight to a configuration and API reference, with nothing covering the work itself. Linked from the README and the built-in help.

- MYTHEN(2) acquisitions are documented. Support has shipped for some time — a dedicated reader, its own error handling, its own tests — but no document a user reads mentioned it, so nobody with a strip detector could discover it. The README, the format list and the in-app help now cover opening an acquisition through its `.cfg`.

- The built-in help (F1) lists the keyboard shortcuts. The app advertises twelve in its own menus and the help described two, so the command palette, dataset conversion and animation export were reachable only by chance.

- The built-in help stopped describing a dialog that no longer exists: it named settings groups from before Settings was regrouped into tabs, and pointed at the README for API details that live in the Power User Guide. Both are corrected, and the guide is now linked from the help.

- Troubleshooting covers what actually goes wrong: a file that will not open yet, the `403` you get behind a reverse proxy before setting an allowed host, settings that appear to have reset because the config could not be read, and the first-launch warning on Windows.

- The README no longer implies 1.0 has shipped, and its highlights list mentions exports, remote performance and the thirteen-language interface.


### Security

- ALBIS no longer answers requests a web page made on the user's behalf. It has no authentication, on the reasoning that the only person who can reach a local viewer is the person in front of it — but while ALBIS runs, every page the user visits can send requests to it that arrive carrying the user's own local access. Two of those got through. A page whose own domain resolves to `127.0.0.1` was same-origin with ALBIS as far as the browser was concerned, which made the API readable, `/api/browse` included. And `POST /api/upload` uses a form encoding that predates CORS and is sent with no preflight, so any page could write a file into the data directory and simply not read the reply. ALBIS now checks the `Host` header, and refuses state-changing requests — plus the two endpoints that open a native file dialog — when the browser reports them as coming from another site.

  Nothing changes for normal use. A local browser, a `0.0.0.0` bind, and clients that are not browsers are all unaffected; the last of those is deliberate, since the Remote Stream API exists to be called by detector-side scripts and a non-browser client can set any header it likes. **One case needs configuration:** a reverse proxy in front of a loopback bind forwards its own hostname, so add it to the new `server.allowed_hosts` (see *Reverse Proxies and Remote Access* in the Power User Guide). A refused request is logged and answers `403` naming the setting.

### Changed

- Settings covers the whole configuration again. **Allowed hosts**, **Response compression** and **Health check timeout** had no control, so the two settings a reverse-proxy deployment needs could only be reached by hand-editing `albis.config.json` — and getting `allowed_hosts` wrong there means every request is refused. Allowed hosts accepts a comma-separated list and applies as soon as it is saved.

- Settings is grouped into **Viewer**, **Connection**, **Data** and **Logging** tabs instead of four stacked sections. Twenty-three controls competed for attention on one scrolling page; six to eight are now on screen at a time and the dialog no longer scrolls. The tabs are the same control the viewer panel uses.

- Settings says which fields need a restart, and only once one has changed. Instead of a marker on ten of the twenty-four controls — enough repetition that it read as decoration — the footer names what you actually edited: *Restart to apply: Port, Response compression*. A value with a special meaning is explained under its field rather than inside its label, which had made twelve of sixteen labels wrap.

- **Save** now closes the settings dialog, and **Save & Close** is gone. Two buttons for the same intent made you choose between them for no benefit.

- The external-connections checkbox keeps its label when ticked. It used to be replaced by the warning text, which left the control no longer describing what it does — and no label at all for switching it back — while repeating the warning already shown beneath it.

- Stepping through frames is markedly faster on large detectors. Every frame recomputed whole-image statistics, and the median was found by copying every pixel into a fresh array and selecting through it twice — on a 4M detector that was 150 ms and 114 MB of garbage per frame, on a 16M detector 560 ms and 431 MB, and it ran on every arrow-key press, not just during playback. Detector data is integer, so the median now comes from a counting pass over reused buffers, exact as before; the two per-pixel objects the loop allocated are gone; and auto-contrast no longer makes a scan for a saturation value the caller already knows, nor takes a logarithm per pixel to find extremes it can get by transforming the two endpoints. Measured on frames with overflow pixels and the mask on, whole-image statistics drop from 150 ms to 47 ms on a 4M detector and 560 ms to 181 ms on a 16M one, with the total per-frame cost roughly halved. Every value is unchanged.

- Docker images no longer allow absolute paths. The desktop default stays `true` — on a workstation, whoever browses to an absolute path already owns the machine — but the image listens on `0.0.0.0` with no authentication, where that reasoning does not hold and anything reaching the port could otherwise read the whole container filesystem. The documented setup is unaffected, since it already mounts data at `/app/data`, which is `data.root`; several mounts side by side there work fine. Set `data.allow_abs_paths` back to `true` in a mounted config if you deliberately want paths outside it.

### Fixed

- A mask can no longer end up attached to the wrong file. Switching files while the pixel mask was still loading let the previous file's mask land on the new one, and because masks from the same detector have the same shape, nothing detected it: the frame rendered and its statistics were computed against a mask belonging to another dataset. It did not correct itself either — the mask was recorded under the new file's name, so it was treated as already loaded for the rest of the session.

- An HDF5 file whose data cannot be decompressed now says so instead of failing as a server error. Opening a file and reading from it are separate steps: detector data needs a compression filter that HDF5 loads only when the pixels are actually pulled in, so a missing filter surfaced past the check added for unreadable files.

- A config ALBIS cannot read no longer stops it from starting. Configuration is loaded before anything else, including logging, so a file it could not parse ended the process before there was any way to report it — from a double-clicked desktop build, nothing appeared to happen at all. Two ordinary situations reach this: a config truncated by a crash while settings were being saved, and one written by a newer ALBIS whose keys an older build rejects, which is what a downgrade looks like. ALBIS now starts on defaults instead, says so in the log, and leaves the file untouched in case it can be repaired by hand; saving settings replaces it.
- Saving settings can no longer leave a damaged config behind. The file was rewritten in place, so an interruption mid-write truncated it — the very thing that used to stop the next start. It is now written alongside and renamed into place, so the config on disk is always either the old contents or the new ones.

- Big-endian detector data no longer fails to load. Any frame whose bytes are stored most-significant-first — an HDF5 stack written that way, a raw frame pushed to the Remote Stream API with a `>u2`-style dtype, or a JUNGFRAUJOCH preview image, whose CBOR typed-array tags are big-endian by definition — returned a server error instead of an image. The byte-order swap they all pass through used a NumPy call that NumPy 2.0 removed, so it failed for exactly the data that needed swapping and for no other. The three copies of that swap are now one shared helper, which is what let two of them drift onto the removed call while the third was fixed.
- A file that cannot be decoded is now reported as such instead of as an ALBIS failure. Opening a truncated or corrupt HDF5, TIFF, CBF, or EDF — most often a file the filewriter has not finished writing — surfaced a bare "Internal Server Error". It now names the file and the reason, so it reads as something to retry rather than something broken.
- A truncated EDF is no longer displayed as if it were complete. fabio zero-fills the part of the frame it could not read and reports the shortfall only in a log line, so the missing region rendered as genuine zero counts, with nothing to distinguish it from real data.
- Live metadata from an external producer can no longer break the frame it describes. Text supplied by a remote stream or a JUNGFRAUJOCH series — a display name, a sample name, a timestamp — is sent back in response headers, and three kinds of value broke that response for as long as the frame stayed current: a character outside Latin-1 (a 500), a line break (the connection dropped mid-response), and a very long value (past what the client will read). Such text is now percent-encoded on the way out and decoded for display, so a non-ASCII sample name survives intact rather than being dropped or mangled.

## [0.10.9] - 2026-08-20

### Added

- Recently viewed frames are kept in memory, so stepping back to a frame — or replaying a stretch you have already watched — costs no transfer and renders immediately. The budget is memory rather than a frame count, since a frame ranges from about 4 MB on an EIGER 1M to about 18 MB on a 4M detector; tune it in **Settings -> Viewer** or with `ui.frame_cache_mb` (default 256 MB, `0` disables it). Frames are never cached while autoload is running or a watch is armed, because the file may still be growing under the filewriter.
- Data source: the SIMPLON address field remembers detector addresses that answered — a successful connection test or a started monitor — and offers them back as autocomplete. Only addresses that worked are stored, so a failed typo is never suggested.
- Data source: **Test** button for the JUNGFRAUJOCH preview endpoint, reporting whether the port accepts connections and naming the cause when it does not (unknown host, refused port, timeout). It is a reachability check: frames are confirmed once the preview starts.
- Backend `GET /api/jfjoch/probe`: TCP reachability check for a preview endpoint.
- `GET /api/health` now reports `compression_encodings`, the response encodings the running build can produce. zstd needs a native extension that a packaged build could fail to bundle, in which case remote sessions would quietly fall back to gzip; this makes that visible rather than silent.

### Fixed

- Settings: opening the settings dialog and saving no longer discards configuration that has no field in it. Every section was rebuilt from the dialog's controls alone, so hand-edited keys without a control — `server.compression` and `ui.frame_cache_mb` — were dropped on save and silently reset to their defaults.
- Playback no longer freezes at higher frame rates. When a frame took longer to load than the gap between ticks, each tick aborted the load the previous one had started, so nothing was ever displayed until playback was stopped — and the faster the selected rate, the more certain the stall. Playback now waits for the frame in flight instead of cancelling it, so the selected rate acts as a ceiling and a slow source simply plays more slowly. Changing speed mid-playback now takes effect without restarting it.
- Data source: the SIMPLON **API Version** field is no longer a guessing game. When the configured version is absent but a known one answers, the connection test adopts the working version, applies it to the field and says so.
- Data source: the JUNGFRAUJOCH preview endpoint accepts a bare `host:port` and fills in `tcp://`, repairing mistyped separators. A missing port is now rejected up front with wording that says what to enter, instead of failing later inside ZeroMQ. Path transports such as `ipc:///tmp/x` are left untouched.

### Changed

- Remote sessions transfer far less data. Frames travel as raw pixel bytes, so a single EIGER 1M frame is 4.4 MB on the wire and a 4M frame is around 18 MB — the reason the UI felt sluggish when the browser was not on the same machine as the server. Responses to remote clients are now compressed, using zstd where the browser supports it and gzip otherwise: measured on real EIGER data, a frame drops to 1.9 MB and the frontend's cold load drops from 1134 KB to 323 KB. Nothing changes for local use — a browser on the same machine is never compressed, since the transfer was already instant, and a browser that does not support zstd is never sent it.
- Reloading the UI over a remote link no longer refetches the whole frontend. Modules, styles and locales were previously marked `no-store` and re-downloaded in full on every single load; they are now revalidated instead, so an unchanged file comes back empty. Entry documents are still never stored, so an upgraded backend is never paired with a stale UI.
- New `server.compression` setting: `"auto"` (default, compress for everyone except a local browser), `"on"` (always — use this behind a reverse proxy, where every request otherwise looks local), or `"off"`.
- New dependency: `zstandard` (BSD-3-Clause). It is optional at runtime — without it ALBIS serves gzip instead of failing.
- Dependencies: `starlette` is now pinned explicitly (`1.6.0`). FastAPI only requires `starlette>=0.46.0` with no upper bound, so the version actually installed could drift between machines and CI — and the response-compression code builds on Starlette's compression responder, whose interface changed in 1.6.
- Development: ESLint reports zero warnings after removing two dead symbols, so a new warning is visible immediately.

## [0.10.8] - 2026-08-05

### Added

- Data source: a **Test** button beside the SIMPLON monitor address (Enter in the field works too) reports whether the detector answers. On success it names the detector and serial number, so the address can be confirmed to point at the intended instrument; on failure it names the cause — unknown host, refused port, wrong API version, or timeout.
- Backend `GET /api/simplon/probe`: connection test for a SIMPLON address. A detector that does not answer is a successful diagnosis (`200` with `status: "error"` and a classified `code`), not a transport error; only an unusable address returns `400`.

### Fixed

- Data source: the SIMPLON monitor address now accepts what operators actually type. A bare hostname or IP gains `http://`, a mistyped scheme separator is repaired, and a URL pasted from the SIMPLON docs has its `/monitor/api/<version>` path stripped. The field shows the canonical form it will use, so typing `192.168.1.10` becomes `http://192.168.1.10`. An explicit port is always kept; omitting one means the detector default of 80. Previously a bare IP was rejected outright and the placeholder suggested port 5000, which SIMPLON does not use.
- Data source: a failing SIMPLON poll now says why. The status line reads, for example, `SIMPLON: Connection refused on port 5000 — SIMPLON normally listens on port 80.` instead of `SIMPLON: error`, and the same wording is used by the connection test. Addresses persisted by earlier versions are normalized when settings load, so a saved bare host starts working instead of failing forever.

### Changed

- API: `502` responses from `/api/simplon/monitor`, `/api/simplon/mask` and `/api/simplon/mode` now carry a classified object detail (`{"detail": {"summary", "code", ...}}`) rather than a plain string, so clients can localize the reason. The `url` query parameter accepts a bare hostname. See `docs/API_CONTRACTS.md`.
- Dependencies: fastapi 0.141.1, uvicorn 0.52.1, cbor2 6.1.4, jsdom 30.0.1.
- Development tooling moved to Node 24 (Node 20 reached end of life in April 2026, and jsdom 30 requires Node 22.22 or newer). The supported floor is Node `>=22.22.2`; `npm ci` now fails immediately on an older Node instead of installing an incomplete tree. This affects contributors only — no packaged artifact contains Node.

## [0.10.7] - 2026-08-03

### Fixed

- Linux AppImage: build against the oldest supported glibc (2.35 / Ubuntu 22.04) so it no longer fails on startup with `version 'GLIBC_2.38' not found`. Also stop stripping/UPX-ing the numpy-vendored OpenBLAS library on Linux, which corrupted its segment alignment and broke `import numpy` ("ELF load command address/offset not page-aligned") on glibc 2.35.
- Viewer: hovering a pannable (zoomed) image now shows the normal arrow cursor instead of an open-hand "grab" cursor; the closed-hand still indicates an active drag.
- Launcher: `logging.level` is now honored by uvicorn's own loggers, so setting `CRITICAL` silences the access log (`INFO: ... "GET /..." 200 OK`) instead of printing it regardless.

### Added

- CI: a glibc-floor guard (`scripts/check_glibc_floor.sh`) that fails the Linux build if the packaged bundle requires a glibc newer than 2.35, preventing this class of regression.

## [0.10.6] - 2026-07-03

### Added

- File browser: click a Details-view column header (**Name / Type / Modified / Size**) to sort, click again to flip direction, with a caret indicating order. Adds Size and reverse-Type sorting; the header and the Sort dropdown stay in sync.
- File browser: the Path field is now editable — type or paste a directory and press Enter to jump there (a file path drops you in its folder). A non-existent path shows a clear "Path not found" instead of silently redirecting to Root.
- File browser: press Enter in the search box to jump to the first match (and open it if it is the only match). Folder/file counts now appear in the section headers (e.g. `Image Files (241)`), and a failed load offers a one-click **Retry**.
- Backend `/api/browse`: new `requestedPathMissing` flag plus `type_desc` / `size_asc` / `size_desc` sort modes. New i18n strings localized across all 13 locales.

### Changed

- File browser: larger, responsive window that scales to the screen; the file list expands to the bottom instead of a small fixed height.
- File browser: Details view and **First image only** are now the defaults.
- Selection now survives a re-sort — reloading the same directory restores the highlighted file by path, so the Select button no longer blinks off when reordering.
- Series and sort choices are no longer reset when passing through a folder that cannot use them; the control disables and the choice returns on the next compatible folder.
- Folder-select mode is a true folder picker: the file list and file-only filters step aside so a file can't be picked when a folder is required. The browser focuses the first entry on open for immediate keyboard navigation.

### Notes

- The file-browser polish release: bigger window, click-to-sort columns, editable path, better defaults, and smarter search.
- This release is called: "Browse Like You Mean It."

## [0.10.5] - 2026-07-01

### Added

- File browser: remembers the last directory across sessions and reopens there instead of returning to root; a vanished path safely falls back to root.
- File browser: the **First image only** filter now collapses a DECTRIS HDF5 series (`PREFIX_master.h5` + `PREFIX_data_000001.h5…`) down to the single master file, with a badge counting the hidden data files. Standalone and summed `.h5` files are left in place.

### Changed

- When ALBIS asks you to choose a *folder*, the dialog now shows folders only: the file list, format filter, and series filter step aside, the folders pane goes full-width, and the current folder is pre-selected so **Select** is armed on open. File-open mode is unchanged.
- HDF5 series detection uses a strict `_master.h5` / `_data_NNNNNN.h5` matcher, so ordinary numbered `.h5` files are never grouped by accident; backend and frontend aggregate identically.
- Housekeeping: bumped `hdf5plugin`, several Python deps, and ESLint via Dependabot.

### Notes

- The file browser gains a memory, series-to-master collapsing, and a real folder picker.
- This release is called: "Now Where Was I?"

## [0.10.4] - 2026-07-01

### Added

- Peak Finder: every peak now shows its local signal-to-noise (SNR) in the table — the score the finder already ranked by, previously discarded.
- Peak Finder: with detector geometry set, the list gains a **d (Å)** column showing each reflection's resolution shell; it updates instantly when distance or beam center changes and hides itself when there is no geometry.

### Changed

- Peak Finder: peaks now land on their intensity-weighted centre of mass (sub-pixel centroids) instead of the brightest pixel.
- Peak Finder: local-maxima detection went from 4- to 8-connected, so spots behind a diagonal neighbour are no longer missed and flat plateaus are not double-counted.

### Fixed

- Peak Finder: a footprint check now rejects lone hot pixels and cosmic-ray spikes (zingers) before they reach the list. Still a single linear full-resolution pass — no downsampling, same speed.

### Notes

- The Peak Finder release: cleaner spots, sub-pixel centroids, and SNR + resolution right in the table.
- This release is called: "Spot On."

## [0.10.3] - 2026-06-30

### Added

- Added support for non-square ("strixel") detectors such as DECTRIS POLLUX PANORAMA HR. The per-axis pixel sizes from the master HDF5 (`x_pixel_size` / `y_pixel_size`) are now kept distinct instead of averaged into one scalar, and the image is displayed with the correct physical aspect ratio. The data matrix stays pixelwise — only the display mapping is stretched (`pixelAspect = y_pixel_size / x_pixel_size`).
- Added physical ROI readouts alongside the pixel values: ROI size in mm and area in mm², computed from the per-axis pixel sizes.

### Changed

- Threaded the per-axis display aspect through every screen↔image transform — cursor probe, box/line/circle ROIs, peak markers, resolution rings, the overview thumbnail, cross-window sync, and visible-region export — so all overlays stay aligned on anisotropic detectors. Square detectors are unaffected (aspect = 1, every path identical to before).
- Circular/annulus ROIs are now physical resolution shells: a true circle on screen that coincides with the resolution rings (an ellipse in pixel space). Their radial profile is binned by physical radius and labels the x-axis in millimetres, so the azimuthal integration is correct for elongated pixels.
- Resolution rings and the per-pixel resolution readout now use the true per-axis physical distance.
- Added DECTRIS JUNGFRAU and POLLUX (including rectangular "strixel" pixels) to the README compatibility list.

### Notes

- ALBIS `0.10.3` teaches the viewer that pixels aren't always square — DECTRIS POLLUX's elongated "strixels" now display, measure, and integrate with correct physics.
- This release is called: "Strixel Things."

## [0.10.2] - 2026-06-28

### Added

- Added a secondary resolution axis to the ROI line and radial (circle/annulus) profiles: a top axis that maps the pixel/radius x-axis to d-spacing, styled to match the primary axes (same color, centered title). It appears only when the geometry is calibrated (distance, pixel size, energy) and, for radial profiles, only when the ROI is centered on the beam.
- Added automatic feature detection on those profiles: a lightweight 1D peak finder marks prominent peaks and labels each with its resolution, so you can read d-spacing straight off the diffraction features.
- Added a d (Å) ↔ Q (1/nm) unit toggle in the profile plot's settings menu (default d, with Q = 2π/d). The choice drives both the axis ticks and the peak labels; Q yields evenly spaced ticks since it is linear in reciprocal space.
- Added a "Center on beam" button for circle/annulus ROIs that snaps the ROI center onto the beam center, making the radial profile's resolution axis exact.

### Notes

- ALBIS `0.10.2` teaches the ROI profiles to speak resolution: a d-spacing/Q axis, auto-labeled peaks, and one-click beam centering.
- This release is called: "Mind The Gap (Spacing)."

## [0.10.1] - 2026-06-28

### Added

- Added full keyboard navigation to the menu bar and dropdowns: Up/Down move between items, Home/End jump to the ends, Left/Right switch top-level menus, and ArrowDown opens a focused menu. Escape closes the menu and returns focus to its button; the "Save As…" submenu opens via Right/Enter/Space. Arrow keys are only claimed while a menu is open, so frame navigation is unaffected otherwise.
- Added ARIA semantics across the remaining interactive surfaces: the menu bar is a role=menubar/menu/menuitem tree, the command palette is a role=combobox + role=listbox with aria-activedescendant tracking, peak-finder rows carry descriptive labels and aria-pressed, and inspector group toggles report aria-expanded.
- Added `:focus-visible` focus rings to 15 interactive controls (menu/dropdown items, breadcrumbs, command palette, peak rows, inspector toggles, ROI/toolbar/rings/series buttons) so keyboard focus is visible everywhere.
- Added opt-in per-call fetch timeouts: image headers (30s), series and dataset scans (60s), and binary image/frame loads (120s).
- Added `THIRD_PARTY_LICENSES.md` plus verbatim Apache-2.0 and MPL-2.0 license texts, bundled into the packaged app via the PyInstaller spec, attributing every shipped dependency ahead of the MIT release.
- Added test suites for menu keyboard navigation, menu ARIA, command-palette ARIA, and the HTTP error/timeout layer.

### Changed

- HTTP requests now throw localized, structured errors (carrying status and detail) instead of raw "Request failed: 500" strings, including a friendly network-failure message. Added 7 `http.error.*` keys and 2 new ARIA i18n keys across all 13 locales.
- Decluttered the ROI panel: hid the duplicated status line when a region is active (the badge and stats grid already convey it), and gave the stats grid muted labels with a column divider for clearer hierarchy.

### Fixed

- Resolution-ring label collision avoidance now nudges crowded labels along the ring tangent (biased upward, away from the beamstop) instead of outward along the radius, so a label no longer flies past adjacent rings and stays attached to its own ring.
- Fixed a latent bug where a timed-out binary image/frame load was silently swallowed by the AbortError check, leaving the loading spinner up indefinitely.

### Notes

- ALBIS `0.10.1` is the accessibility and polish release: drive the menus from the keyboard, full screen-reader semantics, localized error messages, request timeouts, and third-party license attribution for the MIT release.
- This release is called: "Mind Your Manners (And Your Keyboard)."

## [0.10.0] - 2026-06-27

### Added

- Added animated GIF export for image series and multi-frame datasets (File → Export Animation…, the Playback popover's Export GIF…, or ⌘G). The dialog offers a frame range with a "use every Nth frame" step, full-image or visible-area region, output scale, playback speed, and a loop-forever toggle, with a live frame-count/dimension/size estimate. Frames are rendered client-side so the GIF matches the on-screen colormap, contrast, invert, mask, and saturation highlighting, using a dependency-free encoder that streams frames to keep memory bounded.
- Added direct manipulation of the resolution-ring overlay: drag the beam-center marker to reposition it (planar and geometry mode) and drag a ring in/out to change its d-spacing (planar mode), with a grab cursor and handle highlight on hover. Drags write back into the geometry input fields, keeping the inputs the single source of truth for validation and redraw.
- Added a geometry lock so manually corrected geometry persists while a live source (SIMPLON/remote/JFJ) is streaming, instead of being overwritten by every incoming frame. Includes a Live/Locked pill under the geometry inputs and a "Reset to live" control, and surfaces the geometry inputs at the top level instead of inside the collapsible submenu.
- Added a "Min. SNR" control to the Peak Finder (default 5; 0 restores intensity ranking).

### Changed

- Peak Finder now ranks spots by local signal-to-noise instead of brightest local maximum, estimating background from an annulus around each candidate via summed-area tables. It rejects noise sitting on a high background (beam stop, hot modules) and surfaces faint genuine reflections.
- Reviewed and corrected all 12 locale translations at native level: filled English gaps, fixed mistranslations (e.g. network "Port" rendered as harbor), and standardized Swiss German dialect.
- Restructured documentation ahead of 1.0.0: added a beginner Getting Started section to the README and split the Developer Guide into four focused, cross-linked docs (DEVELOPER_GUIDE, ARCHITECTURE, CODE_MAP, API_CONTRACTS).

### Fixed

- Auto contrast now rejects detached extreme-pixel clusters (e.g. summed gap/dead-pixel sentinels) that previously dragged the 99.9th percentile into orbit and blew the whole image out to white, while preserving isolated bright Bragg peaks.
- Fixed the update check in packaged builds by verifying TLS against the bundled certifi CA bundle.
- Capped the default ROI Line/Annulus/Circle projection plot height (120px) so it matches the box and histogram plots instead of stretching to fill the side panel.

### Notes

- ALBIS `0.10.0` is the direct-manipulation release: grab the rings, drag the beam center, lock your geometry against live frames, and export the whole series as a GIF.
- This release is called: "Grab It And Drag It."

## [0.9.15] - 2026-06-24

### Added

- Added a toast notification system so failures, warnings, and completion confirmations are surfaced to the user instead of only updating the footer status pill.
- Added a native Save As dialog using the File System Access API, with real folder selection and overwrite confirmation (falls back to a filename-only download on browsers without the API).
- Added promise-based in-app modal prompt/confirm dialogs that replace the native browser `prompt()`/`confirm()` boxes.
- Added a frontend architecture section to the Developer Guide (layered mental model, wiring patterns, and a worked example) for new contributors.

### Changed

- Consolidated the File menu into a single Save As submenu (Full Image / Visible Area / Viewer Window) plus a top-level Convert Dataset action, removing the redundant Export submenu.
- Localized the About dialog and remaining input placeholders, and simplified the page and About titles.
- Renamed the ALBIS backronym to drop "AI-engineered".

### Fixed

- Fixed the misleading Save As path field: browser downloads always discarded the typed directory, so Save As now opens a real native save dialog where the chosen folder is honored.
- Fixed a backend type annotation (`logger: any` -> `Any`) and added tooltips explaining when the mask toggles are disabled.

### Notes

- ALBIS `0.9.15` is the polish-and-feedback release: the app now talks back, Save As can actually pick a folder, and the menus stopped doing the same job twice.
- This release is called: "The App Finally Talks Back."

## [0.9.14] - 2026-06-04

### Added

- Added linked viewer windows so multiple ALBIS browser windows can synchronize the same image-space view.
- Added live position synchronization while panning, zooming, using the overview, or changing the viewport.
- Added selectable sync options for Position, Contrast, and ROI, with all three enabled by default.
- Added contrast synchronization for levels, auto-scale state, colormap, and invert mode.
- Added ROI selection synchronization for line, box, circle, and annulus geometry.

### Fixed

- Fixed zoomed-out linked views so synchronized image centers preserve the same image-space location even when the rendered image is smaller than the viewport.
- Fixed Windows release signing so unsigned Windows builds correctly skip the signing step when no Windows signing secrets are configured.

### Notes

- ALBIS `0.9.14` is the long-requested window synchronization release: open two viewers, link them, and stop manually chasing the same detector pixel twice.
- This release is called: "Happy Birthday, Tilman."
- This release is also called: "The Windows Finally Talk To Each Other."

## [0.9.13] - 2026-06-04

### Added

- Added fixed-bin controls for ROI histograms, with Auto bins plus selectable manual bin counts.
- Added per-plot settings menus for ROI X/Y profiles and histograms, including manual X/Y axis minimum/maximum controls and per-plot log scale toggles.
- Added Azure Artifact Signing support for Windows CI signing, including GitHub OIDC login, SignTool/dlib setup, and Inno Setup uninstaller signing.

### Changed

- ROI plot controls now live in each plot's cog menu; histogram bins and scale settings share one menu, and the old global Log plot/Autoscale checkboxes were removed.
- Axis spinner changes now redraw plots continuously while values are adjusted.
- Refreshed GitHub Actions, Python, and npm dependency pins for release/tooling maintenance.

### Fixed

- ROI histogram y-axis minimum is clamped at zero so count plots no longer imply negative counts.
- Fixed per-plot log toggles so log mode applies to the selected ROI plot.
- Fixed ROI plot settings menus so they remain open when clicked and can extend beyond the plot area without clipping.
- Fixed release input verification so optional Windows/macOS signing configurations only fail when partially configured.
- Fixed dev npm audit findings.

### Notes

- ALBIS `0.9.13` is the ROI plot tune-up: histograms get sane floors, plots get their own settings, and axis limits finally sit where the user can reach them.
- This release is called: "Cogs, Logs, and Zero Floors."
- This release is called: "Histogram, but Make It Behave."

## [0.9.12] - 2026-06-02

### Added

- Added **File -> Convert Dataset...** for batch conversion of HDF5 datasets and image series to TIFF or CBF.
- Added Dectris-style TIFF header metadata in private tag `0xC7F8` for exported detector metadata.
- Added miniCBF-style header contents for CBF exports, including detector, pixel size, exposure, wavelength/energy, distance, beam center, and angle metadata when available.
- Added `Cmd+Shift+X` / `Ctrl+Shift+X` as a keyboard shortcut for dataset conversion.

### Changed

- TIFF and CBF exports now write signed integer images and use `-1` for module gaps and `-2` for bad or saturated pixels.
- Converted export outputs can be opened directly from the completed export action.
- Localized the new data-export UI strings across shipped locales.

### Fixed

- Preserved Dectris/Jungfrau saturated sentinel pixels as `-2` instead of expanding exported images to 64-bit integer data.
- Handled Dectris/Jungfrau sensor-thickness metadata that reports micrometer values with a meter unit.
- Fixed the data-export dialog close behavior after opening the first converted output image.

### Notes

- ALBIS `0.9.12` is the data-export release: datasets go out as TIFF or CBF, detector headers come along for the ride, and gap/bad pixels keep their detector conventions.
- This release is called: "Mind the Gaps, Export the Frames."
- This release is called: "Header, I Barely Know Her."

## [0.9.11] - 2026-05-21

### Fixed

- Tightened macOS distribution signing so public releases require Developer ID notarization secrets, CI verifies notarized artifacts, and zipped apps are rebuilt after stapling.

## [0.9.10] - 2026-05-19

### Added

- Added clearer drag-and-drop overlays so remote sessions explicitly say dropped files will be uploaded.

### Changed

- Local browser sessions now disable drag-and-drop uploads and point users to **File -> Open...** so detector data opens from its existing path instead of being copied.
- Refined pointer-anchored zoom behavior so zooming feels steadier around the pixel you are inspecting.
- Saturated pixels now use a cyan overlay for better visual separation from other masks and highlights.
- Refreshed backend and frontend dependency pins, including `cbor2`, Python package updates, and `jsdom`.

### Fixed

- Fixed macOS picker support for `.cbf.gz` files.

### Notes

- ALBIS `0.9.10` is the "no accidental data cloning" release: drag-and-drop now says what it means, local data stays where it lives, and remote uploads get a proper signpost.
- This release is called: "Drop Responsibly."
- This release is called: "Look, Don't Duplicate."

## [0.9.9] - 2026-04-27

### Fixed

- Fixed viewport pan bounds so zoomed detector images no longer snap back toward the edge when you try to center the top or side rows.
- Fixed the high-zoom visibility guard so it keeps only a small sliver of the image reachable instead of forcing a large fraction of the frame to remain on screen.

### Notes

- ALBIS `0.9.9` is a small but satisfying release: less fighting at the detector edge, less surprise snapping, and more of the image exactly where you want it.
- This release is called: "No More Top-Edge Tantrums."
- This release is called: "Zoom In, Stay There."

## [0.9.8] - 2026-04-27

### Added

- Added an in-app backend log viewer so packaged ALBIS builds can inspect backend logs without leaving the app.

### Changed

- Reworked the web file picker with better navigation, richer file metadata, series-aware browsing, geometry-file filters, and translated modal copy across shipped locales.
- Refreshed backend and frontend dependency pins, including `fastapi`, `uvicorn`, `python-multipart`, `eslint`, and `vitest`.

### Fixed

- Fixed circular ROI placement so centers can sit just outside the image while mask, interaction, and viewport behavior stay aligned.
- Fixed playback-era UI regressions affecting the peak finder table, watch-folder toggling after returning to file mode, and ROI hover tooltips.
- Fixed Firefox rendering failures on integer-texture paths by avoiding a WebGL upload mode it rejects.

### Notes

- This is a test release packed with small quality-of-life improvements and bug fixes.
- This release is called: "Tiny Tweaks, Better Beamtime."
- This release is called: "Logs in-app, ROIs off-road, Firefox less dramatic."

## [0.9.7] - 2026-04-09

### Changed

- Windows interactive installs now always show the destination page while keeping the default per-user install path.
- Windows installer metadata now uses a stable `AppId=ALBIS`, adds publisher/support/update links, and shows the ALBIS icon in Add/Remove Programs.
- Windows packaging now signs the generated Inno uninstaller when signing secrets are configured, and installer smoke coverage verifies that signature before uninstall.
- Windows installer and uninstaller now detect running `ALBIS.exe` instances, request a graceful shutdown first, and fall back to `taskkill` so upgrades and uninstall are less likely to fail on the background process.

## [0.9.6] - 2026-04-08

### Fixed

- Fixed Windows native file picking in packaged builds by switching the local picker path from Tk to a PowerShell/WinForms dialog that does not fail in FastAPI worker threads.
- Fixed repeated Open File clicks from launching overlapping picker requests while the first picker is still active.

### Notes

- This release is called: "Pick Me, Maybe."
- This release is called: "Less fallback browser, more actual file picker."

## [0.9.5] - 2026-04-08

### Added

- Added pauseable live history for live sources so recent frames can be inspected without immediately snapping back to the newest image.
- Added a confirmation step before enabling external access from the settings dialog.
- Added packaged-binary smoke coverage that verifies the frontend module entrypoint is served as JavaScript.

### Changed

- Hardened backend shared-state services and root-scan cache startup behavior for more predictable runtime state.
- Tightened live-frame loading, playback, and autoload orchestration for live sources.
- Updated Vitest to `4.1.2` and refreshed desktop platform support documentation.

### Fixed

- Fixed Windows packaged builds that could open the browser window but leave the UI inert because `app.js` was served with the wrong MIME type.
- Fixed summed-output frame status handling and root scan cache warm-start behavior.
- Fixed the release checklist coverage command so the documented pytest invocation matches the enforced CI gate.

### Notes

- This release is called: "Pause, Breathe, Keep Scrolling."
- This release is called: "Same live stream, fewer mysteries, zero dead buttons."

## [0.9.4] - 2026-03-26

### Changed

- Startup now keeps its bound socket alive before handing control to Uvicorn, closing the race that could make launches fail on a just-claimed port.
- Startup timing now uses separate socket-ready and health-check budgets, so a slow import chain is less likely to consume the entire boot window.
- Series summing now estimates required disk space before writing HDF5 or TIFF outputs and aborts early when free space is too tight.
- Remote metadata parsing now rejects payloads larger than `1 MB` before JSON decode.
- Frontend loading and autoload polling now guard against stale-request and overlapping-tick races.

### Fixed

- Uploads to a read-only destination now fail explicitly with `HTTP 503` instead of silently redirecting files into temporary storage.
- HDF5 linked-stack traversal now logs skipped nodes and broken links instead of swallowing those failures invisibly.
- Logging fallback no longer crashes startup if its temporary-directory fallback cannot be created.

### Notes

- This release is called: "No More Ghost Writes."
- This release is called: "Start Fast, Fail Loud, Keep Your Data."

## [0.9.3] - 2026-03-25

### Added

- Added a manual in-app release check that queries GitHub for the latest ALBIS release and links directly to the release page.
- Added `scripts/bootstrap_macos_signing_ci.sh` plus developer-guide coverage for one-step macOS signing and optional notarization secret bootstrap via `gh`.

### Changed

- Pixel-value overlays now render labels all-or-none when cells are too cramped, avoiding half-visible numbers across detector tiles.
- Refreshed backend and frontend dependency pins, including `fastapi`, `cbor2`, `pyzmq`, `eslint`, `jsdom`, and `vitest`.

### Fixed

- Manual update checks now fall back cleanly when GitHub release metadata is missing, invalid, or times out.

### Notes

- This release is called: "Check Yourself Before You Tag Yourself."
- This release is called: "Fresh deps, tidy labels, and a slightly smug updater."

## [0.9.2] - 2026-03-25

### Added

- Added manual Pilatus 12M geometry overrides for detector-center and distance workflows.
- Summed HDF5 outputs now embed the effective geometry used for downstream ring analysis.

### Changed

- Pixel-value overlays now format float datasets more sensibly and reduce label density when text would spill across cell boundaries.
- Refined non-English UI copy further, including broader translation cleanups and Swiss German polish.

### Fixed

- Refreshed the pinned Docker Python base image and runtime package/toolchain upgrades so the GHCR publish scan no longer fails on stale Debian, `setuptools`, and `wheel` CVEs.
- Fixed HDF5 manual geometry ring loading for ring overlays and metadata-driven geometry workflows.
- Fixed ROI plot redraw behavior after panel resizes.
- Fixed export overlay and autoload regression handling.

### Notes

- This release is called: "Sharper labels, steadier rings."
- This release is called: "Now with fewer overlapping numbers and fewer reasons to squint."

## [0.9.1] - 2026-03-19

### Added

- Added geometry-aware resolution rings for Dectris Pilatus 12M CBF images, using DIALS `imported.expt` detector geometry for the C-shaped vacuum detector layout.
- Added installer-path smoke coverage for Windows installers and mounted macOS DMGs in artifact/release workflows.
- Added pinned AppImage installation tooling with checksum verification for Linux packaging workflows.

### Changed

- Pilatus 12M geometry mode now supports manual beam-center and detector-distance adjustments, and default rings now use `1.0`, `3.67`, and `11.01 Å`.
- Raised enforced backend coverage gates from `20%` to `50%` across CI, artifact validation, release verification, and contributor docs.
- Pinned backend runtime dependencies and PyInstaller build inputs for more reproducible source, packaging, and Docker builds.
- Refactored the Docker image to a pinned multi-stage build that runs as a dedicated non-root user.
- Updated README, release docs, and the power-user guide to position ALBIS `1.0` as a local-first desktop app for localhost and trusted lab/LAN use.
- Tag releases now require Linux signing credentials, while Windows code signing and macOS signing/notarization remain optional unless configured.

### Fixed

- Fixed FileWriter2 pixel mask loading.
- Fixed Docker runtime file access when mounted data is read-only.

### Security

- Security reporting now points to GitHub private vulnerability reporting instead of public issues, with supported-version and response-target guidance.

### Notes

- This release is called: "Bent detector, straight rings."
- This release is called: "Curved hardware, less curved release math."

## [0.9.0] - 2026-03-13

### Added

- Added a translation review workflow with glossary, allowlist, technical-domain reporting, and CI visibility for locale QA.
- Added committed desktop icon assets for Linux, Windows, and macOS packaging, including prebuilt `.ico` and `.icns` bundles.

### Changed

- Refined technical terminology and UI wording across German, Swiss German, Swedish, Danish, French, Spanish, Italian, Portuguese, Japanese, and Simplified Chinese locales.
- Updated desktop packaging and runtime asset wiring to use the new icon set consistently across app builds and frontend-served assets.

### Fixed

- macOS x64 release packaging now retries DMG creation when `hdiutil` hits a transient `Resource busy` failure on GitHub runners.

### Notes

- This release is called: "Icons aligned, translations sharpened."
- This release is called: "From workflow dry-run to release-ready."

## [0.8.17] - 2026-03-13

### Added

- Frontend locale coverage now includes `en`, `zh-CN`, `ja`, `fr`, `es`, `it`, `pt`, `rm`, `de`, `sv`, `da`, `mi`, and `gsw`.
- Added first-pass UI translations for French, Spanish, Italian, Portuguese, Romansh, German, Swedish, Danish, Māori, and Swiss German.

### Changed

- Swiss German (`gsw`) copy now reads more consistently in Basel-style wording across the splash screen, menus, and series workflow.
- German locale wording now uses informal phrasing throughout the UI.

### Fixed

- Startup language selection now respects persisted backend preferences instead of sticking to an inferred browser fallback.
- Language switching now refreshes playback controls, splash-screen status text, and dynamic file/dataset placeholders immediately.
- Added locale integrity coverage for key parity, placeholder-token parity, and expanded alias normalization across all shipped locales.

### Notes

- This release is called: "One beamline, thirteen tongues."
- This release is called: "Kia ora, Grüezi, bonjour, and please mind the diffraction peaks."

## [0.8.16] - 2026-03-13

### Added

- Multilingual UI support across key viewer surfaces with `en`, `zh-CN`, and `ja` locales.
- Expanded localization for menus, tooltips, inspector/data panels, playback dropdowns, ROI overlays, and cursor readouts.

### Fixed

- Language switching now refreshes dynamic ROI labels immediately (no follow-up click required).
- Locale test fixture path resolution is now CI-safe across environments.
- Locale-specific CJK font fallback order now prefers Japanese glyph forms under `ja`.

### Notes

- This release is called: "Konnichiwa, Ni Hao, and welcome to ALBIS."
- This release is called: "One viewer, three languages, zero passport control."

## [0.8.15] - 2026-03-12

### Fixed

- Docker GHCR publish no longer fails on Trivy setup/runtime drift.
- Trivy release gate now ignores unfixed vulnerabilities while still failing on fixable `HIGH/CRITICAL` findings.
- Docker image build now upgrades `setuptools`, `wheel`, and `jaraco.context` to patched versions required by the Trivy gate.

### Notes

- This release is called: "Now scanning, still shipping."
- This release is called: "Red gate, green build."

## [0.8.14] - 2026-03-12

### Fixed

- Docker release publishing no longer fails in the Trivy setup phase due to stale Trivy binary resolution.
- Updated Docker publish scanning to use current `aquasecurity/trivy-action` with `version: latest`.

### Notes

- This release is called: "Scan me maybe."
- This release is called: "No more tripping before the Trivy starts."

## [0.8.13] - 2026-03-12

### Fixed

- Docker drag-and-drop now works even when data is mounted read-only (`:ro`).
- Upload handling now falls back to a writable container temp path (`/tmp/albis-uploads`) when `/app/data` cannot be written.
- Added regression coverage for read-only upload targets to prevent future Docker regressions.

### Notes

- This release is called: "Drop it like it's docked."
- This release is called: "Read-only mount, write-capable mood."

## [0.8.12] - 2026-03-12

### Added

- Dedicated Docker CI/release workflow (`.github/workflows/docker.yml`) with:
  - PR/branch validation build for `linux/amd64`.
  - Container smoke checks for `/api/health` and `/api/files` against mounted test data.
  - Tag-triggered GHCR publish pipeline for multi-arch images (`linux/amd64`, `linux/arm64`).
  - Release-blocking Trivy vulnerability gate for `HIGH,CRITICAL` findings.
- Docker documentation updates covering GHCR pull/run usage and published image tags.

### Fixed

- Docker backend-log UX now works in browser:
  - Help -> backend log keeps desktop open behavior when available.
  - Container/headless environments now fall back to opening `/api/log-file` in a new tab.

### Changed

- Docker image startup now runs via `uvicorn backend.app:app`, fixing package import issues in containers.
- Docker build dependencies now include native compiler toolchain support required for `dectris-compression` builds on ARM.

### Notes

- This release is called: "Container? Consider it contained."
- This release is called: "From 'it builds on my machine' to 'it builds in every machine-shaped box.'"

## [0.8.10] - 2026-03-10

### Added

- `VERSION` as repository version source of truth.
- `albis.config.schema.json` for configuration schema validation in tooling.
- `albis.config.example.jsonc` as commented configuration template.
- `docs/configuration.md` with config lookup/normalization behavior and field reference.
- Release and security automation via GitHub workflows.
- Dependabot configuration for Python, npm, and GitHub Actions.
- `docs/RELEASE_CHECKLIST.md` with release dry-run and tagging steps.
- `docs/API_CONTRACTS.md` with binary-header semantics, status-code behavior, and integration guidance.
- API contract tests for strict request-body validation.
- Extended OpenAPI contract tests for binary payload/header endpoints and remote meta conflict response.

### Changed

- Backend runtime version now loads from `VERSION` via `backend/version.py`.
- Build metadata helper (`scripts/version_info.py`) now reads from `VERSION`.
- Frontend UI version display no longer hardcodes a static release value.
- Config normalization now rejects unknown sections/keys and invalid section value types.
- `system`, `analysis`, `stream`, `files`, `hdf5`, and `frames` routes now use explicit Pydantic request/response models.
- Binary download endpoints now publish explicit OpenAPI payload/header contracts (`octet-stream`, CSV, and metadata headers).
- Backend path-resolution and route workflows now include focused docstrings for maintainability and contributor onboarding.
- CI and release verification now enforce backend coverage with `pytest-cov` and `--cov-fail-under=20`.
- `docs/CODE_MAP.md` now reflects modular route/service architecture and contribution touchpoints.

## [0.8.9] - 2026-03-09

### Added

- Linux release assets now include `install_linux_appimage.sh` and `uninstall_linux.sh` so AppImage distribution is self-contained.

### Changed

- Windows installer now defaults to per-user install (`%LOCALAPPDATA%\Programs\ALBIS`) and runs without admin elevation.
- Linux AppImage packaging now includes the full PyInstaller runtime payload (including `_internal`), fixing missing `libpython3.10.so.1.0` at runtime.
- Artifact/release workflows now build and validate both Linux `.tar.gz` + `.AppImage` and Windows `.zip` + installer `.exe`.
- Release publish workflow now performs stronger asset checks and multi-step retry backoff for transient GitHub rate limits.
- Distribution variants now ship as architecture-explicit assets (`linux-x64`, `windows-x64`, `macos-arm64`, `macos-x64`) with stable target naming.
- Release and artifacts workflows now publish both installer and portable variants per supported OS.
- Packaging pipeline now includes per-artifact integrity checks, cross-platform packaged-binary smoke tests, and optional signing/notarization hooks.

### Notes

- This release is called: "No more scavenger hunts for missing runtime files."
- This release is called: "Now with fewer mysteries and more binaries."

## [0.8.2] - 2026-02-24

### Added

- UI facelift baseline for the `0.8` milestone.

### Changed

- Backend/frontend architecture and tests expanded as part of the `0.7` to `0.8` refactoring track.

[Unreleased]: https://github.com/SaschaAndresGrimm/ALBIS/compare/v0.12.0...HEAD
[0.12.0]: https://github.com/SaschaAndresGrimm/ALBIS/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/SaschaAndresGrimm/ALBIS/compare/v0.10.9...v0.11.0
[0.10.9]: https://github.com/SaschaAndresGrimm/ALBIS/compare/v0.10.8...v0.10.9
[0.10.8]: https://github.com/SaschaAndresGrimm/ALBIS/compare/v0.10.7...v0.10.8
[0.10.7]: https://github.com/SaschaAndresGrimm/ALBIS/compare/v0.10.6...v0.10.7
[0.10.6]: https://github.com/SaschaAndresGrimm/ALBIS/compare/v0.10.5...v0.10.6
[0.10.5]: https://github.com/SaschaAndresGrimm/ALBIS/compare/v0.10.4...v0.10.5
[0.10.4]: https://github.com/SaschaAndresGrimm/ALBIS/compare/v0.10.3...v0.10.4
[0.10.3]: https://github.com/SaschaAndresGrimm/ALBIS/compare/v0.10.2...v0.10.3
[0.9.15]: https://github.com/SaschaAndresGrimm/ALBIS/compare/v0.9.14...v0.9.15
[0.9.14]: https://github.com/SaschaAndresGrimm/ALBIS/compare/v0.9.13...v0.9.14
[0.9.13]: https://github.com/SaschaAndresGrimm/ALBIS/compare/v0.9.12...v0.9.13
[0.9.12]: https://github.com/SaschaAndresGrimm/ALBIS/compare/v0.9.11...v0.9.12
[0.9.11]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.9.11
[0.9.10]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.9.10
[0.9.9]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.9.9
[0.9.8]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.9.8
[0.9.7]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.9.7
[0.9.6]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.9.6
[0.9.5]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.9.5
[0.9.4]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.9.4
[0.9.3]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.9.3
[0.9.2]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.9.2
[0.9.1]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.9.1
[0.9.0]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.9.0
[0.8.17]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.17
[0.8.16]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.16
[0.8.15]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.15
[0.8.14]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.14
[0.8.13]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.13
[0.8.12]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.12
[0.8.11]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.11
[0.8.10]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.10
[0.8.9]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.9
[0.8.2]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.2
