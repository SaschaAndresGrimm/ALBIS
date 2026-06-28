# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/SaschaAndresGrimm/ALBIS/compare/v0.9.14...HEAD
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
