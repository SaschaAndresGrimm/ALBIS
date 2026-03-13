# ALBIS (ALBIS WEB VIEW)
![ALBIS screenshot](frontend/ressources/albis.png)

ALBIS is an **ALBULA‑style**, browser‑based image viewer for large HDF5 stacks and other common DECTRIS camera formats. It is platform‑independent, free, and open source.

It targets modern **DECTRIS** detectors (SELUN, EIGER2, PILATUS4) and supports **filewriter1** and **filewriter2** layouts, including multi‑threshold (multi‑channel) data.

Image sources can be:
- Files on disk (`.h5/.hdf5` stacks and common detector image formats `.tif/.tiff`, `.cbf/.cbf.gz`, `.edf`).
- The detector **SIMPLON monitor** stream for live viewing.
- **JUNGFRAUJOCH Preview** ZeroMQ PUB stream (CBOR image messages + reflection spots).
- The **Remote Stream API** (`/api/remote/v1/*`) for externally pushed frames + metadata.

ALBIS includes quick statistics tools, an HDF5 dataset inspector, and many small workflow optimizations.

ALBIS `1.0` is a **local-first desktop viewer** for workstation and beamline use.
Official public support covers:
- **Windows x64**
- **macOS arm64 / x64**
- **Linux x64**

Docker images are published for **local and trusted lab deployments** on `linux/amd64` and `linux/arm64`.
Public internet exposure is **not** a supported deployment mode for `1.0`.


## Highlights

- ALBULA‑style UI with fast navigation and contrast control.
- Full support for DECTRIS filewriter1 and filewriter2 (multi‑threshold data with selector).
- Live SIMPLON monitor mode with mask prefetch.
- JUNGFRAUJOCH Preview mode (ZeroMQ CBOR stream bridge with reflection overlays).
- Remote Stream mode for live external producers (with optional ring parameters and colored peak overlays).
- ROI tools (line, box, circle, annulus) with statistics and plots.
- Pixel mask support (gaps and defective pixels).
- spotfinding & resolution rings overlay
- multi language support

## Downloads / Installation

You can download ready-to-use standalone binaries for your operating system. No Python installation is required for these.
Public releases include signed desktop artifacts where the platform supports them, plus `SHA256SUMS.txt` for download verification.

Check the [Releases](https://github.com/SaschaAndresGrimm/ALBIS/releases) page for the latest packages:

- **macOS Apple Silicon (arm64)**: `ALBIS-macos-arm64-v<version>-<commit>.dmg` (installer) or `.zip` (portable).
- **macOS Intel (x64)**: `ALBIS-macos-x64-v<version>-<commit>.dmg` (installer) or `.zip` (portable).
- **Windows x64**: `ALBIS-Setup-windows-x64-v<version>-<commit>.exe` (installer) or `ALBIS-windows-x64-v<version>-<commit>.zip` (portable).
- **Linux x64**: `ALBIS-<version>-x86_64.AppImage`, `ALBIS-<version>-x86_64-appimage-bundle.tar.gz` (AppImage + install/uninstall scripts), and `ALBIS-linux-x64-v<version>-<commit>.tar.gz`.

ALBIS also runs directly in Python, see the [Power User Guide](docs/POWER_USER_GUIDE.md)

## Keyboard Shortcuts

- `⌘O` / `Ctrl+O` Open File
- `⌘W` / `Ctrl+W` Close File
- `⌘S` / `Ctrl+S` Save As
- `⌘E` / `Ctrl+E` Export Image
- `F1` Documentation
- `Tab` Play/Pause
- `←`/`→` Previous/Next frame
- `↑`/`↓` Jump by Step setting (or threshold change when multi‑threshold is active)

## Advanced Usage & Contributing

For power users looking to configure the server, use the advanced Stream API, or run ALBIS from source:
- [Power User Guide](docs/POWER_USER_GUIDE.md)

For developers looking to build, test, and contribute:
- [Developer Guide](docs/DEVELOPER_GUIDE.md)
- [Contributing](CONTRIBUTING.md)

## Acknowledgements and Contributions
This project stands on the shoulders of a giant: ALBULA. Thanks to Volker Pilipp for creating such an intuitive image viewer that set the benchmark.
Thanks to Tilman Donath and Nicolas Pilet for testing, breaking, and giving useful feedback for improvements.
