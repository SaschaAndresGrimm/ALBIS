# Packaging and Builds

How ALBIS is turned into the installers on the Releases page: the PyInstaller
bundle, the per-platform build scripts, and what each build pins. For
maintainers cutting a release, and for anyone reproducing a build; day-to-day
development needs none of it (see the [Developer Guide](DEVELOPER_GUIDE.md)).
Releasing itself is in the [Release Checklist](RELEASE_CHECKLIST.md), signing
on Windows in [Windows Signing](WINDOWS_SIGNING.md).

## PyInstaller

ALBIS can be bundled into a **platform‑native app** (no Python required) using PyInstaller.

Build scripts for Linux/Windows now use an **isolated build venv by default** (`.build-venv-linux` / `.build-venv-windows`) to keep artifacts reproducible and avoid accidental dependency bloat from the host Python environment.

### Build (macOS)

```bash
./scripts/build_mac.sh
```

This produces versioned artifacts in `dist/`, e.g.:

- `ALBIS-macos-<arch>-v<version>-<commit>.zip`
- `ALBIS-macos-<arch>-v<version>-<commit>.dmg`

`build_mac.sh` builds with whichever `python3` is first on `PATH`, or `PYTHON_BIN`.
Release builds use the same python-build-standalone interpreter as Linux, because
PyInstaller copies the interpreter into the app with its OpenSSL, SQLite and expat,
and `actions/setup-python`'s macOS builds carry OpenSSL from the 3.0 line. To build
the way the release does:

```bash
./scripts/install_python_standalone.sh /tmp/albis-python
PYTHON_BIN=/tmp/albis-python/bin/python3 ./scripts/build_mac.sh
```

`build_mac.sh` also attempts to create a macOS `.app` bundle with icon support (from `frontend/resources/icon.png`).
DMG images get their layout — background image, icon positions, an `Applications`
shortcut for drag-and-drop installation — from `scripts/build_styled_dmg.sh`, which
both this script and `scripts/sign_macos.sh` call so a local dev build and a signed
release look the same. See `docs/RELEASE_CHECKLIST.md`'s "macOS DMG Layout" section
to change the background or verify it looks right; the geometry itself lives in
`scripts/dmg_layout.py`.

To sign on build, set your Developer ID certificate and password before running the build:

```bash
export MACOS_SIGN_CERT_PATH="/path/to/your/albis-dev-id.p12"
export MACOS_SIGN_CERT_PASSWORD="<p12-password>"
export MACOS_SIGNING_IDENTITY="Developer ID Application: Your Name Or Company (TEAMID)"  # optional if the p12 contains a single signing identity
./scripts/build_mac.sh
```

Optional notarization env vars:

- `APPLE_ID`
- `APPLE_TEAM_ID`
- `APPLE_APP_SPECIFIC_PASSWORD`

If the signing env vars are set, `build_mac.sh` signs the `.app`, creates a signed `.dmg`, and notarizes/staples the macOS artifacts when Apple credentials are present. The release `.zip` is rebuilt after stapling so the zipped `.app` carries the notarization ticket.

Public tag releases require both signing and notarization secrets. CI rejects incomplete macOS signing configuration rather than publishing macOS artifacts that Gatekeeper would report as unidentified or potentially malicious.

### Build (Linux)

```bash
./scripts/build_linux.sh
./scripts/package_linux_appimage.sh
```

Example output:

- `ALBIS-linux-<arch>-v<version>-<commit>.tar.gz`
- `ALBIS-<version>-<appimage-arch>.AppImage`
- `install_linux_appimage.sh` and `uninstall_linux.sh` (kept in `scripts/`; release workflow bundles them with the AppImage)

AppImage assets use AppImage-standard architecture tags such as `x86_64`.

Optional build env controls:

- `ALBIS_BUILD_ISOLATED=0` uses your current Python environment instead of the build venv.
- `ALBIS_BUILD_CLEAN_VENV=0` reuses an existing build venv.
- `ALBIS_BUILD_VENV=/custom/path` overrides the build venv location.

`scripts/package_linux_appimage.sh` requires `appimagetool` on `PATH`.

The bundle only runs on a glibc at least as new as the one it was built against,
so a build on your own distribution may not start on older ones. Release builds
run inside the `manylinux_2_28` image (AlmaLinux 8, glibc 2.28) with a
python-build-standalone interpreter, because PyInstaller needs a shared
`libpython` and the image's own interpreters have none. To reproduce one:

```bash
docker run --rm -it --platform linux/amd64 -v "$PWD:/src" -w /src \
  quay.io/pypa/manylinux_2_28_x86_64 bash
# inside the container:
git config --global --add safe.directory /src
./scripts/install_python_standalone.sh /opt/albis-python
export PYTHON_BIN=/opt/albis-python/bin/python3 CC=gcc CXX=g++
# keep the Linux build venv out of the checkout you mounted
export ALBIS_BUILD_VENV=/tmp/albis-build-venv
./scripts/build_linux.sh
./scripts/check_glibc_floor.sh 2.28 dist/ALBIS
```

`CC=gcc` is needed because the interpreter records clang as its compiler, and
`dectris-compression` is built from source. `build_linux.sh` runs
`scripts/strip_linux_bundle.sh` after PyInstaller: the standalone Python and several
Linux wheels carry their DWARF debug info, which made the Linux download three times
the size of the others. It strips debug info only, and leaves the wheels' vendored
`*.libs/` folders alone, because stripping numpy's OpenBLAS once broke its page
alignment on glibc 2.35 (why `ALBIS.spec` keeps `strip=False` on Linux).
`./scripts/smoke_linux_distro.sh IMAGE TARBALL`
then starts the tarball in another distribution's container, the way the
`smoke_linux` CI job does for Rocky Linux 8 and 9, Ubuntu 22.04 and Ubuntu 24.04. It also starts
ALBIS with a stand-in `xdg-open` and fails if the browser would inherit the bundle's
`LD_LIBRARY_PATH`, or if the bundle ships its own `libstdc++` (see `backend/host_env.py`).

Public GitHub Releases publish a Linux bundle:

- `ALBIS-<version>-<appimage-arch>-appimage-bundle.tar.gz`
  - includes `.AppImage`, `install_linux_appimage.sh`, and `uninstall_linux.sh`
  - release also includes standalone `ALBIS-<version>-<appimage-arch>.AppImage` and `ALBIS-linux-<arch>-v<version>-<commit>.tar.gz`

Recommended local desktop integration (user scope, AppImage):

```bash
./scripts/install_linux_appimage.sh dist/ALBIS-<version>-<appimage-arch>.AppImage
```

Manual tarball integration (user scope):

```bash
./scripts/install_linux.sh
```

Both installation scripts integrate ALBIS under `~/.local` (launcher + desktop entry + icon).

To remove it again (default keeps user data/config):

```bash
./scripts/uninstall_linux.sh
```

To also remove `~/ALBIS-data` and `~/.config/albis`:

```bash
./scripts/uninstall_linux.sh --purge-user-data
```

### Build (Windows)

```powershell
.\scripts\build_windows.ps1
.\scripts\package_windows_innosetup.ps1
```

`build_windows.ps1` makes its build venv from `PYTHON_BIN`, or `python` on `PATH`.
Release builds set `PYTHON_BIN` to the same python-build-standalone interpreter as
Linux and macOS, installed from Git Bash with
`scripts/install_python_standalone.sh`, because the interpreter ships inside the
app with its OpenSSL, SQLite and expat.

Example output:

- `ALBIS-windows-<arch>-v<version>-<commit>.zip`
- `ALBIS-Setup-windows-<arch>-v<version>-<commit>.exe`

Windows signing setup and low-cost certificate options are covered in [`docs/WINDOWS_SIGNING.md`](WINDOWS_SIGNING.md).

Optional build env controls:

- `$env:ALBIS_BUILD_ISOLATED = "0"` uses your current Python environment.
- `$env:ALBIS_BUILD_CLEAN_VENV = "0"` reuses an existing build venv.
- `$env:ALBIS_BUILD_VENV = "C:\\path\\to\\venv"` overrides the build venv location.

Cross-target naming controls (all platforms):

- `ALBIS_TARGET_OS=<linux|windows|macos>` overrides normalized target OS tag.
- `ALBIS_TARGET_ARCH=<x64|arm64|...>` overrides normalized target architecture tag.

Public tag releases require the following signing/notarization environment variables in CI:

- macOS: `MACOS_SIGN_CERT_B64`, `MACOS_SIGN_CERT_PASSWORD`, `APPLE_ID`, `APPLE_TEAM_ID`, `APPLE_APP_SPECIFIC_PASSWORD`
  - optional: `MACOS_SIGNING_IDENTITY` to force a specific imported identity; otherwise the first Developer ID Application identity from the `.p12` is used
  - `MACOS_SIGN_CERT_B64` should contain the base64-encoded `.p12` payload
  - the `.p12` must contain a `Developer ID Application` certificate with its private key
- Windows: either Azure Artifact Signing variables/secrets or the legacy PFX variables
  - Azure variables: `AZURE_ARTIFACT_SIGNING_ENDPOINT`, `AZURE_ARTIFACT_SIGNING_ACCOUNT`, `AZURE_ARTIFACT_SIGNING_CERT_PROFILE`
  - Azure secrets: `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`
  - legacy PFX secrets: `WINDOWS_SIGN_CERT_B64`, `WINDOWS_SIGN_CERT_PASSWORD`, `WINDOWS_SIGN_TIMESTAMP_URL`
  - when configured, the Windows pipeline signs `dist/ALBIS/ALBIS.exe`, the setup `.exe`, and the generated `unins*.exe`
- Linux (GPG detached signatures): `LINUX_GPG_PRIVATE_KEY_B64`, `LINUX_GPG_PASSPHRASE`, `LINUX_GPG_KEY_ID`
  - `LINUX_GPG_PRIVATE_KEY_B64` accepts base64-encoded private key data (recommended), a raw ASCII-armored private key block, or an escaped armored block using `\n`.

On a local Mac, you can bootstrap the macOS GitHub secrets in one step:

```bash
./scripts/bootstrap_macos_signing_ci.sh
```

The bootstrap script auto-detects `~/Documents/albis-dev-id.p12` when present, validates the certificate by importing it into a temporary keychain, infers `MACOS_SIGNING_IDENTITY`, and uploads the macOS signing secrets to the current GitHub repository via `gh`.

If `APPLE_ID`, `APPLE_TEAM_ID`, and `APPLE_APP_SPECIFIC_PASSWORD` are available from command-line flags, the current shell, or recent shell history, it uploads those too; otherwise GitHub CI still signs macOS artifacts and skips notarization.

The Inno installer creates Start Menu entries for:

- `ALBIS`
- `Open Logs`
- `Open Data Folder`
- `Edit Config`

Installer defaults:

- Per-user install scope under `%LOCALAPPDATA%\Programs\ALBIS`.
- Interactive installs show the standard Inno Setup destination page, so users can choose a custom install directory.
- The wizard carries ALBIS artwork in the macOS DMG's light palette: a background image behind every page, the icon over it on the Welcome and Finished pages, and the icon in the top-right corner of the others. There is one image per DPI step in `albis_assets/installer/`; regenerate them with `scripts/generate_installer_images.py`.
- Needs Inno Setup 6.7.0 or later, the first with `WizardBackImageFile`. CI builds with exactly the version pinned in `scripts/package_windows_innosetup.ps1`; a local build accepts any 6.7.0 or later and warns if it differs from the pin.
- Windows Add/Remove Programs uses the stable `AppId=ALBIS`, shows the ALBIS icon, and links support/updates to the GitHub project.
- Installer and uninstaller try to stop a running `ALBIS.exe` gracefully before falling back to forced termination, so upgrades and removals work better for the background-process model.
- No admin rights required (`PrivilegesRequired=lowest`).
- Portable `.zip` remains available from local builds and the `Build Artifacts` workflow.

Public GitHub Releases publish architecture-specific install + portable assets:

- macOS: `ALBIS-macos-arm64-*` and `ALBIS-macos-x64-*` (`.dmg` + `.zip`)
- Windows: `ALBIS-Setup-windows-x64-*` + `ALBIS-windows-x64-*.zip`
- Linux: `ALBIS-linux-x64-*.tar.gz`, `ALBIS-*-x86_64.AppImage`, and `ALBIS-*-x86_64-appimage-bundle.tar.gz`, and the same three for arm64 (`ALBIS-linux-arm64-*`, `*-aarch64*`)
- every target: `ALBIS-<target>-*.cdx.json`, the CycloneDX bill of materials for that bundle

The bill of materials is written by `scripts/generate_sbom.py`, which each build
script runs right after PyInstaller with the same interpreter. It reads the TOC
files PyInstaller leaves in `build/ALBIS/` and lists every installed package
with a file in the bundle (so on macOS only the pyobjc frameworks ALBIS
imports, and no build tools), the native libraries those carry that report a
version (HDF5, OpenBLAS, libzmq, libzstd, Pillow's codecs; OpenSSL, zlib, expat
and SQLite in the interpreter), the interpreter, the PyInstaller bootloader and
the files vendored into `frontend/`. A library the interpreter takes from the
operating system (on macOS, zlib from `/usr/lib`) is not shipped and is only
named on the CPython entry, so a scanner does not hold Apple's copy against
ALBIS. The build fails if an installed package
does not match its pin. The filter plugins inside hdf5plugin, and the
compression libraries in them, report no version at run time; they are listed
from hdf5plugin's own documentation of each release, kept in
`HDF5PLUGIN_EMBEDDED` in `scripts/sbom_bundle.py`. Bumping the hdf5plugin pin
means adding its list there (a test fails until it is), copied from
`doc/information.rst` at that release's tag.

Native libraries carry a CPE (`cpe:2.3:a:<vendor>:<product>:<version>`) where NVD
lists one, from `CPE_PRODUCTS` in `scripts/sbom_bundle.py`: a scanner has no package
ecosystem to match a C library by, only its CPE. `scripts/scan_sbom.sh` runs grype
(pinned, checksum-verified) over the SBOMs. The release scans them before it signs
anything and stops on an unreviewed Critical finding; the weekly Security workflow
scans the latest release's and fails on High. Reviewed findings that do not apply go
in `.grype.yaml`, each with its reason. To scan locally:
`scripts/scan_sbom.sh dist/*.cdx.json`.

The Docker image carries its own bill of materials and provenance as registry
attestations, made by BuildKit from the finished image:
`docker buildx imagetools inspect ghcr.io/<owner>/albis:<tag> --format '{{ json .SBOM }}'`.

Public release inputs are pinned:

- runtime dependencies in `backend/requirements.txt`
- PyInstaller via the build scripts
- Docker base image by digest
- Linux build images (`manylinux_2_28`, x86_64 and aarch64 of the same date) and the distribution images `smoke_linux` runs on, by digest
- the Python bundled on Linux, macOS and Windows (python-build-standalone) by release + checksum per platform in `scripts/install_python_standalone.sh`; the weekly `python-standalone-bump` workflow pushes a branch and opens an issue when a newer release is two weeks old
- AppImage tooling by explicit version + checksum in CI

### Output

The unpacked app payload is created under `dist/ALBIS/` (and on macOS additionally `dist/ALBIS.app`).
Use `albis.config.json` to change data path, host/port, logging, and launcher behavior.
