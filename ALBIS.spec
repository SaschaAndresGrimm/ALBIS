# -*- mode: python ; coding: utf-8 -*-

from __future__ import annotations

import os
import sys
from PyInstaller.utils.hooks import collect_all

block_cipher = None
bundle_version = (os.environ.get("ALBIS_BUNDLE_VERSION", "").strip() or "0.0.0")
bundle_build = (os.environ.get("ALBIS_BUNDLE_BUILD", "").strip() or bundle_version)
is_linux = sys.platform.startswith("linux")
is_macos = sys.platform == "darwin"

icon_path = os.environ.get("ALBIS_ICON", "").strip()
if not icon_path:
    if sys.platform == "darwin":
        candidates = (
            os.path.abspath("albis_assets/icon.icns"),
            os.path.abspath("frontend/resources/icon.icns"),
            os.path.abspath("albis_assets/icon_1024x1024.png"),
            os.path.abspath("frontend/resources/icon.png"),
        )
    elif sys.platform == "win32":
        candidates = (
            os.path.abspath("albis_assets/icon.ico"),
            os.path.abspath("frontend/resources/icon.ico"),
        )
    else:
        candidates = (
            os.path.abspath("albis_assets/icon_512x512.png"),
            os.path.abspath("frontend/resources/icon.png"),
        )
    for candidate in candidates:
        if os.path.exists(candidate):
            icon_path = candidate
            break
if icon_path and not os.path.exists(icon_path):
    icon_path = ""

datas = [("frontend", "frontend"), ("VERSION", ".")]
# The commit this build came from, written by scripts/stamp_build.py before
# packaging. Bundled only when present: an unstamped build reports no commit
# and shows its version alone, which is better than failing to package.
if os.path.exists(os.path.abspath("BUILD_COMMIT")):
    datas.append((os.path.abspath("BUILD_COMMIT"), "."))
# License and third-party attribution files (required for redistribution).
for license_file in (
    "LICENSE",
    "THIRD_PARTY_LICENSES.md",
    os.path.join("licenses", "LICENSE-APACHE-2.0.txt"),
    os.path.join("licenses", "LICENSE-MPL-2.0.txt"),
):
    license_path = os.path.abspath(license_file)
    if os.path.exists(license_path):
        datas.append((license_path, os.path.dirname(license_file) or "."))
for asset_name in (
    "albis_splash_1920x1080.png",
    "albis_splash_3840x2160.png",
    "icon.ico",
    "icon.icns",
    "icon_16x16.png",
    "icon_16x16@2x.png",
    "icon_32x32.png",
    "icon_32x32@2x.png",
    "icon_64x64.png",
    "icon_128x128.png",
    "icon_256x256.png",
    "icon_512x512.png",
    "icon_1024x1024.png",
):
    asset_path = os.path.abspath(os.path.join("albis_assets", asset_name))
    if os.path.exists(asset_path):
        datas.append((asset_path, "albis_assets"))
binaries: list = []
hiddenimports: list = ["backend.app", "backend.config"]
hiddenimports += [
    "fabio.cbfimage",
    "fabio.edfimage",
    "fabio.tifimage",
    "fabio.pilatusimage",
    "fabio.fabioimage",
    "fabio.fabioutils",
    "fabio.fabioformats",
    "fabio.openimage",
    "fabio.compression",
    "fabio.compression.compression",
    "fabio.ext._cif",
]
hiddenimports += [
    "fabio.adscimage",
    "fabio.binaryimage",
    "fabio.bruker100image",
    "fabio.brukerimage",
    "fabio.dm3image",
    "fabio.dtrekimage",
    "fabio.eigerimage",
    "fabio.esperantoimage",
    "fabio.fit2dimage",
    "fabio.fit2dmaskimage",
    "fabio.fit2dspreadsheetimage",
    "fabio.GEimage",
    "fabio.hdf5image",
    "fabio.HiPiCimage",
    "fabio.jpeg2kimage",
    "fabio.jpegimage",
    "fabio.kcdimage",
    "fabio.limaimage",
    "fabio.mar345image",
    "fabio.marccdimage",
    "fabio.mpaimage",
    "fabio.mrcimage",
    "fabio.numpyimage",
    "fabio.OXDimage",
    "fabio.pixiimage",
    "fabio.pnmimage",
    "fabio.raxisimage",
    "fabio.sparseimage",
    "fabio.speimage",
    "fabio.xcaliburimage",
    "fabio.xsdimage",
]
if sys.platform == "darwin":
    hiddenimports += ["AppKit", "Foundation", "objc", "Cocoa"]

collected_datas, collected_binaries, collected_hiddenimports = collect_all("hdf5plugin")
datas += collected_datas
binaries += collected_binaries
hiddenimports += collected_hiddenimports

# Bundle certifi's CA bundle so HTTPS (e.g. the GitHub update check) verifies in
# the packaged app, which otherwise has no system trust store.
certifi_datas, certifi_binaries, certifi_hiddenimports = collect_all("certifi")
datas += certifi_datas
binaries += certifi_binaries
hiddenimports += certifi_hiddenimports

# zstd response compression for remote sessions. The extension module is loaded
# through a C entry point that PyInstaller's static analysis does not see, so it
# needs collecting explicitly. A packaged build that misses it still runs and
# falls back to gzip, which would make the loss silent — hence the smoke test in
# scripts/smoke_packaged_binary.py.
zstd_datas, zstd_binaries, zstd_hiddenimports = collect_all("zstandard")
datas += zstd_datas
binaries += zstd_binaries
hiddenimports += zstd_hiddenimports

a = Analysis(
    ["albis_launcher.py"],
    pathex=[os.path.abspath(".")],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=[
        "PyQt5",
        "PyQt6",
        "PySide2",
        "PySide6",
        "shiboken2",
        "shiboken6",
        "torch",
        "torchvision",
        "torchaudio",
        "pyarrow",
        "pandas",
        "scipy",
        "sklearn",
        "matplotlib",
        "bokeh",
        "dask",
        "distributed",
        "numba",
        "sphinx",
        "pyFAI",
        "silx",
        "IPython",
        "jupyter",
    ],
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="ALBIS",
    debug=False,
    bootloader_ignore_signals=False,
    # Do not strip/UPX on Linux: stripping the numpy-vendored OpenBLAS .so with
    # older binutils (e.g. Ubuntu 22.04) corrupts its PT_LOAD page alignment,
    # yielding "ELF load command address/offset not page-aligned" at import on
    # glibc 2.35. Costs some size but keeps the payload loadable everywhere.
    strip=False,
    upx=not is_linux,
    console=False,
    disable_windowed_traceback=False,
    # macOS does not put the path of a double-clicked document on argv: it
    # sends an Apple Event instead, and on a cold launch that event arrives
    # while ALBIS is still starting its server, long before there is a delegate
    # to receive it -- so the file was silently dropped and the viewer opened
    # empty. This makes the bootloader catch that event before Python starts
    # and append the paths to argv, where the launcher's positional argument
    # already handles them. A running instance still gets the document through
    # application:openFiles:, which is the case that always worked.
    argv_emulation=is_macos,
    icon=icon_path or None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    # See EXE() above: stripping/UPX on Linux breaks the OpenBLAS .so alignment.
    strip=False,
    upx=not is_linux,
    name="ALBIS",
)

if sys.platform == "darwin":
    app_icon = icon_path if icon_path.lower().endswith(".icns") else None
    app = BUNDLE(
        coll,
        name="ALBIS.app",
        icon=app_icon,
        bundle_identifier="com.saschaandresgrimm.albis",
        info_plist={
            "CFBundleDisplayName": "ALBIS",
            "CFBundleName": "ALBIS",
            "CFBundleShortVersionString": bundle_version,
            "CFBundleVersion": bundle_build,
            # What ALBIS offers to open. Launch Services reads this when the
            # bundle is registered, which is what puts ALBIS in the Finder's
            # "Open With" menu; the launcher's application:openFiles: delegate
            # then receives the document, since macOS does not re-launch a
            # running app with the path on argv.
            #
            # Viewer, not Editor: ALBIS never writes back to the file it opened.
            # Kept in step with backend/file_associations.py by
            # tests/test_file_associations.py.
            "CFBundleDocumentTypes": [
                {
                    "CFBundleTypeName": "HDF5 data file",
                    "CFBundleTypeRole": "Viewer",
                    "LSHandlerRank": "Alternate",
                    "LSItemContentTypes": ["org.hdfgroup.hdf5"],
                },
                {
                    "CFBundleTypeName": "Crystallographic Binary Format image",
                    "CFBundleTypeRole": "Viewer",
                    "LSHandlerRank": "Owner",
                    "LSItemContentTypes": ["com.saschaandresgrimm.albis.cbf"],
                },
                {
                    "CFBundleTypeName": "ESRF Data Format image",
                    "CFBundleTypeRole": "Viewer",
                    "LSHandlerRank": "Owner",
                    "LSItemContentTypes": ["com.saschaandresgrimm.albis.edf"],
                },
                {
                    "CFBundleTypeName": "TIFF image",
                    "CFBundleTypeRole": "Viewer",
                    "LSHandlerRank": "Alternate",
                    "LSItemContentTypes": ["public.tiff"],
                },
            ],
            # `public.tiff` is Apple's own and `org.hdfgroup.hdf5` is declared
            # by the HDF Group's tools, so both are imported rather than
            # defined -- redefining a type another application owns is what
            # makes Launch Services pick the wrong handler. Importing
            # org.hdfgroup.hdf5 also means the association works on a machine
            # with no HDF5 tooling installed, where nothing else declares it.
            "UTImportedTypeDeclarations": [
                {
                    "UTTypeIdentifier": "org.hdfgroup.hdf5",
                    "UTTypeDescription": "HDF5 data file",
                    "UTTypeConformsTo": ["public.data"],
                    "UTTypeTagSpecification": {"public.filename-extension": ["h5", "hdf5"]},
                },
            ],
            # CBF and EDF have no owner anywhere, so ALBIS declares them.
            # Extension-only tags: a CBF opens with a comment block whose first
            # bytes vary by writer, and an EDF with an ASCII header too plain
            # to match on without also claiming unrelated text files.
            "UTExportedTypeDeclarations": [
                {
                    "UTTypeIdentifier": "com.saschaandresgrimm.albis.cbf",
                    "UTTypeDescription": "Crystallographic Binary Format image",
                    "UTTypeConformsTo": ["public.data", "public.image"],
                    "UTTypeTagSpecification": {
                        "public.filename-extension": ["cbf"],
                        "public.mime-type": ["image/x-cbf"],
                    },
                },
                {
                    "UTTypeIdentifier": "com.saschaandresgrimm.albis.edf",
                    "UTTypeDescription": "ESRF Data Format image",
                    "UTTypeConformsTo": ["public.data", "public.image"],
                    "UTTypeTagSpecification": {
                        "public.filename-extension": ["edf"],
                        "public.mime-type": ["image/x-edf"],
                    },
                },
            ],
        },
    )
