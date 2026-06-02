# -*- mode: python ; coding: utf-8 -*-

from __future__ import annotations

import os
import sys
from PyInstaller.utils.hooks import collect_all

block_cipher = None
bundle_version = (os.environ.get("ALBIS_BUNDLE_VERSION", "").strip() or "0.0.0")
bundle_build = (os.environ.get("ALBIS_BUNDLE_BUILD", "").strip() or bundle_version)
is_linux = sys.platform.startswith("linux")

icon_path = os.environ.get("ALBIS_ICON", "").strip()
if not icon_path:
    if sys.platform == "darwin":
        candidates = (
            os.path.abspath("albis_assets/icon.icns"),
            os.path.abspath("frontend/ressources/icon.icns"),
            os.path.abspath("albis_assets/icon_1024x1024.png"),
            os.path.abspath("frontend/ressources/icon.png"),
        )
    elif sys.platform == "win32":
        candidates = (
            os.path.abspath("albis_assets/icon.ico"),
            os.path.abspath("frontend/ressources/icon.ico"),
        )
    else:
        candidates = (
            os.path.abspath("albis_assets/icon_512x512.png"),
            os.path.abspath("frontend/ressources/icon.png"),
        )
    for candidate in candidates:
        if os.path.exists(candidate):
            icon_path = candidate
            break
if icon_path and not os.path.exists(icon_path):
    icon_path = ""

datas = [("frontend", "frontend"), ("VERSION", ".")]
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
    strip=is_linux,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    icon=icon_path or None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=is_linux,
    upx=True,
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
        },
    )
