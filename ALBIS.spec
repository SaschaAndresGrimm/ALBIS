# -*- mode: python ; coding: utf-8 -*-

from __future__ import annotations

import os
import sys
from PyInstaller.utils.hooks import collect_all

block_cipher = None
bundle_version = (os.environ.get("ALBIS_BUNDLE_VERSION", "").strip() or "0.0.0")
bundle_build = (os.environ.get("ALBIS_BUNDLE_BUILD", "").strip() or bundle_version)

icon_path = os.environ.get("ALBIS_ICON", "").strip()
if not icon_path:
    if sys.platform == "darwin":
        candidates = (
            os.path.abspath("albis_assets/albis_macos.icns"),
            os.path.abspath("frontend/ressources/icon.icns"),
            os.path.abspath("albis_assets/albis_1024x1024.png"),
            os.path.abspath("frontend/ressources/icon.png"),
        )
    elif sys.platform == "win32":
        candidates = (
            os.path.abspath("albis_assets/albis_256x256.png"),
            os.path.abspath("frontend/ressources/icon.png"),
        )
    else:
        candidates = (
            os.path.abspath("albis_assets/albis_512x512.png"),
            os.path.abspath("frontend/ressources/icon.png"),
        )
    for candidate in candidates:
        if os.path.exists(candidate):
            icon_path = candidate
            break
if icon_path and not os.path.exists(icon_path):
    icon_path = ""

datas = [("frontend", "frontend")]
if os.path.exists("albis.config.json"):
    datas.append(("albis.config.json", "."))
binaries: list = []
hiddenimports: list = ["backend.app", "backend.config"]
if sys.platform == "darwin":
    hiddenimports += ["AppKit", "Foundation", "objc", "Cocoa"]

for name in ("hdf5plugin", "fabio"):
    collected_datas, collected_binaries, collected_hiddenimports = collect_all(name)
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
    strip=False,
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
    strip=False,
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
