# -*- mode: python ; coding: utf-8 -*-

from __future__ import annotations

import os
from PyInstaller.utils.hooks import collect_all

block_cipher = None

datas = [("frontend", "frontend")]
binaries: list = []
hiddenimports: list = ["backend.app", "backend.config"]

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
