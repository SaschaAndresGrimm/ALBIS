# -*- mode: python ; coding: utf-8 -*-

from __future__ import annotations

import os
from PyInstaller.utils.hooks import collect_all

block_cipher = None

datas = [("frontend", "frontend")]
binaries: list = []
hiddenimports: list = []

for name in ("hdf5plugin", "fabio"):
    collected = collect_all(name)
    datas += collected.datas
    binaries += collected.binaries
    hiddenimports += collected.hiddenimports

a = Analysis(
    ["albis_launcher.py"],
    pathex=[os.path.abspath(".")],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
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
