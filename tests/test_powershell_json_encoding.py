"""No PowerShell script may write JSON with `Set-Content -Encoding UTF8`.

Windows PowerShell 5.1 and PowerShell 7 disagree about what `-Encoding UTF8`
means: 5.1 writes a UTF-8 *byte order mark*, 7 does not. Every JSON consumer in
this project rejects that BOM --

  * the Azure signing dlib parses its metadata with System.Text.Json, which
    fails with "'0xEF' is an invalid start of a value";
  * `backend/config.py` opens config.json with ``encoding="utf-8"``, so
    ``json.load`` fails on the same byte.

-- and the disagreement only shows up under whichever host happens to run the
script. It cost a release build once: signing worked from the workflow step,
which declares ``shell: pwsh``, and failed inside Inno Setup, which shells out
to ``powershell.exe``. Same script, same inputs, different PowerShell.

A CI job cannot catch this, because CI runs pwsh. A static check can.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = sorted(ROOT.glob("scripts/*.ps1"))

# `Set-Content ... -Encoding UTF8` (but not -Encoding UTF8NoBOM, which is
# unambiguous -- and pwsh-only, so it would break 5.1 for a different reason).
SET_CONTENT_UTF8 = re.compile(r"Set-Content\b[^\r\n|]*-Encoding\s+UTF8(?!NoBOM)\b", re.IGNORECASE)
# Out-File has exactly the same split behaviour.
OUT_FILE_UTF8 = re.compile(r"Out-File\b[^\r\n|]*-Encoding\s+UTF8(?!NoBOM)\b", re.IGNORECASE)


def test_there_are_powershell_scripts_to_check() -> None:
    """Guards the guard: a moved scripts/ directory would pass vacuously."""
    assert SCRIPTS, "no scripts/*.ps1 found; has the layout changed?"


def test_no_script_writes_a_bom_where_json_is_parsed() -> None:
    offenders: list[str] = []
    for script in SCRIPTS:
        for number, line in enumerate(script.read_text(encoding="utf-8").splitlines(), 1):
            # Comments are skipped, or this test fails on the notes explaining
            # itself -- which is exactly what happened when it was written.
            if line.lstrip().startswith("#"):
                continue
            for pattern in (SET_CONTENT_UTF8, OUT_FILE_UTF8):
                match = pattern.search(line)
                if match:
                    offenders.append(
                        f"{script.relative_to(ROOT)}:{number}: {match.group(0).strip()}"
                    )

    assert not offenders, (
        "These write UTF-8 with a BOM under Windows PowerShell 5.1:\n  "
        + "\n  ".join(offenders)
        + "\n\nUse an explicit BOM-less encoding instead, which behaves the "
        "same on 5.1 and 7:\n"
        "  $utf8NoBom = New-Object System.Text.UTF8Encoding $false\n"
        "  [System.IO.File]::WriteAllText($absolutePath, $json, $utf8NoBom)\n"
        "(WriteAllText resolves relative paths against the process working "
        "directory, not PowerShell's location, so pass an absolute path.)"
    )


def test_the_signing_metadata_is_written_bom_free() -> None:
    """The specific file whose BOM broke a release build."""
    text = (ROOT / "scripts" / "sign_windows.ps1").read_text(encoding="utf-8")
    assert "UTF8Encoding" in text, "sign_windows.ps1 no longer sets an explicit encoding"
    assert "WriteAllText" in text
