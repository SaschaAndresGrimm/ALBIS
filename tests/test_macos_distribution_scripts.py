"""Ordering rules in the macOS release scripts that nothing else can catch.

A notarization ticket is stapled into a bundle, so *when* it is stapled
decides which copies carry it. v0.20.0 signed the app, built the DMG from it,
notarized the DMG, and only then stapled the app -- leaving the copy sealed
inside the DMG without a ticket. Every existing check passed, because they
looked at `dist/ALBIS.app` and at the DMG container, never at the app the DMG
actually ships. Users who installed from the DMG got an app that asked Apple
for a ticket on every launch, which hangs on a slow or filtered network.

None of this can be tested by running the scripts: they need an Apple
Developer identity, notarization credentials and a macOS runner. What can be
tested is the order of the steps, which is what went wrong.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SIGN_SCRIPT = REPO_ROOT / "scripts" / "sign_macos.sh"
VERIFY_SCRIPT = REPO_ROOT / "scripts" / "verify_macos_distribution.sh"

# The script serves two paths: one for a build with no notarization
# credentials, which just packages what it has, and one that notarizes. Only
# the second has an ordering to get wrong, so the checks below read it alone
# -- otherwise the unsigned path's own `build_dmg` and `create_zip` calls,
# which legitimately come first, would be mistaken for the ordered ones.
_NOTARIZED_PATH_MARKER = "xcrun not available for notarization"


def _notarized_path() -> str:
    text = SIGN_SCRIPT.read_text(encoding="utf-8")
    assert _NOTARIZED_PATH_MARKER in text, "The notarization path is no longer recognisable"
    return text.split(_NOTARIZED_PATH_MARKER, 1)[1]


def _step_line(section: str, needle: str) -> int:
    """Where a step runs, ignoring comments that merely mention it."""
    for number, line in enumerate(section.splitlines(), start=1):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        if needle == stripped or needle in stripped:
            return number
    raise AssertionError(f"{needle!r} no longer appears as a step in the script")


def test_the_app_is_stapled_before_the_dmg_seals_a_copy_of_it() -> None:
    section = _notarized_path()

    staple_app = _step_line(section, 'xcrun stapler staple "$APP_PATH"')
    build_dmg = _step_line(section, "build_dmg")

    assert staple_app < build_dmg, (
        "The DMG is built before the app is stapled, so the copy inside it "
        "ships without a notarization ticket."
    )


def test_the_app_is_notarized_in_its_own_right_not_only_via_the_dmg() -> None:
    # Stapling needs a ticket for the app's own cdhash. Submitting only the
    # DMG does register the nested code, but the app has to be stapled before
    # the DMG exists -- so the app is submitted on its own, first.
    section = _notarized_path()

    submit_app = _step_line(section, 'notarize "$APP_NOTARIZE_ZIP"')
    staple_app = _step_line(section, 'xcrun stapler staple "$APP_PATH"')
    submit_dmg = _step_line(section, 'notarize "$DMG_OUT"')
    staple_dmg = _step_line(section, 'xcrun stapler staple "$DMG_OUT"')

    assert submit_app < staple_app < submit_dmg < staple_dmg


def test_the_distributable_zip_is_built_after_the_app_is_stapled() -> None:
    # The zip is a copy too. It was the one artifact that already got this
    # right, by accident of ordering; keep it that way on purpose.
    section = _notarized_path()

    staple_app = _step_line(section, 'xcrun stapler staple "$APP_PATH"')
    create_zip = _step_line(section, "create_zip")

    assert create_zip > staple_app


def test_the_verifier_looks_inside_the_dmg_and_not_only_at_it() -> None:
    text = VERIFY_SCRIPT.read_text(encoding="utf-8")

    assert "verify_dmg_contents" in text, (
        "Nothing verifies the app the DMG ships; the container passing its own "
        "checks is what let an unstapled app through."
    )
    assert "hdiutil attach" in text
    # It must run verify_app on the mounted bundle: that is what applies the
    # signature, hardened-runtime and stapler checks to it.
    body = text.split("verify_dmg_contents()", 1)[1]
    assert 'verify_app "$dmg_app"' in body


def test_the_verifier_is_actually_called_for_the_dmg() -> None:
    text = VERIFY_SCRIPT.read_text(encoding="utf-8")
    invocation = text.split('if [ -n "$DMG_PATH" ]; then', 1)[1].split("fi", 1)[0]
    assert "verify_dmg_contents" in invocation
