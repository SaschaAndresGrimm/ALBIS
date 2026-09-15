"""Settings shows the paths ALBIS is using, not two empty boxes.

`data.root` and `logging.dir` both default to an empty string, and empty is not
a missing value -- it is a rule: work the location out at start, from whether
this is a packaged build and where the config file sits. The settings dialog
showed the configured value, so on a default install it showed nothing twice,
for paths that plainly existed and were in use. A tester on Windows reported
exactly that.

Only the backend can resolve those rules, so it reports the result. The
interface puts it in the placeholder rather than the value, which
`frontend/tests/settings_effective_paths.test.js` covers: filling the value in
looks identical and then pins the path on the next save, freezing a location
meant to follow the installation.
"""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from backend.app import app, runtime_state
from backend.config import resolve_data_dir, resolve_log_dir


def test_settings_report_the_paths_albis_actually_uses() -> None:
    payload = TestClient(app).get("/api/settings").json()
    effective = payload.get("effective") or {}

    assert effective.get("data", {}).get("root") == str(
        resolve_data_dir(runtime_state.config, runtime_state.config_path)
    )
    assert effective.get("logging", {}).get("dir") == str(
        resolve_log_dir(runtime_state.config, runtime_state.config_path)
    )


def test_the_reported_paths_are_absolute() -> None:
    """A relative path in the box is what the user could not resolve anyway.

    `./data` is relative to the config file's directory, which a browser has no
    way to join it against -- so reporting it unresolved would be no more use
    than reporting nothing at all.
    """
    effective = TestClient(app).get("/api/settings").json()["effective"]

    assert Path(effective["data"]["root"]).is_absolute()
    assert Path(effective["logging"]["dir"]).is_absolute()


def test_defaults_still_cannot_answer_this() -> None:
    """Why a new field was needed rather than reusing `defaults`.

    `defaults` is DEFAULT_CONFIG, where both keys are the empty string. If that
    ever changes to hold real paths this test fails, and the extra field can go.
    """
    payload = TestClient(app).get("/api/settings").json()

    assert payload["defaults"]["data"]["root"] == ""
    assert payload["defaults"]["logging"]["dir"] == ""
