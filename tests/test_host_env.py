"""Programs ALBIS starts get the host's library path, not the bundle's.

See backend/host_env.py: on Rocky Linux 9 the browser inherited the bundle's
LD_LIBRARY_PATH, loaded its older libstdc++ and died, while the launcher said
"opening browser".
"""

from __future__ import annotations

import pytest

import albis_launcher
from backend.host_env import restore_host_library_path

BUNDLE = "/opt/ALBIS/_internal"


def test_the_users_own_library_path_comes_back() -> None:
    env = {"LD_LIBRARY_PATH": f"{BUNDLE}:/opt/site/lib", "LD_LIBRARY_PATH_ORIG": "/opt/site/lib"}

    assert restore_host_library_path(env, frozen=True, platform="linux")
    assert env == {"LD_LIBRARY_PATH": "/opt/site/lib"}


@pytest.mark.parametrize("orig", [None, ""])
def test_no_library_path_before_means_none_after(orig: str | None) -> None:
    env = {"LD_LIBRARY_PATH": BUNDLE}
    if orig is not None:
        env["LD_LIBRARY_PATH_ORIG"] = orig

    assert restore_host_library_path(env, frozen=True, platform="linux")
    assert env == {}


def test_a_second_call_changes_nothing() -> None:
    env = {"LD_LIBRARY_PATH": BUNDLE}
    restore_host_library_path(env, frozen=True, platform="linux")

    assert not restore_host_library_path(env, frozen=True, platform="linux")
    assert env == {}


@pytest.mark.parametrize(
    ("frozen", "platform"),
    [(False, "linux"), (True, "darwin"), (True, "win32")],
    ids=["source checkout", "macOS build", "Windows build"],
)
def test_only_a_packaged_linux_build_is_touched(frozen: bool, platform: str) -> None:
    env = {"LD_LIBRARY_PATH": "/opt/site/lib", "LD_LIBRARY_PATH_ORIG": "/elsewhere"}

    assert not restore_host_library_path(env, frozen=frozen, platform=platform)
    assert env == {"LD_LIBRARY_PATH": "/opt/site/lib", "LD_LIBRARY_PATH_ORIG": "/elsewhere"}


def test_when_no_browser_opens_the_launcher_says_where_to_point_one(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(albis_launcher.sys, "platform", "linux")
    monkeypatch.setattr(albis_launcher.webbrowser, "open", lambda _url, **_kwargs: False)

    assert not albis_launcher._open_browser("127.0.0.1", 36061)
    assert "http://127.0.0.1:36061" in capsys.readouterr().out


def test_an_opened_browser_needs_no_message(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(albis_launcher.webbrowser, "open", lambda _url, **_kwargs: True)

    assert albis_launcher._open_browser("127.0.0.1", 36061)
    assert capsys.readouterr().out == ""
