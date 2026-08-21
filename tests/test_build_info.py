"""Where the running program learns which build it is.

`VERSION` names a release; two builds of one release are indistinguishable by
it, which is the distinction a bug report needs. The commit was already computed
at packaging time for artifact filenames and then discarded, so these tests pin
down the resolution order that now carries it into the process -- and, just as
importantly, that an unstamped build reports nothing rather than guessing.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from backend import build_info

# The autouse fixture below stubs the git lookup out for every other test, so
# hold a reference to the real one for the test that is about the git lookup.
_REAL_COMMIT_FROM_GIT = build_info._commit_from_git


@pytest.fixture(autouse=True)
def _isolate_from_the_real_repo(monkeypatch, tmp_path: Path):
    """Keep the developer's own checkout out of every assertion."""
    monkeypatch.delenv("ALBIS_BUILD_COMMIT", raising=False)
    monkeypatch.setattr(build_info, "_repo_root", lambda: tmp_path)
    monkeypatch.setattr(build_info, "_commit_from_git", lambda: "")
    return tmp_path


def test_environment_wins_because_that_is_how_the_image_is_stamped() -> None:
    """The Docker image has no .git and no file; it gets a build argument."""
    import os

    os.environ["ALBIS_BUILD_COMMIT"] = "abcdef1"
    try:
        assert build_info.read_commit() == "abcdef1"
    finally:
        del os.environ["ALBIS_BUILD_COMMIT"]


def test_a_full_length_sha_is_shortened_for_display(monkeypatch) -> None:
    """CI passes `github.sha`, which is 40 characters; the footer shows seven."""
    monkeypatch.setenv("ALBIS_BUILD_COMMIT", "0123456789abcdef0123456789abcdef01234567")

    assert build_info.read_commit() == "0123456"


def test_the_bundled_file_is_read_when_there_is_no_environment_variable(
    _isolate_from_the_real_repo: Path,
) -> None:
    (_isolate_from_the_real_repo / "BUILD_COMMIT").write_text("fedcba9\n", encoding="utf-8")

    assert build_info.read_commit() == "fedcba9"


def test_an_unstamped_build_reports_nothing_rather_than_guessing() -> None:
    """The interface falls back to showing the version alone, which is honest."""
    assert build_info.read_commit() == ""


@pytest.mark.parametrize(
    "garbage",
    [
        "not-a-sha",
        "v0.11.0",
        "abc",  # too short to be a commit
        "",
        "   ",
        "zzzzzzz",  # right length, not hex
        "abcdef1\nrm -rf /",  # a whole line, not a token
    ],
)
def test_junk_never_reaches_a_bug_report(monkeypatch, garbage: str) -> None:
    """This string is displayed and pasted into issues, so it stays a commit."""
    monkeypatch.setenv("ALBIS_BUILD_COMMIT", garbage)

    assert build_info.read_commit() == ""


def test_git_is_not_consulted_inside_a_packaged_build(monkeypatch, tmp_path: Path) -> None:
    """A frozen app has no repository, and must not shell out looking for one.

    Not merely that the answer is empty -- that spawning `git` at every startup
    of a packaged desktop app never happens at all.
    """

    def _must_not_run(*args: object, **kwargs: object) -> None:
        raise AssertionError("git was invoked inside a packaged build")

    monkeypatch.setattr(build_info.subprocess, "run", _must_not_run)
    monkeypatch.setattr(build_info.sys, "_MEIPASS", str(tmp_path), raising=False)

    assert _REAL_COMMIT_FROM_GIT() == ""


def test_health_reports_the_commit_it_resolved() -> None:
    """The value has to actually reach the endpoint the interface reads."""
    from fastapi.testclient import TestClient

    from backend.app import ALBIS_COMMIT, app

    payload = TestClient(app, client=("127.0.0.1", 5555)).get("/api/health").json()

    assert payload["commit"] == ALBIS_COMMIT
    assert "version" in payload


def test_module_constant_matches_the_resolver(monkeypatch) -> None:
    """`ALBIS_COMMIT` is computed at import; reimporting must agree with it."""
    monkeypatch.setenv("ALBIS_BUILD_COMMIT", "1234567")
    reloaded = importlib.reload(build_info)
    try:
        assert reloaded.ALBIS_COMMIT == "1234567"
    finally:
        monkeypatch.delenv("ALBIS_BUILD_COMMIT", raising=False)
        importlib.reload(build_info)
