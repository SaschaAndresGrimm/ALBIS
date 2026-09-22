"""The release-side facts the in-app update notification depends on.

The notification resolves what a user should do from how ALBIS was installed,
and two pieces of that live outside the Python package: the Dockerfile tells a
container that it is one, and the release workflow decides whether a tag
becomes the release GitHub reports as "latest". Either can be changed without
touching `backend/`, and neither would fail loudly -- a container would offer
its user a desktop installer, and a release candidate would be pushed at every
stable install -- so both are pinned here.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = REPO_ROOT / "Dockerfile"
RELEASE_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "release.yml"


def test_dockerfile_marks_the_image_as_a_container_install() -> None:
    # `backend/install_kind.py` reads this variable. /.dockerenv is only a
    # fallback: it does not exist under Podman and other runtimes.
    assert re.search(r"^\s*ALBIS_IN_DOCKER=1\s*\\?\s*$", DOCKERFILE.read_text(), re.MULTILINE)


def _publication_stage_pattern() -> str:
    """The tag test the release workflow uses to choose prerelease or latest."""
    match = re.search(r"grep -Eq -- '([^']+)'", RELEASE_WORKFLOW.read_text())
    assert match, "Release workflow no longer classifies tags before publishing"
    return match.group(1)


def test_release_workflow_publishes_candidates_as_prereleases() -> None:
    # GitHub does not infer prerelease status from a semver tag. Publishing
    # v1.0.0-rc1 without the flag makes it the repository's "latest" release,
    # which is exactly what /releases/latest -- and therefore every stable
    # install's update check -- would then be told about.
    pattern = _publication_stage_pattern()

    for tag in ("v1.0.0-rc1", "v1.0.0-rc.2", "v1.0.0-beta.1", "v1.0.0-alpha", "v2.0.0-preview"):
        assert re.search(pattern, tag), f"{tag} would be published as a stable release"

    for tag in ("v1.0.0", "v0.19.0", "v1.2.3"):
        assert not re.search(pattern, tag), f"{tag} would be published as a pre-release"


def test_release_workflow_passes_the_stage_to_both_create_and_edit() -> None:
    # A re-run of a release job takes the `gh release edit` path, and a stage
    # applied only on create would be silently dropped there.
    workflow = RELEASE_WORKFLOW.read_text()
    assert workflow.count('"${release_stage[@]}"') >= 4
    assert "release_stage=(--prerelease)" in workflow
    assert "release_stage=(--latest)" in workflow


def test_appimage_architecture_token_matches_the_name_the_build_produces() -> None:
    """The AppImage filename uses the kernel's spelling, not the release one.

    `appimagetool` requires it, so `scripts/version_info.py` maps the target
    architecture when it builds the filename, and the update check has to do the
    same mapping to find that file. Two copies of one mapping is exactly the
    thing that drifts, so they are compared here rather than trusted.
    """
    spec = importlib.util.spec_from_file_location(
        "albis_version_info", REPO_ROOT / "scripts" / "version_info.py"
    )
    assert spec and spec.loader
    version_info = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(version_info)

    from backend.services.update_check import _APPIMAGE_ARCH_TOKENS

    for target_arch, token in _APPIMAGE_ARCH_TOKENS.items():
        assert version_info._appimage_arch_for_target_arch(target_arch) == token


def test_packaging_bundles_the_signing_key_when_one_is_present() -> None:
    """The signature check needs the key inside the bundle, not in the repo.

    A packaged build has no checkout to read from, so `ALBIS.spec` has to carry
    the key in the same way it carries `VERSION` and `BUILD_COMMIT`. Without
    this the signature state would silently read `unavailable` on exactly the
    builds it was added for.
    """
    from backend.services.update_download import SIGNING_KEY_NAME

    spec = (REPO_ROOT / "ALBIS.spec").read_text()
    assert SIGNING_KEY_NAME in spec, "ALBIS.spec does not bundle the release signing key"
    # Conditional, like BUILD_COMMIT: a build without a key must still package.
    assert f'os.path.exists(os.path.abspath("{SIGNING_KEY_NAME}"))' in spec


def test_the_signing_key_is_looked_for_beside_version() -> None:
    """Where `bundled_signing_key` searches has to match where the spec puts it.

    The spec copies the key to the bundle root, which is `sys._MEIPASS` at
    runtime and the repository root in a checkout.
    """
    from backend.services.update_download import SIGNING_KEY_NAME, bundled_signing_key

    found = bundled_signing_key()
    if found is None:
        # No key in this checkout, which is the current state: the feature
        # reports the signature as unavailable rather than failing.
        assert not (REPO_ROOT / SIGNING_KEY_NAME).exists()
    else:
        assert found == REPO_ROOT / SIGNING_KEY_NAME


def test_the_version_override_only_moves_the_comparison(monkeypatch) -> None:
    """The override must not become a way to misreport the running build.

    Its whole purpose is to make a flow testable that is otherwise unreachable
    before it ships, so it is deliberately scoped to the one comparison. The
    About dialog, the footer, export provenance and bug reports all read
    `ALBIS_VERSION`, which it does not touch.
    """
    import logging

    from backend.services.update_check import (
        UPDATE_CHECK_VERSION_ENV,
        ReleaseCheckService,
    )
    from backend.version import ALBIS_VERSION, read_version

    logger = logging.getLogger("test")
    monkeypatch.setenv(UPDATE_CHECK_VERSION_ENV, "0.1.0")

    service = ReleaseCheckService(current_version=ALBIS_VERSION, logger=logger)
    assert service.current_version == "0.1.0"
    # The build's own identity is untouched.
    assert read_version() == ALBIS_VERSION != "0.1.0"


def test_an_unparseable_version_override_is_ignored_not_obeyed(monkeypatch) -> None:
    # A typo in the variable must not turn the update check off.
    import logging

    from backend.services.update_check import (
        UPDATE_CHECK_VERSION_ENV,
        ReleaseCheckService,
    )

    monkeypatch.setenv(UPDATE_CHECK_VERSION_ENV, "not-a-version")
    service = ReleaseCheckService(current_version="1.2.3", logger=logging.getLogger("test"))
    assert service.current_version == "1.2.3"


def test_the_install_kind_override_accepts_only_published_kinds(monkeypatch) -> None:
    from backend.install_kind import (
        INSTALL_KIND_ENV_VAR,
        INSTALL_KINDS,
        install_kind_override,
        read_install_kind,
    )

    for kind in INSTALL_KINDS:
        monkeypatch.setenv(INSTALL_KIND_ENV_VAR, kind.upper())
        assert install_kind_override() == kind
        assert read_install_kind() == kind

    # An unrecognised value would otherwise offer the user an asset for a
    # platform they are not on.
    for hostile in ("windows", "linux", "", "   ", "appimage; rm -rf /"):
        monkeypatch.setenv(INSTALL_KIND_ENV_VAR, hostile)
        assert install_kind_override() == ""


def test_both_testing_overrides_are_documented_where_a_developer_looks() -> None:
    from backend.install_kind import INSTALL_KIND_ENV_VAR
    from backend.services.update_check import UPDATE_CHECK_VERSION_ENV

    guide = (REPO_ROOT / "docs" / "DEVELOPER_GUIDE.md").read_text(encoding="utf-8")
    for name in (UPDATE_CHECK_VERSION_ENV, INSTALL_KIND_ENV_VAR):
        assert name in guide, f"{name} is not documented in the Developer Guide"


def test_the_committed_signing_key_is_a_public_key_and_only_that() -> None:
    """A private key committed here would be a disclosure, not a bug.

    The export is a one-line command a keystroke away from
    `--export-secret-keys`, and the result looks similar enough at a glance.
    This refuses to let that reach a commit.
    """
    from backend.services.update_download import SIGNING_KEY_NAME

    key = (REPO_ROOT / SIGNING_KEY_NAME).read_text(encoding="utf-8")
    assert key.startswith("-----BEGIN PGP PUBLIC KEY BLOCK-----")
    assert "PRIVATE KEY BLOCK" not in key


def test_the_signing_key_fingerprint_is_documented_where_users_verify() -> None:
    """A key nobody can identify is not much of a trust anchor.

    Someone checking a download by hand needs to know which key to expect, so
    the fingerprint is stated where they will look rather than only living in
    the file.
    """
    fingerprint_start = "F96C C112 D6B4 9230"
    for name in ("README.md", "docs/RELEASE_CHECKLIST.md"):
        text = (REPO_ROOT / name).read_text(encoding="utf-8")
        assert fingerprint_start in text, f"{name} does not state the signing key fingerprint"
