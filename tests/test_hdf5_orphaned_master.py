"""A master file whose data files are missing must say so, not melt.

An ARINA burn-in master landed in a colleague's Downloads folder and took ALBIS
down with it. The file was not corrupt: it was a NeXus master written one
companion file per frame, `nimages=1000 x ntrigger=5000`, so `/entry/data` held
5,000,000 external links -- and not one companion file had been downloaded
alongside it. HDFView died on the same file, which is what made it look like
corruption rather than an incomplete download.

Three separate costs, all measured on that file:

  * Iterating the group by name made libhdf5 order the whole link table first.
    The *first* name took 122 s; every name after it was free.
  * `/api/datasets` then spent ~213 s in `resolve_external_path`, which stats
    the filesystem once per link. Six minutes, to report nothing found.
  * `/api/hdf5/tree` modelled every child: 5M dicts plus 5M Pydantic models,
    7.2 GiB, then an 800 MB response. That is the crash.

And the diagnosis the user got was a log line per link -- five million WARNING
lines -- ending in a viewer that offered the master's `flatfield`, because a
master carries 2D calibration arrays even when it has no frames.

After the fix the same file answers in ~2 s with a 422 that names the missing
companion, and the tree lists 10,000 of 5,000,000 with the count stated.
"""

from __future__ import annotations

import logging
from pathlib import Path
from unittest import mock

import numpy as np
import pytest
from fastapi.testclient import TestClient

h5py = pytest.importorskip("h5py")

from backend.app import app  # noqa: E402
from backend.services.hdf5_stack import (  # noqa: E402
    GROUP_CHILD_LIMIT,
    HDF5StackService,
    WalkReport,
)

DATA_PATH = "/entry/data/data"


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _service(data_dir: Path) -> HDF5StackService:
    def is_within(target: Path, root: Path) -> bool:
        try:
            target.resolve().relative_to(root.resolve())
        except Exception:
            return False
        return True

    return HDF5StackService(
        data_dir=data_dir,
        get_allow_abs_paths=lambda: True,
        is_within=is_within,
        get_h5py=lambda: h5py,
    )


def _write_companion(path: Path, frames: int = 2) -> None:
    with h5py.File(path, "w") as h5:
        h5.create_dataset(DATA_PATH, data=np.zeros((frames, 2, 3), dtype=np.uint16))


def _write_master(path: Path, links: int, *, present: int = 0) -> None:
    """A master linking `links` companions, of which the first `present` exist."""
    for index in range(1, present + 1):
        _write_companion(path.parent / f"{path.stem}_data_{index:06d}.h5")
    with h5py.File(path, "w") as h5:
        group = h5.require_group("/entry/data")
        for index in range(1, links + 1):
            group[f"data_{index:06d}"] = h5py.ExternalLink(
                f"{path.stem}_data_{index:06d}.h5", DATA_PATH
            )
        # Calibration arrays a real master carries. Both are 2D, so both read as
        # displayable images -- the reason "no image datasets" cannot stand in
        # for "the frames are missing".
        specific = h5.require_group("/entry/instrument/detector/detectorSpecific")
        specific.create_dataset("flatfield", data=np.ones((4, 4), dtype=np.float32))
        specific.create_dataset("pixel_mask", data=np.zeros((4, 4), dtype=np.uint32))


def test_group_child_names_caps_and_reports_the_real_size(tmp_path: Path) -> None:
    master = tmp_path / "capped_master.h5"
    _write_master(master, links=40)

    service = _service(tmp_path)
    with h5py.File(master, "r") as h5:
        names, total = service.group_child_names(h5["/entry/data"], limit=10)

    assert len(names) == 10, "the listing must stop at the cap"
    assert total == 40, "the caller still learns the group's real size"
    assert len(set(names)) == 10, "no name may be repeated"


def test_group_under_the_cap_is_listed_in_name_order(tmp_path: Path) -> None:
    """The ordinary case keeps name order: the fast path is only for the cap.

    Past the cap the sample is taken in the file's own heap order, which is
    arbitrary. That must not leak into normal files.
    """
    master = tmp_path / "small_master.h5"
    _write_master(master, links=12)

    service = _service(tmp_path)
    with h5py.File(master, "r") as h5:
        names, total = service.group_child_names(h5["/entry/data"], limit=100)

    assert total == 12
    assert names == sorted(names)


def test_orphaned_master_is_reported_as_missing_data_files(
    tmp_path: Path, client: TestClient
) -> None:
    master = tmp_path / "orphan_master.h5"
    _write_master(master, links=25)

    response = client.get("/api/datasets", params={"file": str(master)})

    assert response.status_code == 422
    detail = response.json()["detail"]
    # Structured so the interface can say this in the user's own language; the
    # counts live here in full even though the localized wording is short.
    assert detail["code"] == "master_data_missing"
    assert detail["group"] == "/entry/data"
    assert detail["count"] == 25
    assert detail["example"] == "orphan_master_data_000001.h5", "a file the user can look for"
    assert "same folder" in detail["message"], "the English fallback still says what to do"


def test_a_partly_downloaded_master_still_opens(tmp_path: Path, client: TestClient) -> None:
    """Some companions present is not a dead master: serve what arrived.

    This is why the check looks at whether the group produced anything rather
    than counting missing links. A series still transferring would otherwise go
    from "shows the frames you have" to "refuses to open".
    """
    master = tmp_path / "partial_master.h5"
    _write_master(master, links=25, present=3)

    response = client.get("/api/datasets", params={"file": str(master)})

    assert response.status_code == 200
    stacks = [d for d in response.json()["datasets"] if d.get("linked_stack")]
    assert stacks, "the three companions that exist must still form a stack"
    assert stacks[0]["shape"][0] == 6, "two frames from each of the three present files"


def test_a_master_with_all_its_data_is_untouched(tmp_path: Path, client: TestClient) -> None:
    master = tmp_path / "complete_master.h5"
    _write_master(master, links=4, present=4)

    response = client.get("/api/datasets", params={"file": str(master)})

    assert response.status_code == 200
    stacks = [d for d in response.json()["datasets"] if d.get("linked_stack")]
    assert stacks and stacks[0]["shape"][0] == 8


def test_tree_listing_is_capped_and_says_so(tmp_path: Path, client: TestClient) -> None:
    master = tmp_path / "tree_master.h5"
    _write_master(master, links=GROUP_CHILD_LIMIT + 25)

    response = client.get("/api/hdf5/tree", params={"file": str(master), "path": "/entry/data"})

    assert response.status_code == 200
    body = response.json()
    assert len(body["children"]) == GROUP_CHILD_LIMIT
    assert body["childCount"] == GROUP_CHILD_LIMIT + 25
    assert body["truncated"] is True


def test_tree_listing_of_an_ordinary_group_is_not_flagged(
    tmp_path: Path, client: TestClient
) -> None:
    master = tmp_path / "plain_master.h5"
    _write_master(master, links=3)

    response = client.get("/api/hdf5/tree", params={"file": str(master), "path": "/entry/data"})

    body = response.json()
    assert body["truncated"] is False
    assert body["childCount"] == 3
    assert len(body["children"]) == 3


def test_missing_links_are_logged_once_not_once_each(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """The log line per link is the denial of service, not just noise.

    Five million WARNING lines is what the user's log actually held, and writing
    them is unbounded work on the request thread as well as on disk.
    """
    master = tmp_path / "noisy_master.h5"
    _write_master(master, links=30)

    service = _service(tmp_path)
    report = WalkReport()
    results: list[dict] = []
    with caplog.at_level(logging.WARNING, logger="albis.hdf5_stack"):
        with h5py.File(master, "r") as h5:
            service.walk_datasets(h5["/"], "/", master, results, set(), {master: h5}, report)
        report.log_summary(master)

    assert report.missing_external_count == 30
    per_link = [r for r in caplog.records if "data_000007" in r.getMessage()]
    assert not per_link, "no link may get a line of its own"
    summary = [r for r in caplog.records if "external link(s)" in r.getMessage()]
    assert len(summary) == 1, f"expected one aggregated line, got {len(summary)}"
    assert "30" in summary[0].getMessage()


def test_a_dangling_link_read_back_as_none_is_not_taken_for_a_hard_link(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """`Group.get(getlink=True)` returning None must not fall through.

    h5py returns its default whenever `name not in group`, and on some libhdf5
    builds `__contains__` resolves the link -- so a dangling external link reads
    back as None. That used to reach the hard-link branch and raise there, which
    is the line the user's log was full of:

        Skipping node /entry/data/data_3597435 in ...:
        'Unable to synchronously open object (identifier is not of specified type)'

    The build in requirements returns a proper ExternalLink, so the condition is
    forced here rather than waited for.
    """
    master = tmp_path / "none_master.h5"
    _write_master(master, links=12)

    service = _service(tmp_path)
    report = WalkReport()
    results: list[dict] = []

    real_get = h5py.Group.get

    def get_returning_none_for_links(self, name, *args, **kwargs):
        if kwargs.get("getlink") and str(name).startswith("data_"):
            return None
        return real_get(self, name, *args, **kwargs)

    with (
        caplog.at_level(logging.WARNING, logger="albis.hdf5_stack"),
        mock.patch.object(h5py.Group, "get", get_returning_none_for_links),
        h5py.File(master, "r") as h5,
    ):
        service.walk_datasets(h5["/"], "/", master, results, set(), {master: h5}, report)

    assert report.missing_external_count == 12, "each link is still accounted for"
    assert not report.skipped, f"nothing may be misfiled as an unopenable node: {report.skipped}"
    assert report.dead_link_groups, "the group is still recognised as dead"
    # The filename survives the None, so the 422 can still name a file.
    assert any("none_master_data_" in name for name in report.missing_external)


def test_dangling_link_target_names_the_file_without_following_it(tmp_path: Path) -> None:
    master = tmp_path / "target_master.h5"
    _write_master(master, links=2)

    service = _service(tmp_path)
    with h5py.File(master, "r") as h5:
        group = h5["/entry/data"]
        assert service.dangling_link_target(group, "data_000001") == "target_master_data_000001.h5"
        # A real group, not an external link: nothing to report.
        assert service.dangling_link_target(h5["/entry"], "data") is None


def test_the_frame_path_of_a_partial_master_is_also_capped_and_quiet(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Choosing the dataset must not re-run the unbounded walk.

    `/api/datasets` is only the first request. Once the stack is selected,
    `resolve_group_linked_stack` enumerates the same group again to build the
    segment list -- so leaving a partly-downloaded master openable is only safe
    if this path is bounded too.
    """
    master = tmp_path / "frames_master.h5"
    _write_master(master, links=GROUP_CHILD_LIMIT + 40, present=2)

    service = _service(tmp_path)

    # Counted, because the cap is otherwise invisible from the outside: an
    # uncapped run returns the same stack, it just does 40 links more work. With
    # `links` set just past the cap, the count alone separates the two.
    attempts = 0
    real_resolve = service.resolve_external_path

    def counting_resolve(base_file, filename):
        nonlocal attempts
        attempts += 1
        return real_resolve(base_file, filename)

    service.resolve_external_path = counting_resolve  # type: ignore[method-assign]

    opened: list = []
    with caplog.at_level(logging.WARNING, logger="albis.hdf5_stack"):
        with h5py.File(master, "r") as h5:
            view = service.resolve_group_linked_stack(
                h5["/entry/data"], "/entry/data", master, opened
            )
        for handle in opened:
            handle.close()

    assert view is not None, "the two companions that exist still form a stack"
    assert view["shape"][0] == 4, "two frames from each of the two present files"
    assert len(view["segments"]) == 2
    assert (
        attempts <= GROUP_CHILD_LIMIT
    ), f"the frame path looked at {attempts} links, past the {GROUP_CHILD_LIMIT} cap"

    per_member = [r for r in caplog.records if "data_005000" in r.getMessage()]
    assert not per_member, "no member may get a log line of its own"
    assert (
        len(caplog.records) <= 4
    ), f"expected an aggregated summary, got {len(caplog.records)} lines"


def test_data_files_that_are_present_but_unreadable_are_not_called_missing(
    tmp_path: Path, client: TestClient
) -> None:
    """ "Copy the files into this folder" is the wrong advice for files already there.

    `resolve_external_path` has already confirmed the target exists and sits
    inside the data root by the time the open is attempted, so a failure there
    is never absence: it is no descriptors left, no permission, or a mount that
    dropped out. Reporting it as missing would send the user looking for data
    that is exactly where they put it.
    """
    master = tmp_path / "unreadable_master.h5"
    _write_master(master, links=6)
    # Present, right suffix, inside the root -- and not HDF5.
    for index in range(1, 7):
        (tmp_path / f"unreadable_master_data_{index:06d}.h5").write_bytes(b"not hdf5 at all")

    response = client.get("/api/datasets", params={"file": str(master)})

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail["code"] == "master_data_unreadable"
    assert detail["example"] == "unreadable_master_data_000001.h5"
    assert detail["reason"], "name what stopped the read"
    message = detail["message"]
    assert "will not open" in message
    assert "not next to this master" not in message, "the files are next to it"
    assert "Copy the linked data files" not in message, "copying them would change nothing"


def test_a_mix_of_absent_and_unreadable_files_reports_both(
    tmp_path: Path, client: TestClient
) -> None:
    master = tmp_path / "mixed_master.h5"
    _write_master(master, links=6)
    for index in (1, 2):
        (tmp_path / f"mixed_master_data_{index:06d}.h5").write_bytes(b"not hdf5 at all")

    response = client.get("/api/datasets", params={"file": str(master)})

    detail = response.json()["detail"]
    assert response.status_code == 422
    # The advice follows the four that are genuinely absent, and the two that
    # are present but unreadable are still counted rather than dropped.
    assert detail["code"] == "master_data_missing"
    assert detail["unreadable"] == 2
    assert "not next to this master" in detail["message"], "four files really are absent"
    assert "2 were present but would not open" in detail["message"]
