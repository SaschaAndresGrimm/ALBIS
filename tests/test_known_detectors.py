"""Known detectors: the shared list under the SIMPLON address fields."""

from __future__ import annotations

import copy
import json
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend.app import app, runtime_state
from backend.services import known_detectors as kd
from backend.services.known_detectors import KnownDetectorStore

P4 = {"address": "192.168.20.191", "model": "PILATUS4 2M", "serial": "P4-0042"}
E2 = {"address": "192.168.20.88", "model": "EIGER2 X 9M", "serial": "E-08-0123"}


# ---------- the rules ----------


def test_records_the_address_with_what_answered_there() -> None:
    assert kd.record([], **P4, now=10) == [
        {
            "address": "http://192.168.20.191",
            "name": "",
            "model": "PILATUS4 2M",
            "serial": "P4-0042",
            "kept": False,
            "lastUsed": 10,
        }
    ]


def test_one_entry_per_address_however_it_was_typed() -> None:
    entries = kd.record([], "192.168.20.191", now=1)
    entries = kd.record(entries, "http://192.168.20.191/", now=2)
    assert [(e["address"], e["lastUsed"]) for e in entries] == [("http://192.168.20.191", 2)]


def test_an_address_only_test_keeps_model_and_serial() -> None:
    entries = kd.record(kd.record([], **P4, now=1), P4["address"], now=2)
    assert entries[0]["model"] == "PILATUS4 2M" and entries[0]["serial"] == "P4-0042"


def test_a_detector_moving_takes_its_name_and_star_along() -> None:
    entries = kd.update(kd.record([], **P4, now=1), P4["address"], {"name": "Lab 2", "kept": True})
    entries = kd.record(entries, "10.0.0.5", P4["model"], P4["serial"], now=2)
    assert len(entries) == 1
    assert entries[0]["address"] == "http://10.0.0.5"
    assert entries[0]["name"] == "Lab 2" and entries[0]["kept"] is True


def test_another_detector_at_a_known_address_loses_the_old_name() -> None:
    entries = kd.update(kd.record([], **P4, now=1), P4["address"], {"name": "Lab 2", "kept": True})
    entries = kd.record(entries, P4["address"], E2["model"], E2["serial"], now=2)
    assert entries[0]["name"] == "" and entries[0]["model"] == "EIGER2 X 9M"
    assert entries[0]["kept"] is True


def test_keeps_every_kept_detector_and_the_last_few_others() -> None:
    entries = kd.update(kd.record([], **P4, now=0), P4["address"], {"kept": True})
    for i in range(1, kd.MAX_RECENT + 4):
        entries = kd.record(entries, f"10.0.0.{i}", now=i)
    assert sum(e["kept"] for e in entries) == 1
    assert sum(not e["kept"] for e in entries) == kd.MAX_RECENT
    assert "http://10.0.0.1" not in {e["address"] for e in entries}


def test_drops_what_is_not_a_detector_entry() -> None:
    raw = [
        None,
        "http://a",
        {"address": ""},
        {"address": "ftp://x"},
        {"address": "det"},
        {"address": "det"},
    ]
    assert [e["address"] for e in kd.sanitize(raw)] == ["http://det"]
    assert kd.sanitize("nope") == []
    long = kd.sanitize([{"address": "det", "name": "x" * 500, "lastUsed": True}])[0]
    assert len(long["name"]) == kd.NAME_LIMIT and long["lastUsed"] == 0


def test_merge_keeps_names_and_stars_and_their_order() -> None:
    incoming = [
        {"address": "http://old", "lastUsed": 1},
        {"address": "http://new", "name": "Bench", "kept": True, "lastUsed": 5},
    ]
    entries = kd.merge(kd.record([], **P4, now=3), incoming)
    by_address = {e["address"]: e for e in entries}
    assert by_address["http://new"]["name"] == "Bench" and by_address["http://new"]["kept"]
    assert [e["address"] for e in entries if not e["kept"]] == [
        "http://192.168.20.191",
        "http://old",
    ]


def test_path_setting() -> None:
    assert kd.resolve_path("") == Path.home() / ".config" / "albis" / "known_detectors.json"
    assert kd.resolve_path("lab.json", Path("/etc/albis/albis.config.json")) == Path(
        "/etc/albis/lab.json"
    )
    assert kd.resolve_path("~/x.json") == Path.home() / "x.json"


# ---------- the file ----------


def test_store_writes_only_when_something_changed(tmp_path: Path) -> None:
    path = tmp_path / "sub" / "known.json"
    store = KnownDetectorStore(path)
    assert store.list() == []
    assert not path.exists()
    store.change(lambda entries: kd.record(entries, **P4))
    assert json.loads(path.read_text())[0]["model"] == "PILATUS4 2M"
    mtime = path.stat().st_mtime_ns
    store.change(lambda entries: kd.update(entries, "unknown.host", {"name": "x"}))
    assert path.stat().st_mtime_ns == mtime
    assert not list(path.parent.glob(".*.tmp"))


def test_a_damaged_file_reads_as_empty(tmp_path: Path) -> None:
    path = tmp_path / "known.json"
    path.write_text("{not json")
    assert KnownDetectorStore(path).list() == []


def test_picks_up_changes_made_to_the_file_elsewhere(tmp_path: Path) -> None:
    path = tmp_path / "known.json"
    one, other = KnownDetectorStore(path), KnownDetectorStore(path)
    one.change(lambda entries: kd.record(entries, **P4))
    other.change(lambda entries: kd.record(entries, **E2))
    assert {e["model"] for e in one.list()} == {"PILATUS4 2M", "EIGER2 X 9M"}


def test_simultaneous_changes_all_land(tmp_path: Path) -> None:
    store = KnownDetectorStore(tmp_path / "known.json")
    store.change(lambda entries: kd.record(entries, **P4))
    threads = [
        threading.Thread(
            target=lambda i=i: store.change(lambda entries: kd.record(entries, f"10.0.1.{i}"))
        )
        for i in range(6)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    # Every thread's detector is there: none lost to another's write.
    assert len(store.list()) == 7


# ---------- the routes ----------


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    config = copy.deepcopy(runtime_state.config)
    config.setdefault("ui", {})["known_detectors_file"] = str(tmp_path / "known.json")
    monkeypatch.setattr(runtime_state, "config", config)
    return TestClient(app, base_url="http://127.0.0.1")


def test_routes_answer_with_the_whole_list(client: TestClient, tmp_path: Path) -> None:
    assert client.get("/api/known-detectors").json() == {"detectors": []}
    seen = client.post("/api/known-detectors/seen", json=P4).json()["detectors"]
    assert seen[0]["address"] == "http://192.168.20.191"
    kept = client.patch(
        "/api/known-detectors", json={"address": P4["address"], "kept": True, "name": "Lab 2"}
    ).json()["detectors"]
    assert kept[0]["kept"] is True and kept[0]["name"] == "Lab 2"
    # An omitted field stays as it is.
    renamed = client.patch("/api/known-detectors", json={"address": P4["address"], "name": "P4"})
    assert renamed.json()["detectors"][0]["kept"] is True
    assert json.loads((tmp_path / "known.json").read_text())[0]["name"] == "P4"
    gone = client.delete("/api/known-detectors", params={"address": "192.168.20.191"})
    assert gone.json() == {"detectors": []}


def test_import_merges_a_browser_list(client: TestClient) -> None:
    client.post("/api/known-detectors/seen", json=E2)
    merged = client.post(
        "/api/known-detectors/import",
        json={
            "detectors": [
                {"address": "http://192.168.20.191", "name": "Lab 2", "kept": True, "lastUsed": 1}
            ]
        },
    ).json()["detectors"]
    assert {e["address"] for e in merged} == {"http://192.168.20.191", "http://192.168.20.88"}


def test_rejects_what_is_not_a_detector_address(client: TestClient) -> None:
    assert client.post("/api/known-detectors/seen", json={"address": "ftp://x"}).status_code == 400
    assert client.post("/api/known-detectors/seen", json={"address": ""}).status_code == 422
    assert client.post("/api/known-detectors/seen", json={**P4, "extra": 1}).status_code == 422


def test_a_list_that_cannot_be_saved_is_an_error_naming_the_file(
    client: TestClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    blocker = tmp_path / "blocker"
    blocker.write_text("a file, not a folder")
    runtime_state.config["ui"]["known_detectors_file"] = str(blocker / "known.json")
    answer = client.post("/api/known-detectors/seen", json=P4)
    assert answer.status_code == 500
    assert "blocker" in answer.json()["detail"]


def test_changes_from_another_site_are_refused(client: TestClient) -> None:
    answer = client.post(
        "/api/known-detectors/seen", json=P4, headers={"Origin": "https://evil.example"}
    )
    assert answer.status_code == 403
