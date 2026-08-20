"""Caching policy for statically served frontend assets.

Entry documents must never be stored, so an upgraded backend is never paired with
a stale shell. Everything else should be storable-but-revalidated, which is what
turns a reload from a full ~1.3 MB refetch into a series of empty 304s.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from backend.app import app

# Loopback keeps gzip out of the way; these tests are about cache headers only.
LOCAL_CLIENT = ("127.0.0.1", 5555)


def _client() -> TestClient:
    return TestClient(app, client=LOCAL_CLIENT)


def test_entry_documents_are_never_stored() -> None:
    client = _client()

    assert client.get("/").headers["cache-control"] == "no-store"
    assert client.get("/docs.html").headers["cache-control"] == "no-store"


def test_module_assets_are_revalidated_rather_than_uncacheable() -> None:
    client = _client()

    for path in ("/app.js", "/modules/file_browser.js", "/style.css", "/locales/en.json"):
        response = client.get(path)
        assert response.status_code == 200, path
        assert response.headers["cache-control"] == "no-cache", path
        # Revalidation is only useful if StaticFiles gives the browser a validator.
        assert response.headers.get("etag"), path


def test_unchanged_asset_revalidates_to_an_empty_304() -> None:
    client = _client()

    first = client.get("/modules/file_browser.js")
    assert first.status_code == 200
    assert len(first.content) > 0

    second = client.get(
        "/modules/file_browser.js",
        headers={"If-None-Match": first.headers["etag"]},
    )

    assert second.status_code == 304
    assert second.content == b""


def test_api_responses_keep_their_own_cache_semantics() -> None:
    """The static policy must not reach into /api/ or /assets/."""
    client = _client()

    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.headers.get("cache-control") is None
