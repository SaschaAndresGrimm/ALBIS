from __future__ import annotations

import sys
from pathlib import Path

# Ensure tests can import local modules (e.g. backend.app) without editable install.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import fastapi.testclient  # noqa: E402
import starlette.testclient  # noqa: E402

_UpstreamTestClient = starlette.testclient.TestClient


class _LoopbackTestClient(_UpstreamTestClient):  # type: ignore[misc, valid-type]
    """A TestClient that addresses ALBIS the way a real local browser does.

    The default `base_url` is `http://testserver`, which reaches the app with a
    `Host` header no real deployment ever sends. That now matters: the request
    guard rejects a `Host` a loopback bind could not legitimately be addressed
    by, since that is what a DNS-rebinding request looks like.

    Rewriting the default to loopback makes every test exercise the same path as
    the desktop app instead of a fictional hostname, and keeps the guard active
    for the endpoints under test rather than switching it off wholesale.
    `test_request_guard.py` sets `Host` explicitly where the check itself is the
    subject.
    """

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("base_url", "http://127.0.0.1:8000")
        super().__init__(*args, **kwargs)


# Rebind on both modules: tests import from `fastapi.testclient`, which re-exports
# starlette's under its own name at import time.
starlette.testclient.TestClient = _LoopbackTestClient
fastapi.testclient.TestClient = _LoopbackTestClient
