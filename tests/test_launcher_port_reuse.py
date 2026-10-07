"""With server.port 0, the launcher starts on the previous start's port again.

The browser keeps localStorage per origin, port included, so a new random port
on every start emptied Open Recent, the geometry override, the language and the
panel layout. These check that the last port is reused when it is free, and that
a port something else now holds is left alone.
"""

from __future__ import annotations

import socket

import pytest

import albis_launcher


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def test_a_free_last_port_is_bound_again() -> None:
    port = _free_port()
    sock = albis_launcher._bind_last_port("127.0.0.1", port)
    try:
        assert sock is not None
        assert sock.getsockname()[1] == port
    finally:
        if sock is not None:
            sock.close()


def test_a_port_another_program_listens_on_is_left_alone() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as other:
        other.bind(("127.0.0.1", 0))
        other.listen()
        port = int(other.getsockname()[1])
        assert albis_launcher._bind_last_port("127.0.0.1", port) is None
        # Also when ALBIS would listen on every interface: the probe goes to
        # loopback, where the other program answers.
        assert albis_launcher._bind_last_port("0.0.0.0", port) is None


@pytest.mark.parametrize("port", [0, -1, 70000])
def test_no_usable_last_port_means_none(port: int) -> None:
    assert albis_launcher._bind_last_port("127.0.0.1", port) is None


def test_auto_port_reuses_the_last_port_when_free() -> None:
    port = _free_port()
    sock, reused = albis_launcher._bind_auto_port("127.0.0.1", port)
    try:
        assert reused is True
        assert sock.getsockname()[1] == port
    finally:
        sock.close()


def test_auto_port_takes_a_free_port_when_the_last_is_taken() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as other:
        other.bind(("127.0.0.1", 0))
        other.listen()
        taken = int(other.getsockname()[1])
        sock, reused = albis_launcher._bind_auto_port("127.0.0.1", taken)
        try:
            assert reused is False
            assert sock.getsockname()[1] not in (0, taken)
        finally:
            sock.close()


def test_auto_port_without_a_last_start_takes_a_free_port() -> None:
    sock, reused = albis_launcher._bind_auto_port("127.0.0.1", 0)
    try:
        assert reused is False
        assert sock.getsockname()[1] > 0
    finally:
        sock.close()
