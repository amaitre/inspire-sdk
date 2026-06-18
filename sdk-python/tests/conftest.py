"""Pytest fixtures: spawn a real Mosquitto on a random port for each test
session, so wire-format and retain/LWT semantics get the same broker
implementation as production.

Mosquitto is a Phase 5 prereq (spec migration guide §1) so it is acceptable
to require it in PATH for tests.
"""

from __future__ import annotations

import os
import socket
import subprocess
import tempfile
import time
from collections.abc import Iterator

import pytest


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _wait_for_port(host: str, port: int, timeout_s: float = 3.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"mosquitto did not open {host}:{port} within {timeout_s}s")


@pytest.fixture
def broker_port() -> Iterator[int]:
    """Spawn a fresh Mosquitto on a random port, yield the port, terminate.

    Each test gets its own broker so retained messages from one test don't
    leak into the next.
    """
    port = _free_port()
    with tempfile.NamedTemporaryFile("w", suffix=".conf", delete=False) as cfg:
        cfg.write(f"listener {port} 127.0.0.1\n")
        cfg.write("allow_anonymous true\n")
        cfg.write("persistence false\n")
        cfg_path = cfg.name

    proc = subprocess.Popen(
        ["mosquitto", "-c", cfg_path],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_port("127.0.0.1", port)
        yield port
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=2.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        os.unlink(cfg_path)
