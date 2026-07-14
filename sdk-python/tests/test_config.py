"""Config-resolution parity tests. Mirrors sdk-node test/config.spec.ts
(unit: walk-up discovery, parsing, graceful failure, opts > env > file >
defaults precedence) and test/config-integration.spec.ts (end-to-end:
Inspire.start() with no broker= reads .inspire/config.toml / env).
"""

from __future__ import annotations

import threading
import time

import paho.mqtt.client as mqtt
import pytest

from inspire_sdk import Inspire
from inspire_sdk._config import (
    find_inspire_config_path,
    load_inspire_config,
    resolve_broker,
)

DEFAULTS = {"host": "127.0.0.1", "port": 1883}
ENV_KEYS = ["INSPIRE_BROKER_HOST", "INSPIRE_BROKER_PORT", "BROKER_HOST", "BROKER_PORT"]


@pytest.fixture(autouse=True)
def _clean_broker_env(monkeypatch: pytest.MonkeyPatch):
    """These tests manipulate broker env vars; start each from a clean slate."""
    for key in ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def _write_config(directory, contents: str) -> None:
    inspire_dir = directory / ".inspire"
    inspire_dir.mkdir(parents=True, exist_ok=True)
    (inspire_dir / "config.toml").write_text(contents)


class TestFindInspireConfigPath:
    def test_finds_config_in_start_dir(self, tmp_path):
        _write_config(tmp_path, '[broker]\nhost = "h"\n')
        assert find_inspire_config_path(str(tmp_path)) == str(
            tmp_path / ".inspire" / "config.toml"
        )

    def test_walks_up_parent_directories(self, tmp_path):
        _write_config(tmp_path, '[broker]\nhost = "h"\n')
        nested = tmp_path / "a" / "b" / "c"
        nested.mkdir(parents=True)
        assert find_inspire_config_path(str(nested)) == str(
            tmp_path / ".inspire" / "config.toml"
        )

    def test_returns_none_when_no_config_up_to_root(self, tmp_path):
        assert find_inspire_config_path(str(tmp_path)) is None


class TestLoadInspireConfig:
    def test_parses_canonical_broker_and_reporting_format(self, tmp_path):
        _write_config(
            tmp_path,
            'schema_version = 1\n[broker]\nhost = "192.168.1.10"\nport = 1884\n'
            "[reporting]\nheartbeat_interval_s = 10\n",
        )
        cfg = load_inspire_config(str(tmp_path))
        assert cfg["broker"] == {"host": "192.168.1.10", "port": 1884}
        assert cfg["reporting"]["heartbeat_interval_ms"] == 10_000

    def test_tolerates_partial_broker_table_host_only(self, tmp_path):
        _write_config(tmp_path, '[broker]\nhost = "only-host"\n')
        assert load_inspire_config(str(tmp_path))["broker"] == {
            "host": "only-host",
            "port": None,
        }

    def test_returns_empty_when_no_config_file(self, tmp_path):
        assert load_inspire_config(str(tmp_path)) == {}

    def test_returns_empty_on_malformed_toml_instead_of_raising(self, tmp_path):
        _write_config(tmp_path, "[broker\nhost = busted")
        assert load_inspire_config(str(tmp_path)) == {}


class TestResolveBrokerPrecedence:
    """opts > env > file > defaults — mirrors node config.spec.ts."""

    def test_uses_defaults_when_nothing_set(self):
        assert resolve_broker(None, {}, DEFAULTS) == DEFAULTS

    def test_file_config_beats_defaults(self):
        file_cfg = {"broker": {"host": "file-host", "port": 2000}}
        assert resolve_broker(None, file_cfg, DEFAULTS) == {
            "host": "file-host",
            "port": 2000,
        }

    def test_env_beats_file_config(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("INSPIRE_BROKER_HOST", "env-host")
        monkeypatch.setenv("INSPIRE_BROKER_PORT", "3000")
        file_cfg = {"broker": {"host": "file-host", "port": 2000}}
        assert resolve_broker(None, file_cfg, DEFAULTS) == {
            "host": "env-host",
            "port": 3000,
        }

    def test_inspire_env_beats_bare_broker_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("INSPIRE_BROKER_HOST", "canonical-host")
        monkeypatch.setenv("INSPIRE_BROKER_PORT", "3001")
        monkeypatch.setenv("BROKER_HOST", "bare-host")
        monkeypatch.setenv("BROKER_PORT", "3002")
        assert resolve_broker(None, {}, DEFAULTS) == {
            "host": "canonical-host",
            "port": 3001,
        }

    def test_bare_broker_env_names_also_work(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("BROKER_HOST", "bare-host")
        monkeypatch.setenv("BROKER_PORT", "3100")
        assert resolve_broker(None, {}, DEFAULTS) == {"host": "bare-host", "port": 3100}

    def test_explicit_opts_beat_everything(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("INSPIRE_BROKER_HOST", "env-host")
        file_cfg = {"broker": {"host": "file-host", "port": 2000}}
        assert resolve_broker(
            {"host": "opt-host", "port": 9999}, file_cfg, DEFAULTS
        ) == {"host": "opt-host", "port": 9999}

    def test_ignores_invalid_env_port_and_falls_through(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setenv("INSPIRE_BROKER_PORT", "not-a-number")
        file_cfg = {"broker": {"port": 2500}}
        assert resolve_broker(None, file_cfg, DEFAULTS)["port"] == 2500


def _await_presence(port: int, timeout_s: float = 3.0) -> bool:
    """Subscribe inspire/presence/+/+ on the given broker; True if a retained
    presence message (non-empty payload) shows up within the timeout."""
    saw = threading.Event()
    sub = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)

    def on_connect(c, _u, _f, _rc, _p):
        c.subscribe("inspire/presence/+/+", qos=1)

    def on_message(_c, _u, msg):
        if len(msg.payload) > 0:
            saw.set()

    sub.on_connect = on_connect
    sub.on_message = on_message
    sub.connect("127.0.0.1", port, keepalive=10)
    sub.loop_start()
    try:
        return saw.wait(timeout=timeout_s)
    finally:
        sub.loop_stop()
        sub.disconnect()


class TestStartBrokerResolutionIntegration:
    """Mirrors node config-integration.spec.ts: Inspire.start() with NO
    broker= must resolve host/port from env / .inspire/config.toml. The
    broker sits on a RANDOM port, so presence landing there is the only
    honest proof the chain was consulted (a real broker may own 1883)."""

    def test_start_reads_broker_from_config_toml_when_no_broker_arg(
        self, broker_port: int, tmp_path, monkeypatch: pytest.MonkeyPatch
    ):
        _write_config(
            tmp_path,
            f'schema_version = 1\n[broker]\nhost = "127.0.0.1"\nport = {broker_port}\n',
        )
        monkeypatch.chdir(tmp_path)
        client = Inspire.start(
            slug="cfg-int-app",
            version="0.1.0",
            node_id="host-1",
            heartbeat_interval_s=60.0,
        )
        try:
            assert _await_presence(broker_port), (
                "presence never landed on the config.toml-specified broker"
            )
        finally:
            client.stop()

    def test_start_honors_inspire_broker_env_over_config_toml(
        self, broker_port: int, tmp_path, monkeypatch: pytest.MonkeyPatch
    ):
        # config.toml points at a dead port; env points at the live broker.
        _write_config(
            tmp_path,
            'schema_version = 1\n[broker]\nhost = "127.0.0.1"\nport = 1\n',
        )
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("INSPIRE_BROKER_HOST", "127.0.0.1")
        monkeypatch.setenv("INSPIRE_BROKER_PORT", str(broker_port))
        client = Inspire.start(
            slug="cfg-env-app",
            version="0.1.0",
            node_id="host-1",
            heartbeat_interval_s=60.0,
        )
        try:
            assert _await_presence(broker_port), (
                "presence never landed on the env-specified broker"
            )
        finally:
            client.stop()
