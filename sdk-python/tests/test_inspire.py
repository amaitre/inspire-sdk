"""Integration tests: spin up a Mosquitto broker on a random port, start
the SDK, snoop the bus with a raw paho subscriber, assert wire format /
heartbeat cadence / graceful-stop behavior.

Mirrors sdk-node/test/inspire.spec.ts test-by-test so both libraries are
verified against the same scenarios.

Heartbeat interval is dialed down to 0.15s in tests via the test-only
override so we don't sit here for 10 seconds per case. Production default
of 10s is verified by reading the constants in _client.py.
"""

from __future__ import annotations

import json
import threading
import time

import paho.mqtt.client as mqtt
import pytest

from inspire_sdk import Inspire


def _raw_subscriber(
    port: int,
    topic_filter: str,
    qos: int = 1,
) -> tuple[mqtt.Client, list[mqtt.MQTTMessage], threading.Event]:
    """Connect a raw paho subscriber, subscribe, accumulate messages.

    Returns the client (so the test can disconnect it), the message list
    (lives mutated by the on_message callback), and an Event that fires on
    each new message so the test can wait_for / repeat as needed.
    """
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    received: list[mqtt.MQTTMessage] = []
    arrived = threading.Event()
    subscribed = threading.Event()

    def on_connect(c, _u, _f, _rc, _p):
        c.subscribe(topic_filter, qos=qos)

    def on_subscribe(_c, _u, _mid, _granted, _props):
        subscribed.set()

    def on_message(_c, _u, msg):
        received.append(msg)
        arrived.set()

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.connect("127.0.0.1", port, keepalive=10)
    client.loop_start()
    if not subscribed.wait(timeout=2.0):
        client.loop_stop()
        client.disconnect()
        raise TimeoutError(f"raw subscriber did not subscribe to {topic_filter}")
    return client, received, arrived


class TestInspireSDK:
    """Mirrors sdk-node `describe('Inspire SDK', ...)`."""

    def test_publishes_retained_presence_after_start(self, broker_port: int):
        client = Inspire.start(
            slug="mock-app",
            version="0.1.0",
            broker={"host": "127.0.0.1", "port": broker_port},
            node_id="host-1",
            heartbeat_interval_s=60.0,  # suppress heartbeats for this test
        )
        try:
            sub, received, arrived = _raw_subscriber(
                broker_port, "inspire/presence/mock-app/host-1"
            )
            try:
                assert arrived.wait(timeout=2.0), "no retained presence within 2s"
                assert received[0].topic == "inspire/presence/mock-app/host-1"
                parsed = json.loads(received[0].payload.decode("utf-8"))
                assert parsed["v"] == 1
                assert parsed["app_slug"] == "mock-app"
                assert parsed["node_id"] == "host-1"
                assert parsed["version"] == "0.1.0"
                assert parsed["service_mode"] is False
                assert isinstance(parsed["pid"], int)
            finally:
                sub.loop_stop()
                sub.disconnect()
        finally:
            client.stop()

    def test_heartbeat_fires_at_configured_interval(self, broker_port: int):
        client = Inspire.start(
            slug="mock-app",
            version="0.1.0",
            broker={"host": "127.0.0.1", "port": broker_port},
            node_id="host-1",
            heartbeat_interval_s=0.15,
        )
        try:
            sub, received, _arrived = _raw_subscriber(
                broker_port, "inspire/heartbeat/mock-app/host-1", qos=0
            )
            try:
                deadline = time.monotonic() + 1.5
                while time.monotonic() < deadline and len(received) < 2:
                    time.sleep(0.05)
                assert len(received) >= 2, (
                    f"only saw {len(received)} heartbeats in 1.5s"
                )
                parsed = json.loads(received[0].payload.decode("utf-8"))
                assert parsed["v"] == 1
                assert isinstance(parsed["uptime_s"], int)
                assert isinstance(parsed["rss_mb"], int)
            finally:
                sub.loop_stop()
                sub.disconnect()
        finally:
            client.stop()

    def test_set_status_publishes_retained_status_msg(self, broker_port: int):
        client = Inspire.start(
            slug="mock-app",
            version="0.1.0",
            broker={"host": "127.0.0.1", "port": broker_port},
            node_id="host-1",
            heartbeat_interval_s=60.0,
        )
        try:
            client.set_status("ready", "all systems online")
            sub, received, arrived = _raw_subscriber(
                broker_port, "inspire/status/mock-app/host-1"
            )
            try:
                assert arrived.wait(timeout=2.0), "no status received in 2s"
                parsed = json.loads(received[0].payload.decode("utf-8"))
                assert parsed["state"] == "ready"
                assert parsed["detail"] == "all systems online"
            finally:
                sub.loop_stop()
                sub.disconnect()
        finally:
            client.stop()

    def test_stop_clears_retained_presence(self, broker_port: int):
        client = Inspire.start(
            slug="mock-app",
            version="0.1.0",
            broker={"host": "127.0.0.1", "port": broker_port},
            node_id="host-1",
            heartbeat_interval_s=60.0,
        )

        # Subscribe BEFORE stop so we receive both the original retained
        # presence (on subscribe) and the cleared one (after stop publishes
        # empty retained).
        sub, received, _arrived = _raw_subscriber(
            broker_port, "inspire/presence/mock-app/host-1"
        )
        try:
            client.stop()
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if any(len(m.payload) == 0 for m in received):
                    break
                time.sleep(0.05)
            assert any(len(m.payload) == 0 for m in received), (
                f"never saw empty (cleared) payload; "
                f"received {[len(m.payload) for m in received]}"
            )
        finally:
            sub.loop_stop()
            sub.disconnect()

    def test_ac4_wire_path_under_2s(self, broker_port: int):
        """Spec §10 AC 4: SDK presence appears within 2s on fresh subscribe."""
        started_at = time.monotonic()
        client = Inspire.start(
            slug="mock-app",
            version="0.1.0",
            broker={"host": "127.0.0.1", "port": broker_port},
            node_id="host-1",
            heartbeat_interval_s=60.0,
        )
        try:
            sub, _received, arrived = _raw_subscriber(
                broker_port, "inspire/presence/+/+"
            )
            try:
                assert arrived.wait(timeout=2.0), "AC 4 wire path > 2s"
                elapsed = time.monotonic() - started_at
                assert elapsed < 2.0
            finally:
                sub.loop_stop()
                sub.disconnect()
        finally:
            client.stop()
