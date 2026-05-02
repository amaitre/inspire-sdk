"""inspire-sdk client implementation. Mirrors sdk-node/src/index.ts.

Wire format per spec INSPIRE_ATRIUM_SPEC_ADDENDUM_2 §4.2 — bytes on the bus
are identical to sdk-node so atrium cannot tell which language an app uses.

What `Inspire.start()` does:
  1. Connects to broker (defaults to 127.0.0.1:1883).
  2. Sets LWT: empty retained payload on `inspire/presence/<slug>/<nodeId>`
     so atrium auto-cleans on crash (spec §4.1).
  3. Publishes retained PresenceMsg.
  4. Starts a 10s heartbeat thread (overridable for tests).
  5. Subscribes to `inspire/cmd/<slug>/<nodeId>` for atrium → app commands.

What `stop()` does:
  1. Stops the heartbeat thread.
  2. Publishes empty retained payload on the presence topic (graceful clear).
  3. Disconnects.

The bootstrap surface (Stage 2 install agent, preflight, diagnostics) from the
Phase 6 design is NOT in this library. This is the runtime surface only.
"""

from __future__ import annotations

import json
import os
import resource
import socket
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import paho.mqtt.client as mqtt

from ._topics import (
    slugify_node_id,
    topic_cmd,
    topic_heartbeat,
    topic_log,
    topic_presence,
    topic_status,
)
from ._types import (
    CommandMsg,
    HeartbeatMsg,
    LogLevel,
    LogMsg,
    PresenceMsg,
    StatusMsg,
    StatusState,
)


DEFAULT_BROKER_HOST = "127.0.0.1"
DEFAULT_BROKER_PORT = 1883
DEFAULT_HEARTBEAT_S = 10.0
DEFAULT_CONNECT_TIMEOUT_S = 8.0
PUBLISH_ACK_TIMEOUT_S = 5.0


def _utc_now_iso() -> str:
    """RFC3339 UTC with millisecond precision and Z suffix.

    Matches sdk-node's `new Date().toISOString()` output exactly.
    """
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _process_rss_mb() -> int:
    """Resident set size in MB. POSIX-only via resource.getrusage.

    On Linux, ru_maxrss is in KB; spec §2 limits us to Linux so this is correct.
    """
    rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss_kb // 1024


class _HeartbeatThread(threading.Thread):
    """Background thread that publishes HeartbeatMsg every interval_s.

    Runs as a daemon so it doesn't block process exit if the caller forgets
    to stop(). The stop_event-driven sleep makes shutdown immediate rather
    than waiting out the next interval.
    """

    def __init__(
        self,
        client: mqtt.Client,
        slug: str,
        node_id: str,
        interval_s: float,
    ) -> None:
        super().__init__(daemon=True, name=f"inspire-heartbeat-{slug}")
        self._client = client
        self._slug = slug
        self._node_id = node_id
        self._interval_s = interval_s
        self._stop_event = threading.Event()
        self._start_monotonic = time.monotonic()

    def run(self) -> None:
        # Don't fire immediately — atrium already has the retained presence
        # message and uses a freshness window, not a heartbeat count.
        while not self._stop_event.wait(self._interval_s):
            try:
                self._publish_one()
            except Exception:
                # Heartbeat publishes are best-effort. If we throw, the thread
                # would die silently. Swallow so the loop continues.
                pass

    def _publish_one(self) -> None:
        msg: HeartbeatMsg = {
            "v": 1,
            "ts": _utc_now_iso(),
            "uptime_s": int(time.monotonic() - self._start_monotonic),
            "rss_mb": _process_rss_mb(),
            # cpu_pct: cheap approximation matching sdk-node. Better
            # implementations would sample process CPU usage deltas.
            "cpu_pct": 0.0,
        }
        self._client.publish(
            topic_heartbeat(self._slug, self._node_id),
            json.dumps(msg),
            qos=0,
            retain=False,
        )

    def stop(self) -> None:
        self._stop_event.set()


class InspireClient:
    """Live SDK session. Returned by Inspire.start()."""

    def __init__(
        self,
        *,
        slug: str,
        node_id: str,
        version: str,
        service_mode: bool,
        client: mqtt.Client,
        heartbeat_interval_s: float,
    ) -> None:
        self.slug = slug
        self.node_id = node_id
        self._version = version
        self._service_mode = service_mode
        self._client = client
        self._started_at_iso = _utc_now_iso()
        self._heartbeat = _HeartbeatThread(
            client=client,
            slug=slug,
            node_id=node_id,
            interval_s=heartbeat_interval_s,
        )
        self._command_handlers: dict[str, Callable[[CommandMsg], None]] = {}
        self._stopped = False
        self._stop_lock = threading.Lock()

    def set_status(self, state: StatusState, detail: str) -> None:
        """Publish a retained StatusMsg. Blocks until QoS 1 ack or timeout."""
        msg: StatusMsg = {
            "v": 1,
            "state": state,
            "detail": detail,
            "ts": _utc_now_iso(),
        }
        info = self._client.publish(
            topic_status(self.slug, self.node_id),
            json.dumps(msg),
            qos=1,
            retain=True,
        )
        info.wait_for_publish(timeout=PUBLISH_ACK_TIMEOUT_S)

    def log(
        self,
        level: LogLevel,
        msg: str,
        fields: Optional[dict[str, Any]] = None,
    ) -> None:
        """Publish a LogMsg. Atrium gates these by per-app verbose toggle."""
        payload: LogMsg = {
            "v": 1,
            "ts": _utc_now_iso(),
            "level": level,
            "msg": msg,
        }
        if fields is not None:
            payload["fields"] = fields
        self._client.publish(
            topic_log(self.slug, self.node_id),
            json.dumps(payload),
            qos=0,
            retain=False,
        )

    def on_command(
        self,
        cmd: str,
        handler: Callable[[CommandMsg], None],
    ) -> None:
        """Register a handler for an inbound CommandMsg verb."""
        self._command_handlers[cmd] = handler

    def stop(self) -> None:
        """Tear down: clear retained presence, stop heartbeat, disconnect.

        Idempotent — calling stop() twice does nothing the second time.
        """
        with self._stop_lock:
            if self._stopped:
                return
            self._stopped = True

        self._heartbeat.stop()
        self._heartbeat.join(timeout=2.0)

        # Only attempt the retained-clear publish if we're actually connected.
        # Offline, paho's publish() queues forever; client.disconnect() flushes
        # and drops. LWT (set up at connect time) handles cleanup on the
        # broker's side when the connection drops.
        if self._client.is_connected():
            info = self._client.publish(
                topic_presence(self.slug, self.node_id),
                b"",
                qos=1,
                retain=True,
            )
            info.wait_for_publish(timeout=PUBLISH_ACK_TIMEOUT_S)

        self._client.loop_stop()
        self._client.disconnect()

    # --- Internal entry called by Inspire.start() once the broker connection
    # is established. Publishes presence, subscribes to commands, starts the
    # heartbeat thread.

    def _start(self) -> None:
        msg: PresenceMsg = {
            "v": 1,
            "app_slug": self.slug,
            "node_id": self.node_id,
            "version": self._version,
            "started_at": self._started_at_iso,
            "pid": os.getpid(),
            "service_mode": self._service_mode,
        }
        info = self._client.publish(
            topic_presence(self.slug, self.node_id),
            json.dumps(msg),
            qos=1,
            retain=True,
        )
        info.wait_for_publish(timeout=PUBLISH_ACK_TIMEOUT_S)

        cmd_topic = topic_cmd(self.slug, self.node_id)
        self._client.subscribe(cmd_topic, qos=1)
        self._client.on_message = self._on_message

        self._heartbeat.start()

    def _on_message(
        self,
        client: mqtt.Client,
        userdata: Any,
        message: mqtt.MQTTMessage,
    ) -> None:
        # Defensive: paho also delivers messages from any other subscription
        # we might add later. Filter by topic before parsing.
        if message.topic != topic_cmd(self.slug, self.node_id):
            return
        try:
            parsed: CommandMsg = json.loads(message.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return  # malformed — ignore (matches sdk-node behavior)
        cmd = parsed.get("cmd")
        if cmd is None:
            return
        handler = self._command_handlers.get(cmd)
        if handler is not None:
            handler(parsed)


class Inspire:
    """Entry point. Use `Inspire.start(slug=..., version=...)`."""

    @staticmethod
    def start(
        *,
        slug: str,
        version: str,
        broker: Optional[dict[str, Any]] = None,
        node_id: Optional[str] = None,
        service_mode: bool = False,
        heartbeat_interval_s: float = DEFAULT_HEARTBEAT_S,
        client_id: Optional[str] = None,
        connect_timeout_s: float = DEFAULT_CONNECT_TIMEOUT_S,
    ) -> InspireClient:
        """Connect to the broker, publish retained presence, start heartbeat,
        subscribe to commands. Resolves once the initial presence has been
        published — the point at which atrium can render the row.

        Raises:
            ValueError: if `slug` is missing.
            TimeoutError: if the broker connection doesn't establish in time.
            ConnectionError: if the broker explicitly rejected the connection.
        """
        if not slug:
            raise ValueError("Inspire.start: slug is required")
        resolved_node_id = node_id or slugify_node_id(socket.gethostname())
        broker = broker or {}
        host = broker.get("host", DEFAULT_BROKER_HOST)
        port = broker.get("port", DEFAULT_BROKER_PORT)
        resolved_client_id = (
            client_id or f"{slug}-{resolved_node_id}-{os.getpid()}"
        )

        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=resolved_client_id,
            clean_session=True,
            protocol=mqtt.MQTTv311,
        )
        # LWT — empty retained payload so atrium drops the row on crash.
        client.will_set(
            topic_presence(slug, resolved_node_id),
            payload=b"",
            qos=1,
            retain=True,
        )

        connected = threading.Event()
        connect_failure: list[str] = []

        def on_connect(
            _client: mqtt.Client,
            _userdata: Any,
            _flags: Any,
            reason_code: Any,
            _properties: Any,
        ) -> None:
            # paho v2 callback API: reason_code is a ReasonCode object.
            # is_failure is True when the broker refused the connection.
            if reason_code.is_failure:
                connect_failure.append(
                    f"broker rejected connection: {reason_code}"
                )
            connected.set()  # unblock the waiter regardless

        client.on_connect = on_connect
        client.connect_async(host, port, keepalive=60)
        client.loop_start()

        if not connected.wait(timeout=connect_timeout_s):
            # Without an explicit cleanup, paho keeps the loop thread retrying
            # in the background while the caller already received an exception
            # — silent connection leak. Match sdk-node's `client.end(true)`.
            client.loop_stop()
            client.disconnect()
            raise TimeoutError(
                f"Inspire.start: connect timed out after {connect_timeout_s}s"
            )
        if connect_failure:
            client.loop_stop()
            client.disconnect()
            raise ConnectionError(connect_failure[0])

        instance = InspireClient(
            slug=slug,
            node_id=resolved_node_id,
            version=version,
            service_mode=service_mode,
            client=client,
            heartbeat_interval_s=heartbeat_interval_s,
        )
        instance._start()
        return instance
