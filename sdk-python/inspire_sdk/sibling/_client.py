"""Sibling-comm async client + aiomqtt connection layer + config loader.

Ported verbatim-on-the-wire from the original standalone `Sibling.py`. The
public API (`async with Sibling(cfg)`, `chat`/`cmd`/`event`/`request`/
`respond`/`subscribe`) is the contract the fleet's listener services and
inspire-live-music's event broadcaster depend on, so it is preserved exactly.

aiomqtt is imported lazily inside `_Connection` so the broker-agnostic
surface (Envelope, topic helpers) loads even where aiomqtt isn't installed —
e.g. `python -m inspire_sdk.sibling --self-test`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from ._topics import (
    cmd_topic,
    chat_topic,
    events_topic,
    presence_topic,
    rpc_req_topic,
    rpc_res_topic,
    topic_matches,
)
from ._types import Envelope, SiblingConfig, SiblingName, new_id, now_iso

log = logging.getLogger(__name__)


class Sibling:
    """Async client for sibling-to-sibling messaging.

    Construct with a `SiblingConfig`; use as an async context manager so
    presence heartbeats and connection lifecycle are managed automatically.
    """

    def __init__(self, cfg: SiblingConfig) -> None:
        self.cfg = cfg
        # v3 convention: the slug IS the canonical identifier; no transform.
        self.from_ = cfg.self_name
        self._conn: _Connection | None = None
        self._presence_task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    async def __aenter__(self) -> "Sibling":
        self._conn = _Connection(self.cfg)
        await self._conn.connect()
        self._presence_task = asyncio.create_task(self._presence_loop())
        return self

    async def __aexit__(self, *_exc) -> None:
        self._stop.set()
        if self._presence_task is not None:
            self._presence_task.cancel()
            try:
                await self._presence_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._conn is not None:
            await self._conn.close()

    # ─── primitive ops ───

    def _envelope(
        self,
        type_: str,
        body: dict[str, Any],
        *,
        to: str | None = None,
        corr_id: str | None = None,
        msg_id: str | None = None,
    ) -> Envelope:
        return Envelope(
            v=3,
            from_=self.from_,
            to=to,
            ts=now_iso(),
            type_=type_,
            body=body,
            corr_id=corr_id,
            id=msg_id or new_id(),
        )

    async def publish(
        self,
        topic: str,
        type_: str,
        body: dict[str, Any],
        *,
        to: str | None = None,
        qos: int = 0,
        retain: bool = False,
        corr_id: str | None = None,
        msg_id: str | None = None,
    ) -> str:
        """Publish a single v3 envelope. Always mints (or echoes) a UUID-style
        `id` field for dedup / tracing. Returns the envelope id."""
        env = self._envelope(type_, body, to=to, corr_id=corr_id, msg_id=msg_id)
        assert self._conn is not None
        await self._conn.publish(topic, env.to_json(), qos=qos, retain=retain)
        return env.id  # type: ignore[return-value]

    async def subscribe(self, topic_pattern: str) -> AsyncIterator[Envelope]:
        assert self._conn is not None
        async for raw in self._conn.subscribe(topic_pattern):
            try:
                yield Envelope.from_json(raw)
            except (json.JSONDecodeError, KeyError, ValueError) as exc:
                log.warning("subscribe(%s): dropping malformed message — %s", topic_pattern, exc)

    # ─── high-level conveniences ───

    async def chat(self, text: str) -> str:
        """Post a free-form chat message under our own slug. Returns the
        envelope id. Subscribers receive on `inspire/sibling/chat/+`."""
        return await self.publish(chat_topic(self.cfg.self_name), "chat", {"text": text}, qos=0)

    async def cmd(self, target: SiblingName, line: str) -> str:
        """Fire-and-forget CLI command for a sibling."""
        return await self.publish(cmd_topic(target), "cmd", {"line": line}, to=target, qos=1)

    async def event(self, type_: str, body: dict[str, Any]) -> str:
        """Publish a domain event on this sibling's events topic."""
        return await self.publish(events_topic(self.cfg.self_name), type_, body, qos=1)

    async def request(
        self,
        target: SiblingName,
        type_: str,
        body: dict[str, Any],
        timeout: float | None = None,
    ) -> Envelope:
        """RPC: publish a request and await a single response. Correlation: a
        `corr_id` UUID is generated and carried on the request; the responder
        echoes it. Reply lands on `inspire/sibling/rpc/<self>/res/<corr_id>`
        which we subscribe to BEFORE publishing the request."""
        timeout = timeout if timeout is not None else self.cfg.request_default_timeout_s
        corr_id = new_id()
        res_topic = rpc_res_topic(self.cfg.self_name, corr_id)
        sub_iter = self.subscribe(res_topic)

        async def _await_reply() -> Envelope:
            async for env in sub_iter:
                if env.corr_id == corr_id:
                    return env
            raise RuntimeError("subscribe iterator ended without a reply")

        await self.publish(
            rpc_req_topic(target, corr_id), type_, body, to=target, qos=1, corr_id=corr_id
        )
        try:
            return await asyncio.wait_for(_await_reply(), timeout=timeout)
        except asyncio.TimeoutError as exc:
            raise TimeoutError(f"rpc to {target} timed out after {timeout}s") from exc

    async def respond(
        self, request_env: Envelope, body: dict[str, Any], type_: str = "rpc.response"
    ) -> str:
        """Respond to an RPC request. Carries the request's `corr_id` and
        routes to the requester's slug-keyed reply topic."""
        if request_env.corr_id is None:
            raise ValueError("respond(): request envelope has no corr_id to echo")
        topic = rpc_res_topic(request_env.from_, request_env.corr_id)
        return await self.publish(
            topic, type_, body, to=request_env.from_, qos=1, corr_id=request_env.corr_id
        )

    # ─── presence heartbeat ───

    async def _presence_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self.publish(
                    presence_topic(self.cfg.self_name),
                    "presence",
                    {"status": "alive", "host": os.uname().nodename},
                    qos=0,
                    retain=True,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("presence: publish failed — %s", exc)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.cfg.presence_interval_s)
            except asyncio.TimeoutError:
                pass


class _Connection:
    """aiomqtt-backed MQTT 3.1.1 client.

    Subscribers register interest by adding a per-topic-pattern queue here;
    the dispatcher routes each incoming message to every queue whose pattern
    matches. Per-call publish() is delegated to the underlying client.
    """

    def __init__(self, cfg: SiblingConfig) -> None:
        import aiomqtt  # type: ignore[import-untyped]  # lazy: keeps the offline surface importable

        self._aiomqtt = aiomqtt
        self.cfg = cfg
        self._client: Any = None
        self._client_cm: Any = None
        self._dispatch_task: asyncio.Task | None = None
        self._subs: dict[str, list[asyncio.Queue]] = {}
        self._stopping = asyncio.Event()

    async def connect(self) -> None:
        # Per-process unique client_id so a long-running listener and a one-shot
        # publish from the same box don't displace each other on the broker.
        client_id = f"{self.cfg.username or self.cfg.self_name}-{os.getpid()}-{new_id()[:8]}"
        client = self._aiomqtt.Client(
            hostname=self.cfg.broker_host,
            port=self.cfg.broker_port,
            username=self.cfg.username,
            password=self.cfg.password,
            identifier=client_id,
            tls_params=self._aiomqtt.TLSParameters() if self.cfg.broker_tls else None,
        )
        self._client_cm = client
        self._client = await client.__aenter__()
        log.info(
            "Sibling._Connection: connected to %s:%d as %s",
            self.cfg.broker_host,
            self.cfg.broker_port,
            self.cfg.username or self.cfg.self_name,
        )
        self._dispatch_task = asyncio.create_task(self._dispatch_loop())

    async def close(self) -> None:
        self._stopping.set()
        if self._dispatch_task is not None:
            self._dispatch_task.cancel()
            try:
                await self._dispatch_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._client_cm is not None:
            try:
                await self._client_cm.__aexit__(None, None, None)
            except Exception as exc:  # noqa: BLE001
                log.warning("Sibling._Connection.close: aiomqtt teardown raised %s", exc)
        self._client = None
        self._client_cm = None

    async def publish(
        self, topic: str, body: bytes, *, qos: int = 0, retain: bool = False
    ) -> None:
        if self._client is None:
            raise RuntimeError("publish before connect()")
        await self._client.publish(topic, payload=body, qos=qos, retain=retain)

    async def subscribe(self, topic_pattern: str) -> AsyncIterator[bytes]:
        """Yield raw payloads matching `topic_pattern`. The first call for a
        pattern issues an MQTT SUBSCRIBE; further calls share it but each get
        an independent fan-out queue."""
        if self._client is None:
            raise RuntimeError("subscribe before connect()")
        queue: asyncio.Queue = asyncio.Queue(maxsize=1024)
        first_subscriber_for_pattern = topic_pattern not in self._subs
        self._subs.setdefault(topic_pattern, []).append(queue)
        if first_subscriber_for_pattern:
            await self._client.subscribe(topic_pattern, qos=1)
        try:
            while not self._stopping.is_set():
                payload = await queue.get()
                yield payload
        finally:
            try:
                self._subs.get(topic_pattern, []).remove(queue)
            except ValueError:
                pass
            if not self._subs.get(topic_pattern):
                self._subs.pop(topic_pattern, None)
                try:
                    await self._client.unsubscribe(topic_pattern)
                except Exception:  # noqa: BLE001
                    pass

    async def _dispatch_loop(self) -> None:
        """Single reader on `client.messages`. Fans each incoming message out
        to every queue whose subscribed pattern matches the topic."""
        try:
            async for msg in self._client.messages:
                topic = str(msg.topic)
                payload = msg.payload if isinstance(msg.payload, bytes) else bytes(msg.payload or b"")
                for pattern, queues in list(self._subs.items()):
                    if topic_matches(pattern, topic):
                        for q in queues:
                            try:
                                q.put_nowait(payload)
                            except asyncio.QueueFull:
                                log.warning("subscribe queue full for %s; dropping message", pattern)
        except self._aiomqtt.MqttError as exc:
            log.warning("MQTT connection lost: %s", exc)
        except asyncio.CancelledError:
            pass


def load_config(self_name: SiblingName, path: Path | None = None) -> SiblingConfig:
    """Read SiblingConfig from ~/.claude/PAI/USER/Config/sibling.yaml.

    Schema (YAML): broker_host, broker_port, broker_tls, username, password,
    presence_interval_s. Uses PyYAML if present, else a tiny KEY: VALUE shim.
    """
    cfg_path = path or (Path.home() / ".claude" / "PAI" / "USER" / "Config" / "sibling.yaml")
    if not cfg_path.exists():
        raise FileNotFoundError(
            f"sibling config not present at {cfg_path}; create it with broker details"
        )
    raw = cfg_path.read_text()
    try:
        import yaml  # type: ignore[import-untyped]

        d = yaml.safe_load(raw)
    except ImportError:
        d = {}
        for line in raw.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or ":" not in line:
                continue
            k, _, v = line.partition(":")
            d[k.strip()] = v.strip().strip('"').strip("'")
    return SiblingConfig(
        self_name=self_name,
        broker_host=str(d["broker_host"]),
        broker_port=int(d.get("broker_port", 1883)),
        broker_tls=bool(d.get("broker_tls", False)),
        username=d.get("username"),
        password=d.get("password"),
        presence_interval_s=float(d.get("presence_interval_s", 10.0)),
    )
