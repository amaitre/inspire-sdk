"""Sibling-comm wire types — the v3 envelope + client config.

The sibling layer is a SECOND surface on the same broker as the app bus,
used for agent-to-agent (DA-to-DA) chat, events, fire-and-forget commands,
and request/response RPC. It is intentionally distinct from the app-bus
typed messages in `inspire_sdk._types`: the app bus uses one TypedDict per
topic (v1), while the sibling layer uses a single generic `Envelope` (v3)
whose `type` field discriminates — the right shape for free-form chat and
forward-additive events.

Wire format is byte-identical to the original `Sibling.py` (v3 spec); the
move into the SDK is a packaging change, not a protocol change.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Any

# v3 convention: the slug IS the canonical identifier — kebab-case with an
# `atlas-` prefix (e.g. "atlas-ata", "atlas-lyra", "atlas-mingus"). It appears
# verbatim as the MQTT username, the topic <from>/<to> segment, and the
# envelope `from`/`to` field. No further transformation.
SiblingName = str


def now_iso() -> str:
    """ISO 8601 UTC with milliseconds, matching the v3 spec —
    e.g. `2026-05-05T21:58:43.962Z`."""
    now = time.time()
    millis = int((now - int(now)) * 1000)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now)) + f".{millis:03d}Z"


def new_id() -> str:
    return uuid.uuid4().hex


@dataclass(frozen=True)
class Envelope:
    """v3 wire envelope — matches the sibling-comm spec.

    Field shape:
      v:       int (3)
      from:    sender slug, e.g. "atlas-lyra"
      to:      recipient slug — present for cmd / rpc / addressed events,
               OMITTED for chat / presence / broadcast
      ts:      ISO 8601 UTC with milliseconds
      type:    discriminator: "chat" | "cmd" | "rpc.request" | "rpc.response"
               | "presence" | "event" | future-additive
      body:    type-specific payload object
      corr_id: correlation id — present on rpc.request / rpc.response only
      id:      additive in spec v3, used for dedup / tracing

    Forward-compat: additive only; unknown `type` and unknown body fields are
    tolerated by consumers. `from_` / `type_` are used in Python because
    `from`/`type` are reserved; they serialize as "from"/"type".
    """

    v: int
    from_: str
    to: str | None
    ts: str
    type_: str
    body: dict[str, Any]
    corr_id: str | None = None
    id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "v": self.v,
            "from": self.from_,
            "ts": self.ts,
            "type": self.type_,
            "body": self.body,
        }
        if self.to is not None:
            d["to"] = self.to
        if self.corr_id is not None:
            d["corr_id"] = self.corr_id
        if self.id is not None:
            d["id"] = self.id
        return d

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), separators=(",", ":")).encode()

    @classmethod
    def from_json(cls, raw: bytes | str) -> "Envelope":
        d = json.loads(raw if isinstance(raw, str) else raw.decode())
        return cls(
            v=int(d.get("v", 3)),
            from_=str(d["from"]),
            to=d.get("to"),
            ts=str(d["ts"]),
            type_=str(d["type"]),
            body=dict(d.get("body", {})),
            corr_id=d.get("corr_id"),
            id=d.get("id"),
        )


@dataclass
class SiblingConfig:
    self_name: SiblingName
    broker_host: str
    broker_port: int = 1883
    broker_tls: bool = False
    username: str | None = None
    password: str | None = None
    presence_interval_s: float = 10.0
    request_default_timeout_s: float = 5.0
