"""Wire-format payload shapes per spec INSPIRE_ATRIUM_SPEC_ADDENDUM_2 §4.2.

These TypedDicts describe what gets serialized to JSON on the bus. Bytes on
the wire match sdk-node exactly so atrium cannot tell which language an app
is written in.
"""

from typing import Literal, TypedDict

try:  # NotRequired landed in typing in 3.11; fall back for 3.10 (Jetson).
    from typing import NotRequired
except ImportError:  # pragma: no cover
    from typing_extensions import NotRequired


LogLevel = Literal["debug", "info", "warn", "error"]
StatusState = Literal["starting", "ready", "degraded", "stopping", "error"]


class PresenceMsg(TypedDict):
    v: Literal[1]
    app_slug: str
    node_id: str
    version: str
    started_at: str  # RFC3339 UTC
    pid: int
    service_mode: bool


class HeartbeatMsg(TypedDict):
    v: Literal[1]
    ts: str  # RFC3339 UTC
    uptime_s: int
    rss_mb: int
    cpu_pct: float


class StatusMsg(TypedDict):
    v: Literal[1]
    state: StatusState
    detail: str
    ts: str  # RFC3339 UTC


class LogMsg(TypedDict):
    v: Literal[1]
    ts: str  # RFC3339 UTC
    level: LogLevel
    msg: str
    fields: NotRequired[dict]


class CommandMsg(TypedDict):
    v: Literal[1]
    cmd: str
    args: NotRequired[dict]
    request_id: NotRequired[str]


# ── RPC + capability manifest (spec §6, additive over §4) ──


class VerbSpec(TypedDict):
    name: str
    description: NotRequired[str]
    input: NotRequired[dict]
    output: NotRequired[dict]


class CapabilityManifestMsg(TypedDict):
    v: Literal[1]
    app_slug: str
    node_id: str
    version: str
    ts: str
    verbs: list  # list[VerbSpec]


class RpcRequestMsg(TypedDict):
    v: Literal[1]
    corr_id: str
    reply_to: str
    verb: str
    args: dict
    ts: str


class RpcResponseMsg(TypedDict):
    v: Literal[1]
    corr_id: str
    ok: bool
    result: NotRequired[object]
    error: NotRequired[dict]
    ts: str
