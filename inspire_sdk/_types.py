"""Wire-format payload shapes per spec INSPIRE_ATRIUM_SPEC_ADDENDUM_2 §4.2.

These TypedDicts describe what gets serialized to JSON on the bus. Bytes on
the wire match sdk-node exactly so atrium cannot tell which language an app
is written in.
"""

from typing import Literal, NotRequired, TypedDict


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
