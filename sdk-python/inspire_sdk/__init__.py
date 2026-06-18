"""inspire-sdk — Python SDK for inspire-* apps to participate in the
inspire-atrium MQTT messaging fabric.

Wire format per spec INSPIRE_ATRIUM_SPEC_ADDENDUM_2 §4.2.

Usage (the 5-line "near boot" snippet from the migration guide):

    from inspire_sdk import Inspire

    client = Inspire.start(slug="inspire-music", version="0.1.0")
    client.set_status("ready", "library loaded")
    # on shutdown:
    client.stop()
"""

from ._client import Inspire, InspireClient, RpcHandler
from ._types import (
    CapabilityManifestMsg,
    CommandMsg,
    HeartbeatMsg,
    LogLevel,
    LogMsg,
    PresenceMsg,
    RpcRequestMsg,
    RpcResponseMsg,
    StatusMsg,
    StatusState,
    VerbSpec,
)

__all__ = [
    "Inspire",
    "InspireClient",
    "RpcHandler",
    "CommandMsg",
    "HeartbeatMsg",
    "LogLevel",
    "LogMsg",
    "PresenceMsg",
    "StatusMsg",
    "StatusState",
    "CapabilityManifestMsg",
    "RpcRequestMsg",
    "RpcResponseMsg",
    "VerbSpec",
]

__version__ = "0.1.0"
