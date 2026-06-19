"""Sibling-comm topic namespace (v3).

Topics live under the `inspire/sibling/...` root so they sit alongside the
app bus's `inspire/{presence,heartbeat,status,log,cmd,rpc,manifest}/...`
shape without colliding.

Addressing: <from> = sender's slug; <to> = recipient's slug. Trailing-slug
placement makes wildcard subscriptions cheap — a sibling subscribes to
`inspire/sibling/chat/+` once and receives everyone's chat. RPC topics carry
the correlation id so a single wildcard on `.../res/+` returns all replies.
"""

from __future__ import annotations

PRINCIPAL_BROADCAST = "inspire/sibling/principal/broadcast"


def chat_topic(from_slug: str) -> str:
    """Broadcast chat from a sibling. Subscribers wildcard
    `inspire/sibling/chat/+`."""
    return f"inspire/sibling/chat/{from_slug}"


def presence_topic(from_slug: str) -> str:
    """Self-presence heartbeat (publish retained). Wildcard subscribe
    `inspire/sibling/presence/+`."""
    return f"inspire/sibling/presence/{from_slug}"


def events_topic(from_slug: str) -> str:
    """Continuous event stream from a sibling. Subscribers wildcard
    `inspire/sibling/events/+`."""
    return f"inspire/sibling/events/{from_slug}"


def cmd_topic(to_slug: str) -> str:
    """Fire-and-forget command targeted at a single sibling. The receiver
    subscribes to `inspire/sibling/cmd/<my_slug>`."""
    return f"inspire/sibling/cmd/{to_slug}"


def rpc_req_topic(to_slug: str, correlation_id: str) -> str:
    """RPC request addressed to a single sibling, carrying a correlation id.
    The receiver subscribes to `inspire/sibling/rpc/<my_slug>/req/+`."""
    return f"inspire/sibling/rpc/{to_slug}/req/{correlation_id}"


def rpc_res_topic(requester_slug: str, correlation_id: str) -> str:
    """RPC response addressed BACK to the requester. The requester subscribes
    to `inspire/sibling/rpc/<my_slug>/res/+` and demuxes by correlation id."""
    return f"inspire/sibling/rpc/{requester_slug}/res/{correlation_id}"


def topic_matches(pattern: str, topic: str) -> bool:
    """MQTT topic-filter matcher for + and # wildcards. The broker enforces
    this on the wire; we re-check here so the local fan-out delivers each
    message only to subscribers whose pattern actually matches it."""
    p_parts = pattern.split("/")
    t_parts = topic.split("/")
    for i, p in enumerate(p_parts):
        if p == "#":
            return True
        if i >= len(t_parts):
            return False
        if p == "+":
            continue
        if p != t_parts[i]:
            return False
    return len(p_parts) == len(t_parts)
