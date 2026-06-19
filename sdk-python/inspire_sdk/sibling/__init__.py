"""inspire_sdk.sibling — Atlas-family DA-to-DA (agent-to-agent) messaging.

A second surface on the inspire bus, distinct from the app-bus client in
`inspire_sdk`. Where the app bus is paho/sync with one typed message per
topic (v1), the sibling layer is aiomqtt/async with a single generic v3
`Envelope` whose `type` discriminates — the right shape for free-form chat,
fire-and-forget commands, continuous events, and request/response RPC
between DAs (and inspire-live-music's event broadcaster).

aiomqtt is an optional extra: ``pip install inspire-sdk[sibling]``.

    from inspire_sdk.sibling import Sibling, load_config

    cfg = load_config("atlas-mingus")
    async with Sibling(cfg) as s:
        await s.chat("hello team")
        async for env in s.subscribe("inspire/sibling/chat/+"):
            print(env.from_, env.body)

CLI (drop-in for the fleet listener services):

    python -m inspire_sdk.sibling listen atlas-ata "inspire/sibling/chat/+"
    python -m inspire_sdk.sibling chat atlas-mingus "hello team"
    python -m inspire_sdk.sibling --self-test
"""

from ._client import Sibling, load_config
from ._topics import (
    PRINCIPAL_BROADCAST,
    chat_topic,
    cmd_topic,
    events_topic,
    presence_topic,
    rpc_req_topic,
    rpc_res_topic,
    topic_matches,
)
from ._types import Envelope, SiblingConfig, SiblingName, new_id, now_iso

__all__ = [
    "Sibling",
    "SiblingConfig",
    "SiblingName",
    "Envelope",
    "load_config",
    "now_iso",
    "new_id",
    "PRINCIPAL_BROADCAST",
    "chat_topic",
    "presence_topic",
    "events_topic",
    "cmd_topic",
    "rpc_req_topic",
    "rpc_res_topic",
    "topic_matches",
]
