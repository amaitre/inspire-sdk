"""Unit tests for the sibling-comm layer (inspire_sdk.sibling).

Broker-agnostic: no Mosquitto needed. Asserts the v3 envelope round-trips,
the topic helpers produce the exact strings the fleet listeners subscribe to,
the matcher handles +/# correctly, and the wire is byte-compatible with the
original Sibling.py (so the move into the SDK is invisible on the bus).
"""

from __future__ import annotations

import json

from inspire_sdk.sibling import (
    Envelope,
    SiblingConfig,
    chat_topic,
    cmd_topic,
    events_topic,
    presence_topic,
    rpc_req_topic,
    rpc_res_topic,
    topic_matches,
)


def test_topic_helpers_exact_strings():
    assert chat_topic("atlas-lyra") == "inspire/sibling/chat/atlas-lyra"
    assert presence_topic("atlas-lyra") == "inspire/sibling/presence/atlas-lyra"
    assert cmd_topic("atlas-mingus") == "inspire/sibling/cmd/atlas-mingus"
    assert events_topic("atlas-lyra") == "inspire/sibling/events/atlas-lyra"
    assert rpc_req_topic("atlas-ata", "X1") == "inspire/sibling/rpc/atlas-ata/req/X1"
    assert rpc_res_topic("atlas-lyra", "abc") == "inspire/sibling/rpc/atlas-lyra/res/abc"


def test_envelope_round_trip():
    env = Envelope(
        v=3,
        from_="atlas-lyra",
        to="atlas-mingus",
        ts="2026-05-05T20:00:00.000Z",
        type_="chat",
        body={"text": "hello"},
        corr_id=None,
        id="abc123",
    )
    parsed = Envelope.from_json(env.to_json())
    assert parsed == env
    assert parsed.corr_id is None
    assert parsed.id == "abc123"


def test_envelope_omits_optional_fields_on_wire():
    """to / corr_id / id absent → not serialized (matches v3 spec)."""
    env = Envelope(v=3, from_="atlas-mingus", to=None, ts="t", type_="chat", body={})
    d = json.loads(env.to_json())
    assert "to" not in d and "corr_id" not in d and "id" not in d
    assert d == {"v": 3, "from": "atlas-mingus", "ts": "t", "type": "chat", "body": {}}


def test_parses_original_sibling_wire():
    """Byte-for-byte sample emitted by the old Sibling.py must still parse."""
    ming_wire = (
        b'{"v":3,"from":"atlas-mingus","ts":"2026-05-05T21:58:43.962Z",'
        b'"type":"chat","body":{"text":"Ming round 2"}}'
    )
    p = Envelope.from_json(ming_wire)
    assert p.from_ == "atlas-mingus"
    assert p.ts == "2026-05-05T21:58:43.962Z"
    assert p.type_ == "chat"
    assert p.body == {"text": "Ming round 2"}
    assert p.to is None
    assert p.corr_id is None


def test_respond_res_topic_derivation():
    request_env = Envelope(
        v=3, from_="atlas-ata", to="atlas-lyra", ts="t", type_="rpc.request",
        body={}, corr_id="REQ123",
    )
    assert (
        rpc_res_topic(request_env.from_, request_env.corr_id)
        == "inspire/sibling/rpc/atlas-ata/res/REQ123"
    )


def test_config_defaults():
    cfg = SiblingConfig(self_name="atlas-lyra", broker_host="example.invalid")
    assert cfg.broker_port == 1883
    assert cfg.broker_tls is False
    assert cfg.presence_interval_s == 10.0


def test_topic_matcher():
    assert topic_matches("inspire/sibling/chat/lyra", "inspire/sibling/chat/lyra") is True
    assert topic_matches("inspire/sibling/chat/+", "inspire/sibling/chat/lyra") is True
    assert topic_matches("inspire/sibling/rpc/lyra/res/+", "inspire/sibling/rpc/lyra/res/abc") is True
    assert topic_matches("inspire/sibling/#", "inspire/sibling/chat/lyra") is True
    assert topic_matches("inspire/sibling/chat/lyra", "inspire/sibling/chat/ming") is False
    assert topic_matches("inspire/sibling/chat/+", "inspire/sibling/chat") is False
    assert topic_matches("inspire/sibling/chat", "inspire/sibling/chat/lyra") is False
    assert topic_matches("a/+", "a/b/c") is False
