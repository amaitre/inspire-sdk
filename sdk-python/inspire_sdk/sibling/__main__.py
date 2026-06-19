"""CLI entry for the sibling layer — `python -m inspire_sdk.sibling ...`.

A 1:1 replacement for the old `Sibling.py` CLI the fleet listener services
invoke, so a systemd ExecStart only needs its path swapped:

    Sibling.py            listen atlas-ata "inspire/sibling/chat/+"
    -m inspire_sdk.sibling listen atlas-ata "inspire/sibling/chat/+"

Usage:
  python -m inspire_sdk.sibling --self-test
  python -m inspire_sdk.sibling chat <self_slug> "<text>"
  python -m inspire_sdk.sibling listen <self_slug> <topic_pattern>
"""

from __future__ import annotations

import asyncio
import sys

from ._client import Sibling, load_config
from ._topics import (
    chat_topic,
    cmd_topic,
    events_topic,
    presence_topic,
    rpc_res_topic,
    topic_matches,
)
from ._types import Envelope


def _self_test() -> int:
    """Smoke-test envelope, topic helpers, and the topic matcher with no
    broker connectivity. Exit 0 on success."""
    failures: list[str] = []

    def expect(label: str, actual, expected) -> None:
        if actual != expected:
            failures.append(f"{label}: expected {expected!r}, got {actual!r}")

    expect("chat_topic", chat_topic("atlas-lyra"), "inspire/sibling/chat/atlas-lyra")
    expect("presence_topic", presence_topic("atlas-lyra"), "inspire/sibling/presence/atlas-lyra")
    expect("cmd_topic", cmd_topic("atlas-mingus"), "inspire/sibling/cmd/atlas-mingus")
    expect("events_topic", events_topic("atlas-lyra"), "inspire/sibling/events/atlas-lyra")
    expect("rpc_res_topic", rpc_res_topic("atlas-lyra", "abc"), "inspire/sibling/rpc/atlas-lyra/res/abc")

    env = Envelope(
        v=3, from_="atlas-lyra", to="atlas-mingus", ts="2026-05-05T20:00:00.000Z",
        type_="chat", body={"text": "hello"}, corr_id=None, id="abc123",
    )
    parsed = Envelope.from_json(env.to_json())
    expect("envelope round-trip", parsed, env)

    ming_wire = b'{"v":3,"from":"atlas-mingus","ts":"2026-05-05T21:58:43.962Z","type":"chat","body":{"text":"Ming round 2"}}'
    p = Envelope.from_json(ming_wire)
    expect("ming wire from", p.from_, "atlas-mingus")
    expect("ming wire to absent", p.to, None)

    expect("match plus", topic_matches("inspire/sibling/chat/+", "inspire/sibling/chat/lyra"), True)
    expect("match hash", topic_matches("inspire/sibling/#", "inspire/sibling/chat/lyra"), True)
    expect("no-match", topic_matches("a/+", "a/b/c"), False)

    if failures:
        for f in failures:
            print(f"FAIL  {f}")
        print(f"\n{len(failures)} failure(s)")
        return 1
    print("inspire_sdk.sibling self-test: all checks passed")
    return 0


async def _cli_chat(slug: str, text: str) -> int:
    cfg = load_config(slug)
    async with Sibling(cfg) as s:
        msg_id = await s.chat(text)
        print(f"posted ok (id={msg_id})")
    return 0


async def _cli_listen(slug: str, pattern: str) -> int:
    cfg = load_config(slug)
    async with Sibling(cfg) as s:
        print(f"subscribed to {pattern}; press ^C to stop", flush=True)
        try:
            async for env in s.subscribe(pattern):
                print(
                    f"[{env.ts}] {env.from_} -> {env.to or '(broadcast)'} "
                    f"type={env.type_}: {env.body}",
                    flush=True,
                )
        except KeyboardInterrupt:
            pass
    return 0


def main(argv: list[str]) -> int:
    if len(argv) >= 1 and argv[0] == "--self-test":
        return _self_test()
    if len(argv) >= 1 and argv[0] in ("--help", "-h"):
        print(__doc__)
        return 0
    if len(argv) >= 3 and argv[0] == "chat":
        return asyncio.run(_cli_chat(argv[1], " ".join(argv[2:])))
    if len(argv) >= 3 and argv[0] == "listen":
        return asyncio.run(_cli_listen(argv[1], argv[2]))
    print(__doc__)
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
