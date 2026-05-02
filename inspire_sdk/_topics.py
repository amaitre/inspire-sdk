"""Topic helpers per spec §4. Mirrors sdk-node/src/index.ts:81-95.

`<node_id>` is the lowercase hostname with non-alphanumerics replaced by '-'.
"""

import re


def slugify_node_id(hostname: str) -> str:
    """Lowercase + non-alphanumerics → '-'. Identical to sdk-node's slugifier."""
    if not hostname:
        return "unknown"
    lowered = hostname.lower()
    dashed = re.sub(r"[^a-z0-9]+", "-", lowered)
    trimmed = dashed.strip("-")
    return trimmed if trimmed else "unknown"


def topic_presence(slug: str, node_id: str) -> str:
    return f"inspire/presence/{slug}/{node_id}"


def topic_heartbeat(slug: str, node_id: str) -> str:
    return f"inspire/heartbeat/{slug}/{node_id}"


def topic_status(slug: str, node_id: str) -> str:
    return f"inspire/status/{slug}/{node_id}"


def topic_log(slug: str, node_id: str) -> str:
    return f"inspire/log/{slug}/{node_id}"


def topic_cmd(slug: str, node_id: str) -> str:
    return f"inspire/cmd/{slug}/{node_id}"
