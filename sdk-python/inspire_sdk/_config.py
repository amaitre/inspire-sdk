"""inspire-sdk config resolution. Mirrors sdk-node/src/config.ts exactly.

The shared convention (INSPIRE_CONVENTION + sdk README) promises that a
`.inspire/config.toml` overrides the broker the app connects to. This module
is the single place that knows the format, so `Inspire.start()` can default
to it and consumers can delete their bespoke broker plumbing.

Format (canonical, matches inspire-automation/.inspire/config.toml):

    schema_version = 1
    [broker]
    host = "127.0.0.1"
    port = 1883
    [reporting]
    heartbeat_interval_s = 10
    verbose_default = false
"""

from __future__ import annotations

import os
from typing import Any, Optional, TypedDict

try:
    import tomllib  # stdlib, python >= 3.11
except ModuleNotFoundError:  # pragma: no cover - exercised only on 3.10
    try:
        import tomli as tomllib  # type: ignore[no-redef]  # 3.10 (Jetson/Lyra)
    except ModuleNotFoundError:
        tomllib = None  # type: ignore[assignment]  # degrade: config file ignored


class BrokerConfig(TypedDict, total=False):
    host: Optional[str]
    port: Optional[int]


class ReportingConfig(TypedDict, total=False):
    heartbeat_interval_ms: Optional[float]


class InspireConfig(TypedDict, total=False):
    broker: BrokerConfig
    reporting: ReportingConfig


def find_inspire_config_path(start_dir: Optional[str] = None) -> Optional[str]:
    """Walk up from `start_dir` (default cwd) to the filesystem root looking
    for `.inspire/config.toml`. Returns the first match, or None.

    Mirrors node's `findInspireConfigPath` (src/config.ts).
    """
    directory = os.path.abspath(start_dir if start_dir is not None else os.getcwd())
    while True:
        candidate = os.path.join(directory, ".inspire", "config.toml")
        if os.path.exists(candidate):
            return candidate
        parent = os.path.dirname(directory)
        if parent == directory:
            return None  # reached root
        directory = parent


def load_inspire_config(start_dir: Optional[str] = None) -> InspireConfig:
    """Resolve the inspire config by walking up for `.inspire/config.toml`
    and parsing the fields the SDK cares about. Never raises: a missing or
    malformed file resolves to `{}` so a bad config can't take an app down
    on boot — the caller falls back to env/defaults.

    Mirrors node's `loadInspireConfig` (src/config.ts).
    """
    file = find_inspire_config_path(start_dir)
    if file is None or tomllib is None:
        return {}
    try:
        with open(file, "rb") as fh:
            raw = tomllib.load(fh)
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}

    out: InspireConfig = {}

    broker = raw.get("broker")
    if isinstance(broker, dict):
        host = broker.get("host")
        port = broker.get("port")
        host = host if isinstance(host, str) else None
        # bool is an int subclass in python; TOML `port = true` must not pass.
        port = port if isinstance(port, int) and not isinstance(port, bool) else None
        if host is not None or port is not None:
            out["broker"] = {"host": host, "port": port}

    reporting = raw.get("reporting")
    if isinstance(reporting, dict):
        interval_s = reporting.get("heartbeat_interval_s")
        if isinstance(interval_s, (int, float)) and not isinstance(interval_s, bool):
            out["reporting"] = {"heartbeat_interval_ms": interval_s * 1000}

    return out


def _env_int(name: str) -> Optional[int]:
    """Parse an env var to a positive integer, or None if unset/invalid.

    Mirrors node's `envInt` (src/config.ts).
    """
    v = os.environ.get(name)
    if v is None or v == "":
        return None
    try:
        n = int(v)
    except ValueError:
        return None
    return n if n > 0 else None


def _first_set(*values: Any) -> Any:
    """First value that is not None — python's stand-in for node's `??` chain."""
    for v in values:
        if v is not None:
            return v
    return None


def resolve_broker(
    opts: Optional[dict[str, Any]],
    file_config: InspireConfig,
    defaults: dict[str, Any],
) -> dict[str, Any]:
    """Resolve the broker host/port using the full precedence chain:
      explicit opts broker  >  env  >  .inspire/config.toml  >  defaults.
    Env accepts both the canonical INSPIRE_BROKER_* and the bare BROKER_*
    names that several apps already use, so adoption needs no env churn.

    Mirrors node's `resolveBroker` (src/config.ts) field-for-field.
    """
    file_broker = file_config.get("broker") or {}
    host = _first_set(
        opts.get("host") if opts else None,
        os.environ.get("INSPIRE_BROKER_HOST"),
        os.environ.get("BROKER_HOST"),
        file_broker.get("host"),
        defaults["host"],
    )
    port = _first_set(
        opts.get("port") if opts else None,
        _env_int("INSPIRE_BROKER_PORT"),
        _env_int("BROKER_PORT"),
        file_broker.get("port"),
        defaults["port"],
    )
    return {"host": host, "port": port}
