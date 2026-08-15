---
slug: inspire-sdk
name: Inspire SDK
tagline: "Node+Python library for the inspire-* MQTT bus: presence, heartbeat,
  capability manifests, RPC, and DA sibling messaging"
status: active
icon_emoji: "🔌"
primary_color: ""
repo_url: https://github.com/amaitre/inspire-sdk
seeded_at: 2026-08-15T18:30:00.000Z
seeded_version: 1
quality_config:
  test_command: "bun test test/"
  test_runner: bun:test
---

## Overview

The suite's shared bus SDK — a library, not an app. One repo, two language
implementations that version together under a single git tag: the Node/TS root
(`src/`) and a Python mirror (`sdk-python/inspire_sdk/`), kept byte-compatible
on the wire by a cross-language conformance test (`test/conformance.spec.ts`).
Apps call `Inspire.start()` to join the app bus (retained presence + LWT, 10s
heartbeat, retained status, capability manifest, RPC server); consumers/hubs
call `Inspire.observe()` for the typed observer + caller role atrium and
inspire-projects previously hand-rolled. The Python package additionally ships
`inspire_sdk.sibling` — the aiomqtt-based v3-envelope DA-to-DA messaging layer
(optional extra). `docs/BUS.md` is the fleet-bus front door: topology, the
two-bus distinction, onboarding, and tribal knowledge. Nine node repos consume
it at `github:amaitre/inspire-sdk#v0.3.0`; it is the only inspire-* repo with CI.

## Tech Stack

- language: TypeScript 5.6.2 — Node SDK source, compiled to CJS in `dist/`
- language: Python >=3.10 — mirror SDK + sibling layer (3.10 floor for Jetson/Lyra)
- library: mqtt 5.10.1 — Node MQTT client (MQTT 3.1.1)
- library: smol-toml 1.6.1 — `.inspire/config.toml` broker resolution
- library: paho-mqtt >=2.0.0 — Python sync client (app bus)
- library: aiomqtt >=2.0.0 — Python async client, `[sibling]` optional extra
- library: tomli / typing_extensions — Python 3.10 backports
- tool: Bun — package manager, script runner, bun:test
- tool: TypeScript compiler — `tsc -p tsconfig.json` build
- tool: aedes 0.51.3 — in-process broker for Node tests (no Mosquitto needed)
- tool: Mosquitto (dev) — real broker spawned per-session by the pytest suite
- tool: hatchling — Python build backend (`sdk-python/pyproject.toml`)
- service: GitHub Actions — `ci.yml` node + python jobs on push/PR

## Design

Dual-implementation library with the Node side as the canonical wire contract:
`src/types.ts` defines the v1 envelopes (one interface per topic), `src/topics.ts`
is the single source of topic strings, and Python's `_types.py` / `_topics.py`
mirror them. Conformance is enforced structurally — the test captures the Node
SDK's actual emissions over an in-process aedes broker and diffs them against
the parsed Python TypedDict fields, so a field added in one language fails the
suite. Two client roles: `index.ts` (app: announce + serve verbs) and `bus.ts`
(consumer/hub: observe everything + call anyone). Broker resolution is identical
in both languages: explicit opts → `INSPIRE_BROKER_HOST/PORT` → `BROKER_HOST/PORT`
→ `.inspire/config.toml` walk-up → `127.0.0.1:1883`. The sibling module is a
deliberately separate surface (v3 generic Envelope, own config file, Python-only).
