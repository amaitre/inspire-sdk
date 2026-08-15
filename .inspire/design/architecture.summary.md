---
view: summary
source: architecture.md
summarized_at: 2026-08-15T18:40:00+00:00
summarizer_version: 1
---

# inspire-sdk — Design Summary

## TL;DR

The suite's shared bus SDK — a library, not an app. One repo, two languages versioning under one git tag: the Node/TS root is the canonical wire contract (v1 envelopes in `types.ts`, single-source topic constructors in `topics.ts`), the Python package mirrors it byte-for-byte, and a cross-language conformance test captures Node's *actual* broker emissions and diffs them against Python's TypedDict fields. `docs/BUS.md` is the fleet-bus front door. It is the only inspire-* repo with CI.

## Stack at a glance

Node: `mqtt` 5.x + smol-toml, built with tsc, tested on an in-process aedes broker. Python (>=3.10 for the Jetson): paho-mqtt sync core, aiomqtt as the `[sibling]` optional extra, pytest against a real spawned Mosquitto. Nine node repos pin `#v0.3.0`; live-music pins python v0.2.0 (known skew).

## Architecture (high-level)

Two client roles: `Inspire.start()` gives an app retained presence with a crash-clearing LWT, a 10s heartbeat, retained status, an RPC server, and a retained capability manifest that republishes on every `onCall()` — discovery is one wildcard subscribe. `Inspire.observe()` is the consumer/hub role (typed events + `call()`), extracted from the ~280-line state machines atrium and inspire-projects each hand-rolled. RPC correlates on `corr_id`, subscribes the reply channel before publishing, and fails closed (8s app / 20s hub). Graceful stop clears manifest→presence in that order to avoid a dead-but-advertised window; on crash, LWT covers presence only and the hub evicts the stale manifest. The Python-only `sibling/` layer is a separate wire contract entirely (generic v3 Envelope, aiomqtt, `sibling.yaml`, hub-and-spoke) for DA-to-DA messaging.

## Pain points

- Python parity fixes (broker env resolution, stop ordering, rss_mb) sit untagged after v0.3.0 — BUS.md's "upgrade to fix" gotcha points at a release that doesn't exist
- live-music's python v0.2.0 pin vs node's v0.3.0
- README "Install" section and a `forwardClaudeSession` comment still describe the pre-extraction monorepo
- `cpu_pct` hardcoded 0; log topic write-only; BusClient events typed by convention only; no coverage wiring; version string triplicated

Full detail + refactoring proposals: architecture.md.
