---
profile: bun_pytest_quality
mode: execute
generated_at: 2026-08-15
score:
  measured:
    test_count: 33
    pass_count: 33
    fail_count: 0
    coverage_lines_pct: null
    coverage_branches_pct: null
  inferred:
    gap_count: 4
    risk_level: low
---

## TL;DR

**Measured (gated command): 33/33 node.** Full picture — 63/63 across both languages (run 2026-08-15, Ming): Node **33/33** via `bun test test/` (6 files, 66 expects, ~1.3s, in-process aedes broker) + Python **30/30** via pytest (3 files, ~12.5s, against a real spawned Mosquitto). The Python run used the inspire-live-music venv for paho/pytest with `PYTHONPATH` pinning this repo's `sdk-python/` — import location verified (`inspire_sdk.__file__` → repo tree, not the venv's installed v0.2.0), so the numbers measure the working tree. Coverage is not wired in either language — pass/fail only. **Risk: low.**

## Inventory

- **Node runner:** `bun:test`, command `bun test test/` (package.json `scripts.test`). Files: `bus`, `config`, `config-integration`, `conformance`, `inspire`, `rpc` specs. Broker: in-process aedes on a random port — no Mosquitto needed.
- **Python runner:** pytest, `sdk-python/tests/` (`test_config`, `test_inspire`, `test_sibling`). `conftest.py` spawns a real Mosquitto per session, so retain/LWT semantics are tested against the production broker implementation. Sibling tests are broker-agnostic (envelope round-trip, topic strings, matcher) and don't require aiomqtt.
- **Cross-language:** `test/conformance.spec.ts` captures the Node SDK's actual emissions (presence/heartbeat/status/manifest) over a live broker and diffs field sets against Python's parsed TypedDicts.
- **CI:** `.github/workflows/ci.yml` runs both jobs (bun build+test; apt-installed mosquitto + pytest) on push/PR — the only inspire-* repo with CI.

## Findings

- Both suites green with zero flakes on this run; the Python suite's real-broker fixture makes it the stronger of the two for wire semantics (LWT, retained clears).
- The conformance test guards structure for the four telemetry envelopes; Command/Log/Rpc envelopes are parity-by-mirroring only, not emission-captured.
- Tests at HEAD exercise the post-v0.3.0 parity code (broker env resolution, stop ordering) that no released tag yet contains — green here does not mean consumers on `#v0.3.0` run this code.

## Gaps

1. No coverage measurement in either language; numbers are pass/fail only. Category: unit
2. No in-repo Python venv — local pytest runs depend on a borrowed environment or CI; a `sdk-python/.venv` bootstrap would make the run reproducible on any box. Category: infrastructure
3. Conformance capture covers 4 of 8 wire message types; RpcRequest/RpcResponse/Command/Log parity rests on the mirrored declarations alone. Category: integration
4. The aiomqtt live path of `sibling/` (connect, subscribe stream, RPC) is untested — only its pure envelope/topic layer is covered. Category: integration
