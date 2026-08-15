---
view: summary
source: quality.md
summarized_at: 2026-08-15T18:45:00+00:00
summarizer_version: 1
---

# inspire-sdk — Quality Summary

**63/63 pass** (run 2026-08-15, Ming): Node 33/33 (`bun test test/`, in-process aedes, ~1.3s) + Python 30/30 (pytest against a real spawned Mosquitto, ~12.5s; repo tree verified as the import source, not an installed copy). No coverage wiring in either language. Only inspire-* repo with CI (both jobs). Risk **low**.

Gaps: pass/fail only (no coverage); no in-repo Python venv for reproducible local runs; conformance emission-capture covers 4 of 8 wire types (Rpc/Command/Log are parity-by-declaration); sibling's live aiomqtt path untested. Note: green at HEAD includes post-v0.3.0 parity code no released tag contains.
