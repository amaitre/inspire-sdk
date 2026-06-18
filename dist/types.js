"use strict";
// Wire-format types for the inspire-* MQTT bus, per spec §4.2 of
// `INSPIRE_ATRIUM_SPEC_ADDENDUM_2.md`. This module is the CANONICAL source of
// truth for the wire contract: atrium re-exports these from `inspire-sdk`
// (see src/main/inspire/types.ts) instead of maintaining a parallel copy, and
// the Python SDK (sdk-python/inspire_sdk/_types.py) mirrors them — field parity
// is enforced by the cross-language conformance test (test/conformance.spec.ts).
//
// `AtriumPresenceMsg` stays atrium-local — only atrium publishes it.
Object.defineProperty(exports, "__esModule", { value: true });
//# sourceMappingURL=types.js.map