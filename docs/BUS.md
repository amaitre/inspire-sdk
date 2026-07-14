# The Fleet Bus — Map, Onboarding, and Tribal Knowledge

> **Read this first.** One page that maps the fleet's MQTT world, routes you to the
> canonical spec for each concept, and writes down everything that previously lived
> only in source code. It does NOT restate the specs — it indexes them and covers
> only what no other doc covers.
>
> **Owner & update trigger:** this file lives with inspire-sdk and MUST be updated in
> the same commit as any change to topics, envelopes, broker/bridge configs, or SDK
> connection behavior (convention: `fleet-ops-centralization/FLEET-REPO-CONVENTIONS.md`).
> Last full audit: 2026-07-13.

## 1. There are TWO buses

They share brokers, vocabulary (`corr_id`, presence, RPC), and the `inspire/` prefix —
but they are different wire contracts. Know which one you're joining.

| | **DA sibling bus** | **inspire-* app bus** |
|---|---|---|
| Purpose | DA↔DA messaging (Ming/Ata/Lyra assistants) | app telemetry, discovery, cross-app RPC |
| Namespace | `inspire/sibling/#` | `inspire/#` (presence/heartbeat/status/log/cmd/manifest/rpc) |
| Envelope | **v3** single generic `Envelope` (v3.1 adds `id`) | **v1** one TypedDict per topic |
| Spec | `docs/sibling-comm-spec.md` (broker section has a 2026-07-13 status update) | envelope: atrium Addendum 2 §4.2 · RPC/manifest/federation: atrium **Addendum 3** |
| Impl | `sdk-python/inspire_sdk/sibling/` (**Python only** — no node impl exists) | `src/` (node) + `sdk-python/inspire_sdk/` (python) |
| Topology | **hub-and-spoke on Ming's broker** — every box connects directly to `192.168.1.156:1883` via `~/.claude/PAI/USER/Config/sibling.yaml` (verified all 3 boxes 2026-07-13) | **federated per-host brokers** bridged through Ata (below) |
| Config | `sibling.yaml` (own format: broker_host/port/tls/user/pass) | both SDKs (parity 2026-07-14): explicit opts → env → `.inspire/config.toml` → localhost |

## 2. App-bus topology (the part three .conf files encode)

```
   Ming ──bridge──▶ Ata ◀──bridge── Lyra          (Ming↔Lyra never talk directly)
 (local broker)  (hub broker)   (local broker)
```

- Every box runs mosquitto; **apps always connect to their own host's broker** (localhost).
- **Ata is the hub**: `1883` = loopback-only, anonymous (local apps). `1884` = the ONLY
  LAN-reachable listener, auth required (`per_listener_settings`, password file, user
  `inspire-bridge`). Configs: `inspire-atrium/deploy/mosquitto/` (+ its README runbook).
- **Ming's bridge deliberately excludes `inspire/sibling/#`** (enumerated app topics only)
  so the sibling bus never crosses onto the app federation. **Lyra's bridge forwards
  `inspire/#` wholesale** — asymmetric; cosmetic today (no sibling client publishes to
  Lyra's local broker — all sibling clients dial Ming directly), tracked for alignment.
- Bridges: `try_private true`, retained messages DO propagate across bridges (2-hop
  verified by inspire-fleet-test), so the same slug on two hosts legitimately yields two
  `slug|nodeId` rows — consumers must expect duplicates.
- Firewall note: `inspire-utility/firewall/` scripts reference a `10.10.10.0/24` PAI LAN;
  the deployed bridges use `192.168.1.x`. Reconcile before reusing those scripts.

## 3. Canonical doc directory (concept → current truth)

| Concept | Canonical source |
|---|---|
| App-bus envelope (v1 types) | atrium Addendum 2 **§4.2 only** + `src/types.ts` / `_types.py` |
| App-bus RPC + capability manifests | atrium **Addendum 3** §2–3 |
| Federation topology + broker auth | atrium **Addendum 3** §1/§5 + `deploy/mosquitto/README.md` |
| Supervisor / on-demand activation | atrium Addendum 3 §4 (design; no ops doc yet) |
| Sibling envelope v3 + channels | `docs/sibling-comm-spec.md` |
| Cross-box reliability behavior (partition, LWT, retained) | `inspire-fleet-test` README + FINDINGS.md |
| Live bus defects & verified-good behaviors | `inspire-fleet-test/FINDINGS.md` |
| Fleet repo/deploy conventions | `fleet-ops-centralization/FLEET-REPO-CONVENTIONS.md` |

## 4. Adding an app to the app bus (onboarding)

1. **Node**: `Inspire.start({slug, version, serviceMode?})` · **Python**:
   `Inspire.start(slug=..., version=...)`. Broker resolution is IDENTICAL in both
   (python parity landed 2026-07-14): explicit opts/`broker=` → `INSPIRE_BROKER_HOST/PORT`
   → `BROKER_HOST/PORT` → `.inspire/config.toml` → `127.0.0.1:1883`
   (node `src/config.ts`, python `_config.py`).
2. You get for free: retained presence (+LWT), 10s heartbeat, `setStatus`/`set_status`
   (retained), `log`.
3. **Expose verbs**: `onCall(verb, handler, spec)` / `on_call(...)` — each registration
   (re)publishes your retained capability manifest; atrium's ManifestRegistry discovers
   you via `inspire/manifest/+/+` and routes `findByVerb`.
4. **Pick your error style consciously**: some fleet apps return `{error}` payloads
   (quant/financial/investment) and never throw; a thrown handler becomes
   `RpcResponseMsg{code: HANDLER_ERROR}`. Match the callee you're integrating with.
5. Run as `systemd --user` on the owning box (patterns: inspire-contacts, inspire-quant).
6. **Verify** (all retained, so instant):
   `mosquitto_sub -h 127.0.0.1 -t 'inspire/presence/<slug>/+' -v -W 2` (expect payload) ·
   same for `inspire/manifest/<slug>/+` · cross-box: run the same sub on Ata.
   Then `fleet-doctor` and/or `support agent` RPC `health`.
7. **Don't** hand-roll envelopes with raw paho/mqtt clients — use the SDK (a historical
   bypass exists in inspire-live-music's broadcaster; don't copy it).

## 5. Tribal knowledge (code-verified 2026-07-13; file refs are the probes)

- **LWT covers presence ONLY** (one will per connection). A crashed app leaves a **stale
  retained manifest**; atrium's ManifestRegistry evicts it when presence goes null
  (`manifestRegistry.ts:74-78`). The supervisor does NOT read manifests — presence only.
- **Retained-clear ordering on graceful stop is manifest→presence in BOTH SDKs**
  (`src/index.ts` · `_client.py`; python matched node 2026-07-14) — presence-first
  would leave a dead-but-advertised window where the app reads "live" while its verbs
  are already gone. Offline stop skips retained-clear entirely (LWT handles it).
- **Heartbeat does not fire immediately on start** — consumers use a freshness window,
  not a count (`index.ts:218-220`).
- **RPC timeout matrix**: app client 8s (`index.ts:70`) · observer/hub 20s (`bus.ts:40`,
  atrium sets 20s) · python 8.0s (`_client.py:61`). RPC is fail-closed under partition.
- **Known wire quirks**: `cpu_pct` is hardcoded 0 in both SDKs; `rss_mb` is *current* RSS
  in both (python reads `/proc/self/status` VmRSS since 2026-07-14, falling back to peak
  ru_maxrss only where /proc is unavailable); `CommandMsg` is stricter in node
  (verb union + required fields) than python.
- **`inspire/log/#` is write-only today** — both SDKs publish it, nothing subscribes.
- **`inspire/sibling/principal/broadcast`** is a declared-but-never-used dead constant.
- **clientId conventions** (collision-relevant): app `<slug>-<nodeId>-<pid>` · observer
  `consumer-<hostSlug>-<pid>` · atrium caller `atrium-<nodeId>` · sibling `<user>-<pid>-<8hex>`.
- **Protocol**: MQTT 3.1.1 everywhere; node reconnect 2s (fleet-test pins 0 in assertions).

## 6. Debugging the bus live (operator cheat sheet)

```bash
# Who's on the app bus right now (retained → instant):
mosquitto_sub -h 127.0.0.1 -t 'inspire/presence/+/+' -v -W 2
# What can they do:
mosquitto_sub -h 127.0.0.1 -t 'inspire/manifest/+/+' -v -W 2
# Is a specific app heartbeating (10s cadence, wait ≥12s):
mosquitto_sub -h 127.0.0.1 -t 'inspire/heartbeat/<slug>/+' -v -W 13
# Fleet-wide view: run the above ON ATA (the hub sees everything the bridges forward).
# Sibling bus (note: ON MING, it is a different bus):
mosquitto_sub -h 192.168.1.156 -t 'inspire/sibling/presence/+' -v -W 2   # needs creds from sibling.yaml
# Automated: fleet-doctor (fleet-ops-centralization) · support agent RPC: health/diagnose
```

Gotchas when things look wrong: a "live" manifest with no presence = crashed app awaiting
eviction (§5); an app visible on its own box but not on Ata = bridge down (`systemctl
status mosquitto` + bridge conf host); a python app "ignoring" `INSPIRE_BROKER_HOST` =
running a pre-2026-07-14 install — the SDK honors it now (§4.1), so reinstall/upgrade
the app's vendored inspire-sdk.
