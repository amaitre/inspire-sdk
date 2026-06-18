"use strict";
// inspire-sdk — Node SDK for inspire-* apps to participate in the
// inspire-atrium MQTT messaging fabric. Wire format per spec §4.2 of
// `INSPIRE_ATRIUM_SPEC_ADDENDUM_2.md`.
//
// Usage (the 5-line "near boot" snippet from the migration guide):
//
//   import { Inspire } from 'inspire-sdk'
//   const client = await Inspire.start({
//     slug: 'inspire-financial',
//     version: '0.4.2',
//   })
//   await client.setStatus('ready', 'all systems online')
//   process.on('SIGTERM', () => client.stop())
//
// What `start()` does:
//   1. Connects to broker (defaults to `127.0.0.1:1883` per migration guide).
//   2. Configures Last Will & Testament: empty retained payload on
//      `inspire/presence/<slug>/<nodeId>` so atrium auto-cleans on crash
//      (spec §4.1).
//   3. Publishes retained PresenceMsg.
//   4. Starts a 10s heartbeat interval (overridable for tests).
//   5. Subscribes to `inspire/cmd/<slug>/<nodeId>` for atrium → app commands.
//
// What `stop()` does:
//   1. Clears the heartbeat interval.
//   2. Publishes empty retained payloads to clear the manifest, THEN presence
//      (manifest-first so the API surface retires before the liveness row).
//   3. Disconnects.
//
// Slice E will add `forwardClaudeSession()`. NOT in this slice.
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.topics = exports.createBusClient = exports.resolveBroker = exports.loadInspireConfig = exports.findInspireConfigPath = exports.Inspire = void 0;
const node_os_1 = __importDefault(require("node:os"));
const mqtt_1 = __importDefault(require("mqtt"));
const bus_1 = require("./bus");
const config_1 = require("./config");
const topics_1 = require("./topics");
const DEFAULT_BROKER_HOST = '127.0.0.1';
const DEFAULT_BROKER_PORT = 1883;
const DEFAULT_HEARTBEAT_MS = 10_000;
const DEFAULT_RPC_TIMEOUT_MS = 8_000;
let rpcCounter = 0;
/** Process-unique, time-free correlation id (Date.now-free for determinism in tests). */
function newCorrId() {
    rpcCounter += 1;
    return `${process.pid.toString(36)}-${rpcCounter.toString(36)}`;
}
class InspireClientImpl {
    slug;
    nodeId;
    client;
    startedAt;
    serviceMode;
    version;
    heartbeatIntervalMs;
    heartbeatTimer = null;
    commandHandlers = new Map();
    commandMessageHandler = null;
    stopped = false;
    // RPC + manifest state
    rpcHandlers = new Map();
    verbSpecs = new Map();
    rpcServerHandler = null;
    /** reply_to id this instance uses when it acts as an RPC caller. */
    replyTo;
    /** Pending outbound calls awaiting a reply, keyed by corr_id. */
    pendingCalls = new Map();
    rpcReplyHandler = null;
    rpcReplySubscribed = false;
    constructor(opts) {
        this.slug = opts.slug;
        this.nodeId = opts.nodeId;
        this.version = opts.version;
        this.serviceMode = opts.serviceMode;
        this.client = opts.client;
        this.heartbeatIntervalMs = opts.heartbeatIntervalMs;
        this.startedAt = new Date().toISOString();
        this.replyTo = `${opts.slug}-${opts.nodeId}-${process.pid}`;
    }
    get verbs() {
        return [...this.verbSpecs.values()];
    }
    /** Internal: publish initial retained presence. */
    publishPresence() {
        const msg = {
            v: 1,
            app_slug: this.slug,
            node_id: this.nodeId,
            version: this.version,
            started_at: this.startedAt,
            pid: process.pid,
            service_mode: this.serviceMode,
        };
        return new Promise((resolve, reject) => {
            this.client.publish((0, topics_1.topicPresence)(this.slug, this.nodeId), JSON.stringify(msg), { qos: 1, retain: true }, (err) => (err ? reject(err) : resolve()));
        });
    }
    startHeartbeat() {
        if (this.heartbeatTimer)
            return;
        const tick = () => {
            const memMb = Math.round(process.memoryUsage().rss / (1024 * 1024));
            const msg = {
                v: 1,
                ts: new Date().toISOString(),
                uptime_s: Math.floor(process.uptime()),
                rss_mb: memMb,
                // cpu_pct: cheap approximation. Better implementations will
                // sample process.cpuUsage() deltas across an interval. Slice A
                // doesn't gate on accuracy here.
                cpu_pct: 0,
            };
            this.client.publish((0, topics_1.topicHeartbeat)(this.slug, this.nodeId), JSON.stringify(msg), { qos: 0, retain: false });
        };
        // Don't fire immediately — the receiver already has the presence msg
        // and atrium uses a freshness window, not a hb count.
        this.heartbeatTimer = setInterval(tick, this.heartbeatIntervalMs);
        if (typeof this.heartbeatTimer.unref === 'function')
            this.heartbeatTimer.unref();
    }
    subscribeCommands() {
        const cmdTopic = (0, topics_1.topicCmd)(this.slug, this.nodeId);
        this.client.subscribe(cmdTopic, { qos: 1 });
        // Capture the handler so stop() can remove it. Without this, calling
        // Inspire.start() multiple times in one process (e.g. test fixtures)
        // accumulates 'message' listeners that survive each stop().
        const handler = (topic, payload) => {
            if (topic !== cmdTopic)
                return;
            try {
                const parsed = JSON.parse(payload.toString('utf8'));
                const fn = this.commandHandlers.get(parsed.cmd);
                if (fn)
                    fn(parsed);
            }
            catch {
                /* malformed — ignore */
            }
        };
        this.commandMessageHandler = handler;
        this.client.on('message', handler);
    }
    // ── capability manifest ──
    /** Publish the retained capability manifest built from registered verbs. */
    publishManifest() {
        const msg = {
            v: 1,
            app_slug: this.slug,
            node_id: this.nodeId,
            version: this.version,
            ts: new Date().toISOString(),
            verbs: this.verbs,
        };
        return new Promise((resolve, reject) => {
            this.client.publish((0, topics_1.topicManifest)(this.slug, this.nodeId), JSON.stringify(msg), { qos: 1, retain: true }, (err) => (err ? reject(err) : resolve()));
        });
    }
    // ── RPC server (inbound: this app answers verbs) ──
    /** Subscribe the inbound RPC call topic once; dispatch to registered handlers. */
    subscribeRpc() {
        if (this.rpcServerHandler)
            return;
        const callTopic = (0, topics_1.topicRpcCall)(this.slug, this.nodeId);
        this.client.subscribe(callTopic, { qos: 1 });
        const handler = (topic, payload) => {
            if (topic !== callTopic)
                return;
            let req;
            try {
                req = JSON.parse(payload.toString('utf8'));
            }
            catch {
                return; // malformed — ignore
            }
            void this.dispatchRpc(req);
        };
        this.rpcServerHandler = handler;
        this.client.on('message', handler);
    }
    async dispatchRpc(req) {
        const replyTopic = (0, topics_1.topicRpcReply)(req.reply_to, req.corr_id);
        const fn = this.rpcHandlers.get(req.verb);
        let res;
        if (!fn) {
            res = {
                v: 1,
                corr_id: req.corr_id,
                ok: false,
                error: { message: `unknown verb: ${req.verb}`, code: 'UNKNOWN_VERB' },
                ts: new Date().toISOString(),
            };
        }
        else {
            try {
                const result = await fn(req.args ?? {}, req);
                res = { v: 1, corr_id: req.corr_id, ok: true, result, ts: new Date().toISOString() };
            }
            catch (e) {
                res = {
                    v: 1,
                    corr_id: req.corr_id,
                    ok: false,
                    error: { message: e instanceof Error ? e.message : String(e), code: 'HANDLER_ERROR' },
                    ts: new Date().toISOString(),
                };
            }
        }
        this.client.publish(replyTopic, JSON.stringify(res), { qos: 1, retain: false });
    }
    onCall(verb, handler, spec) {
        const isNew = !this.verbSpecs.has(verb);
        this.rpcHandlers.set(verb, handler);
        this.verbSpecs.set(verb, { name: verb, ...spec });
        this.subscribeRpc();
        // Republish the manifest so discovery reflects the new verb. Best-effort:
        // if not yet connected, start() publishes the full manifest anyway.
        if (this.client.connected && (isNew || spec)) {
            void this.publishManifest().catch(() => { });
        }
    }
    // ── RPC client (outbound: this app calls another app's verb) ──
    /** Lazily subscribe this caller's reply wildcard and install the demux handler. */
    ensureReplySubscription() {
        if (this.rpcReplySubscribed)
            return;
        this.rpcReplySubscribed = true;
        this.client.subscribe((0, topics_1.topicRpcReplyWildcard)(this.replyTo), { qos: 1 });
        const handler = (topic, payload) => {
            if (!topic.startsWith(`inspire/rpc/_reply/${this.replyTo}/`))
                return;
            let res;
            try {
                res = JSON.parse(payload.toString('utf8'));
            }
            catch {
                return;
            }
            const pending = this.pendingCalls.get(res.corr_id);
            if (!pending)
                return;
            clearTimeout(pending.timer);
            this.pendingCalls.delete(res.corr_id);
            if (res.ok)
                pending.resolve(res.result);
            else
                pending.reject(new Error(res.error?.message ?? 'rpc error'));
        };
        this.rpcReplyHandler = handler;
        this.client.on('message', handler);
    }
    call(target, verb, args = {}, opts) {
        this.ensureReplySubscription();
        const corrId = newCorrId();
        const timeoutMs = opts?.timeoutMs ?? DEFAULT_RPC_TIMEOUT_MS;
        const req = {
            v: 1,
            corr_id: corrId,
            reply_to: this.replyTo,
            verb,
            args,
            ts: new Date().toISOString(),
        };
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                this.pendingCalls.delete(corrId);
                reject(new Error(`rpc call ${target.slug}/${target.nodeId}.${verb} timed out after ${timeoutMs}ms`));
            }, timeoutMs);
            if (typeof timer.unref === 'function')
                timer.unref();
            this.pendingCalls.set(corrId, { resolve, reject, timer });
            this.client.publish((0, topics_1.topicRpcCall)(target.slug, target.nodeId), JSON.stringify(req), { qos: 1, retain: false }, (err) => {
                if (err) {
                    clearTimeout(timer);
                    this.pendingCalls.delete(corrId);
                    reject(err);
                }
            });
        });
    }
    setStatus(state, detail) {
        const msg = { v: 1, state, detail, ts: new Date().toISOString() };
        return new Promise((resolve, reject) => {
            this.client.publish((0, topics_1.topicStatus)(this.slug, this.nodeId), JSON.stringify(msg), { qos: 1, retain: true }, (err) => (err ? reject(err) : resolve()));
        });
    }
    log(level, msg, fields) {
        const payload = { v: 1, ts: new Date().toISOString(), level, msg, fields };
        return new Promise((resolve, reject) => {
            this.client.publish((0, topics_1.topicLog)(this.slug, this.nodeId), JSON.stringify(payload), { qos: 0, retain: false }, (err) => (err ? reject(err) : resolve()));
        });
    }
    onCommand(cmd, handler) {
        this.commandHandlers.set(cmd, handler);
    }
    async stop() {
        if (this.stopped)
            return;
        this.stopped = true;
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }
        if (this.commandMessageHandler) {
            this.client.off('message', this.commandMessageHandler);
            this.commandMessageHandler = null;
        }
        if (this.rpcServerHandler) {
            this.client.off('message', this.rpcServerHandler);
            this.rpcServerHandler = null;
        }
        if (this.rpcReplyHandler) {
            this.client.off('message', this.rpcReplyHandler);
            this.rpcReplyHandler = null;
        }
        // Fail any in-flight outbound calls so callers don't hang on shutdown.
        for (const [, p] of this.pendingCalls) {
            clearTimeout(p.timer);
            p.reject(new Error('inspire-sdk client stopped'));
        }
        this.pendingCalls.clear();
        // Only attempt the retained-clear publish if we're actually connected.
        // Offline, mqtt's publish callback never fires (it queues), and
        // `client.end(true)` will flush+drop the queue — atrium would then keep
        // showing the row until LWT eventually triggers from the broker side.
        // Skipping the publish when offline is correct: LWT (set up at connect
        // time) handles the cleanup on the broker's reconnect-and-disconnect.
        if (this.client.connected) {
            // Clear the retained manifest FIRST, then presence. Order matters: the
            // manifest advertises the app's API surface and presence is the liveness
            // row. Retiring presence first leaves a window where the app reads "live"
            // while still advertising verbs that are already gone — a caller could
            // dispatch into a vanished target. Manifest-first closes that window.
            // (LWT only covers presence, so the manifest clear is graceful-only.)
            await new Promise((resolve) => {
                this.client.publish((0, topics_1.topicManifest)(this.slug, this.nodeId), Buffer.alloc(0), { qos: 1, retain: true }, () => resolve());
            });
            await new Promise((resolve) => {
                this.client.publish((0, topics_1.topicPresence)(this.slug, this.nodeId), Buffer.alloc(0), { qos: 1, retain: true }, () => resolve());
            });
        }
        await new Promise((resolve) => {
            this.client.end(true, {}, () => resolve());
        });
    }
    /** Internal: complete startup. Resolves when ready to receive commands. */
    async start() {
        await this.publishPresence();
        this.startHeartbeat();
        this.subscribeCommands();
        this.subscribeRpc();
        await this.publishManifest();
    }
}
exports.Inspire = {
    /**
     * Start an inspire-sdk session: connect to the broker, publish presence
     * retained, start the 10s heartbeat, subscribe to commands. Resolves to
     * an `InspireClient` once the initial presence has been published (the
     * point at which atrium can render the row).
     */
    async start(opts) {
        const slug = opts.slug;
        if (!slug)
            throw new Error('Inspire.start: slug is required');
        const nodeId = opts.nodeId ?? (0, topics_1.slugifyNodeId)(node_os_1.default.hostname());
        // Resolve broker via opts > env > .inspire/config.toml > defaults so the
        // documented config.toml override finally works and consumers can drop
        // their hand-rolled broker plumbing. Skippable with loadConfig: false.
        const fileConfig = opts.loadConfig === false ? {} : (0, config_1.loadInspireConfig)();
        const { host, port } = (0, config_1.resolveBroker)(opts.broker, fileConfig, {
            host: DEFAULT_BROKER_HOST,
            port: DEFAULT_BROKER_PORT,
        });
        const heartbeatIntervalMs = opts.heartbeatIntervalMs ?? fileConfig.reporting?.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_MS;
        const serviceMode = opts.serviceMode ?? false;
        const connectOpts = {
            host,
            port,
            protocolVersion: 4,
            clean: true,
            clientId: opts.clientId ?? `${slug}-${nodeId}-${process.pid}`,
            reconnectPeriod: opts.reconnectPeriod ?? 2_000,
            connectTimeout: opts.connectTimeout ?? 8_000,
            // LWT — empty retained payload so atrium drops the row on crash.
            will: {
                topic: (0, topics_1.topicPresence)(slug, nodeId),
                payload: Buffer.alloc(0),
                qos: 1,
                retain: true,
            },
        };
        const client = mqtt_1.default.connect(connectOpts);
        try {
            await new Promise((resolve, reject) => {
                const timer = setTimeout(() => reject(new Error('Inspire.start: connect timed out')), connectOpts.connectTimeout);
                client.once('connect', () => {
                    clearTimeout(timer);
                    resolve();
                });
                client.once('error', (err) => {
                    clearTimeout(timer);
                    reject(err);
                });
            });
        }
        catch (err) {
            // Without an explicit end(), mqtt's default reconnectPeriod keeps the
            // failed client retrying in the background while the caller already
            // received a rejection — silent connection leak.
            client.end(true);
            throw err;
        }
        const inst = new InspireClientImpl({
            slug,
            nodeId,
            version: opts.version,
            serviceMode,
            client,
            heartbeatIntervalMs,
        });
        await inst.start();
        return inst;
    },
    /**
     * Create a consumer/hub bus client (observer + caller): subscribes to every
     * app's presence/heartbeat/status/manifest as typed events and can `call()`
     * any app's RPC verb. Optionally publishes a retained self-presence (hubs).
     * Returns immediately, NOT connected — wire up `.on(...)` then `.connect()`.
     */
    observe(opts = {}) {
        return (0, bus_1.createBusClient)(opts);
    },
};
var config_2 = require("./config");
Object.defineProperty(exports, "findInspireConfigPath", { enumerable: true, get: function () { return config_2.findInspireConfigPath; } });
Object.defineProperty(exports, "loadInspireConfig", { enumerable: true, get: function () { return config_2.loadInspireConfig; } });
Object.defineProperty(exports, "resolveBroker", { enumerable: true, get: function () { return config_2.resolveBroker; } });
var bus_2 = require("./bus");
Object.defineProperty(exports, "createBusClient", { enumerable: true, get: function () { return bus_2.createBusClient; } });
exports.topics = __importStar(require("./topics"));
//# sourceMappingURL=index.js.map