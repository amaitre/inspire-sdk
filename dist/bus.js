"use strict";
// Consumer / hub bus client for inspire-* — the OBSERVER + CALLER role.
//
// Where `Inspire.start()` is for an app that announces itself (presence,
// heartbeat, command/RPC server), `Inspire.observe()` is for a consumer that
// watches the whole bus and invokes verbs on others: it subscribes to every
// app's presence/heartbeat/status/manifest, re-emits them as typed events, and
// can `call()` any app's RPC verb. Optionally it publishes a single retained
// self-presence (with an LWT clear) — used by hubs like atrium.
//
// This is the state machine atrium and inspire-projects each hand-rolled
// (~280 lines apiece, kept in sync with the contract by prose). It now lives
// in the SDK once. Event signatures match atrium's InspireBus so adoption is a
// drop-in swap.
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createBusClient = createBusClient;
const node_events_1 = require("node:events");
const node_os_1 = __importDefault(require("node:os"));
const mqtt_1 = __importDefault(require("mqtt"));
const config_1 = require("./config");
const topics_1 = require("./topics");
const DEFAULT_BROKER_HOST = '127.0.0.1';
const DEFAULT_BROKER_PORT = 1883;
const DEFAULT_RPC_TIMEOUT_MS = 20_000;
class BusClientImpl extends node_events_1.EventEmitter {
    client = null;
    connectedOnce = false;
    shuttingDown = false;
    host;
    port;
    clientId;
    connectTimeout;
    reconnectPeriod;
    rpcReplyTo;
    rpcTimeoutMs;
    selfPresence;
    rpcCounter = 0;
    pendingCalls = new Map();
    constructor(opts) {
        super();
        const fileConfig = opts.loadConfig === false ? {} : (0, config_1.loadInspireConfig)();
        const resolved = (0, config_1.resolveBroker)(opts.broker, fileConfig, {
            host: DEFAULT_BROKER_HOST,
            port: DEFAULT_BROKER_PORT,
        });
        this.host = resolved.host;
        this.port = resolved.port;
        const hostSlug = (0, topics_1.slugifyNodeId)(node_os_1.default.hostname());
        this.clientId = opts.clientId ?? `consumer-${hostSlug}-${process.pid}`;
        this.connectTimeout = opts.connectTimeout ?? 8_000;
        this.reconnectPeriod = opts.reconnectPeriod ?? 2_000;
        this.rpcReplyTo = opts.callerId ?? `consumer-${hostSlug}`;
        this.rpcTimeoutMs = opts.rpcTimeoutMs ?? DEFAULT_RPC_TIMEOUT_MS;
        this.selfPresence = opts.selfPresence;
    }
    connect() {
        if (this.client)
            return; // idempotent
        const connectOpts = {
            host: this.host,
            port: this.port,
            clientId: this.clientId,
            protocolVersion: 4,
            clean: true,
            reconnectPeriod: this.reconnectPeriod,
            connectTimeout: this.connectTimeout,
        };
        if (this.selfPresence) {
            // LWT: empty retained payload clears the self-presence on ungraceful exit.
            connectOpts.will = {
                topic: this.selfPresence.topic,
                payload: Buffer.alloc(0),
                qos: 1,
                retain: true,
            };
        }
        const client = mqtt_1.default.connect(connectOpts);
        this.client = client;
        client.on('connect', () => {
            this.connectedOnce = true;
            this.publishSelfPresence();
            this.subscribeBusTopics();
            this.emit('connect');
        });
        client.on('reconnect', () => this.emit('connecting'));
        client.on('close', () => {
            if (!this.shuttingDown)
                this.emit('disconnect');
        });
        client.on('offline', () => this.emit('disconnect'));
        client.on('error', (err) => this.emit('error', err));
        client.on('message', (topic, payload) => this.dispatchMessage(topic, payload));
    }
    hasEverConnected() {
        return this.connectedOnce;
    }
    publishCommand(slug, nodeId, msg) {
        if (!this.client)
            return;
        this.client.publish((0, topics_1.topicCmd)(slug, nodeId), JSON.stringify(msg), { qos: 1, retain: false });
    }
    call(target, verb, args = {}, opts) {
        const client = this.client;
        if (!client)
            return Promise.reject(new Error('inspire bus not connected'));
        const corrId = `${process.pid.toString(36)}-${(this.rpcCounter += 1).toString(36)}`;
        const timeoutMs = opts?.timeoutMs ?? this.rpcTimeoutMs;
        const req = {
            v: 1,
            corr_id: corrId,
            reply_to: this.rpcReplyTo,
            verb,
            args,
            ts: new Date().toISOString(),
        };
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                this.pendingCalls.delete(corrId);
                reject(new Error(`rpc ${target.slug}/${target.nodeId}.${verb} timed out after ${timeoutMs}ms`));
            }, timeoutMs);
            this.pendingCalls.set(corrId, { resolve, reject, timer });
            client.publish((0, topics_1.topicRpcCall)(target.slug, target.nodeId), JSON.stringify(req), { qos: 1, retain: false }, (err) => {
                if (err) {
                    clearTimeout(timer);
                    this.pendingCalls.delete(corrId);
                    reject(err);
                }
            });
        });
    }
    async stop() {
        if (this.shuttingDown)
            return;
        this.shuttingDown = true;
        for (const [, p] of this.pendingCalls) {
            clearTimeout(p.timer);
            p.reject(new Error('inspire bus shutting down'));
        }
        this.pendingCalls.clear();
        const client = this.client;
        this.client = null;
        if (!client)
            return;
        if (this.selfPresence && client.connected) {
            await new Promise((resolve) => {
                try {
                    client.publish(this.selfPresence.topic, Buffer.alloc(0), { qos: 1, retain: true }, () => resolve());
                }
                catch {
                    resolve();
                }
            });
        }
        await new Promise((resolve) => client.end(true, {}, () => resolve()));
    }
    // -------------------------------------------------------------- internals
    publishSelfPresence() {
        if (!this.client || !this.selfPresence)
            return;
        this.client.publish(this.selfPresence.topic, JSON.stringify(this.selfPresence.message()), { qos: 1, retain: true });
    }
    subscribeBusTopics() {
        if (!this.client)
            return;
        this.client.subscribe([
            topics_1.TOPIC_WILDCARD.presence,
            topics_1.TOPIC_WILDCARD.heartbeat,
            topics_1.TOPIC_WILDCARD.status,
            topics_1.TOPIC_WILDCARD.manifest,
            (0, topics_1.topicRpcReplyWildcard)(this.rpcReplyTo),
        ], { qos: 1 }, (err, granted) => {
            if (err) {
                this.emit('error', err);
                return;
            }
            const denied = granted?.find((g) => g.qos === 128);
            if (denied)
                this.emit('error', new Error(`SUBSCRIBE rejected for ${denied.topic}`));
        });
    }
    handleRpcReply(payload) {
        const res = safeJsonParse(payload);
        if (!res)
            return;
        const pending = this.pendingCalls.get(res.corr_id);
        if (!pending)
            return;
        clearTimeout(pending.timer);
        this.pendingCalls.delete(res.corr_id);
        if (res.ok)
            pending.resolve(res.result);
        else
            pending.reject(new Error(res.error?.message ?? 'rpc error'));
    }
    dispatchMessage(topic, payload) {
        const isEmpty = payload.length === 0;
        const parts = topic.split('/');
        if (parts[0] === 'inspire' && parts[1] === 'rpc' && parts[2] === '_reply') {
            this.handleRpcReply(payload);
            return;
        }
        if (parts.length !== 4 || parts[0] !== 'inspire')
            return;
        const kind = parts[1];
        const slug = parts[2];
        const nodeId = parts[3];
        if (!slug || !nodeId)
            return;
        if (kind === 'presence') {
            if (isEmpty) {
                this.emit('presence', null, slug, nodeId);
                return;
            }
            const parsed = safeJsonParse(payload);
            if (parsed)
                this.emit('presence', parsed, slug, nodeId);
            return;
        }
        if (kind === 'heartbeat') {
            if (isEmpty)
                return;
            const parsed = safeJsonParse(payload);
            if (parsed)
                this.emit('heartbeat', parsed, slug, nodeId);
            return;
        }
        if (kind === 'status') {
            if (isEmpty)
                return;
            const parsed = safeJsonParse(payload);
            if (parsed)
                this.emit('status', parsed, slug, nodeId);
            return;
        }
        if (kind === 'manifest') {
            if (isEmpty) {
                this.emit('manifest', null, slug, nodeId);
                return;
            }
            const parsed = safeJsonParse(payload);
            if (parsed)
                this.emit('manifest', parsed, slug, nodeId);
        }
    }
}
function safeJsonParse(buf) {
    try {
        return JSON.parse(buf.toString('utf8'));
    }
    catch {
        return null;
    }
}
/** Create a consumer/hub bus client (observer + caller). Call `.connect()` after wiring events. */
function createBusClient(opts = {}) {
    return new BusClientImpl(opts);
}
//# sourceMappingURL=bus.js.map