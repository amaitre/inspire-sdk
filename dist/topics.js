"use strict";
// Canonical inspire-* MQTT topic constructors + node-id slugifier.
//
// Single source of truth for topic strings, per spec §4. Both the app client
// (index.ts) and the consumer/bus client (bus.ts) build topics from here so a
// wire-format topic change happens once. Consumers across the suite (atrium,
// inspire-projects) should import these rather than re-declaring them.
Object.defineProperty(exports, "__esModule", { value: true });
exports.TOPIC_WILDCARD = void 0;
exports.slugifyNodeId = slugifyNodeId;
exports.topicPresence = topicPresence;
exports.topicHeartbeat = topicHeartbeat;
exports.topicStatus = topicStatus;
exports.topicLog = topicLog;
exports.topicCmd = topicCmd;
exports.topicManifest = topicManifest;
exports.topicRpcCall = topicRpcCall;
exports.topicRpcReply = topicRpcReply;
exports.topicRpcReplyWildcard = topicRpcReplyWildcard;
/** Slugify a hostname per spec §4: lowercase, non-alphanumerics → '-'. */
function slugifyNodeId(hostname) {
    const lowered = (hostname ?? '').toLowerCase();
    const dashed = lowered.replace(/[^a-z0-9]+/g, '-');
    const trimmed = dashed.replace(/^-+|-+$/g, '');
    return trimmed.length > 0 ? trimmed : 'unknown';
}
function topicPresence(slug, nodeId) {
    return `inspire/presence/${slug}/${nodeId}`;
}
function topicHeartbeat(slug, nodeId) {
    return `inspire/heartbeat/${slug}/${nodeId}`;
}
function topicStatus(slug, nodeId) {
    return `inspire/status/${slug}/${nodeId}`;
}
function topicLog(slug, nodeId) {
    return `inspire/log/${slug}/${nodeId}`;
}
function topicCmd(slug, nodeId) {
    return `inspire/cmd/${slug}/${nodeId}`;
}
function topicManifest(slug, nodeId) {
    return `inspire/manifest/${slug}/${nodeId}`;
}
/** Inbound RPC requests for an app instance. */
function topicRpcCall(slug, nodeId) {
    return `inspire/rpc/${slug}/${nodeId}/call`;
}
/** Reply channel keyed by the caller's reply_to id + corr_id. */
function topicRpcReply(replyTo, corrId) {
    return `inspire/rpc/_reply/${replyTo}/${corrId}`;
}
/** A caller subscribes this wildcard to receive all its RPC replies. */
function topicRpcReplyWildcard(replyTo) {
    return `inspire/rpc/_reply/${replyTo}/+`;
}
/** Wildcard subscriptions a consumer/hub uses to observe the whole bus. */
exports.TOPIC_WILDCARD = {
    presence: 'inspire/presence/+/+',
    heartbeat: 'inspire/heartbeat/+/+',
    status: 'inspire/status/+/+',
    manifest: 'inspire/manifest/+/+',
};
//# sourceMappingURL=topics.js.map