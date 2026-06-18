// Canonical inspire-* MQTT topic constructors + node-id slugifier.
//
// Single source of truth for topic strings, per spec §4. Both the app client
// (index.ts) and the consumer/bus client (bus.ts) build topics from here so a
// wire-format topic change happens once. Consumers across the suite (atrium,
// inspire-projects) should import these rather than re-declaring them.

/** Slugify a hostname per spec §4: lowercase, non-alphanumerics → '-'. */
export function slugifyNodeId(hostname: string): string {
  const lowered = (hostname ?? '').toLowerCase()
  const dashed = lowered.replace(/[^a-z0-9]+/g, '-')
  const trimmed = dashed.replace(/^-+|-+$/g, '')
  return trimmed.length > 0 ? trimmed : 'unknown'
}

export function topicPresence(slug: string, nodeId: string): string {
  return `inspire/presence/${slug}/${nodeId}`
}
export function topicHeartbeat(slug: string, nodeId: string): string {
  return `inspire/heartbeat/${slug}/${nodeId}`
}
export function topicStatus(slug: string, nodeId: string): string {
  return `inspire/status/${slug}/${nodeId}`
}
export function topicLog(slug: string, nodeId: string): string {
  return `inspire/log/${slug}/${nodeId}`
}
export function topicCmd(slug: string, nodeId: string): string {
  return `inspire/cmd/${slug}/${nodeId}`
}
export function topicManifest(slug: string, nodeId: string): string {
  return `inspire/manifest/${slug}/${nodeId}`
}
/** Inbound RPC requests for an app instance. */
export function topicRpcCall(slug: string, nodeId: string): string {
  return `inspire/rpc/${slug}/${nodeId}/call`
}
/** Reply channel keyed by the caller's reply_to id + corr_id. */
export function topicRpcReply(replyTo: string, corrId: string): string {
  return `inspire/rpc/_reply/${replyTo}/${corrId}`
}
/** A caller subscribes this wildcard to receive all its RPC replies. */
export function topicRpcReplyWildcard(replyTo: string): string {
  return `inspire/rpc/_reply/${replyTo}/+`
}

/** Wildcard subscriptions a consumer/hub uses to observe the whole bus. */
export const TOPIC_WILDCARD = {
  presence: 'inspire/presence/+/+',
  heartbeat: 'inspire/heartbeat/+/+',
  status: 'inspire/status/+/+',
  manifest: 'inspire/manifest/+/+',
} as const
