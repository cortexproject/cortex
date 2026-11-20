// Package clientlimiter implements per-(tenant, X-Scope-ClientID) rate
// limiting shared by the write-path (distributor) and read-path
// (query-frontend) client-identity rate limiting features. See
// docs/proposals/client-identity-ingestion-rate-limiting.md and
// docs/proposals/client-identity-query-rate-limiting.md.
//
// GossipLimiter is the distributed enforcement mechanism both proposals
// use: every replica gossips its own per-(tenant, client) counts via a
// memberlist-backed kv.Client and merges every other replica's counts (see
// counter_desc.go), so the admission decision is made from the sum of all
// replicas' counters without any ring, shard ownership, or RPC. See
// pkg/distributor and pkg/querier/tripperware for how each path constructs
// its own kv.Client (distinct KV prefix per path) and GossipLimiter.
package clientlimiter

// Strategy supplies the per-tenant configuration GossipLimiter needs.
// Implementations typically wrap a *validation.Overrides accessor triple
// (e.g. ClientIdentityIngestionRateLimit/BurstSize/TrackedClientsLimit, or
// their query-path equivalents).
type Strategy interface {
	// Limit returns the per-client rate limit (tokens/sec) for tenantID.
	// Callers are expected to gate on this being > 0 before ever calling
	// AllowN -- this package does not itself treat 0 as "disabled", it
	// would just construct a counter that never allows anything.
	Limit(tenantID string) float64
	// Burst returns the per-client burst size for tenantID. GossipLimiter
	// treats this as extra fixed-window headroom (limit*window + burst),
	// an approximation of a token bucket's smooth burst allowance.
	Burst(tenantID string) int
	// TrackedClientsLimit returns the cap on distinct client identities
	// tracked for tenantID before the least-recently-used entry is evicted.
	// See "Bounding memory" in the ingestion proposal.
	TrackedClientsLimit(tenantID string) int
}
