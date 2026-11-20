package clientlimiter

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// GossipLimiter is a multi-tenant, per-client-identity fixed-window counter
// limiter whose counts are gossiped and merged across replicas via a
// memberlist-backed kv.Client, following the same store/Merge pattern
// pkg/ha's HATracker uses for elected-replica state (see counter_desc.go for
// why the merge shape differs: HATracker merges a single last-writer-wins
// value; this merges a per-replica map that's summed at read time).
//
// Unlike Limiter, this answers the *global* question -- "would the sum of
// every replica's count for (tenant, client) allow n more" -- at the cost of
// approximating the smooth token-bucket semantics Limiter provides with a
// fixed window whose global view lags peers by up to one gossipInterval.
// Needs no ring or shard ownership: every replica gossips its own counters
// and merges everyone else's, so this works identically for the distributor
// and the query-frontend (which has no ring to shard over).
type GossipLimiter struct {
	services.Service

	strategy Strategy
	// newClient lazily builds the KV client the first time any tenant actually
	// exercises the limiter (see ensureClient), so a deployment where no tenant
	// opts in never starts a memberlist/consul client or watch. client is only
	// touched from the loop goroutine (and handed to watchPeers by value).
	newClient      KVClientFactory
	client         kv.Client
	nextInitTry    time.Time
	replicaID      string
	windowDuration time.Duration
	gossipInterval time.Duration
	recheckPeriod  time.Duration
	retention      time.Duration
	logger         log.Logger

	tenantsMu sync.RWMutex
	tenants   map[string]*gossipTenantState

	trackedClientsEvictedTotal *prometheus.CounterVec
	kvCASFailures              prometheus.Counter
}

type gossipTenantState struct {
	mu sync.Mutex

	cache *lru.Cache[string, *clientCounterState]

	limit               float64
	burst               int
	trackedClientsLimit int
	recheckAt           time.Time
}

type clientCounterState struct {
	mu sync.Mutex

	windowStart int64 // unix nano, truncated to windowDuration
	localCount  int64
	peerSum     int64
	dirty       bool
}

// KVClientFactory builds the kv.Client a GossipLimiter gossips through.
type KVClientFactory func() (kv.Client, error)

// StaticKVClient adapts an already-built kv.Client to a KVClientFactory.
func StaticKVClient(c kv.Client) KVClientFactory {
	return func() (kv.Client, error) { return c, nil }
}

// kvInitRetryPeriod is how long to wait before retrying a failed KV client
// construction, so a misconfigured store doesn't log on every gossip tick.
const kvInitRetryPeriod = 30 * time.Second

// NewGossipLimiter constructs a GossipLimiter. replicaID identifies this
// process's own contribution in the gossiped state and must be unique
// cluster-wide; use NewReplicaID to generate one. windowDuration is both the
// fixed-window length counters are kept over and, tied 1:1, the interval
// local deltas are gossiped and stale peer entries pruned at.
func NewGossipLimiter(reg prometheus.Registerer, metricPrefix string, strategy Strategy, newClient KVClientFactory, replicaID string, windowDuration time.Duration, recheckPeriod time.Duration, logger log.Logger) *GossipLimiter {
	g := &GossipLimiter{
		strategy:       strategy,
		newClient:      newClient,
		replicaID:      replicaID,
		windowDuration: windowDuration,
		gossipInterval: windowDuration,
		recheckPeriod:  recheckPeriod,
		retention:      5 * windowDuration,
		logger:         logger,
		tenants:        map[string]*gossipTenantState{},
		trackedClientsEvictedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: metricPrefix + "_client_identity_tracked_clients_evicted_total",
			Help: "Total number of client identities evicted from the per-tenant tracked-clients LRU cache because the tenant's configured cap was reached. A high rate here relative to genuine client churn indicates the cap is sized too low for this tenant.",
		}, []string{"user"}),
		kvCASFailures: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: metricPrefix + "_client_identity_gossip_cas_failures_total",
			Help: "Total number of failed CAS calls while gossiping client-identity request counters. Enforcement continues using whatever local/peer state is already known; it does not fail closed.",
		}),
	}
	g.Service = services.NewBasicService(nil, g.loop, nil)
	return g
}

// NewReplicaID generates a random, process-lifetime identifier for use as a
// GossipLimiter replicaID. There's deliberately no ring or lifecycler ID to
// borrow here -- avoiding that dependency (so the query-frontend can
// participate without a ring) is the point of this design. Uniqueness only
// needs to be good enough to avoid collisions with concurrently running
// replicas; a restarted process's stale entries age out via pruning rather
// than being mistaken for the same still-running replica.
func NewReplicaID() string {
	buf := make([]byte, 8)
	_, _ = rand.Read(buf) // crypto/rand.Read never errors on supported platforms
	host, _ := os.Hostname()
	return fmt.Sprintf("%s-%x", host, buf)
}

// AllowN reports whether n tokens may be consumed now for (tenantID,
// clientID), against the sum of this replica's own count and the most
// recently gossiped counts from every other replica.
func (g *GossipLimiter) AllowN(now time.Time, tenantID, clientID string, n int) bool {
	ts := g.getTenantState(now, tenantID)

	ts.mu.Lock()
	cs, ok := ts.cache.Get(clientID)
	if !ok {
		cs = &clientCounterState{}
		ts.cache.Add(clientID, cs)
	}
	limit, burst := ts.limit, ts.burst
	ts.mu.Unlock()

	cs.mu.Lock()
	defer cs.mu.Unlock()

	windowStart := now.Truncate(g.windowDuration).UnixNano()
	if cs.windowStart != windowStart {
		cs.windowStart = windowStart
		cs.localCount = 0
	}

	capacity := int64(limit*g.windowDuration.Seconds()) + int64(burst)
	if cs.localCount+cs.peerSum+int64(n) > capacity {
		return false
	}
	cs.localCount += int64(n)
	cs.dirty = true
	return true
}

func (g *GossipLimiter) getTenantState(now time.Time, tenantID string) *gossipTenantState {
	g.tenantsMu.RLock()
	ts, ok := g.tenants[tenantID]
	g.tenantsMu.RUnlock()

	if ok {
		g.recheckTenantState(now, tenantID, ts)
		return ts
	}

	trackedClientsLimit := g.strategy.TrackedClientsLimit(tenantID)
	if trackedClientsLimit <= 0 {
		// A tenant whose cap is misconfigured to <= 0 still needs a usable
		// cache -- lru.New requires size > 0 -- so track at least one client
		// rather than erroring; callers gate the feature on Limit() > 0, not
		// on this value being sane.
		trackedClientsLimit = 1
	}

	cache, _ := lru.NewWithEvict[string, *clientCounterState](trackedClientsLimit, func(_ string, _ *clientCounterState) {
		g.trackedClientsEvictedTotal.WithLabelValues(tenantID).Inc()
	})

	newState := &gossipTenantState{
		cache:               cache,
		limit:               g.strategy.Limit(tenantID),
		burst:               g.strategy.Burst(tenantID),
		trackedClientsLimit: trackedClientsLimit,
		recheckAt:           now.Add(g.recheckPeriod),
	}

	g.tenantsMu.Lock()
	if existing, ok := g.tenants[tenantID]; ok {
		g.tenantsMu.Unlock()
		return existing
	}
	g.tenants[tenantID] = newState
	g.tenantsMu.Unlock()

	return newState
}

func (g *GossipLimiter) recheckTenantState(now time.Time, tenantID string, ts *gossipTenantState) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if now.Before(ts.recheckAt) {
		return
	}
	ts.recheckAt = now.Add(g.recheckPeriod)

	if newTrackedClientsLimit := g.strategy.TrackedClientsLimit(tenantID); newTrackedClientsLimit > 0 && newTrackedClientsLimit != ts.trackedClientsLimit {
		ts.cache.Resize(newTrackedClientsLimit)
		ts.trackedClientsLimit = newTrackedClientsLimit
	}

	ts.limit = g.strategy.Limit(tenantID)
	ts.burst = g.strategy.Burst(tenantID)
}

// loop gossips locally-changed counters and applies peer updates for as long
// as the service is running. Follows the same shape as ha.HATracker.loop:
// a single WatchPrefix for the lifetime of the service, plus a ticker-driven
// periodic push of local state.
func (g *GossipLimiter) loop(ctx context.Context) error {
	ticker := time.NewTicker(g.gossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if !g.ensureClient(ctx) {
				continue
			}
			g.flush(ctx)
		}
	}
}

// ensureClient builds the KV client and starts the peer watch the first time
// a tenant has exercised the limiter. Until then (and while construction is
// failing) enforcement simply runs on local state, i.e. fails open.
func (g *GossipLimiter) ensureClient(ctx context.Context) bool {
	if g.client != nil {
		return true
	}

	g.tenantsMu.RLock()
	active := len(g.tenants) > 0
	g.tenantsMu.RUnlock()
	if !active {
		return false
	}

	if now := time.Now(); now.Before(g.nextInitTry) {
		return false
	}

	client, err := g.newClient()
	if err != nil {
		g.nextInitTry = time.Now().Add(kvInitRetryPeriod)
		g.kvCASFailures.Inc()
		level.Warn(g.logger).Log("msg", "failed to initialize client-identity rate limit KV client, continuing with local state", "err", err)
		return false
	}

	g.client = client
	go g.watchPeers(ctx, client)
	return true
}

func (g *GossipLimiter) watchPeers(ctx context.Context, client kv.Client) {
	// The KVStore config used to construct g.client should carry a prefix
	// scoping it to this limiter's own namespace (distinct from e.g. the
	// query-path limiter's), so watching everything under it (empty prefix
	// here) is scoped correctly, same reasoning as HATracker.loop.
	client.WatchPrefix(ctx, "", func(tenantID string, value any) bool {
		desc, ok := value.(*ClientCounterDesc)
		if !ok || desc == nil {
			return true
		}
		g.applyPeerView(tenantID, desc)
		return true
	})
}

// applyPeerView updates the cached peerSum for every client of tenantID this
// replica already locally tracks, from the latest gossiped/merged state.
// Clients this replica has never seen locally are ignored -- there's no
// local counter to combine a peer sum with, and no admission decision to be
// made for them here.
func (g *GossipLimiter) applyPeerView(tenantID string, desc *ClientCounterDesc) {
	g.tenantsMu.RLock()
	ts, ok := g.tenants[tenantID]
	g.tenantsMu.RUnlock()
	if !ok {
		return
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	for clientID, entry := range desc.Clients {
		if entry == nil {
			continue
		}
		cs, ok := ts.cache.Peek(clientID) // Peek: don't perturb LRU recency for a peer-driven update.
		if !ok {
			continue
		}

		// Merge already keeps only each source replica's most recent
		// window/count (see ClientCounterDesc.Merge), so every entry here
		// is already that replica's latest known state -- no need to also
		// filter by window here. Filtering on an exact window match would
		// be actively wrong: flush+gossip latency is comparable to (or
		// larger than) the window length itself, so a peer's report for
		// "the current window" almost always arrives after this replica has
		// already rolled over to the next one, and would be discarded every
		// time. Using the latest known count even when it's a window old is
		// the deliberate approximation this design documents (lags peers by
		// up to one gossip window); pruneStaleReplicas is what drops a
		// replica's contribution once it's gone quiet for good.
		var sum int64
		for replicaID, counter := range entry.Replicas {
			if replicaID == g.replicaID || counter == nil {
				continue // our own contribution is already reflected in localCount.
			}
			sum += counter.Count
		}

		cs.mu.Lock()
		cs.peerSum = sum
		cs.mu.Unlock()
	}
}

// flush gossips every tenant's locally-dirty client counters. Pruning of
// stale peer entries piggybacks on the same CAS (see pruneStaleReplicas)
// rather than running as a separate pass, mirroring how HATracker.checkKVStore
// folds its own read-decide-write into one CAS closure.
func (g *GossipLimiter) flush(ctx context.Context) {
	g.tenantsMu.RLock()
	tenantIDs := make([]string, 0, len(g.tenants))
	for tenantID := range g.tenants {
		tenantIDs = append(tenantIDs, tenantID)
	}
	g.tenantsMu.RUnlock()

	now := time.Now()
	for _, tenantID := range tenantIDs {
		g.flushTenant(ctx, tenantID, now)
	}
}

type dirtyClientCounter struct {
	clientID string
	counter  *ReplicaCounter
}

func (g *GossipLimiter) flushTenant(ctx context.Context, tenantID string, now time.Time) {
	ts := g.getTenantState(now, tenantID)

	var dirty []dirtyClientCounter

	ts.mu.Lock()
	for _, clientID := range ts.cache.Keys() {
		cs, ok := ts.cache.Peek(clientID)
		if !ok {
			continue
		}
		cs.mu.Lock()
		if cs.dirty {
			dirty = append(dirty, dirtyClientCounter{clientID, &ReplicaCounter{
				Count:               cs.localCount,
				WindowStartUnixNano: cs.windowStart,
				LastUpdateUnixNano:  now.UnixNano(),
			}})
			cs.dirty = false
		}
		cs.mu.Unlock()
	}
	ts.mu.Unlock()

	if len(dirty) == 0 {
		return
	}

	staleBefore := now.Add(-g.retention).UnixNano()
	err := g.client.CAS(ctx, tenantID, func(in any) (out any, retry bool, err error) {
		desc, ok := in.(*ClientCounterDesc)
		if !ok || desc == nil {
			desc = NewClientCounterDesc()
		}
		if desc.Clients == nil {
			desc.Clients = map[string]*ClientEntry{}
		}

		pruneStaleReplicas(desc, staleBefore)

		for _, e := range dirty {
			entry := desc.Clients[e.clientID]
			if entry == nil {
				entry = &ClientEntry{Replicas: map[string]*ReplicaCounter{}}
				desc.Clients[e.clientID] = entry
			}
			entry.Replicas[g.replicaID] = e.counter
		}
		return desc, true, nil
	})
	if err != nil {
		g.kvCASFailures.Inc()
		level.Warn(g.logger).Log("msg", "failed to gossip client-identity rate limit counters, continuing with local/last-known-peer state", "tenant", tenantID, "err", err)
	}
}

// pruneStaleReplicas drops replica entries that haven't updated since
// staleBefore (e.g. a replica that shut down) and any client left with no
// replicas as a result. Run as part of every CAS rather than a memberlist
// tombstone sweep -- see ClientCounterDesc.RemoveTombstones.
func pruneStaleReplicas(desc *ClientCounterDesc, staleBefore int64) {
	for clientID, entry := range desc.Clients {
		if entry == nil {
			delete(desc.Clients, clientID)
			continue
		}
		for replicaID, counter := range entry.Replicas {
			if counter == nil || counter.LastUpdateUnixNano < staleBefore {
				delete(entry.Replicas, replicaID)
			}
		}
		if len(entry.Replicas) == 0 {
			delete(desc.Clients, clientID)
		}
	}
}
