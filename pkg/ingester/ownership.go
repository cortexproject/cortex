package ingester

import (
	"context"
	"encoding/binary"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var errRingOwnershipRequiresZoneAwareness = errors.New("-ingester.use-ring-ownership-for-limits requires zone-aware replication to be enabled")

// ownershipRefreshPeriod is how often tenant shares are refreshed. The ring
// they are computed from only changes on this ingester's heartbeats.
const ownershipRefreshPeriod = 10 * time.Second

// ringOwnershipDelegate keeps the ring the Lifecycler reads on every
// heartbeat, as of the last change that can move a share, so tracking
// ownership adds no KV store traffic.
type ringOwnershipDelegate struct {
	enabled bool

	mtx sync.Mutex
	// As of the last change: instanceHash of every instance, a copy of the
	// desc that nothing modifies, and the number of changes.
	instances  map[string]uint64
	desc       *ring.Desc
	generation uint64
}

// OnRingInstanceHeartbeat runs inside the Lifecycler's CAS callback, with the
// ring about to be written to the KV store.
func (d *ringOwnershipDelegate) OnRingInstanceHeartbeat(_ *ring.Lifecycler, ringDesc *ring.Desc) {
	if !d.enabled {
		return
	}
	// Heartbeats and state changes other than READONLY change no hash.
	var h instanceHasher
	instances := make(map[string]uint64, len(ringDesc.Ingesters))
	for id, instance := range ringDesc.Ingesters {
		instances[id] = h.hash(instance)
	}
	// Callbacks can overlap.
	d.mtx.Lock()
	defer d.mtx.Unlock()
	if d.desc != nil && maps.Equal(d.instances, instances) {
		return
	}
	d.instances = instances
	d.desc = copyDesc(ringDesc)
	d.generation++
}

// latest returns the desc as of the last change, and the number of changes.
func (d *ringOwnershipDelegate) latest() (*ring.Desc, uint64) {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	return d.desc, d.generation
}

// copyDesc copies desc so that what the KV store does with it after the
// callback, such as merging it into its own state, cannot reach the copy.
func copyDesc(desc *ring.Desc) *ring.Desc {
	out := &ring.Desc{Ingesters: maps.Clone(desc.Ingesters)}
	for id, instance := range out.Ingesters {
		instance.Tokens = slices.Clone(instance.Tokens)
		out.Ingesters[id] = instance
	}
	return out
}

// instanceHasher hashes what shuffle-shard picks and zonal ownership read of
// an instance: its zone, whether it is READONLY, and its tokens. Picks skip
// READONLY instances and zonal ownership leaves them out, so no other state
// matters. It reuses one buffer across calls.
type instanceHasher struct {
	buf []byte
}

func (h *instanceHasher) hash(instance ring.InstanceDesc) uint64 {
	b := h.buf[:0]
	b = binary.LittleEndian.AppendUint32(b, uint32(len(instance.Zone)))
	b = append(b, instance.Zone...)
	if instance.State == ring.READONLY {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	for _, token := range instance.Tokens {
		b = binary.LittleEndian.AppendUint32(b, token)
	}
	h.buf = b
	return xxhash.Sum64(b)
}

// newOwnershipRing returns a ring to compute ownership shares from. Only a
// share per tenant is kept, so subrings are not cached.
func newOwnershipRing(cfg ring.Config, kvClient kv.Client, logger log.Logger) (*ring.Ring, error) {
	cfg.SubringCacheDisabled = true
	return ring.NewDescFedRing(cfg, "ingester-ownership", RingKey, kvClient, logger)
}

// tenantOwnership holds this ingester's share of each tenant's hash space
// within its zone. The map is replaced as a whole on each refresh.
type tenantOwnership struct {
	shares atomic.Pointer[map[string]float64]
}

// share returns the tenant's share. It reports false when the share is unknown
// or 0, which happens before the first refresh, for a tenant whose shard does
// not include this ingester, and while this ingester is not receiving writes.
func (t *tenantOwnership) share(userID string) (float64, bool) {
	shares := t.shares.Load()
	if shares == nil {
		return 0, false
	}
	s, ok := (*shares)[userID]
	return s, ok && s > 0
}

// ownershipRefresher computes tenantOwnership from the ring as of the
// delegate's last change.
type ownershipRefresher struct {
	latest                 func() (*ring.Desc, uint64)
	newRing                func() (*ring.Ring, error)
	instanceID             string
	shuffleShardingEnabled bool
	limits                 *validation.Overrides
	ownership              *tenantOwnership
	shareGauge             *prometheus.GaugeVec
	computations           prometheus.Counter

	// Only touched by refresh. ring is the last one built, and is never
	// updated, so nothing changes while a refresh reads it.
	ring           *ring.Ring
	lastGeneration uint64
	tenants        map[string]tenantShare
}

type tenantShare struct {
	shardSize   int
	fingerprint uint64
	share       float64
	zones       int
	// Computed while a token was shared: recompute after the next change.
	unsure bool
}

// newOwnershipRefresher returns a refresher that reads the ring from latest,
// and builds a ring from it with newRing whenever it changed.
func newOwnershipRefresher(latest func() (*ring.Desc, uint64), newRing func() (*ring.Ring, error), instanceID string, shuffleShardingEnabled bool, limits *validation.Overrides, registerer prometheus.Registerer) *ownershipRefresher {
	return &ownershipRefresher{
		latest:                 latest,
		newRing:                newRing,
		instanceID:             instanceID,
		shuffleShardingEnabled: shuffleShardingEnabled,
		limits:                 limits,
		ownership:              &tenantOwnership{},
		shareGauge: promauto.With(registerer).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ingester_ring_ownership_share",
			Help: "Share of the tenant's hash space this ingester owns within its zone, used to convert global limits to local ones. Only reported for tenants whose limits use it.",
		}, []string{"user"}),
		computations: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_ring_ownership_computations_total",
			Help: "Number of times a ring ownership share was computed.",
		}),
	}
}

func (o *ownershipRefresher) refresh(userIDs []string) {
	o.refreshContext(context.Background(), userIDs)
}

// refreshContext refreshes the shares of userIDs. If ctx is done before every
// tenant is refreshed, it returns without changing the shares.
func (o *ownershipRefresher) refreshContext(ctx context.Context, userIDs []string) {
	desc, generation := o.latest()
	if len(desc.GetIngesters()) == 0 {
		return // No heartbeat has fed the ring yet.
	}
	ringChanged := generation != o.lastGeneration
	if ringChanged {
		r, err := o.newRing()
		if err != nil {
			return // Not reached: New fails on an invalid config.
		}
		r.UpdateDesc(desc)
		o.ring = r
	}
	// A token shared by two instances lets one outside a tenant's subring
	// decide its ownership, which the fingerprint does not cover.
	duplicates := o.ring.HasDuplicateTokens()

	w := wholeRingMemo{o: o}
	tenants := make(map[string]tenantShare, len(userIDs))
	for _, userID := range userIDs {
		if ctx.Err() != nil {
			return
		}
		size := o.shardSize(userID)
		prev, ok := o.tenants[userID]
		known := ok && prev.shardSize == size
		if known && !ringChanged {
			tenants[userID] = prev // Nothing that can move a share changed.
			continue
		}
		sub := o.subring(userID, size)
		fingerprint := w.fingerprintOf(sub)
		if known && prev.fingerprint == fingerprint && !prev.unsure && !duplicates {
			tenants[userID] = prev // The tenant's subring is as before.
			continue
		}
		t := w.compute(sub, fingerprint)
		t.unsure = duplicates
		t.shardSize = size
		tenants[userID] = t
	}
	o.lastGeneration = generation
	o.tenants = tenants
	o.publish(tenants)
}

// publish stores the shares for the limiter and reports them.
func (o *ownershipRefresher) publish(tenants map[string]tenantShare) {
	shares := make(map[string]float64, len(tenants))
	for userID, t := range tenants {
		// Shares describe one copy per zone. When a zone is missing from the
		// ring, Get puts a second copy in another zone, so leave the tenant out
		// and let the limiter use the even split, which accounts for it.
		if t.zones == o.ring.ReplicationFactor() {
			shares[userID] = t.share
		}
	}
	o.ownership.shares.Store(&shares)

	o.shareGauge.Reset()
	for userID, share := range shares {
		o.shareGauge.WithLabelValues(userID).Set(share)
	}
}

func (o *ownershipRefresher) shardSize(userID string) int {
	if !o.shuffleShardingEnabled {
		return 0
	}
	return o.limits.IngestionTenantShardSize(userID)
}

// wholeRingMemo computes, at most once per refresh, the fingerprint and share
// of tenants using the whole ring. Other subrings pass straight through.
type wholeRingMemo struct {
	o           *ownershipRefresher
	fingerprint *uint64
	share       *tenantShare
}

func (w *wholeRingMemo) isWholeRing(sub ring.ReadRing) bool {
	return sub == ring.ReadRing(w.o.ring)
}

func (w *wholeRingMemo) fingerprintOf(sub ring.ReadRing) uint64 {
	if !w.isWholeRing(sub) {
		return subringFingerprint(sub)
	}
	if w.fingerprint == nil {
		f := subringFingerprint(sub)
		w.fingerprint = &f
	}
	return *w.fingerprint
}

// compute returns the share of sub.
func (w *wholeRingMemo) compute(sub ring.ReadRing, fingerprint uint64) tenantShare {
	if !w.isWholeRing(sub) {
		t := w.o.computeShare(sub)
		t.fingerprint = fingerprint
		return t
	}
	if w.share == nil {
		t := w.o.computeShare(sub)
		t.fingerprint = fingerprint
		w.share = &t
	}
	return *w.share
}

// subring returns the ring the distributor routes the tenant's writes with.
func (o *ownershipRefresher) subring(userID string, shardSize int) ring.ReadRing {
	if shardSize <= 0 {
		return o.ring
	}
	return o.ring.ShuffleShard(userID, shardSize)
}

func (o *ownershipRefresher) computeShare(sub ring.ReadRing) tenantShare {
	o.computations.Inc()
	zonal, ok := sub.(ring.ZonalOwnershipRing)
	if !ok {
		return tenantShare{}
	}
	return tenantShare{
		share: zonal.ZonalOwnership()[o.instanceID],
		zones: zonal.ZonesCount(),
	}
}

// subringFingerprint hashes what zonal ownership reads of a subring's members:
// each member's zone, tokens and whether it is READONLY, combined in any order.
// It does not cover the ring-wide token map, which a token shared with an
// instance outside the subring changes.
func subringFingerprint(sub ring.ReadRing) uint64 {
	healthy, unhealthy, _ := sub.GetAllInstanceDescs(ring.Reporting)
	var (
		hasher instanceHasher
		sum    uint64
	)
	for _, members := range [][]ring.InstanceDesc{healthy, unhealthy} {
		for _, member := range members {
			// Summed rather than XORed, so identical members do not cancel.
			sum += hasher.hash(member)
		}
	}
	return sum
}
