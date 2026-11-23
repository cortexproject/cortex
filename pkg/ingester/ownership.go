package ingester

import (
	"maps"
	"sync"
	"time"

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

// ownershipRefreshPeriod is how often tenant shares are recomputed. The
// desc-fed ring itself only changes on this ingester's heartbeats.
const ownershipRefreshPeriod = 10 * time.Second

// ringOwnershipDelegate feeds a desc-fed ring with the ring the Lifecycler
// reads on every heartbeat, so tracking ownership adds no KV store traffic.
type ringOwnershipDelegate struct {
	ring atomic.Pointer[ring.Ring]

	// Bumped after each ring update that can change a shuffle-shard pick or
	// zonal ownership anywhere in the ring.
	generation atomic.Uint64

	mtx       sync.Mutex
	instances map[string]instanceRecord
}

// instanceRecord is what shuffle-shard picks and zonal ownership read of an
// instance.
type instanceRecord struct {
	zone     string
	readOnly bool
	tokens   uint64
}

// OnRingInstanceHeartbeat runs inside the Lifecycler's CAS callback, with the
// ring about to be written to the KV store. UpdateDesc copies it.
func (d *ringOwnershipDelegate) OnRingInstanceHeartbeat(_ *ring.Lifecycler, ringDesc *ring.Desc) {
	r := d.ring.Load()
	if r == nil {
		return
	}
	// Callbacks can overlap, so the ring and the record are updated together.
	d.mtx.Lock()
	defer d.mtx.Unlock()
	// Update the ring before bumping the generation, so a refresh that reads
	// the new generation computes from the new ring.
	r.UpdateDesc(ringDesc)

	// Shuffle-shard picks skip READONLY instances and zonal ownership leaves
	// them out, so only instances, zones, tokens and READONLY matter.
	// Heartbeats and other state changes do not.
	instances := make(map[string]instanceRecord, len(ringDesc.Ingesters))
	for id, instance := range ringDesc.Ingesters {
		instances[id] = instanceRecord{
			zone:     instance.Zone,
			readOnly: instance.State == ring.READONLY,
			tokens:   hashTokens(instance.Tokens),
		}
	}
	if d.instances == nil || !maps.Equal(d.instances, instances) {
		d.instances = instances
		d.generation.Inc()
	}
}

// hashTokens returns a 64-bit FNV-1a hash of tokens.
func hashTokens(tokens []uint32) uint64 {
	h := uint64(14695981039346656037)
	for _, token := range tokens {
		h ^= uint64(token)
		h *= 1099511628211
	}
	return h
}

// newOwnershipRing returns the ring the ownership shares are computed from. It
// is fed by ringOwnershipDelegate and only a share per tenant is kept, so
// subrings are not cached.
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

// ownershipRefresher computes tenantOwnership from the desc-fed ring.
type ownershipRefresher struct {
	ring                   *ring.Ring
	instanceID             string
	shuffleShardingEnabled bool
	limits                 *validation.Overrides
	ownership              *tenantOwnership
	shareGauge             *prometheus.GaugeVec
	computations           prometheus.Counter

	// generation returns a value that changes whenever the ring may give this
	// ingester a different share. Nil recomputes every share on each refresh.
	generation func() uint64

	// The last share computed per tenant. Only touched by refresh.
	tenants map[string]tenantShare
}

type tenantShare struct {
	generation  uint64
	shardSize   int
	fingerprint uint64
	share       float64
	zones       int
	// Computed while tokens were shared across zones, or from a whole ring
	// that changed during the computation: recompute after the next change.
	unsure bool
}

func newOwnershipRefresher(r *ring.Ring, instanceID string, shuffleShardingEnabled bool, limits *validation.Overrides, registerer prometheus.Registerer, generation func() uint64) *ownershipRefresher {
	return &ownershipRefresher{
		ring:                   r,
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
			Help: "Number of times a tenant's ring ownership share was computed.",
		}),
		generation: generation,
	}
}

func (o *ownershipRefresher) refresh(userIDs []string) {
	if o.ring.InstancesCount() == 0 {
		// No heartbeat has fed the ring yet.
		return
	}
	// Read before computing, so a ring change during the refresh makes the next
	// one recompute.
	var generation uint64
	if o.generation != nil {
		generation = o.generation()
	}
	// A token shared by two zones lets an instance outside a tenant's subring
	// decide this zone's ownership, which the fingerprint does not cover.
	duplicates := o.ring.HasDuplicateTokens()

	// The whole ring is read live, so its fingerprint and share are taken
	// once per refresh and kept only if the ring did not change in between.
	var wholeRing *tenantShare
	tenants := make(map[string]tenantShare, len(userIDs))
	shares := make(map[string]float64, len(userIDs))
	for _, userID := range userIDs {
		size := 0
		if o.shuffleShardingEnabled {
			size = o.limits.IngestionTenantShardSize(userID)
		}
		t, ok := o.tenants[userID]
		if o.generation == nil || !ok || t.generation != generation || t.shardSize != size {
			sub := o.subring(userID, size)
			if sub == ring.ReadRing(o.ring) {
				if wholeRing == nil {
					f := subringFingerprint(sub)
					w := o.computeShare(sub)
					w.fingerprint = f
					w.unsure = duplicates || subringFingerprint(sub) != f
					wholeRing = &w
				}
				if o.generation == nil || !ok || t.shardSize != size || t.fingerprint != wholeRing.fingerprint || t.unsure || wholeRing.unsure {
					t = *wholeRing
					t.shardSize = size
				}
			} else {
				fingerprint := subringFingerprint(sub)
				if o.generation == nil || !ok || t.shardSize != size || t.fingerprint != fingerprint || duplicates || t.unsure {
					t = o.computeShare(sub)
					t.shardSize = size
					t.fingerprint = fingerprint
					t.unsure = duplicates
				}
			}
			t.generation = generation
		}
		tenants[userID] = t

		// Shares describe one copy per zone. When a zone is missing from the
		// ring, Get puts a second copy in another zone, so leave the tenant out
		// and let the limiter use the even split, which accounts for it.
		if t.zones != o.ring.ReplicationFactor() {
			continue
		}
		shares[userID] = t.share
	}
	o.tenants = tenants
	o.ownership.shares.Store(&shares)

	o.shareGauge.Reset()
	for userID, share := range shares {
		o.shareGauge.WithLabelValues(userID).Set(share)
	}
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
	var h uint64
	for _, members := range [][]ring.InstanceDesc{healthy, unhealthy} {
		for _, member := range members {
			m := hashTokens(member.Tokens) ^ hashString(member.Zone)
			if member.State == ring.READONLY {
				m = ^m
			}
			h += mix64(m)
		}
	}
	return h
}

func hashString(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// mix64 is the splitmix64 finalizer, so that summing member hashes does not
// let similar members cancel out.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
