package ingester

import (
	"slices"
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

	// Set before ring is stored.
	instanceID string

	// Bumped after each ring update that can change this ingester's shares.
	generation atomic.Uint64

	mtx   sync.Mutex
	seen  bool
	zones int
	// The instances of this ingester's zone, as of the last bump.
	zoneMembers map[string]zoneMember
}

type zoneMember struct {
	tokens   []uint32
	readOnly bool
}

// OnRingInstanceHeartbeat runs inside the Lifecycler's CAS callback, with the
// ring about to be written to the KV store. UpdateDesc copies it.
func (d *ringOwnershipDelegate) OnRingInstanceHeartbeat(_ *ring.Lifecycler, ringDesc *ring.Desc) {
	r := d.ring.Load()
	if r == nil {
		return
	}
	// Update the ring before bumping the generation, so a refresh that reads
	// the new generation computes from the new ring.
	r.UpdateDesc(ringDesc)

	d.mtx.Lock()
	defer d.mtx.Unlock()
	if d.sharesMayChange(ringDesc) {
		d.record(ringDesc)
		d.generation.Inc()
	}
}

// sharesMayChange reports whether, since the last bump, an instance joined or
// left this ingester's zone, changed its tokens, or became or stopped being
// READONLY, or the number of zones changed. Shard picks and ownership are per
// zone, so changes in other zones do not move this ingester's shares; the
// number of zones does, through the number of shard members per zone.
func (d *ringOwnershipDelegate) sharesMayChange(desc *ring.Desc) bool {
	if !d.seen || countZones(desc) != d.zones {
		return true
	}
	zone, members := desc.Ingesters[d.instanceID].Zone, 0
	for id, instance := range desc.Ingesters {
		if instance.Zone != zone {
			continue
		}
		members++
		before, ok := d.zoneMembers[id]
		if !ok || before.readOnly != (instance.State == ring.READONLY) || !slices.Equal(before.tokens, instance.Tokens) {
			return true
		}
	}
	return members != len(d.zoneMembers)
}

func (d *ringOwnershipDelegate) record(desc *ring.Desc) {
	zone := desc.Ingesters[d.instanceID].Zone
	d.seen = true
	d.zones = countZones(desc)
	d.zoneMembers = map[string]zoneMember{}
	for id, instance := range desc.Ingesters {
		if instance.Zone == zone {
			d.zoneMembers[id] = zoneMember{tokens: slices.Clone(instance.Tokens), readOnly: instance.State == ring.READONLY}
		}
	}
}

func countZones(desc *ring.Desc) int {
	zones := map[string]struct{}{}
	for _, instance := range desc.Ingesters {
		zones[instance.Zone] = struct{}{}
	}
	return len(zones)
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

	// generation returns a value that changes whenever the ring may give this
	// ingester a different share. Nil recomputes every share on each refresh.
	generation func() uint64

	// The last share computed per tenant, reused while generation and the
	// tenant's shard size are unchanged. Only touched by refresh.
	tenants map[string]tenantShare
}

type tenantShare struct {
	generation uint64
	shardSize  int
	wholeRing  bool
	share      float64
	zones      int
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

	instances := o.ring.InstancesCount()
	tenants := make(map[string]tenantShare, len(userIDs))
	shares := make(map[string]float64, len(userIDs))
	for _, userID := range userIDs {
		size := 0
		if o.shuffleShardingEnabled {
			size = o.limits.IngestionTenantShardSize(userID)
		}
		// An instance joining or leaving any zone can change this.
		wholeRing := ring.ShuffleShardUsesWholeRing(size, instances)
		t, ok := o.tenants[userID]
		if o.generation == nil || !ok || t.generation != generation || t.shardSize != size || t.wholeRing != wholeRing {
			t = o.computeShare(userID, size)
			t.generation = generation
			t.wholeRing = wholeRing
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

func (o *ownershipRefresher) computeShare(userID string, shardSize int) tenantShare {
	var sub ring.ReadRing = o.ring
	if shardSize > 0 {
		// Same subring the distributor routes the tenant's writes with.
		sub = o.ring.ShuffleShard(userID, shardSize)
	}
	zonal, ok := sub.(ring.ZonalOwnershipRing)
	if !ok {
		return tenantShare{shardSize: shardSize}
	}
	return tenantShare{
		shardSize: shardSize,
		share:     zonal.ZonalOwnership()[o.instanceID],
		zones:     zonal.ZonesCount(),
	}
}
