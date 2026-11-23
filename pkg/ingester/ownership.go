package ingester

import (
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/pkg/ring"
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
}

// OnRingInstanceHeartbeat runs inside the Lifecycler's CAS callback, with the
// ring about to be written to the KV store. UpdateDesc copies it.
func (d *ringOwnershipDelegate) OnRingInstanceHeartbeat(_ *ring.Lifecycler, ringDesc *ring.Desc) {
	if r := d.ring.Load(); r != nil {
		r.UpdateDesc(ringDesc)
	}
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
	writeOp                ring.Operation
	shuffleShardingEnabled bool
	limits                 *validation.Overrides
	ownership              *tenantOwnership
	shareGauge             *prometheus.GaugeVec
}

func newOwnershipRefresher(r *ring.Ring, instanceID string, extendWrites, shuffleShardingEnabled bool, limits *validation.Overrides, registerer prometheus.Registerer) *ownershipRefresher {
	// Must match the operation the distributor writes with, or the shares
	// would not describe where series are sent.
	writeOp := ring.WriteNoExtend
	if extendWrites {
		writeOp = ring.Write
	}
	return &ownershipRefresher{
		ring:                   r,
		instanceID:             instanceID,
		writeOp:                writeOp,
		shuffleShardingEnabled: shuffleShardingEnabled,
		limits:                 limits,
		ownership:              &tenantOwnership{},
		shareGauge: promauto.With(registerer).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ingester_ring_ownership_share",
			Help: "Share of the tenant's hash space this ingester owns within its zone, used to convert global limits to local ones. Only reported for tenants whose limits use it.",
		}, []string{"user"}),
	}
}

func (o *ownershipRefresher) refresh(userIDs []string) {
	if o.ring.InstancesCount() == 0 {
		// No heartbeat has fed the ring yet.
		return
	}
	shares := make(map[string]float64, len(userIDs))
	for _, userID := range userIDs {
		var sub ring.ReadRing = o.ring
		if size := o.limits.IngestionTenantShardSize(userID); o.shuffleShardingEnabled && size > 0 {
			// Same subring the distributor routes the tenant's writes with.
			sub = o.ring.ShuffleShard(userID, size)
		}
		zonal, ok := sub.(ring.ZonalOwnershipRing)
		// Shares describe one copy per zone. When a zone is missing from the
		// ring, Get puts a second copy in another zone, so leave the tenant out
		// and let the limiter use the even split, which accounts for it.
		if !ok || zonal.ZonesCount() != o.ring.ReplicationFactor() {
			continue
		}
		shares[userID] = zonal.ZonalOwnership()[o.instanceID]
	}
	o.ownership.shares.Store(&shares)

	o.shareGauge.Reset()
	for userID, share := range shares {
		o.shareGauge.WithLabelValues(userID).Set(share)
	}
}
