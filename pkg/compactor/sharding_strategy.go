package compactor

import (
	"context"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"hash/fnv"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util"
)

const (
	shardExcludedMeta = "shard-excluded"
)

func filterDisallowedTenants(userIDs []string, logger log.Logger, allowedTenants *util.AllowedTenants) []string {
	filteredUserIDs := []string{}
	for _, userID := range userIDs {
		if !allowedTenants.IsAllowed(userID) {
			level.Debug(logger).Log("msg", "ignoring storage gateway for user, not allowed", "user", userID)
			continue
		}

		filteredUserIDs = append(filteredUserIDs, userID)
	}

	return filteredUserIDs
}

// NoShardingStrategy is a no-op strategy. When this strategy is used, no tenant/block is filtered out.
type NoShardingStrategy struct {
	logger         log.Logger
	allowedTenants *util.AllowedTenants
}

func NewNoShardingStrategy(logger log.Logger, allowedTenants *util.AllowedTenants) *NoShardingStrategy {
	return &NoShardingStrategy{
		logger:         logger,
		allowedTenants: allowedTenants,
	}
}

func (s *NoShardingStrategy) FilterUsers(_ context.Context, userIDs []string) []string {
	return filterDisallowedTenants(userIDs, s.logger, s.allowedTenants)
}

func (s *NoShardingStrategy) FilterBlocks(_ context.Context, _ string, _ map[ulid.ULID]*metadata.Meta, _ map[ulid.ULID]struct{}, _ block.GaugeVec) error {
	return nil
}

// DefaultShardingStrategy is a sharding strategy based on the hash ring formed by store-gateways.
// Not go-routine safe.
type DefaultShardingStrategy struct {
	r              *ring.Ring
	instanceAddr   string
	logger         log.Logger
	allowedTenants *util.AllowedTenants
}

// NewDefaultShardingStrategy creates DefaultShardingStrategy.
func NewDefaultShardingStrategy(r *ring.Ring, instanceAddr string, logger log.Logger, allowedTenants *util.AllowedTenants) *DefaultShardingStrategy {
	return &DefaultShardingStrategy{
		r:            r,
		instanceAddr: instanceAddr,
		logger:       logger,

		allowedTenants: allowedTenants,
	}
}

// FilterUsers implements ShardingStrategy.
func (s *DefaultShardingStrategy) FilterUsers(_ context.Context, userIDs []string) []string {
	var filteredIDs []string
	for _, userID := range filterDisallowedTenants(userIDs, s.logger, s.allowedTenants) {
		// Hash the user ID.
		hasher := fnv.New32a()
		_, _ = hasher.Write([]byte(userID))
		userHash := hasher.Sum32()
		// Check whether this compactor instance owns the user.
		rs, err := s.r.Get(userHash, RingOp, nil, nil, nil)
		if err != nil {
			continue
		}
		if len(rs.Instances) != 1 {
			continue
		}
		if rs.Instances[0].Addr == s.instanceAddr {
			filteredIDs = append(filteredIDs, userID)
		}
	}
	return filteredIDs
}

// FilterBlocks implements ShardingStrategy.
func (s *DefaultShardingStrategy) FilterBlocks(_ context.Context, _ string, metas map[ulid.ULID]*metadata.Meta, loaded map[ulid.ULID]struct{}, synced block.GaugeVec) error {
	return nil
}

// ShuffleShardingStrategy is a shuffle sharding strategy, based on the hash ring formed by store-gateways,
// where each tenant blocks are sharded across a subset of store-gateway instances.
type ShuffleShardingStrategy struct {
	r            *ring.Ring
	instanceID   string
	instanceAddr string
	limits       *validation.Overrides
	logger       log.Logger

	zoneStableShuffleSharding bool
	allowedTenants            *util.AllowedTenants
}

// NewShuffleShardingStrategy makes a new ShuffleShardingStrategy.
func NewShuffleShardingStrategy(r *ring.Ring, instanceID, instanceAddr string, limits *validation.Overrides, logger log.Logger, allowedTenants *util.AllowedTenants, zoneStableShuffleSharding bool) *ShuffleShardingStrategy {
	return &ShuffleShardingStrategy{
		r:            r,
		instanceID:   instanceID,
		instanceAddr: instanceAddr,
		limits:       limits,
		logger:       logger,

		zoneStableShuffleSharding: zoneStableShuffleSharding,
		allowedTenants:            allowedTenants,
	}
}

// FilterUsers implements ShardingStrategy.
func (s *ShuffleShardingStrategy) FilterUsers(_ context.Context, userIDs []string) []string {
	var filteredIDs []string
	for _, userID := range filterDisallowedTenants(userIDs, s.logger, s.allowedTenants) {
		subRing := GetShuffleShardingSubring(s.r, userID, s.limits, s.zoneStableShuffleSharding)

		// Include the user only if it belongs to this store-gateway shard.
		if subRing.HasInstance(s.instanceID) {
			filteredIDs = append(filteredIDs, userID)
		}
	}

	return filteredIDs
}

// FilterBlocks implements ShardingStrategy.
func (s *ShuffleShardingStrategy) FilterBlocks(_ context.Context, userID string, metas map[ulid.ULID]*metadata.Meta, loaded map[ulid.ULID]struct{}, synced block.GaugeVec) error {
	return nil
}

// GetShuffleShardingSubring returns the subring to be used for a given user. This function
// should be used both by store-gateway and querier in order to guarantee the same logic is used.
func GetShuffleShardingSubring(ring *ring.Ring, userID string, limits *validation.Overrides, zoneStableShuffleSharding bool) ring.ReadRing {
	shardSize := limits.CompactorTenantShardSize(userID)

	// A shard size of 0 means shuffle sharding is disabled for this specific user,
	// so we just return the full ring so that blocks will be sharded across all store-gateways.
	if shardSize <= 0 {
		return ring
	}

	if zoneStableShuffleSharding {
		// Zone stability is required for store gateway when shuffle shard, see
		// https://github.com/cortexproject/cortex/issues/5467 for more details.
		return ring.ShuffleShardWithZoneStability(userID, shardSize)
	}
	return ring.ShuffleShard(userID, shardSize)
}
