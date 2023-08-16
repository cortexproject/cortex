package querier

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

func TestBlocksStoreReplicationSet_GetClientsFor(t *testing.T) {
	t.Parallel()

	// The following block IDs have been picked to have increasing hash values
	// in order to simplify the tests.
	block1 := ulid.MustNew(4, nil)  // hash: 122298081
	block2 := ulid.MustNew(1, nil)  // hash: 283204220
	block3 := ulid.MustNew(2, nil)  // hash: 444110359
	block4 := ulid.MustNew(12, nil) // hash: 1124870809
	block5 := ulid.MustNew(9, nil)  // hash: 1285776948
	block6 := ulid.MustNew(10, nil) // hash: 1446683087
	block7 := ulid.MustNew(7, nil)  // hash: 1607589226
	block8 := ulid.MustNew(8, nil)  // hash: 1285776948
	block9 := ulid.MustNew(5, nil)  // hash: 2931974232
	block10 := ulid.MustNew(6, nil) // hash: 3092880371
	block11 := ulid.MustNew(3, nil) // hash: 3092880371

	block1Hash := cortex_tsdb.HashBlockID(block1)
	block2Hash := cortex_tsdb.HashBlockID(block2)
	block3Hash := cortex_tsdb.HashBlockID(block3)
	block4Hash := cortex_tsdb.HashBlockID(block4)
	block5Hash := cortex_tsdb.HashBlockID(block5)
	_ = block5Hash
	block6Hash := cortex_tsdb.HashBlockID(block6)
	_ = block6Hash
	block7Hash := cortex_tsdb.HashBlockID(block7)
	_ = block7Hash
	block8Hash := cortex_tsdb.HashBlockID(block8)
	_ = block8Hash
	block9Hash := cortex_tsdb.HashBlockID(block9)
	_ = block9Hash
	block10Hash := cortex_tsdb.HashBlockID(block10)
	_ = block10Hash
	block11Hash := cortex_tsdb.HashBlockID(block11)
	_ = block11Hash

	userID := "user-A"
	registeredAt := time.Now()

	tests := map[string]struct {
		shardingStrategy     string
		tenantShardSize      float64
		replicationFactor    int
		setup                func(*ring.Desc)
		queryBlocks          []ulid.ULID
		exclude              map[ulid.ULID][]string
		attemptedBlocksZones map[ulid.ULID]map[string]int
		zoneAwarenessEnabled bool
		expectedClients      map[string][]ulid.ULID
		expectedErr          error
	}{
		//
		// Sharding strategy: default
		//
		"default sharding, single instance in the ring with RF = 1": {
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2},
			},
		},
		"default sharding, single instance in the ring with RF = 1 but excluded": {
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			exclude: map[ulid.ULID][]string{
				block1: {"127.0.0.1"},
			},
			expectedErr: fmt.Errorf("no store-gateway instance left after checking exclude for block %s", block1.String()),
		},
		"default sharding, single instance in the ring with RF = 1 but excluded for non queried block": {
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			exclude: map[ulid.ULID][]string{
				block3: {"127.0.0.1"},
			},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2},
			},
		},
		"default sharding, single instance in the ring with RF = 2": {
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2},
			},
		},
		"default sharding, multiple instances in the ring with each requested block belonging to a different store-gateway and RF = 1": {
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block3, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1},
				"127.0.0.3": {block3},
				"127.0.0.4": {block4},
			},
		},
		"default sharding, multiple instances in the ring with each requested block belonging to a different store-gateway and RF = 1 but excluded": {
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block3, block4},
			exclude: map[ulid.ULID][]string{
				block3: {"127.0.0.3"},
			},
			expectedErr: fmt.Errorf("no store-gateway instance left after checking exclude for block %s", block3.String()),
		},
		"default sharding, multiple instances in the ring with each requested block belonging to a different store-gateway and RF = 2": {
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block3, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1},
				"127.0.0.3": {block3},
				"127.0.0.4": {block4},
			},
		},
		"default sharding, multiple instances in the ring with multiple requested blocks belonging to the same store-gateway and RF = 2": {
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2, block3, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block4},
				"127.0.0.2": {block2, block3},
			},
		},
		"default sharding, multiple instances in the ring with each requested block belonging to a different store-gateway and RF = 2 and some blocks excluded but with replacement available": {
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block3, block4},
			exclude: map[ulid.ULID][]string{
				block3: {"127.0.0.3"},
				block1: {"127.0.0.1"},
			},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.2": {block1},
				"127.0.0.4": {block3, block4},
			},
		},
		"default sharding, multiple instances in the ring are JOINING, the requested block + its replicas only belongs to JOINING instances": {
			shardingStrategy:  util.ShardingStrategyDefault,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.JOINING, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.JOINING, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.JOINING, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.4": {block1},
			},
		},
		//
		// Sharding strategy: shuffle sharding
		//
		"shuffle sharding, single instance in the ring with RF = 1, SS = 1": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   1,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2},
			},
		},
		"shuffle sharding, single instance in the ring with RF = 1, SS = 1 but excluded": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   1,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			exclude: map[ulid.ULID][]string{
				block1: {"127.0.0.1"},
			},
			expectedErr: fmt.Errorf("no store-gateway instance left after checking exclude for block %s", block1.String()),
		},
		"shuffle sharding, single instance in the ring with RF = 2, SS = 2": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   2,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2},
			},
		},
		"shuffle sharding, multiple instances in the ring with RF = 1, SS = 1": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   1,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2, block4},
			},
		},
		"shuffle sharding, multiple instances in the ring with RF = 1, SS = 2": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   2,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block4},
				"127.0.0.2": {block2},
			},
		},
		"shuffle sharding, multiple instances in the ring with RF = 1, SS = 4": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   4,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1},
				"127.0.0.2": {block2},
				"127.0.0.4": {block4},
			},
		},
		"shuffle sharding, multiple instances in the ring with RF = 2, SS = 2 with excluded blocks but some replacement available": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   2,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			exclude: map[ulid.ULID][]string{
				block1: {"127.0.0.1"},
				block2: {"127.0.0.1"},
			},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.2": {block1, block2},
			},
		},
		"shuffle sharding, multiple instances in the ring with RF = 2, SS = 2 with excluded blocks and no replacement available": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   2,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks: []ulid.ULID{block1, block2},
			exclude: map[ulid.ULID][]string{
				block1: {"127.0.0.1", "127.0.0.2"},
				block2: {"127.0.0.1"},
			},
			expectedErr: fmt.Errorf("no store-gateway instance left after checking exclude for block %s", block1.String()),
		},
		"shuffle sharding, multiple instances in the ring with RF = 3, SS = 3 and zone awareness enabled": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   3,
			replicationFactor: 3,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "1", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "2", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "3", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "1", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-5", "127.0.0.5", "2", []uint32{block5Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-6", "127.0.0.6", "3", []uint32{block6Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-7", "127.0.0.7", "1", []uint32{block7Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-8", "127.0.0.8", "2", []uint32{block8Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-9", "127.0.0.9", "3", []uint32{block9Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks:          []ulid.ULID{block1, block2},
			zoneAwarenessEnabled: true,
			attemptedBlocksZones: make(map[ulid.ULID]map[string]int, 0),
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {block1},
				"127.0.0.6": {block2},
			},
		},
		"shuffle sharding, multiple instances in the ring with RF = 3, SS = 3, exclude and zone awareness enabled": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   3,
			replicationFactor: 3,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "1", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "2", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "3", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "1", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-5", "127.0.0.5", "2", []uint32{block5Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-6", "127.0.0.6", "3", []uint32{block6Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-7", "127.0.0.7", "1", []uint32{block7Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-8", "127.0.0.8", "2", []uint32{block8Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-9", "127.0.0.9", "3", []uint32{block9Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks:          []ulid.ULID{block1},
			zoneAwarenessEnabled: true,
			exclude: map[ulid.ULID][]string{
				block1: {"127.0.0.1"},
			},
			attemptedBlocksZones: map[ulid.ULID]map[string]int{
				block1: {"1": 1},
			},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.6": {block1},
			},
		},
		"shuffle sharding, multiple instances in the ring with RF = 3, SS = 3, exclude 2 blocks and zone awareness enabled": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   3,
			replicationFactor: 3,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "1", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "2", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "3", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "1", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-5", "127.0.0.5", "2", []uint32{block5Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-6", "127.0.0.6", "3", []uint32{block6Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-7", "127.0.0.7", "1", []uint32{block7Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-8", "127.0.0.8", "2", []uint32{block8Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-9", "127.0.0.9", "3", []uint32{block9Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks:          []ulid.ULID{block1},
			zoneAwarenessEnabled: true,
			exclude: map[ulid.ULID][]string{
				block1: {"127.0.0.1", "127.0.0.6"},
			},
			attemptedBlocksZones: map[ulid.ULID]map[string]int{
				block1: {"1": 1, "3": 1},
			},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.8": {block1},
			},
		},
		"shuffle sharding, multiple instances in the ring with RF = 3, SS = 3, exclude 3 blocks and zone awareness enabled": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   3,
			replicationFactor: 3,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "1", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "2", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "3", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "1", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-5", "127.0.0.5", "2", []uint32{block5Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-6", "127.0.0.6", "3", []uint32{block6Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-7", "127.0.0.7", "1", []uint32{block7Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-8", "127.0.0.8", "2", []uint32{block8Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-9", "127.0.0.9", "3", []uint32{block9Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks:          []ulid.ULID{block1},
			zoneAwarenessEnabled: true,
			exclude: map[ulid.ULID][]string{
				block1: {"127.0.0.1", "127.0.0.6", "127.0.0.8"},
			},
			attemptedBlocksZones: map[ulid.ULID]map[string]int{
				block1: {"1": 1, "2": 1, "3": 1},
			},
			expectedErr: fmt.Errorf("no store-gateway instance left after checking exclude for block %s", block1.String()),
		},
		"shuffle sharding, multiple instances in the ring with RF = 6, SS = 6, exclude 3 blocks and zone awareness enabled": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   6,
			replicationFactor: 6,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "1", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "2", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "3", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "1", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-5", "127.0.0.5", "2", []uint32{block5Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-6", "127.0.0.6", "3", []uint32{block6Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-7", "127.0.0.7", "1", []uint32{block7Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-8", "127.0.0.8", "2", []uint32{block8Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-9", "127.0.0.9", "3", []uint32{block9Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks:          []ulid.ULID{block1},
			zoneAwarenessEnabled: true,
			exclude: map[ulid.ULID][]string{
				block1: {"127.0.0.1", "127.0.0.6", "127.0.0.8"},
			},
			attemptedBlocksZones: map[ulid.ULID]map[string]int{
				block1: {"1": 1, "2": 1, "3": 1},
			},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.2": {block1},
			},
		},
		"shuffle sharding, multiple instances in the ring with RF = 6, SS = 6, exclude 2 blocks and zone awareness enabled": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   6,
			replicationFactor: 6,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "1", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "2", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "3", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "1", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-5", "127.0.0.5", "2", []uint32{block5Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-6", "127.0.0.6", "3", []uint32{block6Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-7", "127.0.0.7", "1", []uint32{block7Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-8", "127.0.0.8", "2", []uint32{block8Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-9", "127.0.0.9", "3", []uint32{block9Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks:          []ulid.ULID{block1},
			zoneAwarenessEnabled: true,
			exclude: map[ulid.ULID][]string{
				block1: {"127.0.0.1", "127.0.0.6"},
			},
			attemptedBlocksZones: map[ulid.ULID]map[string]int{
				block1: {"1": 1, "3": 1},
			},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.2": {block1},
			},
		},
		// This should never happen, just to test the attemptedZoneMap works correctly.
		"shuffle sharding, multiple instances in the ring with RF = 6, SS = 6, no exclude blocks but attempted 2 zones and zone awareness enabled": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   6,
			replicationFactor: 6,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "1", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "2", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "3", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "1", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-5", "127.0.0.5", "2", []uint32{block5Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-6", "127.0.0.6", "3", []uint32{block6Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-7", "127.0.0.7", "1", []uint32{block7Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-8", "127.0.0.8", "2", []uint32{block8Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-9", "127.0.0.9", "3", []uint32{block9Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks:          []ulid.ULID{block1},
			zoneAwarenessEnabled: true,
			attemptedBlocksZones: map[ulid.ULID]map[string]int{
				block1: {"1": 1, "3": 1},
			},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.2": {block1},
			},
		},
		// This should never happen, just to test the attemptedZoneMap works correctly.
		"shuffle sharding, multiple instances in the ring with RF = 6, SS = 6, one exclude block but attempted 2 zones and zone awareness enabled": {
			shardingStrategy:  util.ShardingStrategyShuffle,
			tenantShardSize:   6,
			replicationFactor: 6,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "1", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-2", "127.0.0.2", "2", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-3", "127.0.0.3", "3", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-4", "127.0.0.4", "1", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-5", "127.0.0.5", "2", []uint32{block5Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-6", "127.0.0.6", "3", []uint32{block6Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-7", "127.0.0.7", "1", []uint32{block7Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-8", "127.0.0.8", "2", []uint32{block8Hash + 1}, ring.ACTIVE, registeredAt)
				d.AddIngester("instance-9", "127.0.0.9", "3", []uint32{block9Hash + 1}, ring.ACTIVE, registeredAt)
			},
			queryBlocks:          []ulid.ULID{block1},
			zoneAwarenessEnabled: true,
			exclude: map[ulid.ULID][]string{
				block1: {"127.0.0.2"},
			},
			attemptedBlocksZones: map[ulid.ULID]map[string]int{
				block1: {"1": 1, "3": 1},
			},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.8": {block1},
			},
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			// Setup the ring state.
			ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			require.NoError(t, ringStore.CAS(ctx, "test", func(in interface{}) (interface{}, bool, error) {
				d := ring.NewDesc()
				testData.setup(d)
				return d, true, nil
			}))

			ringCfg := ring.Config{}
			flagext.DefaultValues(&ringCfg)
			ringCfg.ReplicationFactor = testData.replicationFactor
			ringCfg.HeartbeatTimeout = time.Hour

			r, err := ring.NewWithStoreClientAndStrategy(ringCfg, "test", "test", ringStore, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, nil)
			require.NoError(t, err)

			limits := &blocksStoreLimitsMock{
				storeGatewayTenantShardSize: testData.tenantShardSize,
			}

			reg := prometheus.NewPedanticRegistry()
			s, err := newBlocksStoreReplicationSet(r, testData.shardingStrategy, noLoadBalancing, limits, ClientConfig{}, log.NewNopLogger(), reg, testData.zoneAwarenessEnabled, true)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(ctx, s))
			defer services.StopAndAwaitTerminated(ctx, s) //nolint:errcheck

			// Wait until the ring client has initialised the state.
			test.Poll(t, time.Second, true, func() interface{} {
				all, err := r.GetAllHealthy(ring.Read)
				return err == nil && len(all.Instances) > 0
			})

			clients, err := s.GetClientsFor(userID, testData.queryBlocks, testData.exclude, testData.attemptedBlocksZones)
			assert.Equal(t, testData.expectedErr, err)

			if testData.expectedErr == nil {
				assert.Equal(t, testData.expectedClients, getStoreGatewayClientAddrs(clients))

				assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP cortex_storegateway_clients The current number of store-gateway clients in the pool.
					# TYPE cortex_storegateway_clients gauge
					cortex_storegateway_clients{client="querier"} %d
				`, len(testData.expectedClients))), "cortex_storegateway_clients"))
			}
		})
	}
}

func TestBlocksStoreReplicationSet_GetClientsFor_ShouldSupportRandomLoadBalancingStrategy(t *testing.T) {
	t.Parallel()

	const (
		numRuns      = 1000
		numInstances = 3
	)

	ctx := context.Background()
	userID := "user-A"
	registeredAt := time.Now()
	block1 := ulid.MustNew(1, nil)

	// Create a ring.
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	require.NoError(t, ringStore.CAS(ctx, "test", func(in interface{}) (interface{}, bool, error) {
		d := ring.NewDesc()
		for n := 1; n <= numInstances; n++ {
			d.AddIngester(fmt.Sprintf("instance-%d", n), fmt.Sprintf("127.0.0.%d", n), "", []uint32{uint32(n)}, ring.ACTIVE, registeredAt)
		}
		return d, true, nil
	}))

	// Configure a replication factor equal to the number of instances, so that every store-gateway gets all blocks.
	ringCfg := ring.Config{}
	flagext.DefaultValues(&ringCfg)
	ringCfg.ReplicationFactor = numInstances

	r, err := ring.NewWithStoreClientAndStrategy(ringCfg, "test", "test", ringStore, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, nil)
	require.NoError(t, err)

	limits := &blocksStoreLimitsMock{}
	reg := prometheus.NewPedanticRegistry()
	s, err := newBlocksStoreReplicationSet(r, util.ShardingStrategyDefault, randomLoadBalancing, limits, ClientConfig{}, log.NewNopLogger(), reg, false, false)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, s))
	defer services.StopAndAwaitTerminated(ctx, s) //nolint:errcheck

	// Wait until the ring client has initialised the state.
	test.Poll(t, time.Second, true, func() interface{} {
		all, err := r.GetAllHealthy(ring.Read)
		return err == nil && len(all.Instances) > 0
	})

	// Request the same block multiple times and ensure the distribution of
	// requests across store-gateways is balanced.
	distribution := map[string]int{}

	for n := 0; n < numRuns; n++ {
		clients, err := s.GetClientsFor(userID, []ulid.ULID{block1}, nil, nil)
		require.NoError(t, err)
		require.Len(t, clients, 1)

		for addr := range getStoreGatewayClientAddrs(clients) {
			distribution[addr]++
		}
	}

	assert.Len(t, distribution, numInstances)
	for addr, count := range distribution {
		// Ensure that the number of times each client is returned is above
		// the 80% of the perfect even distribution.
		assert.Greaterf(t, float64(count), (float64(numRuns)/float64(numInstances))*0.8, "store-gateway address: %s", addr)
	}
}

func TestBlocksStoreReplicationSet_GetClientsFor_ZoneAwareness(t *testing.T) {
	t.Parallel()

	const (
		numRuns      = 1000
		numInstances = 9
	)

	ctx := context.Background()
	userID := "user-A"
	registeredAt := time.Now()
	block1 := ulid.MustNew(1, nil)

	// Create a ring.
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	require.NoError(t, ringStore.CAS(ctx, "test", func(in interface{}) (interface{}, bool, error) {
		d := ring.NewDesc()
		for n := 1; n <= numInstances; n++ {
			zone := strconv.Itoa((n-1)%3 + 1)
			d.AddIngester(fmt.Sprintf("instance-%d", n), fmt.Sprintf("127.0.0.%d", n), zone, []uint32{uint32(n)}, ring.ACTIVE, registeredAt)
		}
		return d, true, nil
	}))

	// Configure a replication factor equal to the number of instances, so that every store-gateway gets all blocks.
	ringCfg := ring.Config{}
	flagext.DefaultValues(&ringCfg)
	ringCfg.ReplicationFactor = numInstances

	r, err := ring.NewWithStoreClientAndStrategy(ringCfg, "test", "test", ringStore, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, nil)
	require.NoError(t, err)

	limits := &blocksStoreLimitsMock{}
	reg := prometheus.NewPedanticRegistry()
	s, err := newBlocksStoreReplicationSet(r, util.ShardingStrategyDefault, randomLoadBalancing, limits, ClientConfig{}, log.NewNopLogger(), reg, true, false)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, s))
	defer services.StopAndAwaitTerminated(ctx, s) //nolint:errcheck

	// Wait until the ring client has initialised the state.
	test.Poll(t, time.Second, true, func() interface{} {
		all, err := r.GetAllHealthy(ring.Read)
		return err == nil && len(all.Instances) > 0
	})

	// Target hit shouldn't exist in the blocksMap.
	targets := [3]int{3, 2, 1}
	for i := 0; i < numRuns; i++ {
		blocksMap := [3]map[string]int{
			{"1": 1, "2": 1},
			{"1": 1, "3": 1},
			{"2": 1, "3": 1},
		}
		attemptedBlocksZone := map[ulid.ULID]map[string]int{
			block1: blocksMap[i%3],
		}
		clients, err := s.GetClientsFor(userID, []ulid.ULID{block1}, nil, attemptedBlocksZone)
		require.NoError(t, err)
		require.Len(t, clients, 1)
		for c := range clients {
			addr := c.RemoteAddress()
			parts := strings.Split(addr, ".")
			require.True(t, len(parts) > 3)
			id, err := strconv.Atoi(parts[3])
			require.NoError(t, err)
			require.Equal(t, targets[i%3], (id-1)%3+1)
		}
	}
}

func getStoreGatewayClientAddrs(clients map[BlocksStoreClient][]ulid.ULID) map[string][]ulid.ULID {
	addrs := map[string][]ulid.ULID{}
	for c, blockIDs := range clients {
		addrs[c.RemoteAddress()] = blockIDs
	}
	return addrs
}
