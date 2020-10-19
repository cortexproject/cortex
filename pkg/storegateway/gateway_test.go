package storegateway

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/storage/backend/filesystem"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		setup    func(cfg *Config, limits *validation.Limits)
		expected error
	}{
		"should pass by default": {
			setup:    func(cfg *Config, limits *validation.Limits) {},
			expected: nil,
		},
		"should fail if the sharding strategy is invalid": {
			setup: func(cfg *Config, limits *validation.Limits) {
				cfg.ShardingEnabled = true
				cfg.ShardingStrategy = "xxx"
			},
			expected: errInvalidShardingStrategy,
		},
		"should fail if the sharding strategy is shuffle-sharding and shard size has not been set": {
			setup: func(cfg *Config, limits *validation.Limits) {
				cfg.ShardingEnabled = true
				cfg.ShardingStrategy = ShardingStrategyShuffle
			},
			expected: errInvalidTenantShardSize,
		},
		"should pass if the sharding strategy is shuffle-sharding and shard size has been set": {
			setup: func(cfg *Config, limits *validation.Limits) {
				cfg.ShardingEnabled = true
				cfg.ShardingStrategy = ShardingStrategyShuffle
				limits.StoreGatewayTenantShardSize = 3
			},
			expected: nil,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := &Config{}
			limits := &validation.Limits{}
			flagext.DefaultValues(cfg, limits)
			testData.setup(cfg, limits)

			assert.Equal(t, testData.expected, cfg.Validate(*limits))
		})
	}
}

func TestStoreGateway_InitialSyncWithDefaultShardingEnabled(t *testing.T) {
	tests := map[string]struct {
		initialExists bool
		initialState  ring.IngesterState
		initialTokens ring.Tokens
	}{
		"instance not in the ring": {
			initialExists: false,
		},
		"instance already in the ring with PENDING state and has no tokens": {
			initialExists: true,
			initialState:  ring.PENDING,
			initialTokens: ring.Tokens{},
		},
		"instance already in the ring with JOINING state and has some tokens": {
			initialExists: true,
			initialState:  ring.JOINING,
			initialTokens: ring.Tokens{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		"instance already in the ring with ACTIVE state and has all tokens": {
			initialExists: true,
			initialState:  ring.ACTIVE,
			initialTokens: generateSortedTokens(RingNumTokens),
		},
		"instance already in the ring with LEAVING state and has all tokens": {
			initialExists: true,
			initialState:  ring.LEAVING,
			initialTokens: generateSortedTokens(RingNumTokens),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := context.Background()
			gatewayCfg := mockGatewayConfig()
			gatewayCfg.ShardingEnabled = true
			storageCfg, cleanup := mockStorageConfig(t)
			defer cleanup()
			ringStore := consul.NewInMemoryClient(ring.GetCodec())
			bucketClient := &cortex_tsdb.BucketClientMock{}

			// Setup the initial instance state in the ring.
			if testData.initialExists {
				require.NoError(t, ringStore.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
					ringDesc := ring.GetOrCreateRingDesc(in)
					ringDesc.AddIngester(gatewayCfg.ShardingRing.InstanceID, gatewayCfg.ShardingRing.InstanceAddr, "", testData.initialTokens, testData.initialState, time.Now())
					return ringDesc, true, nil
				}))
			}

			g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, defaultLimitsOverrides(t), mockLoggingLevel(), log.NewNopLogger(), nil)
			require.NoError(t, err)
			defer services.StopAndAwaitTerminated(ctx, g) //nolint:errcheck
			assert.False(t, g.ringLifecycler.IsRegistered())

			bucketClient.MockIterWithCallback("", []string{"user-1", "user-2"}, nil, func() {
				// During the initial sync, we expect the instance to always be in the JOINING
				// state within the ring.
				assert.True(t, g.ringLifecycler.IsRegistered())
				assert.Equal(t, ring.JOINING, g.ringLifecycler.GetState())
				assert.Equal(t, RingNumTokens, len(g.ringLifecycler.GetTokens()))
				assert.Subset(t, g.ringLifecycler.GetTokens(), testData.initialTokens)
			})
			bucketClient.MockIter("user-1/", []string{}, nil)
			bucketClient.MockIter("user-2/", []string{}, nil)

			// Once successfully started, the instance should be ACTIVE in the ring.
			require.NoError(t, services.StartAndAwaitRunning(ctx, g))

			assert.True(t, g.ringLifecycler.IsRegistered())
			assert.Equal(t, ring.ACTIVE, g.ringLifecycler.GetState())
			assert.Equal(t, RingNumTokens, len(g.ringLifecycler.GetTokens()))
			assert.Subset(t, g.ringLifecycler.GetTokens(), testData.initialTokens)

			assert.NotNil(t, g.stores.getStore("user-1"))
			assert.NotNil(t, g.stores.getStore("user-2"))
			assert.Nil(t, g.stores.getStore("user-unknown"))
		})
	}
}

func TestStoreGateway_InitialSyncWithShardingDisabled(t *testing.T) {
	ctx := context.Background()
	gatewayCfg := mockGatewayConfig()
	gatewayCfg.ShardingEnabled = false
	storageCfg, cleanup := mockStorageConfig(t)
	defer cleanup()
	bucketClient := &cortex_tsdb.BucketClientMock{}

	g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, nil, defaultLimitsOverrides(t), mockLoggingLevel(), log.NewNopLogger(), nil)
	require.NoError(t, err)
	defer services.StopAndAwaitTerminated(ctx, g) //nolint:errcheck

	bucketClient.MockIter("", []string{"user-1", "user-2"}, nil)
	bucketClient.MockIter("user-1/", []string{}, nil)
	bucketClient.MockIter("user-2/", []string{}, nil)

	require.NoError(t, services.StartAndAwaitRunning(ctx, g))
	assert.NotNil(t, g.stores.getStore("user-1"))
	assert.NotNil(t, g.stores.getStore("user-2"))
	assert.Nil(t, g.stores.getStore("user-unknown"))
}

func TestStoreGateway_InitialSyncFailure(t *testing.T) {
	ctx := context.Background()
	gatewayCfg := mockGatewayConfig()
	gatewayCfg.ShardingEnabled = true
	storageCfg, cleanup := mockStorageConfig(t)
	defer cleanup()
	ringStore := consul.NewInMemoryClient(ring.GetCodec())
	bucketClient := &cortex_tsdb.BucketClientMock{}

	g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, defaultLimitsOverrides(t), mockLoggingLevel(), log.NewNopLogger(), nil)
	require.NoError(t, err)

	bucketClient.MockIter("", []string{}, errors.New("network error"))

	require.NoError(t, g.StartAsync(ctx))
	err = g.AwaitRunning(ctx)
	assert.Error(t, err)
	assert.Equal(t, services.Failed, g.State())

	// We expect a clean shutdown, including unregistering the instance from the ring.
	assert.False(t, g.ringLifecycler.IsRegistered())
}

func TestStoreGateway_BlocksSharding(t *testing.T) {
	storageDir, err := ioutil.TempDir(os.TempDir(), "")
	require.NoError(t, err)
	defer os.RemoveAll(storageDir) //nolint:errcheck

	// This tests uses real TSDB blocks. 24h time range, 2h block range period,
	// 2 users = total (24 / 12) * 2 = 24 blocks.
	numUsers := 2
	numBlocks := numUsers * 12
	now := time.Now()
	require.NoError(t, mockTSDB(path.Join(storageDir, "user-1"), 24, now.Add(-24*time.Hour).Unix()*1000, now.Unix()*1000))
	require.NoError(t, mockTSDB(path.Join(storageDir, "user-2"), 24, now.Add(-24*time.Hour).Unix()*1000, now.Unix()*1000))

	bucketClient, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	tests := map[string]struct {
		shardingStrategy     string // Empty string means disabled.
		tenantShardSize      int    // Used only when the sharding strategy is shuffle-sharding.
		replicationFactor    int
		numGateways          int
		expectedBlocksLoaded int
	}{
		"sharding disabled, 1 gateway": {
			shardingStrategy:     "",
			numGateways:          1,
			expectedBlocksLoaded: numBlocks,
		},
		"sharding disabled, 2 gateways": {
			shardingStrategy:     "",
			numGateways:          2,
			expectedBlocksLoaded: 2 * numBlocks, // each gateway loads all the blocks
		},
		"default sharding strategy, 1 gateway, RF = 1": {
			shardingStrategy:     ShardingStrategyDefault,
			replicationFactor:    1,
			numGateways:          1,
			expectedBlocksLoaded: numBlocks,
		},
		"default sharding strategy, 2 gateways, RF = 1": {
			shardingStrategy:     ShardingStrategyDefault,
			replicationFactor:    1,
			numGateways:          2,
			expectedBlocksLoaded: numBlocks, // blocks are sharded across gateways
		},
		"default sharding strategy, 3 gateways, RF = 2": {
			shardingStrategy:     ShardingStrategyDefault,
			replicationFactor:    2,
			numGateways:          3,
			expectedBlocksLoaded: 2 * numBlocks, // blocks are replicated 2 times
		},
		"default sharding strategy, 5 gateways, RF = 3": {
			shardingStrategy:     ShardingStrategyDefault,
			replicationFactor:    3,
			numGateways:          5,
			expectedBlocksLoaded: 3 * numBlocks, // blocks are replicated 3 times
		},
		"shuffle sharding strategy, 1 gateway, RF = 1, SS = 1": {
			shardingStrategy:     ShardingStrategyShuffle,
			tenantShardSize:      1,
			replicationFactor:    1,
			numGateways:          1,
			expectedBlocksLoaded: numBlocks,
		},
		"shuffle sharding strategy, 5 gateways, RF = 2, SS = 3": {
			shardingStrategy:     ShardingStrategyShuffle,
			tenantShardSize:      3,
			replicationFactor:    2,
			numGateways:          5,
			expectedBlocksLoaded: 2 * numBlocks, // blocks are replicated 2 times
		},
		"shuffle sharding strategy, 20 gateways, RF = 3, SS = 3": {
			shardingStrategy:     ShardingStrategyShuffle,
			tenantShardSize:      3,
			replicationFactor:    3,
			numGateways:          20,
			expectedBlocksLoaded: 3 * numBlocks, // blocks are replicated 3 times
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := context.Background()
			storageCfg, cleanup := mockStorageConfig(t)
			storageCfg.BucketStore.SyncInterval = time.Hour // Do not trigger the periodic sync in this test (we explicitly sync stores).
			defer cleanup()
			ringStore := consul.NewInMemoryClient(ring.GetCodec())

			// Start the configure number of gateways.
			var gateways []*StoreGateway
			var gatewayIds []string
			registries := map[string]*prometheus.Registry{}

			for i := 1; i <= testData.numGateways; i++ {
				instanceID := fmt.Sprintf("gateway-%d", i)

				limits := defaultLimitsConfig()
				gatewayCfg := mockGatewayConfig()
				gatewayCfg.ShardingRing.ReplicationFactor = testData.replicationFactor
				gatewayCfg.ShardingRing.InstanceID = instanceID
				gatewayCfg.ShardingRing.InstanceAddr = fmt.Sprintf("127.0.0.%d", i)
				gatewayCfg.ShardingRing.RingCheckPeriod = time.Hour // Do not check the ring topology changes in this test (we explicitly sync stores).

				if testData.shardingStrategy == "" {
					gatewayCfg.ShardingEnabled = false
				} else {
					gatewayCfg.ShardingEnabled = true
					gatewayCfg.ShardingStrategy = testData.shardingStrategy
					limits.StoreGatewayTenantShardSize = testData.tenantShardSize
				}

				overrides, err := validation.NewOverrides(limits, nil)
				require.NoError(t, err)

				reg := prometheus.NewPedanticRegistry()
				g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, overrides, mockLoggingLevel(), log.NewNopLogger(), reg)
				require.NoError(t, err)
				defer services.StopAndAwaitTerminated(ctx, g) //nolint:errcheck

				require.NoError(t, services.StartAndAwaitRunning(ctx, g))

				gateways = append(gateways, g)
				gatewayIds = append(gatewayIds, instanceID)
				registries[instanceID] = reg
			}

			// Wait until the ring client of each gateway has synced (to avoid flaky tests on subsequent assertions).
			if testData.shardingStrategy != "" {
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				// A gateway is ready for the test once it sees all instances ACTIVE in the ring.
				for _, g := range gateways {
					for _, instanceID := range gatewayIds {
						require.NoError(t, ring.WaitInstanceState(ctx, g.ring, instanceID, ring.ACTIVE))
					}
				}
			}

			// Re-sync the stores because the ring topology has changed in the meanwhile
			// (when the 1st gateway has synched the 2nd gateway didn't run yet).
			for _, g := range gateways {
				g.syncStores(ctx, syncReasonRingChange)
			}

			// Assert on the number of blocks loaded extracting this information from metrics.
			metrics := util.BuildMetricFamiliesPerUserFromUserRegistries(registries)
			assert.Equal(t, float64(testData.expectedBlocksLoaded), metrics.GetSumOfGauges("cortex_bucket_store_blocks_loaded"))
			assert.Equal(t, float64(2*testData.numGateways), metrics.GetSumOfGauges("cortex_bucket_stores_tenants_discovered"))

			if testData.shardingStrategy == ShardingStrategyShuffle {
				assert.Equal(t, float64(testData.tenantShardSize*numBlocks), metrics.GetSumOfGauges("cortex_blocks_meta_synced"))
				assert.Equal(t, float64(testData.tenantShardSize*numUsers), metrics.GetSumOfGauges("cortex_bucket_stores_tenants_synced"))
			} else {
				assert.Equal(t, float64(testData.numGateways*numBlocks), metrics.GetSumOfGauges("cortex_blocks_meta_synced"))
				assert.Equal(t, float64(testData.numGateways*numUsers), metrics.GetSumOfGauges("cortex_bucket_stores_tenants_synced"))
			}
		})
	}
}

func TestStoreGateway_ShouldSupportLoadRingTokensFromFile(t *testing.T) {
	tests := map[string]struct {
		storedTokens      ring.Tokens
		expectedNumTokens int
	}{
		"stored tokens are less than the configured ones": {
			storedTokens:      generateSortedTokens(RingNumTokens - 10),
			expectedNumTokens: RingNumTokens,
		},
		"stored tokens are equal to the configured ones": {
			storedTokens:      generateSortedTokens(RingNumTokens),
			expectedNumTokens: RingNumTokens,
		},
		"stored tokens are more then the configured ones": {
			storedTokens:      generateSortedTokens(RingNumTokens + 10),
			expectedNumTokens: RingNumTokens + 10,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			tokensFile, err := ioutil.TempFile(os.TempDir(), "tokens-*")
			require.NoError(t, err)
			defer os.Remove(tokensFile.Name()) //nolint:errcheck

			// Store some tokens to the file.
			require.NoError(t, testData.storedTokens.StoreToFile(tokensFile.Name()))

			ctx := context.Background()
			gatewayCfg := mockGatewayConfig()
			gatewayCfg.ShardingEnabled = true
			gatewayCfg.ShardingRing.TokensFilePath = tokensFile.Name()

			storageCfg, cleanup := mockStorageConfig(t)
			defer cleanup()
			ringStore := consul.NewInMemoryClient(ring.GetCodec())
			bucketClient := &cortex_tsdb.BucketClientMock{}
			bucketClient.MockIter("", []string{}, nil)

			g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, defaultLimitsOverrides(t), mockLoggingLevel(), log.NewNopLogger(), nil)
			require.NoError(t, err)
			defer services.StopAndAwaitTerminated(ctx, g) //nolint:errcheck
			assert.False(t, g.ringLifecycler.IsRegistered())

			require.NoError(t, services.StartAndAwaitRunning(ctx, g))
			assert.True(t, g.ringLifecycler.IsRegistered())
			assert.Equal(t, ring.ACTIVE, g.ringLifecycler.GetState())
			assert.Len(t, g.ringLifecycler.GetTokens(), testData.expectedNumTokens)
			assert.Subset(t, g.ringLifecycler.GetTokens(), testData.storedTokens)
		})
	}
}

func TestStoreGateway_SyncOnRingTopologyChanged(t *testing.T) {
	registeredAt := time.Now()

	tests := map[string]struct {
		setupRing    func(desc *ring.Desc)
		updateRing   func(desc *ring.Desc)
		expectedSync bool
	}{
		"should sync when an instance is added to the ring": {
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("instance-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt)
			},
			updateRing: func(desc *ring.Desc) {
				desc.AddIngester("instance-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.ACTIVE, registeredAt)
			},
			expectedSync: true,
		},
		"should sync when an instance is removed from the ring": {
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("instance-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt)
				desc.AddIngester("instance-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.ACTIVE, registeredAt)
			},
			updateRing: func(desc *ring.Desc) {
				desc.RemoveIngester("instance-1")
			},
			expectedSync: true,
		},
		"should sync when an instance changes state": {
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("instance-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt)
				desc.AddIngester("instance-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.JOINING, registeredAt)
			},
			updateRing: func(desc *ring.Desc) {
				instance := desc.Ingesters["instance-2"]
				instance.State = ring.ACTIVE
				desc.Ingesters["instance-2"] = instance
			},
			expectedSync: true,
		},
		"should sync when an healthy instance becomes unhealthy": {
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("instance-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt)
				desc.AddIngester("instance-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.ACTIVE, registeredAt)
			},
			updateRing: func(desc *ring.Desc) {
				instance := desc.Ingesters["instance-2"]
				instance.Timestamp = time.Now().Add(-time.Hour).Unix()
				desc.Ingesters["instance-2"] = instance
			},
			expectedSync: true,
		},
		"should sync when an unhealthy instance becomes healthy": {
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("instance-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt)

				instance := desc.AddIngester("instance-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.ACTIVE, registeredAt)
				instance.Timestamp = time.Now().Add(-time.Hour).Unix()
				desc.Ingesters["instance-2"] = instance
			},
			updateRing: func(desc *ring.Desc) {
				instance := desc.Ingesters["instance-2"]
				instance.Timestamp = time.Now().Unix()
				desc.Ingesters["instance-2"] = instance
			},
			expectedSync: true,
		},
		"should NOT sync when an instance updates the heartbeat": {
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("instance-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt)
				desc.AddIngester("instance-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.ACTIVE, registeredAt)
			},
			updateRing: func(desc *ring.Desc) {
				instance := desc.Ingesters["instance-2"]
				instance.Timestamp = time.Now().Add(time.Second).Unix()
				desc.Ingesters["instance-2"] = instance
			},
			expectedSync: false,
		},
		"should NOT sync when an instance is auto-forgotten in the ring but was already unhealthy in the previous state": {
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("instance-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt)
				desc.AddIngester("instance-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.ACTIVE, registeredAt)

				// Set it already unhealthy.
				instance := desc.Ingesters["instance-2"]
				instance.Timestamp = time.Now().Add(-time.Hour).Unix()
				desc.Ingesters["instance-2"] = instance
			},
			updateRing: func(desc *ring.Desc) {
				// Remove the unhealthy instance from the ring.
				desc.RemoveIngester("instance-2")
			},
			expectedSync: false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := context.Background()
			gatewayCfg := mockGatewayConfig()
			gatewayCfg.ShardingEnabled = true
			gatewayCfg.ShardingRing.RingCheckPeriod = 100 * time.Millisecond

			storageCfg, cleanup := mockStorageConfig(t)
			storageCfg.BucketStore.SyncInterval = time.Hour // Do not trigger the periodic sync in this test.
			defer cleanup()

			reg := prometheus.NewPedanticRegistry()
			ringStore := consul.NewInMemoryClient(ring.GetCodec())
			bucketClient := &cortex_tsdb.BucketClientMock{}
			bucketClient.MockIter("", []string{}, nil)

			g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, defaultLimitsOverrides(t), mockLoggingLevel(), log.NewNopLogger(), reg)
			require.NoError(t, err)

			// Store the initial ring state before starting the gateway.
			require.NoError(t, ringStore.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
				ringDesc := ring.GetOrCreateRingDesc(in)
				testData.setupRing(ringDesc)
				return ringDesc, true, nil
			}))

			require.NoError(t, services.StartAndAwaitRunning(ctx, g))
			defer services.StopAndAwaitTerminated(ctx, g) //nolint:errcheck

			// Assert on the initial state.
			metrics := util.BuildMetricFamiliesPerUserFromUserRegistries(map[string]*prometheus.Registry{"test": reg})
			assert.Equal(t, float64(1), metrics.GetSumOfCounters("cortex_storegateway_bucket_sync_total"))

			// Change the ring topology.
			require.NoError(t, ringStore.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
				ringDesc := ring.GetOrCreateRingDesc(in)
				testData.updateRing(ringDesc)
				return ringDesc, true, nil
			}))

			// Assert whether the sync triggered or not.
			if testData.expectedSync {
				test.Poll(t, time.Second, float64(2), func() interface{} {
					metrics := util.BuildMetricFamiliesPerUserFromUserRegistries(map[string]*prometheus.Registry{"test": reg})
					return metrics.GetSumOfCounters("cortex_storegateway_bucket_sync_total")
				})
			} else {
				// Give some time to the store-gateway to trigger the sync (if any).
				time.Sleep(250 * time.Millisecond)

				metrics := util.BuildMetricFamiliesPerUserFromUserRegistries(map[string]*prometheus.Registry{"test": reg})
				assert.Equal(t, float64(1), metrics.GetSumOfCounters("cortex_storegateway_bucket_sync_total"))
			}
		})
	}
}

func TestStoreGateway_RingLifecyclerShouldAutoForgetUnhealthyInstances(t *testing.T) {
	const unhealthyInstanceID = "unhealthy-id"
	const heartbeatTimeout = time.Minute

	ctx := context.Background()
	gatewayCfg := mockGatewayConfig()
	gatewayCfg.ShardingEnabled = true
	gatewayCfg.ShardingRing.HeartbeatPeriod = 100 * time.Millisecond
	gatewayCfg.ShardingRing.HeartbeatTimeout = heartbeatTimeout

	storageCfg, cleanup := mockStorageConfig(t)
	defer cleanup()

	ringStore := consul.NewInMemoryClient(ring.GetCodec())
	bucketClient := &cortex_tsdb.BucketClientMock{}
	bucketClient.MockIter("", []string{}, nil)

	g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, ringStore, defaultLimitsOverrides(t), mockLoggingLevel(), log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, g))
	defer services.StopAndAwaitTerminated(ctx, g) //nolint:errcheck

	// Add an unhealthy instance to the ring.
	require.NoError(t, ringStore.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
		ringDesc := ring.GetOrCreateRingDesc(in)

		instance := ringDesc.AddIngester(unhealthyInstanceID, "1.1.1.1", "", generateSortedTokens(RingNumTokens), ring.ACTIVE, time.Now())
		instance.Timestamp = time.Now().Add(-(ringAutoForgetUnhealthyPeriods + 1) * heartbeatTimeout).Unix()
		ringDesc.Ingesters[unhealthyInstanceID] = instance

		return ringDesc, true, nil
	}))

	// Ensure the unhealthy instance is removed from the ring.
	test.Poll(t, time.Second, false, func() interface{} {
		d, err := ringStore.Get(ctx, RingKey)
		if err != nil {
			return err
		}

		_, ok := ring.GetOrCreateRingDesc(d).Ingesters[unhealthyInstanceID]
		return ok
	})
}

func TestStoreGateway_SeriesQueryingShouldRemoveExternalLabels(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNopLogger()
	userID := "user-1"

	storageDir, err := ioutil.TempDir(os.TempDir(), "")
	require.NoError(t, err)
	defer os.RemoveAll(storageDir) //nolint:errcheck

	// Generate 2 TSDB blocks with the same exact series (and data points).
	numSeries := 2
	now := time.Now()
	minT := now.Add(-1*time.Hour).Unix() * 1000
	maxT := now.Unix() * 1000
	step := (maxT - minT) / int64(numSeries)
	require.NoError(t, mockTSDB(path.Join(storageDir, userID), numSeries, minT, maxT))
	require.NoError(t, mockTSDB(path.Join(storageDir, userID), numSeries, minT, maxT))

	bucketClient, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	// Find the created blocks (we expect 2).
	var blockIDs []string
	require.NoError(t, bucketClient.Iter(ctx, "user-1/", func(key string) error {
		blockIDs = append(blockIDs, strings.TrimSuffix(strings.TrimPrefix(key, userID+"/"), "/"))
		return nil
	}))
	require.Len(t, blockIDs, 2)

	// Inject different external labels for each block.
	for idx, blockID := range blockIDs {
		meta := metadata.Thanos{
			Labels: map[string]string{
				cortex_tsdb.TenantIDExternalLabel:   userID,
				cortex_tsdb.IngesterIDExternalLabel: fmt.Sprintf("ingester-%d", idx),
				cortex_tsdb.ShardIDExternalLabel:    fmt.Sprintf("shard-%d", idx),
			},
			Source: metadata.TestSource,
		}

		_, err := metadata.InjectThanos(logger, filepath.Join(storageDir, userID, blockID), meta, nil)
		require.NoError(t, err)
	}

	// Create a store-gateway used to query back the series from the blocks.
	gatewayCfg := mockGatewayConfig()
	gatewayCfg.ShardingEnabled = false
	storageCfg, cleanup := mockStorageConfig(t)
	defer cleanup()

	g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, nil, defaultLimitsOverrides(t), mockLoggingLevel(), logger, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, g))
	defer services.StopAndAwaitTerminated(ctx, g) //nolint:errcheck

	// Query back all series.
	req := &storepb.SeriesRequest{
		MinTime: minT,
		MaxTime: maxT,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_RE, Name: "__name__", Value: ".*"},
		},
	}

	srv := newBucketStoreSeriesServer(setUserIDToGRPCContext(ctx, userID))
	err = g.Series(req, srv)
	require.NoError(t, err)
	assert.Empty(t, srv.Warnings)
	assert.Len(t, srv.SeriesSet, numSeries)

	for seriesID := 0; seriesID < numSeries; seriesID++ {
		actual := srv.SeriesSet[seriesID]

		// Ensure Cortex external labels have been removed.
		assert.Equal(t, []labelpb.ZLabel{{Name: "series_id", Value: strconv.Itoa(seriesID)}}, actual.Labels)

		// Ensure samples have been correctly queried. The Thanos store also deduplicate samples
		// in most cases, but it's not strictly required guaranteeing deduplication at this stage.
		samples, err := readSamplesFromChunks(actual.Chunks)
		require.NoError(t, err)
		assert.Equal(t, []sample{
			{ts: minT + (step * int64(seriesID)), value: float64(seriesID)},
		}, samples)
	}
}

func TestStoreGateway_SeriesQueryingShouldEnforceMaxChunksPerQueryLimit(t *testing.T) {
	const chunksQueried = 10

	tests := map[string]struct {
		limit       int
		expectedErr string
	}{
		"no limit enforced if zero": {
			limit:       0,
			expectedErr: "",
		},
		"should return NO error if the actual number of queried chunks is <= limit": {
			limit:       chunksQueried,
			expectedErr: "",
		},
		"should return error if the actual number of queried chunks is > limit": {
			limit:       chunksQueried - 1,
			expectedErr: fmt.Sprintf("exceeded chunks limit: limit %d violated (got %d)", chunksQueried-1, chunksQueried),
		},
	}

	ctx := context.Background()
	logger := log.NewNopLogger()
	userID := "user-1"

	storageDir, err := ioutil.TempDir(os.TempDir(), "")
	require.NoError(t, err)
	defer os.RemoveAll(storageDir) //nolint:errcheck

	// Generate 1 TSDB block with chunksQueried series. Since each mocked series contains only 1 sample,
	// it will also only have 1 chunk.
	now := time.Now()
	minT := now.Add(-1*time.Hour).Unix() * 1000
	maxT := now.Unix() * 1000
	require.NoError(t, mockTSDB(path.Join(storageDir, userID), chunksQueried, minT, maxT))

	bucketClient, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	// Create a store-gateway used to query back the series from the blocks.
	gatewayCfg := mockGatewayConfig()
	gatewayCfg.ShardingEnabled = false
	storageCfg, cleanup := mockStorageConfig(t)
	defer cleanup()

	// Prepare the request to query back all series (1 chunk per series in this test).
	req := &storepb.SeriesRequest{
		MinTime: minT,
		MaxTime: maxT,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_RE, Name: "__name__", Value: ".*"},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Customise the limits.
			limits := defaultLimitsConfig()
			limits.MaxChunksPerQuery = testData.limit
			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			g, err := newStoreGateway(gatewayCfg, storageCfg, bucketClient, nil, overrides, mockLoggingLevel(), logger, nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(ctx, g))
			defer services.StopAndAwaitTerminated(ctx, g) //nolint:errcheck

			// Query back all the series (1 chunk per series in this test).
			srv := newBucketStoreSeriesServer(setUserIDToGRPCContext(ctx, userID))
			err = g.Series(req, srv)

			if testData.expectedErr != "" {
				require.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), testData.expectedErr))
			} else {
				require.NoError(t, err)
				assert.Empty(t, srv.Warnings)
				assert.Len(t, srv.SeriesSet, chunksQueried)
			}
		})
	}
}

func mockGatewayConfig() Config {
	cfg := Config{}
	flagext.DefaultValues(&cfg)

	cfg.ShardingRing.InstanceID = "test"
	cfg.ShardingRing.InstanceAddr = "127.0.0.1"

	return cfg
}

func mockStorageConfig(t *testing.T) (cortex_tsdb.BlocksStorageConfig, func()) {
	tmpDir, err := ioutil.TempDir(os.TempDir(), "store-gateway-test-*")
	require.NoError(t, err)

	cfg := cortex_tsdb.BlocksStorageConfig{}
	flagext.DefaultValues(&cfg)

	cfg.BucketStore.ConsistencyDelay = 0
	cfg.BucketStore.SyncDir = tmpDir

	cleanup := func() {
		require.NoError(t, os.RemoveAll(tmpDir))
	}

	return cfg, cleanup
}

// mockTSDB create 1+ TSDB blocks storing numSeries of series, each series
// with 1 sample and its timestamp evenly distributed between minT and maxT.
func mockTSDB(dir string, numSeries int, minT, maxT int64) error {
	// Create a new TSDB on a temporary directory. The blocks
	// will be then snapshotted to the input dir.
	tempDir, err := ioutil.TempDir(os.TempDir(), "tsdb")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir) //nolint:errcheck

	db, err := tsdb.Open(tempDir, nil, nil, &tsdb.Options{
		MinBlockDuration:  2 * time.Hour.Milliseconds(),
		MaxBlockDuration:  2 * time.Hour.Milliseconds(),
		RetentionDuration: 15 * 24 * time.Hour.Milliseconds(),
	})
	if err != nil {
		return err
	}

	db.DisableCompactions()

	step := (maxT - minT) / int64(numSeries)
	for i := 0; i < numSeries; i++ {
		lbls := labels.Labels{labels.Label{Name: "series_id", Value: strconv.Itoa(i)}}

		app := db.Appender(context.Background())
		if _, err := app.Add(lbls, minT+(step*int64(i)), float64(i)); err != nil {
			return err
		}
		if err := app.Commit(); err != nil {
			return err
		}

		if err := db.Compact(); err != nil {
			return err
		}
	}

	if err := db.Snapshot(dir, true); err != nil {
		return err
	}

	return db.Close()
}

func generateSortedTokens(numTokens int) ring.Tokens {
	tokens := ring.GenerateTokens(numTokens, nil)

	// Ensure generated tokens are sorted.
	sort.Slice(tokens, func(i, j int) bool {
		return tokens[i] < tokens[j]
	})

	return ring.Tokens(tokens)
}

func readSamplesFromChunks(rawChunks []storepb.AggrChunk) ([]sample, error) {
	var samples []sample

	for _, rawChunk := range rawChunks {
		c, err := chunkenc.FromData(chunkenc.EncXOR, rawChunk.Raw.Data)
		if err != nil {
			return nil, err
		}

		it := c.Iterator(nil)
		for it.Next() {
			if it.Err() != nil {
				return nil, it.Err()
			}

			ts, v := it.At()
			samples = append(samples, sample{
				ts:    ts,
				value: v,
			})
		}

		if it.Err() != nil {
			return nil, it.Err()
		}
	}

	return samples, nil
}

type sample struct {
	ts    int64
	value float64
}

func defaultLimitsConfig() validation.Limits {
	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	return limits
}

func defaultLimitsOverrides(t *testing.T) *validation.Overrides {
	overrides, err := validation.NewOverrides(defaultLimitsConfig(), nil)
	require.NoError(t, err)

	return overrides
}

type mockShardingStrategy struct {
	mock.Mock
}

func (m *mockShardingStrategy) FilterUsers(ctx context.Context, userIDs []string) []string {
	args := m.Called(ctx, userIDs)
	return args.Get(0).([]string)
}

func (m *mockShardingStrategy) FilterBlocks(ctx context.Context, userID string, metas map[ulid.ULID]*metadata.Meta, synced *extprom.TxGaugeVec) error {
	args := m.Called(ctx, userID, metas, synced)
	return args.Error(0)
}
