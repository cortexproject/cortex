package ingester

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestSeriesLimit_maxSeriesPerMetric(t *testing.T) {
	tests := map[string]struct {
		maxLocalSeriesPerMetric  int
		maxGlobalSeriesPerMetric int
		ringReplicationFactor    int
		ringZoneAwarenessEnabled bool
		ringIngesterCount        int
		ringZonesCount           int
		shardByAllLabels         bool
		shardSize                int
		expectedDefaultSharding  int
		expectedShuffleSharding  int
	}{
		"both local and global limits are disabled": {
			maxLocalSeriesPerMetric:  0,
			maxGlobalSeriesPerMetric: 0,
			ringReplicationFactor:    1,
			ringIngesterCount:        1,
			ringZonesCount:           1,
			shardByAllLabels:         false,
			expectedDefaultSharding:  math.MaxInt32,
			expectedShuffleSharding:  math.MaxInt32,
		},
		"only local limit is enabled": {
			maxLocalSeriesPerMetric:  1000,
			maxGlobalSeriesPerMetric: 0,
			ringReplicationFactor:    1,
			ringIngesterCount:        1,
			ringZonesCount:           1,
			shardByAllLabels:         false,
			expectedDefaultSharding:  1000,
			expectedShuffleSharding:  1000,
		},
		"only global limit is enabled with shard-by-all-labels=false and replication-factor=1": {
			maxLocalSeriesPerMetric:  0,
			maxGlobalSeriesPerMetric: 1000,
			ringReplicationFactor:    1,
			ringIngesterCount:        10,
			ringZonesCount:           1,
			shardByAllLabels:         false,
			shardSize:                5,
			expectedDefaultSharding:  1000,
			expectedShuffleSharding:  1000,
		},
		"only global limit is enabled with shard-by-all-labels=true and replication-factor=1": {
			maxLocalSeriesPerMetric:  0,
			maxGlobalSeriesPerMetric: 1000,
			ringReplicationFactor:    1,
			ringIngesterCount:        10,
			ringZonesCount:           1,
			shardByAllLabels:         true,
			shardSize:                5,
			expectedDefaultSharding:  100,
			expectedShuffleSharding:  200,
		},
		"only global limit is enabled with shard-by-all-labels=true and replication-factor=3": {
			maxLocalSeriesPerMetric:  0,
			maxGlobalSeriesPerMetric: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			ringZonesCount:           1,
			shardByAllLabels:         true,
			shardSize:                5,
			expectedDefaultSharding:  300,
			expectedShuffleSharding:  600,
		},
		"both local and global limits are set with local limit < global limit": {
			maxLocalSeriesPerMetric:  150,
			maxGlobalSeriesPerMetric: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			ringZonesCount:           1,
			shardByAllLabels:         true,
			shardSize:                5,
			expectedDefaultSharding:  150,
			expectedShuffleSharding:  150,
		},
		"both local and global limits are set with local limit > global limit": {
			maxLocalSeriesPerMetric:  800,
			maxGlobalSeriesPerMetric: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			ringZonesCount:           1,
			shardByAllLabels:         true,
			shardSize:                5,
			expectedDefaultSharding:  300,
			expectedShuffleSharding:  600,
		},
		"zone-awareness enabled, global limit enabled and the shard size is NOT divisible by number of zones": {
			maxLocalSeriesPerMetric:  0,
			maxGlobalSeriesPerMetric: 900,
			ringReplicationFactor:    3,
			ringZoneAwarenessEnabled: true,
			ringIngesterCount:        9,
			ringZonesCount:           3,
			shardByAllLabels:         true,
			shardSize:                5, // Not divisible by number of zones.
			expectedDefaultSharding:  300,
			expectedShuffleSharding:  450, // (900 / 6) * 3
		},
		"zone-awareness enabled, global limit enabled and the shard size is divisible by number of zones": {
			maxLocalSeriesPerMetric:  0,
			maxGlobalSeriesPerMetric: 900,
			ringReplicationFactor:    3,
			ringZoneAwarenessEnabled: true,
			ringIngesterCount:        9,
			ringZonesCount:           3,
			shardByAllLabels:         true,
			shardSize:                6, // Divisible by number of zones.
			expectedDefaultSharding:  300,
			expectedShuffleSharding:  450, // (900 / 6) * 3
		},
		"zone-awareness enabled, global limit enabled and the shard size > number of ingesters": {
			maxLocalSeriesPerMetric:  0,
			maxGlobalSeriesPerMetric: 900,
			ringReplicationFactor:    3,
			ringZoneAwarenessEnabled: true,
			ringIngesterCount:        9,
			ringZonesCount:           3,
			shardByAllLabels:         true,
			shardSize:                20, // Greater than number of ingesters.
			expectedDefaultSharding:  300,
			expectedShuffleSharding:  300,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{}
			ring.On("HealthyInstancesCount").Return(testData.ringIngesterCount)
			ring.On("ZonesCount").Return(testData.ringZonesCount)

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxLocalSeriesPerMetric:  testData.maxLocalSeriesPerMetric,
				MaxGlobalSeriesPerMetric: testData.maxGlobalSeriesPerMetric,
				IngestionTenantShardSize: testData.shardSize,
			}, nil)
			require.NoError(t, err)

			// Assert on default sharding strategy.
			limiter := NewLimiter(limits, ring, util.ShardingStrategyDefault, testData.shardByAllLabels, testData.ringReplicationFactor, testData.ringZoneAwarenessEnabled)
			actual := limiter.maxSeriesPerMetric("test")
			assert.Equal(t, testData.expectedDefaultSharding, actual)

			// Assert on shuffle sharding strategy.
			limiter = NewLimiter(limits, ring, util.ShardingStrategyShuffle, testData.shardByAllLabels, testData.ringReplicationFactor, testData.ringZoneAwarenessEnabled)
			actual = limiter.maxSeriesPerMetric("test")
			assert.Equal(t, testData.expectedShuffleSharding, actual)
		})
	}
}

func TestSeriesLimit_maxMetadataPerMetric(t *testing.T) {
	tests := map[string]struct {
		maxLocalMetadataPerMetric  int
		maxGlobalMetadataPerMetric int
		ringReplicationFactor      int
		ringZoneAwarenessEnabled   bool
		ringIngesterCount          int
		ringZonesCount             int
		shardByAllLabels           bool
		shardSize                  int
		expectedDefaultSharding    int
		expectedShuffleSharding    int
	}{
		"both local and global limits are disabled": {
			maxLocalMetadataPerMetric:  0,
			maxGlobalMetadataPerMetric: 0,
			ringReplicationFactor:      1,
			ringIngesterCount:          1,
			ringZonesCount:             1,
			shardByAllLabels:           false,
			expectedDefaultSharding:    math.MaxInt32,
			expectedShuffleSharding:    math.MaxInt32,
		},
		"only local limit is enabled": {
			maxLocalMetadataPerMetric:  1000,
			maxGlobalMetadataPerMetric: 0,
			ringReplicationFactor:      1,
			ringIngesterCount:          1,
			ringZonesCount:             1,
			shardByAllLabels:           false,
			expectedDefaultSharding:    1000,
			expectedShuffleSharding:    1000,
		},
		"only global limit is enabled with shard-by-all-labels=false and replication-factor=1": {
			maxLocalMetadataPerMetric:  0,
			maxGlobalMetadataPerMetric: 1000,
			ringReplicationFactor:      1,
			ringIngesterCount:          10,
			ringZonesCount:             1,
			shardByAllLabels:           false,
			shardSize:                  5,
			expectedDefaultSharding:    1000,
			expectedShuffleSharding:    1000,
		},
		"only global limit is enabled with shard-by-all-labels=true and replication-factor=1": {
			maxLocalMetadataPerMetric:  0,
			maxGlobalMetadataPerMetric: 1000,
			ringReplicationFactor:      1,
			ringIngesterCount:          10,
			ringZonesCount:             1,
			shardByAllLabels:           true,
			shardSize:                  5,
			expectedDefaultSharding:    100,
			expectedShuffleSharding:    200,
		},
		"only global limit is enabled with shard-by-all-labels=true and replication-factor=3": {
			maxLocalMetadataPerMetric:  0,
			maxGlobalMetadataPerMetric: 1000,
			ringReplicationFactor:      3,
			ringIngesterCount:          10,
			ringZonesCount:             1,
			shardByAllLabels:           true,
			shardSize:                  5,
			expectedDefaultSharding:    300,
			expectedShuffleSharding:    600,
		},
		"both local and global limits are set with local limit < global limit": {
			maxLocalMetadataPerMetric:  150,
			maxGlobalMetadataPerMetric: 1000,
			ringReplicationFactor:      3,
			ringIngesterCount:          10,
			ringZonesCount:             1,
			shardByAllLabels:           true,
			shardSize:                  5,
			expectedDefaultSharding:    150,
			expectedShuffleSharding:    150,
		},
		"both local and global limits are set with local limit > global limit": {
			maxLocalMetadataPerMetric:  800,
			maxGlobalMetadataPerMetric: 1000,
			ringReplicationFactor:      3,
			ringIngesterCount:          10,
			ringZonesCount:             1,
			shardByAllLabels:           true,
			shardSize:                  5,
			expectedDefaultSharding:    300,
			expectedShuffleSharding:    600,
		},
		"zone-awareness enabled, global limit enabled and the shard size is NOT divisible by number of zones": {
			maxLocalMetadataPerMetric:  0,
			maxGlobalMetadataPerMetric: 900,
			ringReplicationFactor:      3,
			ringZoneAwarenessEnabled:   true,
			ringIngesterCount:          9,
			ringZonesCount:             3,
			shardByAllLabels:           true,
			shardSize:                  5, // Not divisible by number of zones.
			expectedDefaultSharding:    300,
			expectedShuffleSharding:    450, // (900 / 6) * 3
		},
		"zone-awareness enabled, global limit enabled and the shard size is divisible by number of zones": {
			maxLocalMetadataPerMetric:  0,
			maxGlobalMetadataPerMetric: 900,
			ringReplicationFactor:      3,
			ringZoneAwarenessEnabled:   true,
			ringIngesterCount:          9,
			ringZonesCount:             3,
			shardByAllLabels:           true,
			shardSize:                  6, // Divisible by number of zones.
			expectedDefaultSharding:    300,
			expectedShuffleSharding:    450, // (900 / 6) * 3
		},
		"zone-awareness enabled, global limit enabled and the shard size > number of ingesters": {
			maxLocalMetadataPerMetric:  0,
			maxGlobalMetadataPerMetric: 900,
			ringReplicationFactor:      3,
			ringZoneAwarenessEnabled:   true,
			ringIngesterCount:          9,
			ringZonesCount:             3,
			shardByAllLabels:           true,
			shardSize:                  20, // Greater than number of ingesters.
			expectedDefaultSharding:    300,
			expectedShuffleSharding:    300,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{}
			ring.On("HealthyInstancesCount").Return(testData.ringIngesterCount)
			ring.On("ZonesCount").Return(testData.ringZonesCount)

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxLocalMetadataPerMetric:  testData.maxLocalMetadataPerMetric,
				MaxGlobalMetadataPerMetric: testData.maxGlobalMetadataPerMetric,
				IngestionTenantShardSize:   testData.shardSize,
			}, nil)
			require.NoError(t, err)

			// Assert on default sharding strategy.
			limiter := NewLimiter(limits, ring, util.ShardingStrategyDefault, testData.shardByAllLabels, testData.ringReplicationFactor, testData.ringZoneAwarenessEnabled)
			actual := limiter.maxMetadataPerMetric("test")
			assert.Equal(t, testData.expectedDefaultSharding, actual)

			// Assert on default shuffle strategy.
			limiter = NewLimiter(limits, ring, util.ShardingStrategyShuffle, testData.shardByAllLabels, testData.ringReplicationFactor, testData.ringZoneAwarenessEnabled)
			actual = limiter.maxMetadataPerMetric("test")
			assert.Equal(t, testData.expectedShuffleSharding, actual)
		})
	}
}

func TestSeriesLimit_maxSeriesPerUser(t *testing.T) {
	tests := map[string]struct {
		maxLocalSeriesPerUser    int
		maxGlobalSeriesPerUser   int
		ringReplicationFactor    int
		ringZoneAwarenessEnabled bool
		ringIngesterCount        int
		ringZonesCount           int
		shardByAllLabels         bool
		shardSize                int
		expectedDefaultSharding  int
		expectedShuffleSharding  int
	}{
		"both local and global limits are disabled": {
			maxLocalSeriesPerUser:   0,
			maxGlobalSeriesPerUser:  0,
			ringReplicationFactor:   1,
			ringIngesterCount:       1,
			ringZonesCount:          1,
			shardByAllLabels:        false,
			expectedDefaultSharding: math.MaxInt32,
			expectedShuffleSharding: math.MaxInt32,
		},
		"only local limit is enabled": {
			maxLocalSeriesPerUser:   1000,
			maxGlobalSeriesPerUser:  0,
			ringReplicationFactor:   1,
			ringIngesterCount:       1,
			ringZonesCount:          1,
			shardByAllLabels:        false,
			expectedDefaultSharding: 1000,
			expectedShuffleSharding: 1000,
		},
		"only global limit is enabled with shard-by-all-labels=false and replication-factor=1": {
			maxLocalSeriesPerUser:   0,
			maxGlobalSeriesPerUser:  1000,
			ringReplicationFactor:   1,
			ringIngesterCount:       10,
			ringZonesCount:          1,
			shardByAllLabels:        false,
			expectedDefaultSharding: math.MaxInt32,
			expectedShuffleSharding: math.MaxInt32,
		},
		"only global limit is enabled with shard-by-all-labels=true and replication-factor=1": {
			maxLocalSeriesPerUser:   0,
			maxGlobalSeriesPerUser:  1000,
			ringReplicationFactor:   1,
			ringIngesterCount:       10,
			ringZonesCount:          1,
			shardByAllLabels:        true,
			shardSize:               5,
			expectedDefaultSharding: 100,
			expectedShuffleSharding: 200,
		},
		"only global limit is enabled with shard-by-all-labels=true and replication-factor=3": {
			maxLocalSeriesPerUser:   0,
			maxGlobalSeriesPerUser:  1000,
			ringReplicationFactor:   3,
			ringIngesterCount:       10,
			ringZonesCount:          1,
			shardByAllLabels:        true,
			shardSize:               5,
			expectedDefaultSharding: 300,
			expectedShuffleSharding: 600,
		},
		"both local and global limits are set with local limit < global limit": {
			maxLocalSeriesPerUser:   150,
			maxGlobalSeriesPerUser:  1000,
			ringReplicationFactor:   3,
			ringIngesterCount:       10,
			ringZonesCount:          1,
			shardByAllLabels:        true,
			shardSize:               5,
			expectedDefaultSharding: 150,
			expectedShuffleSharding: 150,
		},
		"both local and global limits are set with local limit > global limit": {
			maxLocalSeriesPerUser:   800,
			maxGlobalSeriesPerUser:  1000,
			ringReplicationFactor:   3,
			ringIngesterCount:       10,
			ringZonesCount:          1,
			shardByAllLabels:        true,
			shardSize:               5,
			expectedDefaultSharding: 300,
			expectedShuffleSharding: 600,
		},
		"zone-awareness enabled, global limit enabled and the shard size is NOT divisible by number of zones": {
			maxLocalSeriesPerUser:    0,
			maxGlobalSeriesPerUser:   900,
			ringReplicationFactor:    3,
			ringZoneAwarenessEnabled: true,
			ringIngesterCount:        9,
			ringZonesCount:           3,
			shardByAllLabels:         true,
			shardSize:                5, // Not divisible by number of zones.
			expectedDefaultSharding:  300,
			expectedShuffleSharding:  450, // (900 / 6) * 3
		},
		"zone-awareness enabled, global limit enabled and the shard size is divisible by number of zones": {
			maxLocalSeriesPerUser:    0,
			maxGlobalSeriesPerUser:   900,
			ringReplicationFactor:    3,
			ringZoneAwarenessEnabled: true,
			ringIngesterCount:        9,
			ringZonesCount:           3,
			shardByAllLabels:         true,
			shardSize:                6, // Divisible by number of zones.
			expectedDefaultSharding:  300,
			expectedShuffleSharding:  450, // (900 / 6) * 3
		},
		"zone-awareness enabled, global limit enabled and the shard size > number of ingesters": {
			maxLocalSeriesPerUser:    0,
			maxGlobalSeriesPerUser:   900,
			ringReplicationFactor:    3,
			ringZoneAwarenessEnabled: true,
			ringIngesterCount:        9,
			ringZonesCount:           3,
			shardByAllLabels:         true,
			shardSize:                20, // Greater than number of ingesters.
			expectedDefaultSharding:  300,
			expectedShuffleSharding:  300,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{}
			ring.On("HealthyInstancesCount").Return(testData.ringIngesterCount)
			ring.On("ZonesCount").Return(testData.ringZonesCount)

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxLocalSeriesPerUser:    testData.maxLocalSeriesPerUser,
				MaxGlobalSeriesPerUser:   testData.maxGlobalSeriesPerUser,
				IngestionTenantShardSize: testData.shardSize,
			}, nil)
			require.NoError(t, err)

			// Assert on default sharding strategy.
			limiter := NewLimiter(limits, ring, util.ShardingStrategyDefault, testData.shardByAllLabels, testData.ringReplicationFactor, testData.ringZoneAwarenessEnabled)
			actual := limiter.maxSeriesPerUser("test")
			assert.Equal(t, testData.expectedDefaultSharding, actual)

			// Assert on shuffle sharding strategy.
			limiter = NewLimiter(limits, ring, util.ShardingStrategyShuffle, testData.shardByAllLabels, testData.ringReplicationFactor, testData.ringZoneAwarenessEnabled)
			actual = limiter.maxSeriesPerUser("test")
			assert.Equal(t, testData.expectedShuffleSharding, actual)
		})
	}
}

func TestLimit_maxMetadataPerUser(t *testing.T) {
	tests := map[string]struct {
		maxLocalMetadataPerUser  int
		maxGlobalMetadataPerUser int
		ringReplicationFactor    int
		ringZoneAwarenessEnabled bool
		ringIngesterCount        int
		ringZonesCount           int
		shardByAllLabels         bool
		shardSize                int
		expectedDefaultSharding  int
		expectedShuffleSharding  int
	}{
		"both local and global limits are disabled": {
			maxLocalMetadataPerUser:  0,
			maxGlobalMetadataPerUser: 0,
			ringReplicationFactor:    1,
			ringIngesterCount:        1,
			ringZonesCount:           1,
			shardByAllLabels:         false,
			expectedDefaultSharding:  math.MaxInt32,
			expectedShuffleSharding:  math.MaxInt32,
		},
		"only local limit is enabled": {
			maxLocalMetadataPerUser:  1000,
			maxGlobalMetadataPerUser: 0,
			ringReplicationFactor:    1,
			ringIngesterCount:        1,
			ringZonesCount:           1,
			shardByAllLabels:         false,
			expectedDefaultSharding:  1000,
			expectedShuffleSharding:  1000,
		},
		"only global limit is enabled with shard-by-all-labels=false and replication-factor=1": {
			maxLocalMetadataPerUser:  0,
			maxGlobalMetadataPerUser: 1000,
			ringReplicationFactor:    1,
			ringIngesterCount:        10,
			ringZonesCount:           1,
			shardByAllLabels:         false,
			expectedDefaultSharding:  math.MaxInt32,
			expectedShuffleSharding:  math.MaxInt32,
		},
		"only global limit is enabled with shard-by-all-labels=true and replication-factor=1": {
			maxLocalMetadataPerUser:  0,
			maxGlobalMetadataPerUser: 1000,
			ringReplicationFactor:    1,
			ringIngesterCount:        10,
			ringZonesCount:           1,
			shardByAllLabels:         true,
			shardSize:                5,
			expectedDefaultSharding:  100,
			expectedShuffleSharding:  200,
		},
		"only global limit is enabled with shard-by-all-labels=true and replication-factor=3": {
			maxLocalMetadataPerUser:  0,
			maxGlobalMetadataPerUser: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			ringZonesCount:           1,
			shardByAllLabels:         true,
			shardSize:                5,
			expectedDefaultSharding:  300,
			expectedShuffleSharding:  600,
		},
		"both local and global limits are set with local limit < global limit": {
			maxLocalMetadataPerUser:  150,
			maxGlobalMetadataPerUser: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			ringZonesCount:           1,
			shardByAllLabels:         true,
			shardSize:                5,
			expectedDefaultSharding:  150,
			expectedShuffleSharding:  150,
		},
		"both local and global limits are set with local limit > global limit": {
			maxLocalMetadataPerUser:  800,
			maxGlobalMetadataPerUser: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			ringZonesCount:           1,
			shardByAllLabels:         true,
			shardSize:                5,
			expectedDefaultSharding:  300,
			expectedShuffleSharding:  600,
		},
		"zone-awareness enabled, global limit enabled and the shard size is NOT divisible by number of zones": {
			maxLocalMetadataPerUser:  0,
			maxGlobalMetadataPerUser: 900,
			ringReplicationFactor:    3,
			ringZoneAwarenessEnabled: true,
			ringIngesterCount:        9,
			ringZonesCount:           3,
			shardByAllLabels:         true,
			shardSize:                5, // Not divisible by number of zones.
			expectedDefaultSharding:  300,
			expectedShuffleSharding:  450, // (900 / 6) * 3
		},
		"zone-awareness enabled, global limit enabled and the shard size is divisible by number of zones": {
			maxLocalMetadataPerUser:  0,
			maxGlobalMetadataPerUser: 900,
			ringReplicationFactor:    3,
			ringZoneAwarenessEnabled: true,
			ringIngesterCount:        9,
			ringZonesCount:           3,
			shardByAllLabels:         true,
			shardSize:                6, // Divisible by number of zones.
			expectedDefaultSharding:  300,
			expectedShuffleSharding:  450, // (900 / 6) * 3
		},
		"zone-awareness enabled, global limit enabled and the shard size > number of ingesters": {
			maxLocalMetadataPerUser:  0,
			maxGlobalMetadataPerUser: 900,
			ringReplicationFactor:    3,
			ringZoneAwarenessEnabled: true,
			ringIngesterCount:        9,
			ringZonesCount:           3,
			shardByAllLabels:         true,
			shardSize:                20, // Greater than number of ingesters.
			expectedDefaultSharding:  300,
			expectedShuffleSharding:  300,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{}
			ring.On("HealthyInstancesCount").Return(testData.ringIngesterCount)
			ring.On("ZonesCount").Return(testData.ringZonesCount)

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxLocalMetricsWithMetadataPerUser:  testData.maxLocalMetadataPerUser,
				MaxGlobalMetricsWithMetadataPerUser: testData.maxGlobalMetadataPerUser,
				IngestionTenantShardSize:            testData.shardSize,
			}, nil)
			require.NoError(t, err)

			// Assert on default sharding strategy.
			limiter := NewLimiter(limits, ring, util.ShardingStrategyDefault, testData.shardByAllLabels, testData.ringReplicationFactor, testData.ringZoneAwarenessEnabled)
			actual := limiter.maxMetadataPerUser("test")
			assert.Equal(t, testData.expectedDefaultSharding, actual)

			// Assert on shuffle sharding strategy.
			limiter = NewLimiter(limits, ring, util.ShardingStrategyShuffle, testData.shardByAllLabels, testData.ringReplicationFactor, testData.ringZoneAwarenessEnabled)
			actual = limiter.maxMetadataPerUser("test")
			assert.Equal(t, testData.expectedShuffleSharding, actual)
		})
	}
}

func TestLimiter_AssertMaxSeriesPerMetric(t *testing.T) {
	tests := map[string]struct {
		maxLocalSeriesPerMetric  int
		maxGlobalSeriesPerMetric int
		ringReplicationFactor    int
		ringIngesterCount        int
		shardByAllLabels         bool
		series                   int
		expected                 error
	}{
		"both local and global limit are disabled": {
			maxLocalSeriesPerMetric:  0,
			maxGlobalSeriesPerMetric: 0,
			ringReplicationFactor:    1,
			ringIngesterCount:        1,
			shardByAllLabels:         false,
			series:                   100,
			expected:                 nil,
		},
		"current number of series is below the limit": {
			maxLocalSeriesPerMetric:  0,
			maxGlobalSeriesPerMetric: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			shardByAllLabels:         true,
			series:                   299,
			expected:                 nil,
		},
		"current number of series is above the limit": {
			maxLocalSeriesPerMetric:  0,
			maxGlobalSeriesPerMetric: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			shardByAllLabels:         true,
			series:                   300,
			expected:                 fmt.Errorf(errMaxSeriesPerMetricLimitExceeded, 0, 1000, 300),
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{}
			ring.On("HealthyInstancesCount").Return(testData.ringIngesterCount)
			ring.On("ZonesCount").Return(1)

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxLocalSeriesPerMetric:  testData.maxLocalSeriesPerMetric,
				MaxGlobalSeriesPerMetric: testData.maxGlobalSeriesPerMetric,
			}, nil)
			require.NoError(t, err)

			limiter := NewLimiter(limits, ring, util.ShardingStrategyDefault, testData.shardByAllLabels, testData.ringReplicationFactor, false)
			actual := limiter.AssertMaxSeriesPerMetric("test", testData.series)

			assert.Equal(t, testData.expected, actual)
		})
	}
}
func TestLimiter_AssertMaxMetadataPerMetric(t *testing.T) {
	tests := map[string]struct {
		maxLocalMetadataPerMetric  int
		maxGlobalMetadataPerMetric int
		ringReplicationFactor      int
		ringIngesterCount          int
		shardByAllLabels           bool
		metadata                   int
		expected                   error
	}{
		"both local and global limit are disabled": {
			maxLocalMetadataPerMetric:  0,
			maxGlobalMetadataPerMetric: 0,
			ringReplicationFactor:      1,
			ringIngesterCount:          1,
			shardByAllLabels:           false,
			metadata:                   100,
			expected:                   nil,
		},
		"current number of metadata is below the limit": {
			maxLocalMetadataPerMetric:  0,
			maxGlobalMetadataPerMetric: 1000,
			ringReplicationFactor:      3,
			ringIngesterCount:          10,
			shardByAllLabels:           true,
			metadata:                   299,
			expected:                   nil,
		},
		"current number of metadata is above the limit": {
			maxLocalMetadataPerMetric:  0,
			maxGlobalMetadataPerMetric: 1000,
			ringReplicationFactor:      3,
			ringIngesterCount:          10,
			shardByAllLabels:           true,
			metadata:                   300,
			expected:                   fmt.Errorf(errMaxMetadataPerMetricLimitExceeded, 0, 1000, 300),
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{}
			ring.On("HealthyInstancesCount").Return(testData.ringIngesterCount)
			ring.On("ZonesCount").Return(1)

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxLocalMetadataPerMetric:  testData.maxLocalMetadataPerMetric,
				MaxGlobalMetadataPerMetric: testData.maxGlobalMetadataPerMetric,
			}, nil)
			require.NoError(t, err)

			limiter := NewLimiter(limits, ring, util.ShardingStrategyDefault, testData.shardByAllLabels, testData.ringReplicationFactor, false)
			actual := limiter.AssertMaxMetadataPerMetric("test", testData.metadata)

			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestLimiter_AssertMaxSeriesPerUser(t *testing.T) {
	tests := map[string]struct {
		maxLocalSeriesPerUser  int
		maxGlobalSeriesPerUser int
		ringReplicationFactor  int
		ringIngesterCount      int
		shardByAllLabels       bool
		series                 int
		expected               error
	}{
		"both local and global limit are disabled": {
			maxLocalSeriesPerUser:  0,
			maxGlobalSeriesPerUser: 0,
			ringReplicationFactor:  1,
			ringIngesterCount:      1,
			shardByAllLabels:       false,
			series:                 100,
			expected:               nil,
		},
		"current number of series is below the limit": {
			maxLocalSeriesPerUser:  0,
			maxGlobalSeriesPerUser: 1000,
			ringReplicationFactor:  3,
			ringIngesterCount:      10,
			shardByAllLabels:       true,
			series:                 299,
			expected:               nil,
		},
		"current number of series is above the limit": {
			maxLocalSeriesPerUser:  0,
			maxGlobalSeriesPerUser: 1000,
			ringReplicationFactor:  3,
			ringIngesterCount:      10,
			shardByAllLabels:       true,
			series:                 300,
			expected:               fmt.Errorf(errMaxSeriesPerUserLimitExceeded, 0, 1000, 300),
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{}
			ring.On("HealthyInstancesCount").Return(testData.ringIngesterCount)
			ring.On("ZonesCount").Return(1)

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxLocalSeriesPerUser:  testData.maxLocalSeriesPerUser,
				MaxGlobalSeriesPerUser: testData.maxGlobalSeriesPerUser,
			}, nil)
			require.NoError(t, err)

			limiter := NewLimiter(limits, ring, util.ShardingStrategyDefault, testData.shardByAllLabels, testData.ringReplicationFactor, false)
			actual := limiter.AssertMaxSeriesPerUser("test", testData.series)

			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestLimiter_AssertMaxMetricsWithMetadataPerUser(t *testing.T) {
	tests := map[string]struct {
		maxLocalMetadataPerUser  int
		maxGlobalMetadataPerUser int
		ringReplicationFactor    int
		ringIngesterCount        int
		shardByAllLabels         bool
		metadata                 int
		expected                 error
	}{
		"both local and global limit are disabled": {
			maxLocalMetadataPerUser:  0,
			maxGlobalMetadataPerUser: 0,
			ringReplicationFactor:    1,
			ringIngesterCount:        1,
			shardByAllLabels:         false,
			metadata:                 100,
			expected:                 nil,
		},
		"current number of metadata is below the limit": {
			maxLocalMetadataPerUser:  0,
			maxGlobalMetadataPerUser: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			shardByAllLabels:         true,
			metadata:                 299,
			expected:                 nil,
		},
		"current number of metadata is above the limit": {
			maxLocalMetadataPerUser:  0,
			maxGlobalMetadataPerUser: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			shardByAllLabels:         true,
			metadata:                 300,
			expected:                 fmt.Errorf(errMaxMetadataPerUserLimitExceeded, 0, 1000, 300),
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{}
			ring.On("HealthyInstancesCount").Return(testData.ringIngesterCount)
			ring.On("ZonesCount").Return(1)

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxLocalMetricsWithMetadataPerUser:  testData.maxLocalMetadataPerUser,
				MaxGlobalMetricsWithMetadataPerUser: testData.maxGlobalMetadataPerUser,
			}, nil)
			require.NoError(t, err)

			limiter := NewLimiter(limits, ring, util.ShardingStrategyDefault, testData.shardByAllLabels, testData.ringReplicationFactor, false)
			actual := limiter.AssertMaxMetricsWithMetadataPerUser("test", testData.metadata)

			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestLimiter_minNonZero(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		first    int
		second   int
		expected int
	}{
		"both zero": {
			first:    0,
			second:   0,
			expected: 0,
		},
		"first is zero": {
			first:    0,
			second:   1,
			expected: 1,
		},
		"second is zero": {
			first:    1,
			second:   0,
			expected: 1,
		},
		"both non zero, second > first": {
			first:    1,
			second:   2,
			expected: 1,
		},
		"both non zero, first > second": {
			first:    2,
			second:   1,
			expected: 1,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, minNonZero(testData.first, testData.second))
		})
	}
}

type ringCountMock struct {
	mock.Mock
}

func (m *ringCountMock) HealthyInstancesCount() int {
	args := m.Called()
	return args.Int(0)
}

func (m *ringCountMock) ZonesCount() int {
	args := m.Called()
	return args.Int(0)
}
