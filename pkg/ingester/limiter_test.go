package ingester

import (
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestLimiter_maxSeriesPerMetric(t *testing.T) {
	applyLimits := func(limits *validation.Limits, localLimit, globalLimit int) {
		limits.MaxLocalSeriesPerMetric = localLimit
		limits.MaxGlobalSeriesPerMetric = globalLimit
	}

	runMaxFn := func(limiter *Limiter) int {
		return limiter.maxSeriesPerMetric("test")
	}

	runLimiterMaxFunctionTest(t, applyLimits, runMaxFn, true)
}

func TestLimiter_maxMetadataPerMetric(t *testing.T) {
	applyLimits := func(limits *validation.Limits, localLimit, globalLimit int) {
		limits.MaxLocalMetadataPerMetric = localLimit
		limits.MaxGlobalMetadataPerMetric = globalLimit
	}

	runMaxFn := func(limiter *Limiter) int {
		return limiter.maxMetadataPerMetric("test")
	}

	runLimiterMaxFunctionTest(t, applyLimits, runMaxFn, true)
}

func TestLimiter_maxSeriesPerUser(t *testing.T) {
	applyLimits := func(limits *validation.Limits, localLimit, globalLimit int) {
		limits.MaxLocalSeriesPerUser = localLimit
		limits.MaxGlobalSeriesPerUser = globalLimit
	}

	runMaxFn := func(limiter *Limiter) int {
		return limiter.maxSeriesPerUser("test")
	}

	runLimiterMaxFunctionTest(t, applyLimits, runMaxFn, false)
}

func TestLimiter_maxMetadataPerUser(t *testing.T) {
	applyLimits := func(limits *validation.Limits, localLimit, globalLimit int) {
		limits.MaxLocalMetricsWithMetadataPerUser = localLimit
		limits.MaxGlobalMetricsWithMetadataPerUser = globalLimit
	}

	runMaxFn := func(limiter *Limiter) int {
		return limiter.maxMetadataPerUser("test")
	}

	runLimiterMaxFunctionTest(t, applyLimits, runMaxFn, false)
}

func runLimiterMaxFunctionTest(
	t *testing.T,
	applyLimits func(limits *validation.Limits, localLimit, globalLimit int),
	runMaxFn func(limiter *Limiter) int,
	globalLimitShardByMetricNameSupport bool,
) {
	tests := map[string]struct {
		localLimit               int
		globalLimit              int
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
			localLimit:              0,
			globalLimit:             0,
			ringReplicationFactor:   1,
			ringIngesterCount:       1,
			ringZonesCount:          1,
			shardByAllLabels:        false,
			expectedDefaultSharding: math.MaxInt32,
			expectedShuffleSharding: math.MaxInt32,
		},
		"only local limit is enabled": {
			localLimit:              1000,
			globalLimit:             0,
			ringReplicationFactor:   1,
			ringIngesterCount:       1,
			ringZonesCount:          1,
			shardByAllLabels:        false,
			expectedDefaultSharding: 1000,
			expectedShuffleSharding: 1000,
		},
		"only global limit is enabled with shard-by-all-labels=false and replication-factor=1": {
			localLimit:            0,
			globalLimit:           1000,
			ringReplicationFactor: 1,
			ringIngesterCount:     10,
			ringZonesCount:        1,
			shardByAllLabels:      false,
			shardSize:             5,
			expectedDefaultSharding: func() int {
				if globalLimitShardByMetricNameSupport {
					return 1000
				}
				return math.MaxInt32
			}(),
			expectedShuffleSharding: func() int {
				if globalLimitShardByMetricNameSupport {
					return 1000
				}
				return math.MaxInt32
			}(),
		},
		"only global limit is enabled with shard-by-all-labels=true and replication-factor=1": {
			localLimit:              0,
			globalLimit:             1000,
			ringReplicationFactor:   1,
			ringIngesterCount:       10,
			ringZonesCount:          1,
			shardByAllLabels:        true,
			shardSize:               5,
			expectedDefaultSharding: 100,
			expectedShuffleSharding: 200,
		},
		"only global limit is enabled with shard-by-all-labels=true and replication-factor=3": {
			localLimit:              0,
			globalLimit:             1000,
			ringReplicationFactor:   3,
			ringIngesterCount:       10,
			ringZonesCount:          1,
			shardByAllLabels:        true,
			shardSize:               5,
			expectedDefaultSharding: 300,
			expectedShuffleSharding: 600,
		},
		"both local and global limits are set with local limit < global limit": {
			localLimit:              150,
			globalLimit:             1000,
			ringReplicationFactor:   3,
			ringIngesterCount:       10,
			ringZonesCount:          1,
			shardByAllLabels:        true,
			shardSize:               5,
			expectedDefaultSharding: 150,
			expectedShuffleSharding: 150,
		},
		"both local and global limits are set with local limit > global limit": {
			localLimit:              800,
			globalLimit:             1000,
			ringReplicationFactor:   3,
			ringIngesterCount:       10,
			ringZonesCount:          1,
			shardByAllLabels:        true,
			shardSize:               5,
			expectedDefaultSharding: 300,
			expectedShuffleSharding: 600,
		},
		"zone-awareness enabled, global limit enabled and the shard size is NOT divisible by number of zones": {
			localLimit:               0,
			globalLimit:              900,
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
			localLimit:               0,
			globalLimit:              900,
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
			localLimit:               0,
			globalLimit:              900,
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
			limits := validation.Limits{IngestionTenantShardSize: testData.shardSize}
			applyLimits(&limits, testData.localLimit, testData.globalLimit)

			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			// Assert on default sharding strategy.
			limiter := NewLimiter(overrides, ring, util.ShardingStrategyDefault, testData.shardByAllLabels, testData.ringReplicationFactor, testData.ringZoneAwarenessEnabled)
			actual := runMaxFn(limiter)
			assert.Equal(t, testData.expectedDefaultSharding, actual)

			// Assert on shuffle sharding strategy.
			limiter = NewLimiter(overrides, ring, util.ShardingStrategyShuffle, testData.shardByAllLabels, testData.ringReplicationFactor, testData.ringZoneAwarenessEnabled)
			actual = runMaxFn(limiter)
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
			expected:                 errMaxSeriesPerMetricLimitExceeded,
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
			expected:                   errMaxMetadataPerMetricLimitExceeded,
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
			expected:               errMaxSeriesPerUserLimitExceeded,
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
			expected:                 errMaxMetadataPerUserLimitExceeded,
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

func TestLimiter_FormatError(t *testing.T) {
	// Mock the ring
	ring := &ringCountMock{}
	ring.On("HealthyInstancesCount").Return(3)
	ring.On("ZonesCount").Return(1)

	// Mock limits
	limits, err := validation.NewOverrides(validation.Limits{
		MaxGlobalSeriesPerUser:              100,
		MaxGlobalSeriesPerMetric:            20,
		MaxGlobalMetricsWithMetadataPerUser: 10,
		MaxGlobalMetadataPerMetric:          3,
	}, nil)
	require.NoError(t, err)

	limiter := NewLimiter(limits, ring, util.ShardingStrategyDefault, true, 3, false)

	actual := limiter.FormatError("user-1", errMaxSeriesPerUserLimitExceeded)
	assert.EqualError(t, actual, "per-user series limit of 100 exceeded, please contact administrator to raise it (local limit: 0 global limit: 100 actual local limit: 100)")

	actual = limiter.FormatError("user-1", errMaxSeriesPerMetricLimitExceeded)
	assert.EqualError(t, actual, "per-metric series limit of 20 exceeded, please contact administrator to raise it (local limit: 0 global limit: 20 actual local limit: 20)")

	actual = limiter.FormatError("user-1", errMaxMetadataPerUserLimitExceeded)
	assert.EqualError(t, actual, "per-user metric metadata limit of 10 exceeded, please contact administrator to raise it (local limit: 0 global limit: 10 actual local limit: 10)")

	actual = limiter.FormatError("user-1", errMaxMetadataPerMetricLimitExceeded)
	assert.EqualError(t, actual, "per-metric metadata limit of 3 exceeded, please contact administrator to raise it (local limit: 0 global limit: 3 actual local limit: 3)")

	input := errors.New("unknown error")
	actual = limiter.FormatError("user-1", input)
	assert.Equal(t, input, actual)
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
