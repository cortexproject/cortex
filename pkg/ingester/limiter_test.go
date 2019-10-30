package ingester

import (
	"fmt"
	"math"
	"testing"

	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestSeriesLimit_maxSeriesPerMetric(t *testing.T) {
	tests := map[string]struct {
		maxLocalSeriesPerMetric  int
		maxGlobalSeriesPerMetric int
		ringReplicationFactor    int
		ringIngesterCount        int
		shardByAllLabels         bool
		expected                 int
	}{
		"both local and global limits are disabled": {
			maxLocalSeriesPerMetric:  0,
			maxGlobalSeriesPerMetric: 0,
			ringReplicationFactor:    1,
			ringIngesterCount:        1,
			shardByAllLabels:         false,
			expected:                 math.MaxInt32,
		},
		"only local limit is enabled": {
			maxLocalSeriesPerMetric:  1000,
			maxGlobalSeriesPerMetric: 0,
			ringReplicationFactor:    1,
			ringIngesterCount:        1,
			shardByAllLabels:         false,
			expected:                 1000,
		},
		"only global limit is enabled with shard-by-all-labels=false and replication-factor=1": {
			maxLocalSeriesPerMetric:  0,
			maxGlobalSeriesPerMetric: 1000,
			ringReplicationFactor:    1,
			ringIngesterCount:        10,
			shardByAllLabels:         false,
			expected:                 1000,
		},
		"only global limit is enabled with shard-by-all-labels=true and replication-factor=1": {
			maxLocalSeriesPerMetric:  0,
			maxGlobalSeriesPerMetric: 1000,
			ringReplicationFactor:    1,
			ringIngesterCount:        10,
			shardByAllLabels:         true,
			expected:                 100,
		},
		"only global limit is enabled with shard-by-all-labels=true and replication-factor=3": {
			maxLocalSeriesPerMetric:  0,
			maxGlobalSeriesPerMetric: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			shardByAllLabels:         true,
			expected:                 300,
		},
		"both local and global limits are set with local limit < global limit": {
			maxLocalSeriesPerMetric:  150,
			maxGlobalSeriesPerMetric: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			shardByAllLabels:         true,
			expected:                 150,
		},
		"both local and global limits are set with local limit > global limit": {
			maxLocalSeriesPerMetric:  500,
			maxGlobalSeriesPerMetric: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			shardByAllLabels:         true,
			expected:                 300,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{}
			ring.On("HealthyInstancesCount").Return(testData.ringIngesterCount)

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxLocalSeriesPerMetric:  testData.maxLocalSeriesPerMetric,
				MaxGlobalSeriesPerMetric: testData.maxGlobalSeriesPerMetric,
			})
			require.NoError(t, err)

			limiter := NewSeriesLimiter(limits, ring, testData.ringReplicationFactor, testData.shardByAllLabels)
			actual := limiter.maxSeriesPerMetric("test")

			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestSeriesLimit_maxSeriesPerUser(t *testing.T) {
	tests := map[string]struct {
		maxLocalSeriesPerUser  int
		maxGlobalSeriesPerUser int
		ringReplicationFactor  int
		ringIngesterCount      int
		shardByAllLabels       bool
		expected               int
	}{
		"both local and global limits are disabled": {
			maxLocalSeriesPerUser:  0,
			maxGlobalSeriesPerUser: 0,
			ringReplicationFactor:  1,
			ringIngesterCount:      1,
			shardByAllLabels:       false,
			expected:               math.MaxInt32,
		},
		"only local limit is enabled": {
			maxLocalSeriesPerUser:  1000,
			maxGlobalSeriesPerUser: 0,
			ringReplicationFactor:  1,
			ringIngesterCount:      1,
			shardByAllLabels:       false,
			expected:               1000,
		},
		"only global limit is enabled with shard-by-all-labels=false and replication-factor=1": {
			maxLocalSeriesPerUser:  0,
			maxGlobalSeriesPerUser: 1000,
			ringReplicationFactor:  1,
			ringIngesterCount:      10,
			shardByAllLabels:       false,
			expected:               math.MaxInt32,
		},
		"only global limit is enabled with shard-by-all-labels=true and replication-factor=1": {
			maxLocalSeriesPerUser:  0,
			maxGlobalSeriesPerUser: 1000,
			ringReplicationFactor:  1,
			ringIngesterCount:      10,
			shardByAllLabels:       true,
			expected:               100,
		},
		"only global limit is enabled with shard-by-all-labels=true and replication-factor=3": {
			maxLocalSeriesPerUser:  0,
			maxGlobalSeriesPerUser: 1000,
			ringReplicationFactor:  3,
			ringIngesterCount:      10,
			shardByAllLabels:       true,
			expected:               300,
		},
		"both local and global limits are set with local limit < global limit": {
			maxLocalSeriesPerUser:  150,
			maxGlobalSeriesPerUser: 1000,
			ringReplicationFactor:  3,
			ringIngesterCount:      10,
			shardByAllLabels:       true,
			expected:               150,
		},
		"both local and global limits are set with local limit > global limit": {
			maxLocalSeriesPerUser:  500,
			maxGlobalSeriesPerUser: 1000,
			ringReplicationFactor:  3,
			ringIngesterCount:      10,
			shardByAllLabels:       true,
			expected:               300,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{}
			ring.On("HealthyInstancesCount").Return(testData.ringIngesterCount)

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxLocalSeriesPerUser:  testData.maxLocalSeriesPerUser,
				MaxGlobalSeriesPerUser: testData.maxGlobalSeriesPerUser,
			})
			require.NoError(t, err)

			limiter := NewSeriesLimiter(limits, ring, testData.ringReplicationFactor, testData.shardByAllLabels)
			actual := limiter.maxSeriesPerUser("test")

			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestSeriesLimiter_AssertMaxSeriesPerMetric(t *testing.T) {
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

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxLocalSeriesPerMetric:  testData.maxLocalSeriesPerMetric,
				MaxGlobalSeriesPerMetric: testData.maxGlobalSeriesPerMetric,
			})
			require.NoError(t, err)

			limiter := NewSeriesLimiter(limits, ring, testData.ringReplicationFactor, testData.shardByAllLabels)
			actual := limiter.AssertMaxSeriesPerMetric("test", testData.series)

			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestSeriesLimiter_AssertMaxSeriesPerUser(t *testing.T) {
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

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxLocalSeriesPerUser:  testData.maxLocalSeriesPerUser,
				MaxGlobalSeriesPerUser: testData.maxGlobalSeriesPerUser,
			})
			require.NoError(t, err)

			limiter := NewSeriesLimiter(limits, ring, testData.ringReplicationFactor, testData.shardByAllLabels)
			actual := limiter.AssertMaxSeriesPerUser("test", testData.series)

			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestSeriesLimiter_minNonZero(t *testing.T) {
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
			limiter := NewSeriesLimiter(nil, nil, 0, false)
			assert.Equal(t, testData.expected, limiter.minNonZero(testData.first, testData.second))
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
