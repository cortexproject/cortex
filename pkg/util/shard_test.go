package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShuffleShardExpectedInstancesPerZone(t *testing.T) {
	tests := []struct {
		shardSize int
		numZones  int
		expected  int
	}{
		{
			shardSize: 1,
			numZones:  1,
			expected:  1,
		},
		{
			shardSize: 1,
			numZones:  3,
			expected:  1,
		},
		{
			shardSize: 3,
			numZones:  3,
			expected:  1,
		},
		{
			shardSize: 4,
			numZones:  3,
			expected:  2,
		},
		{
			shardSize: 6,
			numZones:  3,
			expected:  2,
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.expected, ShuffleShardExpectedInstancesPerZone(test.shardSize, test.numZones))
	}
}

func TestShuffleShardExpectedInstances(t *testing.T) {
	tests := []struct {
		shardSize int
		numZones  int
		expected  int
	}{
		{
			shardSize: 1,
			numZones:  1,
			expected:  1,
		},
		{
			shardSize: 1,
			numZones:  3,
			expected:  3,
		},
		{
			shardSize: 3,
			numZones:  3,
			expected:  3,
		},
		{
			shardSize: 4,
			numZones:  3,
			expected:  6,
		},
		{
			shardSize: 6,
			numZones:  3,
			expected:  6,
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.expected, ShuffleShardExpectedInstances(test.shardSize, test.numZones))
	}
}

func TestDynamicShardSize(t *testing.T) {
	tests := []struct {
		value        float64
		numInstances int
		expected     int
	}{
		{
			value:        0,
			numInstances: 100,
			expected:     0,
		},
		{
			value:        0.1,
			numInstances: 100,
			expected:     10,
		},
		{
			value:        0.01,
			numInstances: 100,
			expected:     1,
		},
		{
			value:        3,
			numInstances: 100,
			expected:     3,
		},
		{
			value:        0.4,
			numInstances: 100,
			expected:     40,
		},
		{
			value:        1,
			numInstances: 100,
			expected:     1,
		},
		{
			value:        0.99999,
			numInstances: 100,
			expected:     100,
		},
		{
			value:        0.5,
			numInstances: 3,
			expected:     2,
		},
		{
			value:        0.8,
			numInstances: 3,
			expected:     3,
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.expected, DynamicShardSize(test.value, test.numInstances))
	}
}
