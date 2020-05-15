package compactor

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
)

func TestHashIngesterID(t *testing.T) {
	tests := []struct {
		first         string
		second        string
		expectedEqual bool
	}{
		{
			first:         "ingester-0",
			second:        "ingester-0",
			expectedEqual: true,
		},
		{
			first:         "ingester-0",
			second:        "ingester-1",
			expectedEqual: false,
		},
	}

	for _, testCase := range tests {
		firstHash := hashIngesterID(testCase.first)
		secondHash := hashIngesterID(testCase.second)
		assert.Equal(t, testCase.expectedEqual, firstHash == secondHash)
	}
}

func TestShardByIngesterID_DistributionForKubernetesStatefulSets(t *testing.T) {
	const (
		numShards             = 3
		distributionThreshold = 0.8
	)

	for _, numIngesters := range []int{10, 30, 50, 100} {
		// Generate the ingester IDs.
		ids := make([]string, numIngesters)
		for i := 0; i < numIngesters; i++ {
			ids[i] = fmt.Sprintf("ingester-%d", i)
		}

		// Compute the shard for each ingester.
		distribution := map[string][]string{}
		for _, id := range ids {
			shard := shardByIngesterID(id, numShards)
			distribution[shard] = append(distribution[shard], id)
		}

		// Ensure the distribution is fair.
		minSizePerShard := distributionThreshold * (float64(numIngesters) / float64(numShards))

		for _, ingesters := range distribution {
			assert.GreaterOrEqual(t, len(ingesters), int(minSizePerShard))
		}
	}
}

func TestBlocksShardingFilter(t *testing.T) {
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	block3 := ulid.MustNew(3, nil)

	tests := map[string]struct {
		numShards uint32
		input     map[ulid.ULID]map[string]string
		expected  map[ulid.ULID]map[string]string
	}{
		"blocks from the same ingester should go into the same shard": {
			numShards: 3,
			input: map[ulid.ULID]map[string]string{
				block1: {cortex_tsdb.IngesterIDExternalLabel: "ingester-0", cortex_tsdb.TenantIDExternalLabel: "user-1"},
				block2: {cortex_tsdb.IngesterIDExternalLabel: "ingester-0", cortex_tsdb.TenantIDExternalLabel: "user-1"},
				block3: {cortex_tsdb.IngesterIDExternalLabel: "ingester-0", cortex_tsdb.TenantIDExternalLabel: "user-1"},
			},
			expected: map[ulid.ULID]map[string]string{
				block1: {cortex_tsdb.ShardIDExternalLabel: "2", cortex_tsdb.TenantIDExternalLabel: "user-1"},
				block2: {cortex_tsdb.ShardIDExternalLabel: "2", cortex_tsdb.TenantIDExternalLabel: "user-1"},
				block3: {cortex_tsdb.ShardIDExternalLabel: "2", cortex_tsdb.TenantIDExternalLabel: "user-1"},
			},
		},
		"blocks from the different ingesters should be sharded": {
			numShards: 3,
			input: map[ulid.ULID]map[string]string{
				block1: {cortex_tsdb.IngesterIDExternalLabel: "ingester-0", cortex_tsdb.TenantIDExternalLabel: "user-1"},
				block2: {cortex_tsdb.IngesterIDExternalLabel: "ingester-1", cortex_tsdb.TenantIDExternalLabel: "user-1"},
				block3: {cortex_tsdb.IngesterIDExternalLabel: "ingester-2", cortex_tsdb.TenantIDExternalLabel: "user-1"},
			},
			expected: map[ulid.ULID]map[string]string{
				block1: {cortex_tsdb.ShardIDExternalLabel: "2", cortex_tsdb.TenantIDExternalLabel: "user-1"},
				block2: {cortex_tsdb.ShardIDExternalLabel: "1", cortex_tsdb.TenantIDExternalLabel: "user-1"},
				block3: {cortex_tsdb.ShardIDExternalLabel: "0", cortex_tsdb.TenantIDExternalLabel: "user-1"},
			},
		},
		"blocks without ingester ID should not be mangled": {
			numShards: 3,
			input: map[ulid.ULID]map[string]string{
				block1: {cortex_tsdb.ShardIDExternalLabel: "2", cortex_tsdb.TenantIDExternalLabel: "user-1"},
				block2: {cortex_tsdb.ShardIDExternalLabel: "1", cortex_tsdb.TenantIDExternalLabel: "user-1"},
				block3: {cortex_tsdb.ShardIDExternalLabel: "0", cortex_tsdb.TenantIDExternalLabel: "user-1"},
			},
			expected: map[ulid.ULID]map[string]string{
				block1: {cortex_tsdb.ShardIDExternalLabel: "2", cortex_tsdb.TenantIDExternalLabel: "user-1"},
				block2: {cortex_tsdb.ShardIDExternalLabel: "1", cortex_tsdb.TenantIDExternalLabel: "user-1"},
				block3: {cortex_tsdb.ShardIDExternalLabel: "0", cortex_tsdb.TenantIDExternalLabel: "user-1"},
			},
		},
		"should remove the ingester ID external label if sharding is disabled": {
			numShards: 1,
			input: map[ulid.ULID]map[string]string{
				block1: {cortex_tsdb.IngesterIDExternalLabel: "ingester-0", cortex_tsdb.TenantIDExternalLabel: "user-1"},
				block2: {cortex_tsdb.IngesterIDExternalLabel: "ingester-1", cortex_tsdb.TenantIDExternalLabel: "user-1"},
				block3: {cortex_tsdb.IngesterIDExternalLabel: "ingester-2", cortex_tsdb.TenantIDExternalLabel: "user-1"},
			},
			expected: map[ulid.ULID]map[string]string{
				block1: {cortex_tsdb.TenantIDExternalLabel: "user-1"},
				block2: {cortex_tsdb.TenantIDExternalLabel: "user-1"},
				block3: {cortex_tsdb.TenantIDExternalLabel: "user-1"},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			metas := map[ulid.ULID]*metadata.Meta{}
			for id, lbls := range testData.input {
				metas[id] = &metadata.Meta{Thanos: metadata.Thanos{Labels: lbls}}
			}

			f := NewBlocksShardingFilter(testData.numShards)
			err := f.Filter(context.Background(), metas, nil)
			require.NoError(t, err)
			assert.Len(t, metas, len(testData.expected))

			for expectedID, expectedLbls := range testData.expected {
				assert.NotNil(t, metas[expectedID])
				assert.Equal(t, metas[expectedID].Thanos.Labels, expectedLbls)
			}
		})
	}
}
