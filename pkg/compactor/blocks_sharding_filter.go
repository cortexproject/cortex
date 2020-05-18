package compactor

import (
	"context"
	"strconv"

	"github.com/oklog/ulid"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extprom"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
)

type BlocksShardingFilter struct {
	shards uint32
}

func NewBlocksShardingFilter(shards uint32) *BlocksShardingFilter {
	return &BlocksShardingFilter{
		shards: shards,
	}
}

func (f *BlocksShardingFilter) Filter(_ context.Context, metas map[ulid.ULID]*metadata.Meta, _ *extprom.TxGaugeVec) error {
	for _, m := range metas {
		// Just remove the ingester ID label if sharding is disabled.
		if f.shards <= 1 {
			delete(m.Thanos.Labels, cortex_tsdb.IngesterIDExternalLabel)
			continue
		}

		// Skip any block already containing the shard ID to avoid any manipulation
		// (ie. in case the sharding algorithm changes over the time).
		if _, ok := m.Thanos.Labels[cortex_tsdb.ShardIDExternalLabel]; ok {
			continue
		}

		// Skip any block without the ingester ID, which means the block has been generated
		// before the introduction of the ingester ID.
		ingesterID, ok := m.Thanos.Labels[cortex_tsdb.IngesterIDExternalLabel]
		if !ok {
			continue
		}

		// Compute the shard ID and replace the ingester label with the shard label
		// so that the compactor will group blocks based on the shard.
		shardID := shardByIngesterID(ingesterID, f.shards)
		delete(m.Thanos.Labels, cortex_tsdb.IngesterIDExternalLabel)
		m.Thanos.Labels[cortex_tsdb.ShardIDExternalLabel] = shardID
	}

	return nil
}

// hashIngesterID returns a 32-bit hash of the ingester ID.
func hashIngesterID(id string) uint32 {
	h := client.HashNew32()
	h = client.HashAdd32(h, id)
	return h
}

// shardByIngesterID returns the shard given an ingester ID.
func shardByIngesterID(id string, numShards uint32) string {
	return strconv.Itoa(int(hashIngesterID(id) % numShards))
}
