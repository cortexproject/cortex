package instantquery

import (
	"testing"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/querier/tripperware/queryrange"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func Test_shardQuery(t *testing.T) {
	t.Parallel()
	userLimit := validation.Limits{
		MaxFetchedSeriesPerQuery: 0,
		MaxFetchedChunkBytesPerQuery: 0,
		MaxChunksPerQuery: 0,
		MaxFetchedDataBytesPerQuery: 0,
	}
	overrides, _ := validation.NewOverrides(userLimit, nil)
	tripperware.TestQueryShardQuery(t, testInstantQueryCodec, queryrange.NewPrometheusCodec(true, "", "protobuf", overrides))
}
