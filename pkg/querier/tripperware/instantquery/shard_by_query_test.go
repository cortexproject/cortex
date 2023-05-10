package instantquery

import (
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/querier/tripperware/queryrange"
)

func Test_shardQuery(t *testing.T) {
	t.Parallel()
	tripperware.TestQueryShardQuery(t, InstantQueryCodec, queryrange.NewPrometheusCodec(true, time.Minute))
}
