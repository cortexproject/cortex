package instantquery

import (
	"testing"

	"github.com/cortexproject/cortex/pkg/querier/tripperware"
	"github.com/cortexproject/cortex/pkg/querier/tripperware/queryrange"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func Test_shardQuery(t *testing.T) {
	t.Parallel()
	tripperware.TestQueryShardQuery(t, testInstantQueryCodec, queryrange.NewPrometheusCodec(true, "", "protobuf", &validation.Overrides{}))
}
