package querier

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/querier/batch"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
)

var result *promql.Result

func BenchmarkChunkQueryable(b *testing.B) {
	for _, query := range queries {
		for _, encoding := range encodings {
			for _, testcase := range testcases {
				b.Run(fmt.Sprintf("%s/step_%s/%s/%s/", query.query, query.step, encoding.name, testcase.name), func(b *testing.B) {
					store, from := makeMockChunkStore(b, 24*30, encoding.e)
					b.ResetTimer()

					var r *promql.Result
					for n := 0; n < b.N; n++ {
						queryable := newChunkStoreQueryable(store, testcase.f)
						r = testQuery(b, queryable, from, query)
					}
					result = r
				})
			}
		}
	}
}

func BenchmarkChunkQueryableFromTar(b *testing.B) {
	chunksFilename := os.Getenv("CHUNKS")
	if len(chunksFilename) == 0 {
		return
	}

	query := os.Getenv("QUERY")
	if len(query) == 0 {
		return
	}

	userID := os.Getenv("USERID")
	if len(query) == 0 {
		return
	}

	from, err := parseTime(os.Getenv("FROM"))
	require.NoError(b, err)

	through, err := parseTime(os.Getenv("THROUGH"))
	require.NoError(b, err)

	step, err := parseDuration(os.Getenv("STEP"))
	require.NoError(b, err)

	chunks, err := loadChunks(userID, chunksFilename)
	require.NoError(b, err)

	store := mockChunkStore{chunks}

	b.Run(fmt.Sprintf("file=\"%s\",query=%s,from=%d,to=%d,step=%f", chunksFilename, query, from.Unix(), through.Unix(), step.Seconds()), func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		queryable := newChunkStoreQueryable(store, batch.NewChunkMergeIterator)
		engine := promql.NewEngine(promql.EngineOpts{
			Logger:        util.Logger,
			MaxConcurrent: 1,
			MaxSamples:    math.MaxInt32,
			Timeout:       10 * time.Minute,
		})
		rangeQuery, err := engine.NewRangeQuery(queryable, query, from, through, step)
		require.NoError(b, err)

		ctx := user.InjectOrgID(context.Background(), "0")
		r := rangeQuery.Exec(ctx)
		_, err = r.Matrix()
		require.NoError(b, err)
	})
}
