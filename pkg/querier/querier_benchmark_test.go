package querier

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/promql"
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
