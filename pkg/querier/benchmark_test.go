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
			for _, queryable := range queryables {
				b.Run(fmt.Sprintf("%s/step_%s/%s/%s/", query.query, query.step, encoding.name, queryable.name), func(b *testing.B) {
					store, from := makeMockChunkStore(b, 24*30, encoding.e)
					b.ResetTimer()

					var r *promql.Result
					for n := 0; n < b.N; n++ {
						r = testQuery(b, queryable.f(store), from, query)
					}
					result = r
				})
			}
		}
	}
}
