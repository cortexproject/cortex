package querier

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/promql"
)

var result *promql.Result

func BenchmarkChunkQueryable(b *testing.B) {
	for _, encoding := range encodings {
		store, from := makeMockChunkStore(b, 24*30, encoding.e)

		for _, q := range queryables {
			b.Run(fmt.Sprintf("%s/%s", q.name, encoding.name), func(b *testing.B) {
				queryable := q.f(store)
				var r *promql.Result
				for n := 0; n < b.N; n++ {
					r = testQuery(b, queryable, from)
				}
				result = r
			})
		}
	}
}
