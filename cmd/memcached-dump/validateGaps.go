package main

import (
	"fmt"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/cortexproject/cortex/pkg/querier/queryrange"
)

func buildValidateGaps(minGap time.Duration, mc *memcache.Client) processFunc {
	totalQueries := 0
	totalGaps := 0

	return func(req *queryrange.CachedResponse, b []byte) {
		hasGaps := false

		for _, e := range req.Extents {
			for _, d := range e.Response.Data.Result {
				if len(d.Samples) > 1 {

					expectedIntervalMs := d.Samples[1].TimestampMs - d.Samples[0].TimestampMs
					for i := 0; i < len(d.Samples)-2; i++ {
						actualIntervalMs := d.Samples[i+1].TimestampMs - d.Samples[i].TimestampMs

						if actualIntervalMs > expectedIntervalMs && time.Duration(actualIntervalMs)*time.Millisecond > minGap {
							hasGaps = true
						}
					}
				}
			}
		}

		totalQueries++
		if hasGaps {
			// kill the old key and requery to see if the same results come back

			totalGaps++
			fmt.Printf("Gaps/Total: %d/%d\n", totalGaps, totalQueries)
			fmt.Println(string(b))
		}
	}
}
