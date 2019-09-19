package main

import (
	"fmt"
	"time"

	"github.com/cortexproject/cortex/pkg/querier/queryrange"
)

func buildProcessGaps(minGap time.Duration) processFunc {
	totalQueries := 0
	totalGaps := 0

	return func(req *queryrange.CachedResponse, b []byte) {
		hasGaps := false

		_, _, expectedIntervalMs, _, err := parseCacheKey(req.Key)
		if err != nil {
			fmt.Println("Error parsing cache key: ", err)
			return
		}

		for _, e := range req.Extents {
			for _, d := range e.Response.Data.Result {
				if len(d.Samples) > 1 {

					for i := 0; i < len(d.Samples)-2; i++ {
						actualIntervalMs := d.Samples[i+1].TimestampMs - d.Samples[i].TimestampMs

						if actualIntervalMs > expectedIntervalMs && time.Duration(actualIntervalMs)*time.Millisecond > minGap {
							hasGaps = true
							fmt.Println("Gap Found: ")
							fmt.Println("extent: ", e.TraceId)
							fmt.Println("stream: ", d.Labels)

							fmt.Printf("Found gap from sample %d to %d.  Expected %d.  Found %d.\n", i, i+1, expectedIntervalMs, actualIntervalMs)
						}
					}
				}
			}
		}

		totalQueries++
		if hasGaps {
			totalGaps++
			fmt.Printf("Gaps/Total: %d/%d (%f)\n", totalGaps, totalQueries, float64(totalGaps)/float64(totalQueries))
			fmt.Println(string(b))
		}
	}
}
