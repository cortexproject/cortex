# PromQLsmith

## Description

A random query generator for PromQL. Its name is inspired by [SQLsmith](https://github.com/anse1/sqlsmith)

## Usage

PromQLsmith is a library that can be used in the test to generate PromQL queries. Example usage can be found under [example](example).

```go
package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/cortexproject/promqlsmith"
)

func main() {
	seriesSet := []labels.Labels{
		labels.FromMap(map[string]string{
			labels.MetricName: "http_requests_total",
			"job":             "prometheus",
			"status_code":     "200",
		}),
		labels.FromMap(map[string]string{
			labels.MetricName: "http_requests_total",
			"job":             "prometheus",
			"status_code":     "400",
		}),
	}

	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	opts := []promqlsmith.Option{
		promqlsmith.WithEnableOffset(true),
		promqlsmith.WithEnableAtModifier(true),
	}
	ps := promqlsmith.New(rnd, seriesSet, opts...)
	// Generate a query that can be used in instant query.
	q1 := ps.WalkInstantQuery()
	// Generate a query that can be used in range query.
	q2 := ps.WalkRangeQuery()
	fmt.Println(q1.Pretty(0))
	fmt.Println(q2.Pretty(2))
}
```