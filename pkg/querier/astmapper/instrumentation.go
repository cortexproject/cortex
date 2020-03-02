package astmapper

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var shardCounter = promauto.NewCounter(prometheus.CounterOpts{
	Namespace: "cortex",
	Name:      "frontend_sharded_queries_total",
	Help:      "Total number of sharded queries",
})
