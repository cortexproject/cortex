package ring

import (
	"context"

	consul "github.com/hashicorp/consul/api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/instrument"
)

var consulRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "cortex",
	Name:      "consul_request_duration_seconds",
	Help:      "Time spent on consul requests.",
	Buckets:   prometheus.DefBuckets,
}, []string{"operation", "status_code"})

func init() {
	prometheus.MustRegister(consulRequestDuration)
}

type consulMetrics struct {
	kv
}

func (c consulMetrics) CAS(p *consul.KVPair, q *consul.WriteOptions) (bool, *consul.WriteMeta, error) {
	var ok bool
	var result *consul.WriteMeta
	err := instrument.TimeRequestHistogram(context.Background(), "CAS", consulRequestDuration, func(_ context.Context) error {
		var err error
		ok, result, err = c.kv.CAS(p, q)
		return err
	})
	return ok, result, err
}

func (c consulMetrics) Get(key string, q *consul.QueryOptions) (*consul.KVPair, *consul.QueryMeta, error) {
	var kvp *consul.KVPair
	var meta *consul.QueryMeta
	err := instrument.TimeRequestHistogram(context.Background(), "Get", consulRequestDuration, func(_ context.Context) error {
		var err error
		kvp, meta, err = c.kv.Get(key, q)
		return err
	})
	return kvp, meta, err
}

func (c consulMetrics) List(path string, q *consul.QueryOptions) (consul.KVPairs, *consul.QueryMeta, error) {
	var kvps consul.KVPairs
	var meta *consul.QueryMeta
	err := instrument.TimeRequestHistogram(context.Background(), "List", consulRequestDuration, func(_ context.Context) error {
		var err error
		kvps, meta, err = c.kv.List(path, q)
		return err
	})
	return kvps, meta, err
}

func (c consulMetrics) Put(p *consul.KVPair, q *consul.WriteOptions) (*consul.WriteMeta, error) {
	var result *consul.WriteMeta
	err := instrument.TimeRequestHistogram(context.Background(), "Put", consulRequestDuration, func(_ context.Context) error {
		var err error
		result, err = c.kv.Put(p, q)
		return err
	})
	return result, err
}
