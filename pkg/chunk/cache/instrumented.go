package cache

import (
	"context"

	ot "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/client_golang/prometheus"
	instr "github.com/weaveworks/common/instrument"
)

var (
	requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "cache_request_duration_seconds",
		Help:      "Total time spent in seconds doing cache requests.",
		// Cache requests are very quick: smallest bucket is 16us, biggest is 1s.
		Buckets: prometheus.ExponentialBuckets(0.000016, 4, 8),
	}, []string{"method", "status_code"})

	fetchedKeys = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "cache_fetched_keys",
		Help:      "Total count of chunks requested from memcache.",
	})

	hits = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "cache_hits",
		Help:      "Total count of chunks found in memcache.",
	})
)

func init() {
	prometheus.MustRegister(requestDuration)
	prometheus.MustRegister(fetchedKeys)
	prometheus.MustRegister(hits)
}

func instrument(name string, cache Cache) Cache {
	return &instrumentedCache{
		name:  name,
		Cache: cache,
	}
}

type instrumentedCache struct {
	name string
	Cache
}

func (i *instrumentedCache) StoreChunk(ctx context.Context, key string, buf []byte) error {
	return instr.TimeRequestHistogram(ctx, i.name+".store", requestDuration, func(ctx context.Context) error {
		return i.Cache.StoreChunk(ctx, key, buf)
	})
}

func (i *instrumentedCache) FetchChunkData(ctx context.Context, keys []string) ([]string, [][]byte, []string, error) {
	var (
		found   []string
		bufs    [][]byte
		missing []string
	)
	err := instr.TimeRequestHistogram(ctx, i.name+".fetch", requestDuration, func(ctx context.Context) error {
		sp := ot.SpanFromContext(ctx)
		sp.LogFields(otlog.Int("chunks requested", len(keys)))

		var err error
		found, bufs, missing, err = i.Cache.FetchChunkData(ctx, keys)

		if err == nil {
			sp.LogFields(otlog.Int("chunks found", len(found)), otlog.Int("chunks missing", len(keys)-len(found)))
		} else {
			sp.LogFields(otlog.Error(err))
		}

		return err
	})
	fetchedKeys.Add(float64(len(keys)))
	hits.Add(float64(len(found)))
	return found, bufs, missing, err
}

func (i *instrumentedCache) Stop() error {
	return i.Cache.Stop()
}
