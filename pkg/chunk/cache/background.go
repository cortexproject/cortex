package cache

import (
	"context"
	"flag"
	"sync"

	opentracing "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// BackgroundConfig is config for a Background Cache.
type BackgroundConfig struct {
	WriteBackGoroutines int `yaml:"writeback_goroutines"`
	WriteBackBuffer     int `yaml:"writeback_buffer"`
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet
func (cfg *BackgroundConfig) RegisterFlagsWithPrefix(prefix string, description string, f *flag.FlagSet) {
	f.IntVar(&cfg.WriteBackGoroutines, prefix+"background.write-back-concurrency", 10, description+"At what concurrency to write back to cache.")
	f.IntVar(&cfg.WriteBackBuffer, prefix+"background.write-back-buffer", 10000, description+"How many key batches to buffer for background write-back.")
}

type backgroundCache struct {
	Cache

	wg       sync.WaitGroup
	quit     chan struct{}
	bgWrites chan backgroundWrite
	name     string

	droppedWriteBack prometheus.Counter
	queueLength      prometheus.Gauge
}

type backgroundWrite struct {
	data map[string][]byte
}

// NewBackground returns a new Cache that does stores on background goroutines.
func NewBackground(name string, cfg BackgroundConfig, cache Cache, reg prometheus.Registerer) Cache {
	c := &backgroundCache{
		Cache:    cache,
		quit:     make(chan struct{}),
		bgWrites: make(chan backgroundWrite, cfg.WriteBackBuffer),
		name:     name,
		droppedWriteBack: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace:   "cortex",
			Name:        "cache_dropped_background_writes_total",
			Help:        "Total count of dropped write backs to cache.",
			ConstLabels: prometheus.Labels{"name": name},
		}),

		queueLength: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace:   "cortex",
			Name:        "cache_background_queue_length",
			Help:        "Length of the cache background write queue.",
			ConstLabels: prometheus.Labels{"name": name},
		}),
	}

	c.wg.Add(cfg.WriteBackGoroutines)
	for i := 0; i < cfg.WriteBackGoroutines; i++ {
		go c.writeBackLoop()
	}

	return c
}

// Stop the background flushing goroutines.
func (c *backgroundCache) Stop() {
	close(c.quit)
	c.wg.Wait()

	c.Cache.Stop()
}

const keysPerBatch = 100

// Store writes keys for the cache in the background.
func (c *backgroundCache) Store(ctx context.Context, data map[string][]byte) {
	batch := make(map[string][]byte, keysPerBatch)
	for len(data) > 0 {
		queueSize := 0
		for k, v := range data {
			if queueSize >= keysPerBatch {
				break
			}
			batch[k] = v
			delete(data, k)
			queueSize++
		}

		bgWrite := backgroundWrite{
			data: batch,
		}
		select {
		case c.bgWrites <- bgWrite:
			c.queueLength.Add(float64(queueSize))
		default:
			c.droppedWriteBack.Add(float64(queueSize))
			sp := opentracing.SpanFromContext(ctx)
			if sp != nil {
				sp.LogFields(otlog.Int("dropped", queueSize))
			}
			return // queue is full; give up
		}
		batch = make(map[string][]byte, keysPerBatch)
	}
}

func (c *backgroundCache) writeBackLoop() {
	defer c.wg.Done()

	for {
		select {
		case bgWrite, ok := <-c.bgWrites:
			if !ok {
				return
			}
			c.queueLength.Sub(float64(len(bgWrite.data)))
			c.Cache.Store(context.Background(), bgWrite.data)

		case <-c.quit:
			return
		}
	}
}
