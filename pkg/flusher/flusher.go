package flusher

import (
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
)

// Config for an Ingester.
type Config struct {
	WALDir            string        `yaml:"wal_dir,omitempty"`
	ConcurrentFlushes int           `yaml:"concurrent_flushes,omitempty"`
	FlushOpTimeout    time.Duration `yaml:"flush_op_timeout,omitempty"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.WALDir, "flusher.wal-dir", "wal", "Directory to read WAL from.")
	f.IntVar(&cfg.ConcurrentFlushes, "flusher.concurrent-flushes", 50, "Number of concurrent goroutines flushing to dynamodb.")
	f.DurationVar(&cfg.FlushOpTimeout, "flusher.flush-op-timeout", 2*time.Minute, "Timeout for individual flush operations.")
}

// Flusher is designed to be used as a job to flush the chunks from the WAL on disk.
type Flusher struct {
	ing *ingester.Ingester
}

const (
	postFlushSleepTime = 1 * time.Minute
)

// New constructs a new Flusher and flushes the data from the WAL.
// The returned Flusher has no other operations.
func New(
	cfg Config,
	ingesterConfig ingester.Config,
	clientConfig client.Config,
	chunkStore ingester.ChunkStore,
	registerer prometheus.Registerer,
) (*Flusher, error) {

	ingesterConfig.WALConfig.Dir = cfg.WALDir

	ing, err := ingester.NewForFlusher(ingesterConfig, clientConfig, chunkStore, registerer)
	if err != nil {
		return nil, err
	}

	return &Flusher{
		ing: ing,
	}, err
}

func (f *Flusher) Flush() {
	f.ing.Flush()

	// Sleeping to give a chance to Prometheus
	// to collect the metrics.
	level.Info(util.Logger).Log("msg", fmt.Sprintf("sleeping for %s to give chance for collection of metrics", postFlushSleepTime.String()))
	time.Sleep(postFlushSleepTime)

	f.ing.Shutdown()
}

// ReadinessHandler returns 204 always.
func (f *Flusher) ReadinessHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func (f *Flusher) Close() error {
	return nil
}
