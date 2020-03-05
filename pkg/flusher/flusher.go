package flusher

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
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
	services.Service

	cfg            Config
	ingesterConfig ingester.Config
	clientConfig   client.Config
	chunkStore     ingester.ChunkStore
	registerer     prometheus.Registerer

	ingester *ingester.Ingester
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
	ingesterConfig.ConcurrentFlushes = cfg.ConcurrentFlushes
	ingesterConfig.FlushOpTimeout = cfg.FlushOpTimeout

	f := &Flusher{
		cfg:            cfg,
		ingesterConfig: ingesterConfig,
		clientConfig:   clientConfig,
		chunkStore:     chunkStore,
		registerer:     registerer,
	}
	f.Service = services.NewBasicService(f.starting, f.running, f.stopping)
	return f, nil
}

func (f *Flusher) starting(ctx context.Context) error {
	// WAL replay happens here. We have it in the starting function and not New
	// so that metrics can be collected in parallel with WAL replay.
	var err error
	f.ingester, err = ingester.NewForFlusher(f.ingesterConfig, f.clientConfig, f.chunkStore, f.registerer)
	return err
}

func (f *Flusher) running(ctx context.Context) error {
	if err := f.ingester.StartAsync(ctx); err != nil {
		return errors.Wrap(err, "start ingester")
	}
	if err := f.ingester.AwaitRunning(ctx); err != nil {
		return errors.Wrap(err, "awaing running ingester")
	}

	f.ingester.Flush()

	// Sleeping to give a chance to Prometheus
	// to collect the metrics.
	level.Info(util.Logger).Log("msg", fmt.Sprintf("sleeping for %s to give chance for collection of metrics", postFlushSleepTime.String()))
	time.Sleep(postFlushSleepTime)

	f.ingester.StopAsync()
	if err := f.ingester.AwaitTerminated(ctx); err != nil {
		return err
	}
	return util.ErrStopCortex
}
func (f *Flusher) stopping() error {
	// Nothing to do here.
	return nil
}

// ReadinessHandler returns 204 always.
func (f *Flusher) ReadinessHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}
