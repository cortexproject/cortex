package ingester

import (
	"flag"
	"path"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/tsdb/wal"

	"github.com/cortexproject/cortex/pkg/util"
)

// WALConfig is config for the Write Ahead Log.
type WALConfig struct {
	enabled            bool
	dir                string
	checkpointDuration time.Duration
	metricsRegisterer  prometheus.Registerer
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *WALConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.enabled, "ingester.wal-enable", false, "Enable the WAL.")
	f.StringVar(&cfg.dir, "ingester.wal-dir", "", "Directory to store the WAL.")
	f.DurationVar(&cfg.checkpointDuration, "ingester.checkpoint-duration", 1*time.Hour, "Duration over which to checkpoint.")
}

// WAL interface allows us to have a no-op WAL when the WAL is disabled.
type WAL interface {
	Log(record *Record) error
	Stop()
}

type noop struct{}

// Log a Record to the WAL.
func (noop) Log(*Record) error {
	return nil
}

// Stop any background WAL processes.
func (noop) Stop() {}

type wrapper struct {
	cfg      WALConfig
	ingester *Ingester
	quit     chan struct{}
	wait     sync.WaitGroup

	lastCheckpointSegment int
	lastSamplesSegment    int
	samples               *wal.WAL
	checkpoints           *wal.WAL
}

func newWAL(cfg WALConfig, ingester *Ingester) (WAL, error) {
	if !cfg.enabled {
		return &noop{}, nil
	}

	var samplesRegistry prometheus.Registerer
	if cfg.metricsRegisterer != nil {
		samplesRegistry = prometheus.WrapRegistererWith(prometheus.Labels{"kind": "samples"}, cfg.metricsRegisterer)
	}
	samples, err := wal.New(util.Logger, samplesRegistry, path.Join(cfg.dir, "samples"), true)
	if err != nil {
		return nil, err
	}

	var checkpointsRegistry prometheus.Registerer
	if cfg.metricsRegisterer != nil {
		checkpointsRegistry = prometheus.WrapRegistererWith(prometheus.Labels{"kind": "checkpoints"}, cfg.metricsRegisterer)
	}
	checkpoints, err := wal.New(util.Logger, checkpointsRegistry, path.Join(cfg.dir, "checkpoints"), true)
	if err != nil {
		return nil, err
	}

	w := &wrapper{
		cfg:         cfg,
		ingester:    ingester,
		quit:        make(chan struct{}),
		samples:     samples,
		checkpoints: checkpoints,
	}

	w.wait.Add(1)
	go w.run()
	return w, nil
}

func (w *wrapper) Stop() {
	close(w.quit)
	w.wait.Wait()

	w.samples.Close()
	w.checkpoints.Close()
}

func (w *wrapper) Log(record *Record) error {
	buf, err := proto.Marshal(record)
	if err != nil {
		return err
	}
	return w.samples.Log(buf)
}

func (w *wrapper) run() {
	defer w.wait.Done()

	for !w.isStopped() {
		if err := w.checkpoint(); err != nil {
			level.Error(util.Logger).Log("msg", "Error checkpointing series", "err", err)
			continue
		}

		if err := w.truncateSamples(); err != nil {
			level.Error(util.Logger).Log("msg", "Error truncating wal", "err", err)
			continue
		}
	}
}

func (w *wrapper) isStopped() bool {
	select {
	case <-w.quit:
		return true
	default:
		return false
	}
}

func (w *wrapper) checkpoint() error {
	// Count number of series - we'll use this to rate limit checkpoints.
	numSeries := 0
	for _, state := range w.ingester.userStates.cp() {
		numSeries += state.fpToSeries.length()
	}
	if numSeries == 0 {
		return nil
	}
	perSeriesDuration := w.cfg.checkpointDuration / time.Duration(numSeries)
	ticker := time.NewTicker(perSeriesDuration)
	defer ticker.Stop()

	for userID, state := range w.ingester.userStates.cp() {
		for pair := range state.fpToSeries.iter() {
			state.fpLocker.Lock(pair.fp)
			err := w.checkpointSeries(userID, pair.fp, pair.series)
			state.fpLocker.Unlock(pair.fp)
			if err != nil {
				return err
			}

			select {
			case <-ticker.C:
			case <-w.quit: // When we're trying to shutdown, finish the checkpoint as fast as possible.
			}
		}
	}

	// Remove the previous checkpoint.
	_, last, err := w.checkpoints.Segments()
	if err != nil {
		return err
	}
	if err := w.checkpoints.Truncate(w.lastCheckpointSegment); err != nil {
		return err
	}
	w.lastCheckpointSegment = last

	return nil
}

func (w *wrapper) checkpointSeries(userID string, fp model.Fingerprint, series *memorySeries) error {
	wireChunks, err := toWireChunks(series.chunkDescs)
	if err != nil {
		return err
	}

	buf, err := proto.Marshal(&Series{
		UserId:      userID,
		Fingerprint: uint64(fp),
		Labels:      series.labels(),
		Chunks:      wireChunks,
	})
	if err != nil {
		return err
	}

	return w.checkpoints.Log(buf)
}

// truncateSamples removed the wal from before the checkpoint.
func (w *wrapper) truncateSamples() error {
	_, last, err := w.samples.Segments()
	if err != nil {
		return err
	}

	if err := w.samples.Truncate(w.lastSamplesSegment); err != nil {
		return err
	}

	w.lastSamplesSegment = last
	return nil
}
