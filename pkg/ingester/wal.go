package ingester

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	tsdb_errors "github.com/prometheus/tsdb/errors"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/wal"

	"github.com/cortexproject/cortex/pkg/ingester/client"
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

type walWrapper struct {
	cfg      WALConfig
	ingester *Ingester
	quit     chan struct{}
	wait     sync.WaitGroup

	wal *wal.WAL

	// Checkpoint metrics.
	checkpointDeleteFail    prometheus.Counter
	checkpointDeleteTotal   prometheus.Counter
	checkpointCreationFail  prometheus.Counter
	checkpointCreationTotal prometheus.Counter
}

func newWAL(cfg WALConfig, ingester *Ingester) (WAL, error) {
	if !cfg.enabled {
		return &noop{}, nil
	}

	level.Info(util.Logger).Log("msg", "recovering from WAL")
	start := time.Now()
	if err := recoverFromWAL(ingester); err != nil {
		return nil, err
	}
	elapsed := time.Since(start)
	level.Info(util.Logger).Log("msg", "recovery from WAL completed", "time", elapsed.String())

	var walRegistry prometheus.Registerer
	if cfg.metricsRegisterer != nil {
		walRegistry = prometheus.WrapRegistererWith(prometheus.Labels{"kind": "wal"}, cfg.metricsRegisterer)
	}
	tsdbWAL, err := wal.New(util.Logger, walRegistry, cfg.dir, true)
	if err != nil {
		return nil, err
	}

	w := &walWrapper{
		cfg:      cfg,
		ingester: ingester,
		quit:     make(chan struct{}),
		wal:      tsdbWAL,
	}

	w.checkpointDeleteFail = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ingester_checkpoint_deletions_failed_total",
		Help: "Total number of checkpoint deletions that failed.",
	})
	w.checkpointDeleteTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ingester_checkpoint_deletions_total",
		Help: "Total number of checkpoint deletions attempted.",
	})
	w.checkpointCreationFail = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ingester_checkpoint_creations_failed_total",
		Help: "Total number of checkpoint creations that failed.",
	})
	w.checkpointCreationTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ingester_checkpoint_creations_total",
		Help: "Total number of checkpoint creations attempted.",
	})
	if cfg.metricsRegisterer != nil {
		cfg.metricsRegisterer.MustRegister(
			w.checkpointDeleteFail,
			w.checkpointDeleteTotal,
			w.checkpointCreationFail,
			w.checkpointCreationTotal,
		)
	}

	w.wait.Add(1)
	go w.run()
	return w, nil
}

func (w *walWrapper) Stop() {
	close(w.quit)
	w.wait.Wait()

	w.wal.Close()
}

func (w *walWrapper) Log(record *Record) error {
	buf, err := proto.Marshal(record)
	if err != nil {
		return err
	}
	return w.wal.Log(buf)
}

func (w *walWrapper) run() {
	defer w.wait.Done()

	ticker := time.NewTicker(w.cfg.checkpointDuration)
	defer ticker.Stop()

	for !w.isStopped() {
		select {
		case <-ticker.C:
			start := time.Now()
			level.Info(util.Logger).Log("msg", "starting checkpoint")
			if err := w.checkpoint(); err != nil {
				level.Error(util.Logger).Log("msg", "error checkpointing series", "err", err)
				continue
			}
			elapsed := time.Since(start)
			level.Info(util.Logger).Log("msg", "checkpoint done", "time", elapsed.String())
		case <-w.quit:
			if err := w.checkpoint(); err != nil {
				level.Error(util.Logger).Log("msg", "error checkpointing series during shutdown", "err", err)
			}
			return
		}
	}
}

func (w *walWrapper) isStopped() bool {
	select {
	case <-w.quit:
		return true
	default:
		return false
	}
}

const checkpointPrefix = "checkpoint."

func (w *walWrapper) checkpoint() (err error) {
	w.checkpointCreationTotal.Inc()
	defer func() {
		if err != nil {
			w.checkpointCreationFail.Inc()
		}
	}()

	_, lastSegment, err := w.wal.Segments()
	if err != nil {
		return err
	}

	_, lastCh, err := lastCheckpoint(w.wal.Dir())
	if err != nil {
		return err
	}

	newIdx := lastCh + 1

	cpdir := filepath.Join(w.wal.Dir(), fmt.Sprintf(checkpointPrefix+"%06d", newIdx))
	level.Info(util.Logger).Log("msg", "attempting checkpoint for", "dir", cpdir)
	cpdirtmp := cpdir + ".tmp"

	if err := os.MkdirAll(cpdirtmp, 0777); err != nil {
		return errors.Wrap(err, "create checkpoint dir")
	}
	cp, err := wal.New(nil, nil, cpdirtmp, true)
	if err != nil {
		return errors.Wrap(err, "open checkpoint")
	}
	defer func() {
		cp.Close()
		os.RemoveAll(cpdirtmp)
	}()

	var wireChunkBuf []client.Chunk
	for userID, state := range w.ingester.userStates.cp() {
		for pair := range state.fpToSeries.iter() {
			state.fpLocker.Lock(pair.fp)
			wireChunkBuf, err = w.checkpointSeries(cp, userID, pair.fp, pair.series, wireChunkBuf)
			state.fpLocker.Unlock(pair.fp)
			if err != nil {
				return err
			}
		}
	}

	if err := cp.Close(); err != nil {
		return errors.Wrap(err, "close checkpoint")
	}
	if err := fileutil.Replace(cpdirtmp, cpdir); err != nil {
		return errors.Wrap(err, "rename checkpoint directory")
	}

	// The last segment might still have been active during the checkpointing,
	// hence delete only the segments before that.
	if err := w.wal.Truncate(lastSegment - 1); err != nil {
		return err
	}

	if lastCh >= 0 {
		if err := w.deleteCheckpoints(lastCh); err != nil {
			level.Error(util.Logger).Log("msg", "error deleting old checkpoint", "err", err)
		}
	}

	return nil
}

// lastCheckpoint returns the directory name and index of the most recent checkpoint.
// If dir does not contain any checkpoints, -1 is returned as index.
func lastCheckpoint(dir string) (string, int, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return "", -1, err
	}
	// Traverse list backwards since there may be multiple checkpoints left.
	for i := len(files) - 1; i >= 0; i-- {
		fi := files[i]

		if !strings.HasPrefix(fi.Name(), checkpointPrefix) {
			continue
		}
		if !fi.IsDir() {
			return "", -1, fmt.Errorf("checkpoint %s is not a directory", fi.Name())
		}
		idx, err := strconv.Atoi(fi.Name()[len(checkpointPrefix):])
		if err != nil {
			continue
		}
		return filepath.Join(dir, fi.Name()), idx, nil
	}
	return "", -1, nil
}

// deleteCheckpoints deletes all checkpoints in a directory which is <= maxIndex.
func (w *walWrapper) deleteCheckpoints(maxIndex int) (err error) {
	w.checkpointDeleteTotal.Inc()
	defer func() {
		if err != nil {
			w.checkpointDeleteFail.Inc()
		}
	}()

	var errs tsdb_errors.MultiError

	files, err := ioutil.ReadDir(w.wal.Dir())
	if err != nil {
		return err
	}
	for _, fi := range files {
		if !strings.HasPrefix(fi.Name(), checkpointPrefix) {
			continue
		}
		index, err := strconv.Atoi(fi.Name()[len(checkpointPrefix):])
		if err != nil || index >= maxIndex {
			continue
		}
		if err := os.RemoveAll(filepath.Join(w.wal.Dir(), fi.Name())); err != nil {
			errs.Add(err)
		}
	}
	return errs.Err()
}

func (w *walWrapper) checkpointSeries(cp *wal.WAL, userID string, fp model.Fingerprint, series *memorySeries, wireChunks []client.Chunk) ([]client.Chunk, error) {
	var err error
	wireChunks, err = toWireChunks(series.chunkDescs, wireChunks)
	if err != nil {
		return wireChunks, err
	}

	buf, err := proto.Marshal(&Series{
		UserId:      userID,
		Fingerprint: uint64(fp),
		Labels:      series.labels(),
		Chunks:      wireChunks,
	})
	if err != nil {
		return wireChunks, err
	}

	return wireChunks, cp.Log(buf)
}

func recoverFromWAL(ingester *Ingester) (err error) {
	walDir := ingester.cfg.WALConfig.dir
	// Use a local userStates, so we don't need to worry about locking.
	userStates := newUserStates(ingester.limits, ingester.cfg)

	lastCheckpointDir, idx, err := lastCheckpoint(walDir)
	if err != nil {
		return err
	}

	if idx >= 0 {
		// Checkpoint exists.
		level.Info(util.Logger).Log("msg", "recovering from checkpoint", "checkpoint", lastCheckpointDir)
		start := time.Now()
		if err := processCheckpoint(lastCheckpointDir, userStates); err != nil {
			return err
		}
		elapsed := time.Since(start)
		level.Info(util.Logger).Log("msg", "recovered from checkpoint", "time", elapsed.String())
	} else {
		level.Info(util.Logger).Log("msg", "no checkpoint found")
	}

	if segExists, err := segmentsExist(walDir); err == nil && !segExists {
		level.Info(util.Logger).Log("msg", "no segments found, skipping recover from segments")
		return nil
	}

	level.Info(util.Logger).Log("msg", "recovering from segments", "dir", walDir)
	start := time.Now()
	if err := processWAL(walDir, userStates); err != nil {
		return err
	}
	elapsed := time.Since(start)
	level.Info(util.Logger).Log("msg", "recovered from segments", "time", elapsed.String())

	ingester.userStatesMtx.Lock()
	ingester.userStates = userStates
	ingester.userStatesMtx.Unlock()

	return nil
}

func processCheckpoint(name string, userStates *userStates) error {
	numSeries := 0
	numChunks := 0
	walSeries := &Series{}

	segmentReader, err := wal.NewSegmentsReader(name)
	if err != nil {
		return err
	}
	defer segmentReader.Close()

	la := []client.LabelAdapter{}
	reader := wal.NewReader(segmentReader)
	for reader.Next() {
		walSeries.Reset()
		if err := proto.Unmarshal(reader.Record(), walSeries); err != nil {
			return err
		}

		numSeries++

		descs, err := fromWireChunks(walSeries.Chunks)
		if err != nil {
			return err
		}
		numChunks += len(descs)

		state := userStates.getOrCreate(walSeries.UserId)

		la = la[:0]
		for _, l := range walSeries.Labels {
			la = append(la, client.LabelAdapter{
				Name:  string(l.Name),
				Value: string(l.Value),
			})
		}
		series, err := state.createSeriesWithFingerprint(model.Fingerprint(walSeries.Fingerprint), la, nil, true)
		if err != nil {
			return err
		}

		if err := series.setChunks(descs); err != nil {
			return err
		}
	}
	if err := reader.Err(); err != nil {
		return err
	}

	level.Info(util.Logger).Log("msg", "checkpoint recovery stats", "num_series", numSeries, "num_chunks", numChunks)

	return nil
}

func processWAL(name string, userStates *userStates) error {
	var (
		chanErr    error
		numProcs   = runtime.GOMAXPROCS(0)
		recordChan = make(chan *Record, 1000)
		errChan    = make(chan error, numProcs)
		wg         = sync.WaitGroup{}
		recordPool = sync.Pool{
			New: func() interface{} {
				return &Record{}
			},
		}
	)

	wg.Add(numProcs)
	for i := 0; i < numProcs; i++ {
		go func() {
			defer func() {
				wg.Done()
			}()
			var err error
			var la []client.LabelAdapter
			for record := range recordChan {
				la, err = processWALRecord(record, userStates, la)
				recordPool.Put(record)
				if err != nil {
					errChan <- err
					return
				}
			}
		}()
	}

	segmentReader, err := wal.NewSegmentsReader(name)
	if err != nil {
		return err
	}
	defer segmentReader.Close()

	reader := wal.NewReader(segmentReader)
Loop:
	for reader.Next() {
		msg := recordPool.Get().(*Record)
		if err := proto.Unmarshal(reader.Record(), msg); err != nil {
			return err
		}

		select {
		case chanErr = <-errChan:
			break Loop
		case recordChan <- msg:
		}
	}
	close(recordChan)
	wg.Wait()

	if chanErr != nil {
		return chanErr
	}
	if err := reader.Err(); err != nil {
		return err
	}
	return nil
}

func processWALRecord(record *Record, userStates *userStates, la []client.LabelAdapter) ([]client.LabelAdapter, error) {
	state := userStates.getOrCreate(record.UserId)
	for _, labels := range record.Labels {
		_, ok := state.fpToSeries.get(model.Fingerprint(labels.Fingerprint))
		if ok {
			continue
		}

		la = la[:0]
		for _, l := range labels.Labels {
			la = append(la, client.LabelAdapter{
				Name:  string(l.Name),
				Value: string(l.Value),
			})
		}
		_, err := state.createSeriesWithFingerprint(model.Fingerprint(labels.Fingerprint), la, nil, true)
		if err != nil {
			return la, err
		}
	}

	for _, sample := range record.Samples {
		state.fpLocker.Lock(model.Fingerprint(sample.Fingerprint))
		series, ok := state.fpToSeries.get(model.Fingerprint(sample.Fingerprint))
		if !ok {
			state.fpLocker.Unlock(model.Fingerprint(sample.Fingerprint))
			return la, nil
		}

		err := series.add(model.SamplePair{
			Timestamp: model.Time(sample.Timestamp),
			Value:     model.SampleValue(sample.Value),
		})
		if err != nil {
			// We can ignore memorySeriesError because duplicate (or) out-of-order samples are possible
			// here because the WAL is not truncated to align with the checkpoint.
			if _, ok := err.(*memorySeriesError); !ok {
				state.fpLocker.Unlock(model.Fingerprint(sample.Fingerprint))
				return la, err
			}
		}
		state.fpLocker.Unlock(model.Fingerprint(sample.Fingerprint))
	}

	return la, nil
}

func segmentsExist(dir string) (bool, error) {
	files, err := fileutil.ReadDir(dir)
	if err != nil {
		return false, err
	}
	for _, fn := range files {
		if _, err := strconv.Atoi(fn); err == nil {
			return true, nil
		}
	}
	return false, nil
}
