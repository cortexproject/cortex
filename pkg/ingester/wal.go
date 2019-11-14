package ingester

import (
	"flag"
	"fmt"
	"io"
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
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/wal"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
)

// WALConfig is config for the Write Ahead Log.
type WALConfig struct {
	walEnabled         bool
	checkpointEnabled  bool
	recover            bool
	dir                string
	checkpointDuration time.Duration
	metricsRegisterer  prometheus.Registerer
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *WALConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.walEnabled, "ingester.wal-enable", false, "Enable the WAL.")
	f.BoolVar(&cfg.checkpointEnabled, "ingester.checkpoint-enable", false, "Enable checkpointing.")
	f.BoolVar(&cfg.recover, "ingester.recover-from-wal", false, "Recover data from existing WAL.")
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

// newWAL creates a WAL object.
// * If the WAL is disabled, then the returned WAL is a no-op WAL.
// * If WAL recovery is enabled, then the userStates is always set for ingester.
func newWAL(cfg WALConfig, ingester *Ingester) (WAL, error) {
	if cfg.recover {
		level.Info(util.Logger).Log("msg", "recovering from WAL")
		start := time.Now()
		if err := recoverFromWAL(ingester); err != nil {
			return nil, err
		}
		elapsed := time.Since(start)
		level.Info(util.Logger).Log("msg", "recovery from WAL completed", "time", elapsed.String())
	}

	if !cfg.walEnabled {
		return &noop{}, nil
	}

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
	if !w.cfg.checkpointEnabled {
		return nil
	}
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

// checkpointSeries write the chunks of the series to the checkpoint.
func (w *walWrapper) checkpointSeries(cp *wal.WAL, userID string, fp model.Fingerprint, series *memorySeries, wireChunks []client.Chunk) ([]client.Chunk, error) {
	var err error
	wireChunks, err = toWireChunks(series.chunkDescs, wireChunks[:0])
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
	userStates := newUserStates(ingester.limiter, ingester.cfg)

	defer func() {
		if err == nil {
			ingester.userStatesMtx.Lock()
			ingester.userStates = userStates
			ingester.userStatesMtx.Unlock()
		}
	}()

	lastCheckpointDir, idx, err := lastCheckpoint(walDir)
	if err != nil {
		return err
	}

	nWorkers := runtime.GOMAXPROCS(0)
	stateCache := make([]map[string]*userState, nWorkers)
	seriesCache := make([]map[string]map[uint64]*memorySeries, nWorkers)
	for i := 0; i < nWorkers; i++ {
		stateCache[i] = make(map[string]*userState)
		seriesCache[i] = make(map[string]map[uint64]*memorySeries)
	}

	if idx >= 0 {
		// Checkpoint exists.
		level.Info(util.Logger).Log("msg", "recovering from checkpoint", "checkpoint", lastCheckpointDir)
		start := time.Now()
		if err := processCheckpoint(lastCheckpointDir, userStates, nWorkers, stateCache, seriesCache); err != nil {
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
	if err := processWAL(walDir, userStates, nWorkers, stateCache, seriesCache); err != nil {
		return err
	}
	elapsed := time.Since(start)
	level.Info(util.Logger).Log("msg", "recovered from segments", "time", elapsed.String())

	return nil
}

// segmentsExist is a stripped down version of
// https://github.com/prometheus/prometheus/blob/4c648eddf47d7e07fbc74d0b18244402200dca9e/tsdb/wal/wal.go#L739-L760.
func segmentsExist(dir string) (bool, error) {
	files, err := fileutil.ReadDir(dir)
	if err != nil {
		return false, err
	}
	for _, fn := range files {
		if _, err := strconv.Atoi(fn); err == nil {
			// First filename which is a number.
			// This is how Prometheus stores and this
			// is how it checks too.
			return true, nil
		}
	}
	return false, nil
}

// processCheckpoint loads the chunks of the series present in the last checkpoint.
func processCheckpoint(name string, userStates *userStates, nWorkers int,
	stateCache []map[string]*userState, seriesCache []map[string]map[uint64]*memorySeries) error {
	var (
		inputs = make([]chan *Series, nWorkers)
		// errChan is to capture the errors from goroutine.
		// The channel size is nWorkers to not block any worker if all of them error out.
		errChan    = make(chan error, nWorkers)
		wg         = sync.WaitGroup{}
		seriesPool = &sync.Pool{
			New: func() interface{} {
				return &Series{}
			},
		}
	)

	reader, closer, err := newWalReader(name)
	if err != nil {
		return err
	}
	defer closer.Close()

	wg.Add(nWorkers)
	for i := 0; i < nWorkers; i++ {
		inputs[i] = make(chan *Series, 300)
		go func(input <-chan *Series, stateCache map[string]*userState, seriesCache map[string]map[uint64]*memorySeries) {
			processCheckpointRecord(userStates, seriesPool, stateCache, seriesCache, input, errChan)
			wg.Done()
		}(inputs[i], stateCache[i], seriesCache[i])
	}

	var errFromChan error
Loop:
	for reader.Next() {
		s := seriesPool.Get().(*Series)
		if err := proto.Unmarshal(reader.Record(), s); err != nil {
			return err
		}

		select {
		case errFromChan = <-errChan:
			// Exit early on an error.
			// Only acts upon the first error received.
			break Loop
		default:
			mod := s.Fingerprint % uint64(nWorkers)
			inputs[mod] <- s
		}
	}
	for i := 0; i < nWorkers; i++ {
		close(inputs[i])
	}
	wg.Wait()

	if errFromChan != nil {
		return errFromChan
	}
	select {
	case errFromChan = <-errChan:
		return errFromChan
	default:
		if err := reader.Err(); err != nil {
			return err
		}
	}
	return nil
}

func processCheckpointRecord(userStates *userStates, seriesPool *sync.Pool, stateCache map[string]*userState,
	seriesCache map[string]map[uint64]*memorySeries, seriesChan <-chan *Series, errChan chan error) {
	var la []client.LabelAdapter
	for s := range seriesChan {
		state, ok := stateCache[s.UserId]
		if !ok {
			state = userStates.getOrCreate(s.UserId)
			stateCache[s.UserId] = state
			seriesCache[s.UserId] = make(map[uint64]*memorySeries)
		}

		la = la[:0]
		for _, l := range s.Labels {
			la = append(la, client.LabelAdapter{
				Name:  string(l.Name),
				Value: string(l.Value),
			})
		}
		series, err := state.createSeriesWithFingerprint(model.Fingerprint(s.Fingerprint), la, nil, true)
		if err != nil {
			errChan <- err
			return
		}

		descs, err := fromWireChunks(s.Chunks)
		if err != nil {
			errChan <- err
			return
		}

		if err := series.setChunks(descs); err != nil {
			errChan <- err
			return
		}

		seriesCache[s.UserId][s.Fingerprint] = series
		seriesPool.Put(s)
	}
}

type samplesWithUserID struct {
	samples []Sample
	userID  string
}

// processWAL processes the records in the WAL concurrently.
func processWAL(name string, userStates *userStates, nWorkers int,
	stateCache []map[string]*userState, seriesCache []map[string]map[uint64]*memorySeries) error {
	var (
		wg      sync.WaitGroup
		inputs  = make([]chan *samplesWithUserID, nWorkers)
		outputs = make([]chan *samplesWithUserID, nWorkers)
		// errChan is to capture the errors from goroutine.
		// The channel size is nWorkers to not block any worker if all of them error out.
		errChan = make(chan error, nWorkers)
		shards  = make([]*samplesWithUserID, nWorkers)
	)

	wg.Add(nWorkers)
	for i := 0; i < nWorkers; i++ {
		outputs[i] = make(chan *samplesWithUserID, 300)
		inputs[i] = make(chan *samplesWithUserID, 300)
		shards[i] = &samplesWithUserID{}

		go func(input <-chan *samplesWithUserID, output chan<- *samplesWithUserID,
			stateCache map[string]*userState, seriesCache map[string]map[uint64]*memorySeries) {
			processWALSamples(userStates, stateCache, seriesCache, input, output, errChan)
			wg.Done()
		}(inputs[i], outputs[i], stateCache[i], seriesCache[i])
	}

	reader, closer, err := newWalReader(name)
	if err != nil {
		return err
	}
	defer closer.Close()

	var (
		la          []client.LabelAdapter
		errFromChan error
		record      = &Record{}
	)
Loop:
	for reader.Next() {
		select {
		case errFromChan = <-errChan:
			// Exit early on an error.
			// Only acts upon the first error received.
			break Loop
		default:
		}
		if err := proto.Unmarshal(reader.Record(), record); err != nil {
			return err
		}

		if len(record.Labels) > 0 {
			state := userStates.getOrCreate(record.UserId)
			// Create the series from labels which do not exist.
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
					return err
				}
			}
		}

		// We split up the samples into chunks of 5000 samples or less.
		// With O(300 * #cores) in-flight sample batches, large scrapes could otherwise
		// cause thousands of very large in flight buffers occupying large amounts
		// of unused memory.
		for len(record.Samples) > 0 {
			m := 5000
			if len(record.Samples) < m {
				m = len(record.Samples)
			}
			for i := 0; i < nWorkers; i++ {
				if len(shards[i].samples) == 0 {
					// It is possible that the previous iteration did not put
					// anything in this shard. In that case no need to get a new buffer.
					shards[i].userID = record.UserId
					continue
				}
				select {
				case buf := <-outputs[i]:
					buf.samples = buf.samples[:0]
					buf.userID = record.UserId
					shards[i] = buf
				default:
					shards[i] = &samplesWithUserID{
						userID: record.UserId,
					}
				}
			}
			for _, sam := range record.Samples[:m] {
				mod := sam.Fingerprint % uint64(nWorkers)
				shards[mod].samples = append(shards[mod].samples, sam)
			}
			for i := 0; i < nWorkers; i++ {
				if len(shards[i].samples) > 0 {
					inputs[i] <- shards[i]
				}
			}
			record.Samples = record.Samples[m:]
		}
	}

	for i := 0; i < nWorkers; i++ {
		close(inputs[i])
		for range outputs[i] {
		}
	}
	wg.Wait()
	// If any worker errored out, some input channels might not be empty.
	// Hence drain them.
	for i := 0; i < nWorkers; i++ {
		for range inputs[i] {
		}
	}

	if errFromChan != nil {
		return errFromChan
	}
	select {
	case errFromChan = <-errChan:
		return errFromChan
	default:
		if err := reader.Err(); err != nil {
			return err
		}
	}

	if err != nil {
		return err
	}

	return nil
}

func processWALSamples(userStates *userStates, stateCache map[string]*userState, seriesCache map[string]map[uint64]*memorySeries,
	input <-chan *samplesWithUserID, output chan<- *samplesWithUserID, errChan chan error) {
	defer close(output)

	sp := model.SamplePair{}
	for samples := range input {
		state, ok := stateCache[samples.userID]
		if !ok {
			state = userStates.getOrCreate(samples.userID)
			stateCache[samples.userID] = state
			seriesCache[samples.userID] = make(map[uint64]*memorySeries)
		}
		sc := seriesCache[samples.userID]
		for i := range samples.samples {
			series, ok := sc[samples.samples[i].Fingerprint]
			if !ok {
				series, ok = state.fpToSeries.get(model.Fingerprint(samples.samples[i].Fingerprint))
				if !ok {
					// This should ideally not happen.
					// If the series was not created in recovering checkpoint or
					// from the labels of any records previous to this, there
					// is no way to get the labels for this fingerprint.
					level.Warn(util.Logger).Log("msg", "series not found for sample during wal recovery", "userid", samples.userID, "fingerprint", model.Fingerprint(samples.samples[i].Fingerprint).String())
					continue
				}
			}

			sp.Timestamp = model.Time(samples.samples[i].Timestamp)
			sp.Value = model.SampleValue(samples.samples[i].Value)
			// There can be many out of order samples because of checkpoint and WAL overlap.
			// Checking this beforehand avoids the allocation of lots of error messages.
			if sp.Timestamp.After(series.lastTime) {
				if err := series.add(sp); err != nil {
					errChan <- err
					return
				}
			}
		}
		output <- samples
	}
}

func newWalReader(name string) (*wal.Reader, io.Closer, error) {
	segmentReader, err := wal.NewSegmentsReader(name)
	if err != nil {
		return nil, nil, err
	}
	return wal.NewReader(segmentReader), segmentReader, nil
}
