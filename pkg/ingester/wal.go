package ingester

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
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
	f.BoolVar(&cfg.walEnabled, "ingester.wal-enabled", false, "Enable the WAL.")
	f.BoolVar(&cfg.checkpointEnabled, "ingester.checkpoint-enabled", false, "Enable checkpointing.")
	f.BoolVar(&cfg.recover, "ingester.recover-from-wal", false, "Recover data from existing WAL.")
	f.StringVar(&cfg.dir, "ingester.wal-dir", "wal", "Directory to store the WAL.")
	f.DurationVar(&cfg.checkpointDuration, "ingester.checkpoint-duration", 1*time.Hour, "Duration over which to checkpoint.")
}

// WAL interface allows us to have a no-op WAL when the WAL is disabled.
type WAL interface {
	// Log marshalls the records and writes it into the WAL.
	Log(*Record) error
	// Stop stops all the WAL operations.
	Stop()
}

type noopWAL struct{}

func (noopWAL) Log(*Record) error { return nil }
func (noopWAL) Stop()             {}

type walWrapper struct {
	cfg  WALConfig
	quit chan struct{}
	wait sync.WaitGroup

	wal           *wal.WAL
	getUserStates func() map[string]*userState

	// Checkpoint metrics.
	checkpointDeleteFail    prometheus.Counter
	checkpointDeleteTotal   prometheus.Counter
	checkpointCreationFail  prometheus.Counter
	checkpointCreationTotal prometheus.Counter
	checkpointDuration      prometheus.Summary
}

// newWAL creates a WAL object. If the WAL is disabled, then the returned WAL is a no-op WAL.
func newWAL(cfg WALConfig, userStatesFunc func() map[string]*userState) (WAL, error) {
	if !cfg.walEnabled {
		return &noopWAL{}, nil
	}

	var walRegistry prometheus.Registerer
	if cfg.metricsRegisterer != nil {
		walRegistry = prometheus.WrapRegistererWith(prometheus.Labels{"kind": "wal"}, cfg.metricsRegisterer)
	}
	tsdbWAL, err := wal.NewSize(util.Logger, walRegistry, cfg.dir, wal.DefaultSegmentSize/4, true)
	if err != nil {
		return nil, err
	}

	w := &walWrapper{
		cfg:           cfg,
		quit:          make(chan struct{}),
		wal:           tsdbWAL,
		getUserStates: userStatesFunc,
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
	w.checkpointDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name:       "ingester_checkpoint_duration_seconds",
		Help:       "Time taken to create a checkpoint.",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
	if cfg.metricsRegisterer != nil {
		cfg.metricsRegisterer.MustRegister(
			w.checkpointDeleteFail,
			w.checkpointDeleteTotal,
			w.checkpointCreationFail,
			w.checkpointCreationTotal,
			w.checkpointDuration,
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
	select {
	case <-w.quit:
		return nil
	default:
		if record == nil {
			return nil
		}
		buf, err := proto.Marshal(record)
		if err != nil {
			return err
		}
		return w.wal.Log(buf)
	}
}

func (w *walWrapper) run() {
	defer w.wait.Done()

	if !w.cfg.checkpointEnabled {
		return
	}

	ticker := time.NewTicker(w.cfg.checkpointDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			start := time.Now()
			level.Info(util.Logger).Log("msg", "starting checkpoint")
			if err := w.performCheckpoint(); err != nil {
				level.Error(util.Logger).Log("msg", "error checkpointing series", "err", err)
				continue
			}
			elapsed := time.Since(start)
			level.Info(util.Logger).Log("msg", "checkpoint done", "time", elapsed.String())
			w.checkpointDuration.Observe(elapsed.Seconds())
		case <-w.quit:
			level.Info(util.Logger).Log("msg", "creating checkpoint before shutdown")
			if err := w.performCheckpoint(); err != nil {
				level.Error(util.Logger).Log("msg", "error checkpointing series during shutdown", "err", err)
			}
			return
		}
	}
}

const checkpointPrefix = "checkpoint."

func (w *walWrapper) performCheckpoint() (err error) {
	if !w.cfg.checkpointEnabled {
		return nil
	}

	w.checkpointCreationTotal.Inc()
	defer func() {
		if err != nil {
			w.checkpointCreationFail.Inc()
		}
	}()

	if w.getUserStates == nil {
		return errors.New("function to get user states not initialised")
	}

	_, lastSegment, err := w.wal.Segments()
	if err != nil {
		return err
	}

	_, lastCh, err := lastCheckpoint(w.wal.Dir())
	if err != nil {
		return err
	}

	// Checkpoint is named after the last WAL segment present so that when replaying the WAL
	// we can start from that particular WAL segment.
	checkpointDir := filepath.Join(w.wal.Dir(), fmt.Sprintf(checkpointPrefix+"%06d", lastSegment))
	level.Info(util.Logger).Log("msg", "attempting checkpoint for", "dir", checkpointDir)
	checkpointDirTemp := checkpointDir + ".tmp"

	if err := os.MkdirAll(checkpointDirTemp, 0777); err != nil {
		return errors.Wrap(err, "create checkpoint dir")
	}
	checkpoint, err := wal.New(nil, nil, checkpointDirTemp, true)
	if err != nil {
		return errors.Wrap(err, "open checkpoint")
	}
	defer func() {
		checkpoint.Close()
		os.RemoveAll(checkpointDirTemp)
	}()

	var wireChunkBuf []client.Chunk
	for userID, state := range w.getUserStates() {
		for pair := range state.fpToSeries.iter() {
			state.fpLocker.Lock(pair.fp)
			wireChunkBuf, err = w.checkpointSeries(checkpoint, userID, pair.fp, pair.series, wireChunkBuf)
			state.fpLocker.Unlock(pair.fp)
			if err != nil {
				return err
			}
		}
	}

	if err := checkpoint.Close(); err != nil {
		return errors.Wrap(err, "close checkpoint")
	}
	if err := fileutil.Replace(checkpointDirTemp, checkpointDir); err != nil {
		return errors.Wrap(err, "rename checkpoint directory")
	}

	// The last segment might still have been active during the checkpointing,
	// hence delete only the segments before that.
	if err := w.wal.Truncate(lastSegment - 1); err != nil {
		// It is fine to have old WAL segments hanging around if deletion failed.
		// We can try again next time.
		level.Error(util.Logger).Log("msg", "error deleting old WAL segments", "err", err)
	}

	if lastCh >= 0 {
		if err := w.deleteCheckpoints(lastCh); err != nil {
			// It is fine to have old checkpoints hanging around if deletion failed.
			// We can try again next time.
			level.Error(util.Logger).Log("msg", "error deleting old checkpoint", "err", err)
		}
	}

	return nil
}

// lastCheckpoint returns the directory name and index of the most recent checkpoint.
// If dir does not contain any checkpoints, -1 is returned as index.
func lastCheckpoint(dir string) (string, int, error) {
	dirs, err := ioutil.ReadDir(dir)
	if err != nil {
		return "", -1, err
	}
	var (
		maxIdx        = -1
		checkpointDir string
	)
	// There may be multiple checkpoints left, so select the one with max index.
	for i := 0; i < len(dirs); i++ {
		di := dirs[i]

		if !strings.HasPrefix(di.Name(), checkpointPrefix) {
			continue
		}
		if !di.IsDir() {
			return "", -1, fmt.Errorf("checkpoint %s is not a directory", di.Name())
		}
		idx, err := strconv.Atoi(di.Name()[len(checkpointPrefix):])
		if err != nil {
			continue
		}
		if idx > maxIdx {
			checkpointDir = di.Name()
			maxIdx = idx
		}
	}
	if maxIdx >= 0 {
		return filepath.Join(dir, checkpointDir), maxIdx, nil
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
		Labels:      client.FromLabelsToLabelAdapters(series.metric),
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

	level.Info(util.Logger).Log("msg", "recovering from WAL", "dir", walDir, "start_segment", idx)
	start := time.Now()
	if err := processWAL(walDir, idx, userStates, nWorkers, stateCache, seriesCache); err != nil {
		return err
	}
	elapsed := time.Since(start)
	level.Info(util.Logger).Log("msg", "recovered from WAL", "time", elapsed.String())

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

	reader, closer, err := newWalReader(name, -1)
	if err != nil {
		return err
	}
	defer closer.Close()

	var (
		inputs = make([]chan *Series, nWorkers)
		// errChan is to capture the errors from goroutine.
		// The channel size is nWorkers+1 to not block any worker if all of them error out.
		errChan    = make(chan error, nWorkers)
		wg         = sync.WaitGroup{}
		seriesPool = &sync.Pool{
			New: func() interface{} {
				return &Series{}
			},
		}
	)

	wg.Add(nWorkers)
	for i := 0; i < nWorkers; i++ {
		inputs[i] = make(chan *Series, 300)
		go func(input <-chan *Series, stateCache map[string]*userState, seriesCache map[string]map[uint64]*memorySeries) {
			processCheckpointRecord(userStates, seriesPool, stateCache, seriesCache, input, errChan)
			wg.Done()
		}(inputs[i], stateCache[i], seriesCache[i])
	}

	var capturedErr error
Loop:
	for reader.Next() {
		s := seriesPool.Get().(*Series)
		if err := proto.Unmarshal(reader.Record(), s); err != nil {
			// We don't return here in order to close/drain all the channels and
			// make sure all goroutines exit.
			capturedErr = err
			break Loop
		}
		// The yoloString from the unmarshal of LabelAdapter gets corrupted
		// when travelling through the channel. Hence making a copy of that.
		// This extra alloc during the read path is fine as it's only 1 time
		// and saves extra allocs during write path by having LabelAdapter.
		s.Labels = copyLabelAdapters(s.Labels)

		select {
		case capturedErr = <-errChan:
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
	// If any worker errored out, some input channels might not be empty.
	// Hence drain them.
	for i := 0; i < nWorkers; i++ {
		for range inputs[i] {
		}
	}

	if capturedErr != nil {
		return capturedErr
	}
	select {
	case capturedErr = <-errChan:
		return capturedErr
	default:
		return reader.Err()
	}
}

func copyLabelAdapters(las []client.LabelAdapter) []client.LabelAdapter {
	for i := range las {
		n, v := make([]byte, len(las[i].Name)), make([]byte, len(las[i].Value))
		copy(n, las[i].Name)
		copy(v, las[i].Value)
		las[i].Name = string(n)
		las[i].Value = string(v)
	}
	return las
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
		memoryChunks.Add(float64(len(descs)))

		seriesCache[s.UserId][s.Fingerprint] = series
		seriesPool.Put(s)
	}
}

type samplesWithUserID struct {
	samples []Sample
	userID  string
}

// processWAL processes the records in the WAL concurrently.
func processWAL(name string, startSegment int, userStates *userStates, nWorkers int,
	stateCache []map[string]*userState, seriesCache []map[string]map[uint64]*memorySeries) error {

	reader, closer, err := newWalReader(name, startSegment)
	if err != nil {
		return err
	}
	defer closer.Close()

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

	var (
		capturedErr error
		record      = &Record{}
	)
Loop:
	for reader.Next() {
		select {
		case capturedErr = <-errChan:
			// Exit early on an error.
			// Only acts upon the first error received.
			break Loop
		default:
		}
		if err := proto.Unmarshal(reader.Record(), record); err != nil {
			// We don't return here in order to close/drain all the channels and
			// make sure all goroutines exit.
			capturedErr = err
			break Loop
		}

		if len(record.Labels) > 0 {
			state := userStates.getOrCreate(record.UserId)
			// Create the series from labels which do not exist.
			for _, labels := range record.Labels {
				_, ok := state.fpToSeries.get(model.Fingerprint(labels.Fingerprint))
				if ok {
					continue
				}
				_, err := state.createSeriesWithFingerprint(model.Fingerprint(labels.Fingerprint), labels.Labels, nil, true)
				if err != nil {
					// We don't return here in order to close/drain all the channels and
					// make sure all goroutines exit.
					capturedErr = err
					break Loop
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

	if capturedErr != nil {
		return capturedErr
	}
	select {
	case capturedErr = <-errChan:
		return capturedErr
	default:
		return reader.Err()
	}
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

// If startSegment is <0, it means all the segments.
func newWalReader(name string, startSegment int) (*wal.Reader, io.Closer, error) {
	var (
		segmentReader io.ReadCloser
		err           error
	)
	if startSegment < 0 {
		segmentReader, err = wal.NewSegmentsReader(name)
		if err != nil {
			return nil, nil, err
		}
	} else {
		first, last, err := SegmentRange(name)
		if err != nil {
			return nil, nil, err
		}
		if startSegment > last {
			return nil, nil, errors.New("start segment is beyond the last WAL segment")
		}
		if first > startSegment {
			startSegment = first
		}
		segmentReader, err = wal.NewSegmentsRangeReader(wal.SegmentRange{
			Dir:   name,
			First: startSegment,
			Last:  -1, // Till the end.
		})
		if err != nil {
			return nil, nil, err
		}
	}
	return wal.NewReader(segmentReader), segmentReader, nil
}

// SegmentRange returns the first and last segment index of the WAL in the dir.
// If https://github.com/prometheus/prometheus/pull/6477 is merged, get rid of this
// method and use from Prometheus directly.
func SegmentRange(dir string) (int, int, error) {
	files, err := fileutil.ReadDir(dir)
	if err != nil {
		return 0, 0, err
	}
	first, last := math.MaxInt32, math.MinInt32
	for _, fn := range files {
		k, err := strconv.Atoi(fn)
		if err != nil {
			continue
		}
		if k < first {
			first = k
		}
		if k > last {
			last = k
		}
	}
	if first == math.MaxInt32 || last == math.MinInt32 {
		return -1, -1, nil
	}
	return first, last, nil
}
