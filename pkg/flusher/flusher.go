package flusher

import (
	"context"
	"flag"
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	ot "github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
)

const (
	maxFlushRetries = 15
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

// Flusher deals with "in flight" chunks.  Based on Prometheus 1.x
// MemorySeriesStorage.
type Flusher struct {
	cfg Config

	chunkStore ingester.ChunkStore

	// One queue per flush thread.  Fingerprint is used to
	// pick a queue.
	seriesFlushedCounter prometheus.Counter
	chunksFlushedCounter prometheus.Counter
}

// New constructs a new Ingester.
func New(
	cfg Config,
	ingesterConfig ingester.Config,
	clientConfig client.Config,
	chunkStore ingester.ChunkStore,
	registerer prometheus.Registerer,
) (*Flusher, error) {
	f := &Flusher{
		cfg:        cfg,
		chunkStore: chunkStore,
	}

	f.registerMetrics(registerer)

	metrics := ingester.NewIngesterMetrics(nil, true)
	userStates := ingester.NewUserStates(nil, ingesterConfig, metrics)

	level.Info(util.Logger).Log("msg", "recovering from WAL")

	start := time.Now()
	if err := ingester.RecoverFromWAL(cfg.WALDir, userStates); err != nil {
		level.Error(util.Logger).Log("msg", "failed to recover from WAL", "time", time.Since(start).String())
		return nil, err
	}
	elapsed := time.Since(start)

	level.Info(util.Logger).Log("msg", "recovery from WAL completed", "time", elapsed.String())

	err := f.flushAllUsers(userStates)

	// Sleeping to give a chance to Prometheus
	// to collect the metrics.
	time.Sleep(1 * time.Minute)

	return f, err
}

func (f *Flusher) registerMetrics(registerer prometheus.Registerer) {
	f.seriesFlushedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_flusher_series_flushed_total",
		Help: "Total number of series flushed.",
	})
	f.chunksFlushedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_flusher_chunks_flushed_total",
		Help: "Total number of chunks flushed.",
	})

	if registerer != nil {
		registerer.MustRegister(
			f.seriesFlushedCounter,
			f.chunksFlushedCounter,
		)
	}
}

func (f *Flusher) flushAllUsers(userStates *ingester.UserStates) error {
	level.Info(util.Logger).Log("msg", "flushing all series")
	var (
		errChan       = make(chan error, f.cfg.ConcurrentFlushes)
		flushDataPool = sync.Pool{
			New: func() interface{} {
				return &flushData{}
			},
		}
		flushDataChan = make(chan *flushData, 2*f.cfg.ConcurrentFlushes)
		wg            sync.WaitGroup
		capturedErr   error
	)

	wg.Add(f.cfg.ConcurrentFlushes)
	for i := 0; i < f.cfg.ConcurrentFlushes; i++ {
		go f.flushUserSeries(flushDataChan, errChan, &flushDataPool, &wg)
	}

Loop:
	for userID, state := range userStates.Copy() {
		for pair := range state.IterateSeries() {
			select {
			case capturedErr = <-errChan:
				break Loop
			default:
			}

			fd := flushDataPool.Get().(*flushData)
			fd.userID = userID
			fd.pair = pair
			flushDataChan <- fd
		}
	}

	close(flushDataChan)
	wg.Wait()

	// In case there was an error, drain the channel.
	for range flushDataChan {
	}

	if capturedErr != nil {
		level.Error(util.Logger).Log("msg", "error while flushing", "err", capturedErr)
		return capturedErr
	}

	select {
	case err := <-errChan:
		level.Error(util.Logger).Log("msg", "error while flushing", "err", err)
		return err
	default:
		level.Info(util.Logger).Log("msg", "flushing done")
		return nil
	}
}

type flushData struct {
	userID string
	pair   ingester.FingerprintSeriesPair
}

func (f *Flusher) flushUserSeries(flushDataChan <-chan *flushData, errChan chan<- error, flushDataPool *sync.Pool, wg *sync.WaitGroup) {
	defer wg.Done()

	for fd := range flushDataChan {
		err := f.flushSeries(fd)
		flushDataPool.Put(fd)
		if err != nil {
			errChan <- err
			return
		}
	}
}

func (f *Flusher) flushSeries(fd *flushData) error {
	fp := fd.pair.Fingerprint
	series := fd.pair.Series
	userID := fd.userID

	// shouldFlushSeries() has told us we have at least one chunk
	chunkDescs := series.GetChunks()
	if len(chunkDescs) == 0 {
		return nil
	}

	// flush the chunks without locking the series, as we don't want to hold the series lock for the duration of the dynamo/s3 rpcs.
	ctx, cancel := context.WithTimeout(context.Background(), f.cfg.FlushOpTimeout)
	defer cancel() // releases resources if slowOperation completes before timeout elapses

	sp, ctx := ot.StartSpanFromContext(ctx, "flushUserSeries")
	defer sp.Finish()
	sp.SetTag("organization", userID)

	util.Event().Log("msg", "flush chunks", "userID", userID, "numChunks", len(chunkDescs), "firstTime", chunkDescs[0].FirstTime, "fp", fp, "series", series.Metric(), "nlabels", len(series.Metric()))

	wireChunks := make([]chunk.Chunk, 0, len(chunkDescs))
	for _, chunkDesc := range chunkDescs {
		c := chunk.NewChunk(userID, fp, series.Metric(), chunkDesc.C, chunkDesc.FirstTime, chunkDesc.LastTime)
		if err := c.Encode(); err != nil {
			return err
		}
		wireChunks = append(wireChunks, c)
	}

	backoff := util.NewBackoff(ctx, util.BackoffConfig{
		MinBackoff: 1 * time.Second,
		MaxBackoff: 10 * time.Second,
		MaxRetries: maxFlushRetries,
	})
	var err error
	for backoff.Ongoing() {
		err = f.chunkStore.Put(ctx, wireChunks)
		if err == nil {
			break
		}
		backoff.Wait()
	}
	if err != nil {
		return err
	}

	for _, chunkDesc := range chunkDescs {
		utilization, length, size := chunkDesc.C.Utilization(), chunkDesc.C.Len(), chunkDesc.C.Size()
		util.Event().Log("msg", "chunk flushed", "userID", userID, "fp", fp, "series", series.Metric(), "nlabels", len(series.Metric()), "utilization", utilization, "length", length, "size", size, "firstTime", chunkDesc.FirstTime, "lastTime", chunkDesc.LastTime)
	}

	f.seriesFlushedCounter.Inc()
	f.chunksFlushedCounter.Add(float64(len(wireChunks)))

	return nil
}

// ReadinessHandler returns 204 always.
func (f *Flusher) ReadinessHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func (f *Flusher) Close() error {
	return nil
}
