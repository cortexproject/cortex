package migrate

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/migrate/mapper"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	ot "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	sentChunks = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "reader_sent_chunks_total",
		Help:      "The total number of chunks sent by this reader.",
	}, []string{"reader_id"})
	streamErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "reader_stream_errors_total",
		Help:      "The total number of errors caused by the chunk stream.",
	}, []string{"reader_id"})
	totalChunks = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "reader_batch_chunks_total",
		Help:      "The total number of chunks this reader is responsible for.",
	}, []string{"reader_id"})
)

// ReaderConfig is a config for a Reader
type ReaderConfig struct {
	Addr                string
	BufferSize          int
	ClientConfig        client.Config
	PlannerConfig       chunk.PlannerConfig
	MapperConfig        mapper.Config
	ReaderIDPrefix      string
	StorageClient       string
	ForwarderNumRetries int
	NumWorkers          int
}

// RegisterFlags adds the flags required to configure this flag set.
func (cfg *ReaderConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.PlannerConfig.RegisterFlags(f)
	cfg.ClientConfig.RegisterFlags(f)
	cfg.MapperConfig.RegisterFlags(f)
	flag.StringVar(&cfg.StorageClient, "chunk.storage-client", "gcp", "Which storage client to use (gcp, cassandra).")
	f.IntVar(&cfg.BufferSize, "reader.buffer-size", 1000, "number of chunk batches to buffer before forwarding")
	f.StringVar(&cfg.Addr, "reader.forward-addr", "", "address of the chunk transfer endpoint")
	f.StringVar(&cfg.ReaderIDPrefix, "reader.prefix", "reader_", "prefix used to identify reader when forwarding data to writer")
	f.IntVar(&cfg.ForwarderNumRetries, "reader.num-forward-retries", 0, "number of times to try forwarding metrics before failing, infinite retries if set to less than 1")
	f.IntVar(&cfg.NumWorkers, "reader.num-workers", 1, "number of scan batches to stream simultaneously")
}

// Reader collects and forwards chunks according to it's planner
type Reader struct {
	cfg ReaderConfig
	id  string // ID is the configured as the reading prefix and the shards assigned to the reader

	storage     chunk.ObjectClient
	planner     *chunk.Planner
	chunkMapper *mapper.Mapper
}

// NewReader returns a Reader struct
func NewReader(cfg ReaderConfig, storage chunk.ObjectClient) (*Reader, error) {
	planner, err := chunk.NewPlanner(cfg.PlannerConfig)
	if err != nil {
		return nil, err
	}
	id := cfg.ReaderIDPrefix + fmt.Sprintf("%d_%d", cfg.PlannerConfig.FirstShard, cfg.PlannerConfig.LastShard)
	chunkMapper, err := mapper.New(cfg.MapperConfig)
	if err != nil {
		return nil, err
	}

	return &Reader{
		cfg:         cfg,
		id:          id,
		planner:     planner,
		chunkMapper: chunkMapper,
		storage:     storage,
	}, nil
}

// TransferData initializes a batch stream from a storage client and
// forwards metrics to writer endpoint
func (r *Reader) TransferData(ctx context.Context) error {
	sp, ctx := ot.StartSpanFromContext(ctx, "TransferData")
	defer sp.Finish()
	sp.LogFields(otlog.String("id", r.id))

	scanner := r.storage.NewScanner()
	scanRequests := r.planner.Plan()
	readCtx, cancel := context.WithCancel(ctx)

	chunkChan := make(chan []chunk.Chunk, r.cfg.BufferSize)
	reqChan := make(chan chunk.ScanRequest)
	errChan := make(chan error, r.cfg.NumWorkers)
	var wg sync.WaitGroup

	for i := 0; i < r.cfg.NumWorkers; i++ {
		go func(ctx context.Context, reqChan chan chunk.ScanRequest) {
			wg.Add(1)
			defer wg.Done()
			for req := range reqChan {
				level.Info(util.Logger).Log("msg", "attempting  scan request", "table", req.Table, "user", req.User, "shard", req.Shard)
				err := scanner.Scan(ctx, req, chunkChan)
				if err != nil {
					level.Error(util.Logger).Log("msg", "error streaming chunks", "err", err)
					errChan <- fmt.Errorf("scan request failed, %v", req)
					return
				}
				level.Info(util.Logger).Log("msg", "completed scan request", "table", req.Table, "user", req.User, "shard", req.Shard)
			}
		}(readCtx, reqChan)
	}

	go func() {
	requestLoop:
		for _, req := range scanRequests {
			select {
			case reqChan <- req:
				continue
			case err, _ := <-errChan:
				level.Error(util.Logger).Log("msg", "error scanning metrics, attempting graceful shutdown", "err", err)
				break requestLoop
			}
		}

		// Close the request channel and wait for all of the worker streams to complete
		// then close the forward chunks channel
		close(reqChan)
		wg.Wait()
		close(chunkChan)
	}()

	err := r.Forward(readCtx, chunkChan)
	cancel()        // If the forwarding metrics errors out the reader will be canceled
	if err != nil { // Loop to ensure streamchunks fails gracefully and returns a log of current progress
		level.Error(util.Logger).Log("msg", "error forwarding metrics, attempting graceful shutdown", "err", err)
		timer := time.NewTimer(time.Second * 60)
		for {
			select {
			case _, ok := <-chunkChan:
				if !ok {
					return err
				}
				continue
			case <-timer.C:
				level.Error(util.Logger).Log("msg", "shutting down reader, timed out")
				return err
			}
		}
	}

	level.Info(util.Logger).Log("msg", "reader completed migration of chunks, shutting down reader")
	return nil
}

// Forward reads batched chunks with the same metric from a channel and wires them
// to a Migrate Writer using the TransferChunks service in the ingester protobuf package
func (r Reader) Forward(ctx context.Context, chunkChan chan []chunk.Chunk) error {
	backoff := util.NewBackoff(ctx, util.BackoffConfig{
		MinBackoff: time.Second * 5,
		MaxBackoff: time.Second * 360,
		MaxRetries: r.cfg.ForwarderNumRetries,
	})

	cli, err := newStreamer(ctx, r.id, r.cfg.Addr, r.cfg.ClientConfig)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			level.Error(util.Logger).Log("msg", "shutting down forwarder")
			return nil
		case chunks, open := <-chunkChan:
			if !open {
				return cli.Close()
			}

			if len(chunks) == 0 {
				level.Warn(util.Logger).Log("msg", "ignoring empty chunk batch")
				continue
			}
			level.Debug(util.Logger).Log("msg", "sending chunk batch", "num_chunks", len(chunks))

			if r.chunkMapper != nil {
				chunks, err = r.chunkMapper.MapChunks(chunks)
				if err != nil {
					return fmt.Errorf("unable to map chunks, %v", err)
				}
			}

			for backoff.Ongoing() {
				err := cli.StreamChunks(ctx, chunks)
				if err != nil {
					level.Error(util.Logger).Log("msg", "streaming chunks failed", "err", err, "# retries", backoff.NumRetries())
					backoff.Wait()
					continue
				}
				backoff.Reset()
				break
			}

			if err != nil {
				level.Error(util.Logger).Log("msg", "streaming error, unable to recover", "err", err)
				return err
			}

			sentChunks.WithLabelValues(r.id).Add(float64(len(chunks)))
		}
	}
}
