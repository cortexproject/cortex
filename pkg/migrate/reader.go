package migrate

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/migrate/mapper"
	"github.com/cortexproject/cortex/pkg/migrate/planner"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/user"
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
	Addr           string
	BufferSize     int
	ClientConfig   client.Config
	PlannerConfig  planner.Config
	MapperConfig   mapper.Config
	ReaderIDPrefix string
	StorageClient  string
}

// RegisterFlags adds the flags required to configure this flag set.
func (cfg *ReaderConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.PlannerConfig.RegisterFlags(f)
	cfg.ClientConfig.RegisterFlags(f)
	cfg.MapperConfig.RegisterFlags(f)
	flag.StringVar(&cfg.StorageClient, "chunk.storage-client", "gcp", "Which storage client to use (gcp, cassandra).")
	f.IntVar(&cfg.BufferSize, "reader.buffer-size", 10000, "number of chunk batches to buffer before forwarding")
	f.StringVar(&cfg.Addr, "reader.forward-addr", "", "address of the chunk transfer endpoint")
	f.StringVar(&cfg.ReaderIDPrefix, "reader.prefix", "reader_", "prefix used to identify reader when forwarding data to writer")
}

// Reader collects and forwards chunks according to it's planner
type Reader struct {
	cfg                   ReaderConfig
	id                    string // ID is the configured as the reading prefix and the shards assigned to the reader
	ingesterClientFactory func(addr string, cfg client.Config) (client.HealthAndIngesterClient, error)

	storage      chunk.ObjectClient
	planner      *planner.Planner
	chunkMapper  *mapper.Mapper
	chunkChannel chan []chunk.Chunk
}

// NewReader returns a Reader struct
func NewReader(cfg ReaderConfig, storage chunk.ObjectClient) (*Reader, error) {
	planner, err := planner.New(cfg.PlannerConfig)
	if err != nil {
		return nil, err
	}
	id := cfg.ReaderIDPrefix + fmt.Sprintf("%d_%d", cfg.PlannerConfig.FirstShard, cfg.PlannerConfig.LastShard)
	out := make(chan []chunk.Chunk, cfg.BufferSize)
	chunkMapper, err := mapper.New(cfg.MapperConfig)
	if err != nil {
		return nil, err
	}

	return &Reader{
		cfg:                   cfg,
		id:                    id,
		planner:               planner,
		chunkMapper:           chunkMapper,
		storage:               storage,
		ingesterClientFactory: client.MakeIngesterClient,
		chunkChannel:          out,
	}, nil
}

// TransferData initializes a batch stream from a storage client and
// forwards metrics to writer endpoint
func (r *Reader) TransferData(ctx context.Context) error {
	batch := r.storage.NewStreamer()
	r.planner.Plan(batch)
	readCtx, cancel := context.WithCancel(ctx)

	go func() {
		size, err := batch.Size(ctx)
		if err != nil {
			level.Error(util.Logger).Log("msg", "error estimating size of batch", "err", err)
		}
		totalChunks.WithLabelValues(r.id).Add(float64(size))
		err = batch.Stream(readCtx, r.chunkChannel)
		if err != nil {
			level.Error(util.Logger).Log("msg", "error streaming chunks", "err", err)
		}
		close(r.chunkChannel)
	}()

	err := r.Forward(readCtx)
	cancel()        // If the forwarding metrics errors out the reader will be canceled
	if err != nil { // Loop to ensure streamchunks fails gracefully and returns a log of current progress
		level.Error(util.Logger).Log("msg", "error forwarding metrics, attempting graceful shutdown", "err", err)
		timer := time.NewTimer(time.Second * 60)
		for {
			select {
			case _, ok := <-r.chunkChannel:
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

	return nil
}

// Forward reads batched chunks with the same metric from a channel and wires them
// to a Migrate Writer using the TransferChunks service in the ingester protobuf package
func (r Reader) Forward(ctx context.Context) error {
	var (
		dryrun bool
		cli    client.HealthAndIngesterClient
		stream client.Ingester_TransferChunksClient
		err    error
	)
	if r.cfg.Addr == "" {
		level.Info(util.Logger).Log("msg", "no address set, dry run mode enabled")
		dryrun = true
	} else {
		cli, err := r.ingesterClientFactory(r.cfg.Addr, r.cfg.ClientConfig)
		if err != nil {
			return err
		}
		defer cli.(io.Closer).Close()

		ctx = user.InjectOrgID(ctx, "1")
		stream, err = cli.TransferChunks(ctx)
		if err != nil {
			return err
		}
	}

	backoff := util.NewBackoff(ctx, util.BackoffConfig{
		MinBackoff: time.Second * 5,
		MaxBackoff: time.Second * 360,
	})
	for chunks := range r.chunkChannel {
		level.Debug(util.Logger).Log("msg", "sending chunk batch", "num_chunks", len(chunks))
		if len(chunks) == 0 {
			continue
		}
		if r.chunkMapper != nil {
			chunks, err = r.chunkMapper.MapChunks(chunks)
			if err != nil {
				return fmt.Errorf("unable to map chunks, %v", err)
			}
		}
		wireChunks, err := chunkcompat.ToChunks(chunks)
		if err != nil {
			return fmt.Errorf("unable to serialize chunks, %v", err)
		}
		labels := client.ToLabelPairs(chunks[0].Metric)
		if dryrun {
			level.Info(util.Logger).Log("msg", "processed metrics", "num", len(chunks))
			for _, c := range chunks {
				level.Debug(util.Logger).Log("chunk", c.Fingerprint.String(), "user", c.UserID)
			}
			continue
		}
		for backoff.Ongoing() {
			err = stream.Send(
				&client.TimeSeriesChunk{
					FromIngesterId: r.id,
					UserId:         chunks[0].UserID,
					Labels:         labels,
					Chunks:         wireChunks,
				},
			)
			if err != nil {
				level.Error(util.Logger).Log("msg", "streaming error", "err", err)
				for backoff.Ongoing() {
					streamErrors.WithLabelValues(r.id).Inc()
					level.Info(util.Logger).Log("msg", "attempting to re-establish connection to writer", "try", backoff.NumRetries())
					stream, err = cli.TransferChunks(ctx)
					if err != nil {
						level.Error(util.Logger).Log("msg", "error initializing client", "err", err)
						backoff.Wait()
						continue
					}
					break
				}
				backoff.Reset()
				continue
			}
			break
		}

		if err != nil {
			level.Error(util.Logger).Log("msg", "streaming error, unable to recover", "err", err)
			return err
		}

		sentChunks.WithLabelValues(r.id).Add(float64(len(chunks)))
	}

	if !dryrun {
		_, err = stream.CloseAndRecv()
		if err.Error() == "EOF" {
			return nil
		}
		return err
	}

	return nil
}
