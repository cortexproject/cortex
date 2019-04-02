package migrate

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/user"
	old_ctx "golang.org/x/net/context"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var (
	receivedChunks = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "writer_received_chunks_total",
		Help:      "The total number of chunks received by this writer",
	}, []string{"reader_id"})
	writeErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "writer_store_errors_total",
		Help:      "The total number of errors caused when storing chunks",
	}, []string{"reader_id"})
)

type writeRequest struct {
	chunks   []chunk.Chunk
	readerID string
	orgID    string
}

func (w writeRequest) encodeChunks() error {
	for i := range w.chunks {
		_, err := w.chunks[i].Encoded()
		if err != nil {
			return err
		}
	}
}

// WriterConfig configures are Writer objects
type WriterConfig struct {
	storageEnabled bool
	bufferSize     int
	numWorkers     int
}

// RegisterFlags adds the flags required to configure this flag set.
func (cfg *WriterConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.storageEnabled, "writer.enable-storage", true, "enable persisting metrics to a storage backend") // Disable for testing
	f.IntVar(&cfg.numWorkers, "writer.num-workers", 50, "number of worker jobs handling backend writes")
	f.IntVar(&cfg.bufferSize, "writer.buffer-size", 1000, "Number of chunk batches to buffer for writes to storage backend")
}

// Writer receives chunks and stores them in a storage backend
type Writer struct {
	cfg WriterConfig

	writeChan      chan writeRequest
	chunkStore     chunk.Store
	storageEnabled bool

	writeQueuesDone sync.WaitGroup
}

// NewWriter returns a Writer object
func NewWriter(cfg WriterConfig, store chunk.Store) *Writer {
	writeChan := make(chan writeRequest, cfg.bufferSize)
	writer := Writer{
		cfg:             cfg,
		chunkStore:      store,
		storageEnabled:  cfg.storageEnabled,
		writeChan:       writeChan,
		writeQueuesDone: sync.WaitGroup{},
	}
	return &writer
}

// Start initializes the writer workers
func (w *Writer) Start(ctx context.Context) {
	for i := 0; i < w.cfg.numWorkers; i++ {
		go w.writeLoop(ctx, i)
		w.writeQueuesDone.Add(1)
	}
}

// Shutdown gracefully closes the writer
func (w *Writer) Shutdown() {
	level.Info(util.Logger).Log("msg", "shutting down writer")
	w.writeQueuesDone.Wait()
	close(w.writeChan)
	return
}

func (w *Writer) writeLoop(ctx context.Context, workerID int) {
	defer func() {
		level.Info(util.Logger).Log("msg", "closing writer goroutine", "worker_id", workerID)
		w.writeQueuesDone.Done()
	}()

	backoff := util.NewBackoff(context.Background(), util.BackoffConfig{
		MinBackoff: time.Second * 5,
		MaxBackoff: time.Second * 360,
	})
	level.Debug(util.Logger).Log("msg", "starting writer goroutine", "worker_id", workerID)
	for req := range w.writeChan {
		userCtx := user.InjectOrgID(ctx, req.orgID)
		if w.storageEnabled {
			for backoff.Ongoing() {
				level.Debug(util.Logger).Log("msg", "writing chunks", "num_chunks", len(req.chunks), "worker_id", workerID)
				select {
				case <-ctx.Done():
					level.Warn(util.Logger).Log("msg", "write loop closing, context canceled", "worker_id", workerID)
					return
				default:
					err := req.encodeChunks()
					if err != nil {
						writeErrors.WithLabelValues(req.readerID).Inc()
						level.Error(util.Logger).Log("msg", "failed encode chunks for storage", "err", err, "retry_attempy", backoff.NumRetries(), "worker_id", workerID)
						backoff.Wait()
						continue
					}

					err = w.chunkStore.Put(userCtx, req.chunks)
					if err != nil {
						writeErrors.WithLabelValues(req.readerID).Inc()
						level.Error(util.Logger).Log("msg", "failed to store chunks", "err", err, "retry_attempy", backoff.NumRetries(), "worker_id", workerID)
						backoff.Wait()
						continue
					}
					backoff.Reset()
				}
				break
			}
		}
	}
}

// TransferChunks reads chunks from a stream and stores them in the configured storage backend
func (w *Writer) TransferChunks(stream client.Ingester_TransferChunksServer) error {
	fromReaderID := ""
	for {
		wireSeries, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// We can't send "extra" fields with a streaming call, so we repeat
		// wireSeries.fromReaderID and assume it is the same every time
		// round this loop.
		if fromReaderID == "" {
			fromReaderID = wireSeries.FromIngesterId
			level.Info(util.Logger).Log("msg", "processing transfer chunks request from reader", "readerID", fromReaderID)
		}
		metric := client.FromLabelPairs(wireSeries.Labels)
		chunks, err := chunkcompat.FromChunks(wireSeries.UserId, metric, wireSeries.Chunks)
		if err != nil {
			level.Error(util.Logger).Log("msg", "unable to decode chunks from stream", "err", err, "readerID", fromReaderID)
			return err
		}

		level.Debug(util.Logger).Log("msg", "storing chunks", "num_chunks", len(chunks))
		w.writeChan <- writeRequest{chunks, fromReaderID, wireSeries.UserId}

		receivedChunks.WithLabelValues(fromReaderID).Add(float64(len(chunks)))
	}
	return nil
}

// Check returns that the writer is ready to receive chunks
func (w *Writer) Check(old_ctx.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// Push is not implemented
func (w *Writer) Push(old_ctx.Context, *client.WriteRequest) (*client.WriteResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// Query is not implemented
func (w *Writer) Query(old_ctx.Context, *client.QueryRequest) (*client.QueryResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// QueryStream is not implemented
func (w *Writer) QueryStream(*client.QueryRequest, client.Ingester_QueryStreamServer) error {
	return fmt.Errorf("not implemented")
}

// LabelNames return all the label names.
func (w *Writer) LabelNames(ctx old_ctx.Context, req *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// LabelValues is not implemented
func (w *Writer) LabelValues(old_ctx.Context, *client.LabelValuesRequest) (*client.LabelValuesResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// UserStats is not implemented
func (w *Writer) UserStats(old_ctx.Context, *client.UserStatsRequest) (*client.UserStatsResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// AllUserStats is not implemented
func (w *Writer) AllUserStats(old_ctx.Context, *client.UserStatsRequest) (*client.UsersStatsResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// MetricsForLabelMatchers is not implemented
func (w *Writer) MetricsForLabelMatchers(old_ctx.Context, *client.MetricsForLabelMatchersRequest) (*client.MetricsForLabelMatchersResponse, error) {
	return nil, fmt.Errorf("not implemented")
}
