package migrate

import (
	"context"
	"flag"
	"fmt"
	"io"

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
		Name:      "writer_received_chunks",
		Help:      "The total number of chunks received by this writer",
	}, []string{"reader_id"})
)

// WriterConfig configures are Writer objects
type WriterConfig struct {
	storageEnabled bool
}

// RegisterFlags adds the flags required to configure this flag set.
func (cfg *WriterConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.storageEnabled, "writer.enable-storage", true, "enable persisting metrics to a storage backend") // Disable for testing
}

// Writer receives chunks and stores them in a storage backend
type Writer struct {
	chunkStore     chunk.Store
	storageEnabled bool
}

// NewWriter returns a Writer object
func NewWriter(cfg WriterConfig, store chunk.Store) Writer {
	return Writer{
		chunkStore:     store,
		storageEnabled: cfg.storageEnabled,
	}
}

// Store passes a batch of metrics to a storage backend
func (w Writer) Store(ctx context.Context, chunks []chunk.Chunk) error {
	level.Debug(util.Logger).Log("msg", "storing chunks", "num_chunks", len(chunks))
	if w.storageEnabled {
		return w.chunkStore.Put(ctx, chunks)
	}
	return nil
}

// TransferChunks reads chunks from a stream and stores them in the configured storage backend
func (w Writer) TransferChunks(stream client.Ingester_TransferChunksServer) error {
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
		userCtx := user.InjectOrgID(stream.Context(), wireSeries.UserId)
		err = w.Store(userCtx, chunks)
		if err != nil {
			level.Error(util.Logger).Log("msg", "error storing chunks", "err", err, "readerID", fromReaderID)
			return err
		}
		receivedChunks.WithLabelValues(fromReaderID).Add(float64(len(chunks)))
	}
	return nil
}

// Check returns that the writer is ready to receive chunks
func (w Writer) Check(old_ctx.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// Push is not implemented
func (w Writer) Push(old_ctx.Context, *client.WriteRequest) (*client.WriteResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// Query is not implemented
func (w Writer) Query(old_ctx.Context, *client.QueryRequest) (*client.QueryResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// QueryStream is not implemented
func (w Writer) QueryStream(*client.QueryRequest, client.Ingester_QueryStreamServer) error {
	return fmt.Errorf("not implemented")
}

// LabelValues is not implemented
func (w Writer) LabelValues(old_ctx.Context, *client.LabelValuesRequest) (*client.LabelValuesResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// UserStats is not implemented
func (w Writer) UserStats(old_ctx.Context, *client.UserStatsRequest) (*client.UserStatsResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// AllUserStats is not implemented
func (w Writer) AllUserStats(old_ctx.Context, *client.UserStatsRequest) (*client.UsersStatsResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// MetricsForLabelMatchers is not implemented
func (w Writer) MetricsForLabelMatchers(old_ctx.Context, *client.MetricsForLabelMatchersRequest) (*client.MetricsForLabelMatchersResponse, error) {
	return nil, fmt.Errorf("not implemented")
}
