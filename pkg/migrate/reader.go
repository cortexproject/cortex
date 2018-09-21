package migrate

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/sirupsen/logrus"
	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/testutils"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/util/chunkcompat"
)

type ReaderConfig struct {
	Addr           string
	ClientConfig   client.Config
	PlannerConfig  PlanConfig
	ReaderIDPrefix string
}

// RegisterFlags adds the flags required to configure this flag set.
func (cfg *ReaderConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.PlannerConfig.RegisterFlags(f)
	cfg.ClientConfig.RegisterFlags(f)
	f.StringVar(&cfg.Addr, "reader.forward_addr", "", "address of the chunk transfer endpoint")
	f.StringVar(&cfg.ReaderIDPrefix, "reader.prefix", "reader_", "prefix used to identify reader when forwarding data to writer")
}

// Reader collects and forwards chunks according to it's planner
type Reader struct {
	Config                ReaderConfig
	ID                    string // ID is the configured as the reading prefix and the shards assigned to the reader
	ingesterClientFactory func(addr string, cfg client.Config) (client.IngesterClient, error)

	storage chunk.StorageClient
	planner Planner
}

func NewReader(cfg ReaderConfig, storage chunk.StorageClient) (*Reader, error) {
	planner, err := NewPlanner(cfg.PlannerConfig)
	if err != nil {
		return nil, err
	}
	id := cfg.ReaderIDPrefix + fmt.Sprintf("%d_%d", planner.firstShard, planner.lastShard)
	return &Reader{
		Config:                cfg,
		ID:                    id,
		planner:               planner,
		storage:               storage,
		ingesterClientFactory: client.MakeIngesterClient,
	}, nil
}

func (r *Reader) TransferData(ctx context.Context) error {
	batch := r.storage.NewStreamBatch()
	r.planner.Plan(batch)

	out := make(chan []chunk.Chunk)

	go func() {
		_, chunks, _ := testutils.CreateChunks(0, 1)
		out <- chunks
		// err := r.storage.StreamChunks(ctx, batch, out)
		// if err != nil {
		// 	logrus.Infof("StreamChunks failed, %v", err)
		// }
		close(out)
	}()

	return r.Forward(ctx, out)
}

// Forward reads batched chunks with the same metric from a channel and wires them
// to a Migrate Writer using the TransferChunks service in the ingester protobuf package
func (r Reader) Forward(ctx context.Context, chunkChan chan []chunk.Chunk) error {
	c, err := r.ingesterClientFactory(r.Config.Addr, r.Config.ClientConfig)
	if err != nil {
		return err
	}
	defer c.(io.Closer).Close()

	ctx = user.InjectOrgID(ctx, "1")
	stream, err := c.TransferChunks(ctx)
	if err != nil {
		return err
	}
	for chunks := range chunkChan {
		if len(chunks) == 0 {
			continue
		}
		logrus.Infof("transfering %v chunks with userID %v and fingerprint %v", len(chunks), chunks[0].UserID, chunks[0].Fingerprint)
		wireChunks, err := chunkcompat.ToChunks(chunks)
		if err != nil {
			return err
		}
		labels := client.ToLabelPairs(chunks[0].Metric)
		err = stream.Send(
			&client.TimeSeriesChunk{
				FromIngesterId: r.ID,
				UserId:         chunks[0].UserID,
				Labels:         labels,
				Chunks:         wireChunks,
			},
		)
	}
	_, err = stream.CloseAndRecv()
	return err
}
