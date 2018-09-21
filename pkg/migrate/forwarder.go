package migrate

import (
	"context"
	"flag"
	"io"

	"github.com/sirupsen/logrus"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/util/chunkcompat"
)

type ForwarderConfig struct {
	Addr         string
	ID           string
	ClientConfig client.Config
}

// RegisterFlags adds the flags required to configure this flag set.
func (cfg *ForwarderConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.ClientConfig.RegisterFlags(f)
	f.StringVar(&cfg.Addr, "forwarder.addr", "", "address of the chunk transfer endpoint")
}

func NewForwarder(cfg ForwarderConfig, ID string) ForwarderClient {
	return ForwarderClient{
		Config:                cfg,
		ID:                    ID,
		ingesterClientFactory: client.MakeIngesterClient,
	}
}

type ForwarderClient struct {
	Addr                  string
	Config                ForwarderConfig
	ID                    string
	ingesterClientFactory func(addr string, cfg client.Config) (client.IngesterClient, error)
}

// Forward reads batched chunks with the same metric from a channel and wires them
// to a Migrate Writer using the TransferChunks service in the ingester protobuf package
func (f ForwarderClient) Forward(ctx context.Context, chunkChan chan []chunk.Chunk) error {
	c, err := f.ingesterClientFactory(f.Config.Addr, f.Config.ClientConfig)
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
				FromIngesterId: f.ID,
				UserId:         chunks[0].UserID,
				Labels:         labels,
				Chunks:         wireChunks,
			},
		)
	}
	_, err = stream.CloseAndRecv()
	return err
}
