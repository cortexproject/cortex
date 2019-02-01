package migrate

import (
	"context"
	"fmt"
	"io"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/go-kit/kit/log/level"
	"github.com/weaveworks/common/user"
)

type chunkStreamClient interface {
	StreamChunks(context.Context, []chunk.Chunk) error
	Close() error
}

type nullStreamClient struct{}

func (n *nullStreamClient) StreamChunks(ctx context.Context, chunks []chunk.Chunk) error {
	level.Info(util.Logger).Log("msg", "processed metrics", "num", len(chunks))
	for _, c := range chunks {
		level.Debug(util.Logger).Log("chunk", c.Fingerprint.String(), "user", c.UserID)
	}
	return nil
}

func (n *nullStreamClient) Close() error {
	return nil
}

type streamClient struct {
	id     string
	cli    client.HealthAndIngesterClient
	stream client.Ingester_TransferChunksClient
	failed bool
}

func (s *streamClient) StreamChunks(ctx context.Context, chunks []chunk.Chunk) error {
	var err error
	if s.failed {
		level.Info(util.Logger).Log("msg", "attempting to re-establish connection to writer")
		s.stream, err = s.cli.TransferChunks(ctx)
		if err != nil {
			streamErrors.WithLabelValues(s.id).Inc()
			return fmt.Errorf("stream unable to be re-initialized, %v", err)
		}
		s.failed = false
	}

	wireChunks, err := chunkcompat.ToChunks(chunks)
	if err != nil {
		return fmt.Errorf("unable to serialize chunks, %v", err)
	}
	labels := client.ToLabelPairs(chunks[0].Metric)

	err = s.stream.Send(
		&client.TimeSeriesChunk{
			FromIngesterId: s.id,
			UserId:         chunks[0].UserID,
			Labels:         labels,
			Chunks:         wireChunks,
		},
	)
	if err != nil {
		streamErrors.WithLabelValues(s.id).Inc()
		s.failed = true
		return fmt.Errorf("stream failed, %v", err)
	}

	return nil
}

func (s *streamClient) Close() error {
	defer s.cli.(io.Closer).Close()
	level.Info(util.Logger).Log("msg", "closing stream")
	_, err := s.stream.CloseAndRecv()
	if err.Error() == "EOF" {
		return nil
	}
	return err
}

func newStreamer(ctx context.Context, id string, addr string, cfg client.Config) (chunkStreamClient, error) {
	// return a null streamer if no address is set, useful for dry run reads
	if addr == "" {
		level.Info(util.Logger).Log("msg", "no address set, dry run mode enabled")
		return &nullStreamClient{}, nil
	}

	cli, err := client.MakeIngesterClient(addr, cfg)
	if err != nil {
		return nil, err
	}

	ctx = user.InjectOrgID(ctx, "1")
	stream, err := cli.TransferChunks(ctx)
	if err != nil {
		return nil, err
	}

	return &streamClient{
		cli:    cli,
		stream: stream,
	}, nil
}
