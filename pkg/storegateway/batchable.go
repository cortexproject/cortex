package storegateway

import (
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

type flushableServer interface {
	storepb.Store_SeriesServer

	Flush() error
}

// copied from thanos pkg/store/flushable.go
func newFlushableServer(
	upstream storepb.Store_SeriesServer,
) flushableServer {
	return &passthroughServer{Store_SeriesServer: upstream}
}

type passthroughServer struct {
	storepb.Store_SeriesServer
}

func (p *passthroughServer) Flush() error {
	// If the underlying server is also flushable, flush it
	if f, ok := p.Store_SeriesServer.(flushableServer); ok {
		return f.Flush()
	}
	return nil
}

// copied from thanos pkg/store/batchable.go
func newBatchableServer(
	upstream storepb.Store_SeriesServer,
	batchSize int,
) storepb.Store_SeriesServer {
	switch batchSize {
	case 0:
		return &passthroughServer{Store_SeriesServer: upstream}
	case 1:
		return &passthroughServer{Store_SeriesServer: upstream}
	default:
		return &batchableServer{
			Store_SeriesServer: upstream,
			batchSize:          batchSize,
			series:             make([]*storepb.Series, 0, batchSize),
		}
	}
}

// batchableServer is a flushableServer that allows sending a batch of Series per message.
type batchableServer struct {
	storepb.Store_SeriesServer
	batchSize int
	series    []*storepb.Series
}

func (b *batchableServer) Flush() error {
	if len(b.series) != 0 {
		if err := b.Store_SeriesServer.Send(storepb.NewBatchResponse(b.series)); err != nil {
			return err
		}
		b.series = make([]*storepb.Series, 0, b.batchSize)
	}

	return nil
}

func (b *batchableServer) Send(response *storepb.SeriesResponse) error {
	series := response.GetSeries()
	if series == nil {
		if len(b.series) > 0 {
			if err := b.Store_SeriesServer.Send(storepb.NewBatchResponse(b.series)); err != nil {
				return err
			}
			b.series = make([]*storepb.Series, 0, b.batchSize)
		}
		return b.Store_SeriesServer.Send(response)
	}

	b.series = append(b.series, series)

	if len(b.series) >= b.batchSize {
		if err := b.Store_SeriesServer.Send(storepb.NewBatchResponse(b.series)); err != nil {
			return err
		}
		b.series = make([]*storepb.Series, 0, b.batchSize)
	}

	return nil
}
