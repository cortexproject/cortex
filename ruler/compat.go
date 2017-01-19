package ruler

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"
	"golang.org/x/net/context"

	"github.com/weaveworks/cortex"
	"github.com/weaveworks/cortex/util"
)

// Pusher is an ingester server that accepts pushes.
type Pusher interface {
	Push(context.Context, *remote.WriteRequest) (*cortex.WriteResponse, error)
}

// appenderAdapter adapts a distributor.Distributor to prometheus.SampleAppender
type appenderAdapter struct {
	pusher Pusher
	ctx    context.Context
}

func (a appenderAdapter) Append(sample *model.Sample) error {
	_, err := a.pusher.Push(a.ctx, util.ToWriteRequest([]*model.Sample{sample}))
	return err
}

func (a appenderAdapter) NeedsThrottling() bool {
	return false
}
