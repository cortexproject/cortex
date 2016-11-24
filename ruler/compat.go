package ruler

import (
	"github.com/prometheus/common/model"
	"golang.org/x/net/context"
)

// SampleAppender is the interface we wish sample appenders had.
// TODO: Try to get this into Prometheus upstream.
type SampleAppender interface {
	AppendMany(context.Context, []*model.Sample) error
}

// appenderAdapter adapts a distributor.Distributor to prometheus.SampleAppender
type appenderAdapter struct {
	appender SampleAppender
	ctx      context.Context
}

func (a appenderAdapter) Append(sample *model.Sample) error {
	return a.appender.AppendMany(a.ctx, []*model.Sample{sample})
}

func (a appenderAdapter) NeedsThrottling() bool {
	return false
}
