package ruler

import (
	"context"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/weaveworks/cortex/pkg/ingester/client"
)

// Pusher is an ingester server that accepts pushes.
type Pusher interface {
	Push(context.Context, *client.WriteRequest) (*client.WriteResponse, error)
}

// appendableAppender adapts a distributor.Distributor to both a ruler.Appendable
// and a storage.Appender.
//
// Distributors need a context and storage.Appender doesn't allow for
// one. See
// https://github.com/prometheus/prometheus/pull/2000#discussion_r79108319 for
// reasons why.
type appendableAppender struct {
	pusher  Pusher
	ctx     context.Context
	samples []model.Sample
}

func (a *appendableAppender) Appender() (storage.Appender, error) {
	return a, nil
}

func (a *appendableAppender) Add(l labels.Labels, t int64, v float64) (uint64, error) {
	m := make(model.Metric, len(l))
	for _, lbl := range l {
		m[model.LabelName(lbl.Name)] = model.LabelValue(lbl.Value)
	}
	a.samples = append(a.samples, model.Sample{
		Metric:    m,
		Timestamp: model.Time(t),
		Value:     model.SampleValue(v),
	})
	return 0, nil
}

func (a *appendableAppender) AddFast(l labels.Labels, ref uint64, t int64, v float64) error {
	_, err := a.Add(l, t, v)
	return err
}

func (a *appendableAppender) Commit() error {
	_, err := a.pusher.Push(a.ctx, client.ToWriteRequest(a.samples, client.RULE))
	a.samples = nil
	return err
}

func (a *appendableAppender) Rollback() error {
	a.samples = nil
	return nil
}
