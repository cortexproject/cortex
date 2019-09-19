package ruler

import (
	"context"
	"sync"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/ingester/client"
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
	sync.Mutex
	pusher  Pusher
	ctx     context.Context
	labels  []labels.Labels
	samples []client.Sample
	userID  string
}

func (a *appendableAppender) Appender() (storage.Appender, error) {
	return a, nil
}

func (a *appendableAppender) Add(l labels.Labels, t int64, v float64) (uint64, error) {
	a.Lock()
	defer a.Unlock()
	a.labels = append(a.labels, l)
	a.samples = append(a.samples, client.Sample{
		TimestampMs: t,
		Value:       v,
	})
	return 0, nil
}

func (a *appendableAppender) AddFast(l labels.Labels, ref uint64, t int64, v float64) error {
	_, err := a.Add(l, t, v)
	return err
}

func (a *appendableAppender) Commit() error {
	a.Lock()
	defer a.Unlock()
	_, err := a.pusher.Push(user.InjectOrgID(context.Background(), a.userID), client.ToWriteRequest(a.labels, a.samples, client.RULE))
	a.labels = nil
	a.samples = nil
	return err
}

func (a *appendableAppender) Rollback() error {
	a.Lock()
	defer a.Unlock()
	a.labels = nil
	a.samples = nil
	return nil
}

// TSDB fulfills the storage.Storage interface for prometheus manager
// it allows for alerts to be restored by the manager
type tsdb struct {
	appender  *appendableAppender
	queryable storage.Queryable
}

// Querier returns a new Querier on the storage.
func (t *tsdb) Querier(ctx context.Context, mint int64, maxt int64) (storage.Querier, error) {
	return t.queryable.Querier(ctx, mint, maxt)
}

// StartTime returns the oldest timestamp stored in the storage.
func (t *tsdb) StartTime() (int64, error) {
	return 0, nil
}

// Appender returns a new appender against the storage.
func (t *tsdb) Appender() (storage.Appender, error) {
	return t.appender, nil
}

// Close closes the storage and all its underlying resources.
func (t *tsdb) Close() error {
	return nil
}
