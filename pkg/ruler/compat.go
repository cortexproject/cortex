package ruler

import (
	"context"
	"time"

	"github.com/cortexproject/cortex/pkg/ruler/rules"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/ingester/client"
)

// Pusher is an ingester server that accepts pushes.
type Pusher interface {
	Push(context.Context, *client.WriteRequest) (*client.WriteResponse, error)
}
type appender struct {
	pusher  Pusher
	labels  []labels.Labels
	samples []client.Sample
	userID  string
}

func (a *appender) Add(l labels.Labels, t int64, v float64) (uint64, error) {
	a.labels = append(a.labels, l)
	a.samples = append(a.samples, client.Sample{
		TimestampMs: t,
		Value:       v,
	})
	return 0, nil
}

func (a *appender) AddFast(_ uint64, _ int64, _ float64) error {
	return storage.ErrNotFound
}

func (a *appender) Commit() error {
	// Since a.pusher is distributor, client.ReuseSlice will be called in a.pusher.Push.
	// We shouldn't call client.ReuseSlice here.
	_, err := a.pusher.Push(user.InjectOrgID(context.Background(), a.userID), client.ToWriteRequest(a.labels, a.samples, nil, client.RULE))
	a.labels = nil
	a.samples = nil
	return err
}

func (a *appender) Rollback() error {
	a.labels = nil
	a.samples = nil
	return nil
}

// TSDB fulfills the storage.Storage interface for prometheus manager
// it allows for alerts to be restored by the manager
type tsdb struct {
	pusher Pusher
	userID string
}

// Appender returns a storage.Appender
func (t *tsdb) Appender() storage.Appender {
	return &appender{
		pusher: t.pusher,
		userID: t.userID,
	}
}

// StartTime returns the oldest timestamp stored in the storage.
func (t *tsdb) StartTime() (int64, error) {
	return 0, nil
}

// Close closes the storage and all its underlying resources.
func (t *tsdb) Close() error {
	return nil
}

// PromDelayedQueryFunc returns a DelayedQueryFunc bound to a promql engine.
// and passing an altered timestamp.
func PromDelayedQueryFunc(engine *promql.Engine) DelayedQueryFunc {
	return func(q storage.Queryable, delay time.Duration) rules.QueryFunc {
		orig := rules.EngineQueryFunc(engine, q)
		return func(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {
			return orig(ctx, qs, t.Add(-delay))
		}
	}
}

// DelayedQueryFunc consumes a queryable and a delay, returning a Queryfunc which
// takes this delay into account when executing against the queryable.
type DelayedQueryFunc = func(storage.Queryable, time.Duration) rules.QueryFunc
