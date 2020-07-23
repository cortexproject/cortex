package ruler

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/ingester/client"
)

// Pusher is an ingester server that accepts pushes.
type Pusher interface {
	Push(context.Context, *client.WriteRequest) (*client.WriteResponse, error)
}
type PusherAppender struct {
	pusher  Pusher
	labels  []labels.Labels
	samples []client.Sample
	userID  string
}

func (a *PusherAppender) Add(l labels.Labels, t int64, v float64) (uint64, error) {
	a.labels = append(a.labels, l)
	a.samples = append(a.samples, client.Sample{
		TimestampMs: t,
		Value:       v,
	})
	return 0, nil
}

func (a *PusherAppender) AddFast(_ uint64, _ int64, _ float64) error {
	return storage.ErrNotFound
}

func (a *PusherAppender) Commit() error {
	// Since a.pusher is distributor, client.ReuseSlice will be called in a.pusher.Push.
	// We shouldn't call client.ReuseSlice here.
	_, err := a.pusher.Push(user.InjectOrgID(context.Background(), a.userID), client.ToWriteRequest(a.labels, a.samples, nil, client.RULE))
	a.labels = nil
	a.samples = nil
	return err
}

func (a *PusherAppender) Rollback() error {
	a.labels = nil
	a.samples = nil
	return nil
}

// PusherAppendable fulfills the storage.Appendable interface for prometheus manager
type PusherAppendable struct {
	pusher Pusher
	userID string
}

// PusherAppender returns a storage.PusherAppender
func (t *PusherAppendable) Appender() storage.Appender {
	return &PusherAppender{
		pusher: t.pusher,
		userID: t.userID,
	}
}

// PromDelayedQueryFunc returns a DelayedQueryFunc bound to a promql engine.
func PromDelayedQueryFunc(engine *promql.Engine, q storage.Queryable) DelayedQueryFunc {
	return func(delay time.Duration) rules.QueryFunc {
		orig := rules.EngineQueryFunc(engine, q)
		return func(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {
			return orig(ctx, qs, t.Add(-delay))
		}
	}
}

// DelayedQueryFunc consumes a queryable and a delay, returning a Queryfunc which
// takes this delay into account when executing against the queryable.
type DelayedQueryFunc = func(time.Duration) rules.QueryFunc

// function adapter for StorageLoader ifc
type StorageLoaderFunc func(userID string) (storage.Appendable, storage.Queryable)

func (fn StorageLoaderFunc) Load(userID string) (storage.Appendable, storage.Queryable) {
	return fn(userID)
}

// PushLoader creates a StorageLoader from a Pusher and Queryable
func PushLoader(p Pusher, q storage.Queryable) StorageLoaderFunc {
	return StorageLoaderFunc(func(userID string) (storage.Appendable, storage.Queryable) {
		return &PusherAppendable{pusher: p, userID: userID}, q
	})
}
