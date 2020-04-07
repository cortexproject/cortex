package ruler

import (
	"context"
	"time"

	"github.com/pkg/errors"
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

func (a *appender) AddFast(l labels.Labels, ref uint64, t int64, v float64) error {
	_, err := a.Add(l, t, v)
	return err
}

func (a *appender) Commit() error {
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
	pusher    Pusher
	userID    string
	queryable storage.Queryable
}

// Appender returns a storage.Appender
func (t *tsdb) Appender() (storage.Appender, error) {
	return &appender{
		pusher: t.pusher,
		userID: t.userID,
	}, nil
}

// Querier returns a new Querier on the storage.
func (t *tsdb) Querier(ctx context.Context, mint int64, maxt int64) (storage.Querier, error) {
	return t.queryable.Querier(ctx, mint, maxt)
}

// StartTime returns the oldest timestamp stored in the storage.
func (t *tsdb) StartTime() (int64, error) {
	return 0, nil
}

// Close closes the storage and all its underlying resources.
func (t *tsdb) Close() error {
	return nil
}

// engineQueryFunc returns a new query function that executes instant queries against
// the given engine, after subtracting the provided delay from the instant query timestamp.
// It converts scalar into vector results.
// Based on https://github.com/prometheus/prometheus/blob/ecda6013edf58bf645c6661b9f78ccce03b1f315/rules/manager.go#L162-L187
func engineQueryFunc(engine *promql.Engine, q storage.Queryable, delay time.Duration) rules.QueryFunc {
	return func(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {
		t = t.Add(-delay)
		q, err := engine.NewInstantQuery(q, qs, t)
		if err != nil {
			return nil, err
		}
		res := q.Exec(ctx)
		if res.Err != nil {
			return nil, res.Err
		}
		switch v := res.Value.(type) {
		case promql.Vector:
			return v, nil
		case promql.Scalar:
			return promql.Vector{promql.Sample{
				Point:  promql.Point(v),
				Metric: labels.Labels{},
			}}, nil
		default:
			return nil, errors.New("rule result is not a vector or scalar")
		}
	}
}
