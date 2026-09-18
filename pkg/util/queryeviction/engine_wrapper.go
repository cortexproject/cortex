package queryeviction

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/engine"
	querier_stats "github.com/cortexproject/cortex/pkg/querier/stats"
	"github.com/cortexproject/cortex/pkg/util/requestmeta"
)

// Compile-time check that ResourceEvictingEngine implements engine.QueryEngine.
var _ engine.QueryEngine = (*ResourceEvictingEngine)(nil)

// ResourceEvictingEngine wraps a QueryEngine to register running queries
// with a QueryRegistry, enabling resource-based eviction.
type ResourceEvictingEngine struct {
	inner    engine.QueryEngine
	registry *QueryRegistry
}

// NewResourceEvictingEngine wraps the given engine.
// If registry is nil, the wrapper is a no-op passthrough.
func NewResourceEvictingEngine(inner engine.QueryEngine, registry *QueryRegistry) *ResourceEvictingEngine {
	return &ResourceEvictingEngine{
		inner:    inner,
		registry: registry,
	}
}

func (e *ResourceEvictingEngine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	query, err := e.inner.NewInstantQuery(ctx, q, opts, qs, ts)
	if err != nil {
		return nil, err
	}
	if e.registry == nil {
		return query, nil
	}
	return e.wrapQuery(ctx, query, qs), nil
}

func (e *ResourceEvictingEngine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	query, err := e.inner.NewRangeQuery(ctx, q, opts, qs, start, end, interval)
	if err != nil {
		return nil, err
	}
	if e.registry == nil {
		return query, nil
	}
	return e.wrapQuery(ctx, query, qs), nil
}

func (e *ResourceEvictingEngine) MakeInstantQueryFromPlan(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, root logicalplan.Node, ts time.Time, qs string) (promql.Query, error) {
	query, err := e.inner.MakeInstantQueryFromPlan(ctx, q, opts, root, ts, qs)
	if err != nil {
		return nil, err
	}
	if e.registry == nil {
		return query, nil
	}
	return e.wrapQuery(ctx, query, qs), nil
}

func (e *ResourceEvictingEngine) MakeRangeQueryFromPlan(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, root logicalplan.Node, start time.Time, end time.Time, interval time.Duration, qs string) (promql.Query, error) {
	query, err := e.inner.MakeRangeQueryFromPlan(ctx, q, opts, root, start, end, interval, qs)
	if err != nil {
		return nil, err
	}
	if e.registry == nil {
		return query, nil
	}
	return e.wrapQuery(ctx, query, qs), nil
}

// wrapQuery creates a trackedQuery that registers/deregisters with the registry.
// It creates a cancellable child context so the evictor can cancel individual queries.
func (e *ResourceEvictingEngine) wrapQuery(ctx context.Context, inner promql.Query, queryExpr string) *trackedQuery {
	childCtx, cancel := context.WithCancel(ctx)

	//lint:ignore faillint wrapper around upstream method
	userID, _ := user.ExtractOrgID(ctx)

	return &trackedQuery{
		inner:     inner,
		registry:  e.registry,
		queryExpr: queryExpr,
		userID:    userID,
		requestID: requestmeta.RequestIdFromContext(ctx),
		cancel:    cancel,
		ctx:       childCtx,
	}
}

// trackedQuery wraps a promql.Query to register/deregister with the registry.
type trackedQuery struct {
	inner     promql.Query
	registry  *QueryRegistry
	queryID   uint64
	queryExpr string
	userID    string
	requestID string
	cancel    context.CancelFunc
	ctx       context.Context // cancellable child context
}

// Exec registers the query, executes it with a cancellable context,
// then deregisters on completion. If the query was evicted (child context
// cancelled by evictor but parent context not cancelled), wraps the error
// as ErrQueryEvicted.
func (q *trackedQuery) Exec(ctx context.Context) *promql.Result {
	queryStats := querier_stats.FromContext(q.ctx)
	q.queryID = q.registry.Register(q.cancel, queryStats, q.queryExpr, q.userID, q.requestID)
	defer q.registry.Deregister(q.queryID)

	result := q.inner.Exec(q.ctx)

	// Detect eviction: child context cancelled but parent context is still active.
	if result.Err != nil && q.ctx.Err() != nil && ctx.Err() == nil {
		return &promql.Result{
			Err: promql.ErrStorage{Err: &ErrQueryEvicted{}},
		}
	}

	return result
}

// Statement delegates to the inner query.
func (q *trackedQuery) Statement() parser.Statement {
	return q.inner.Statement()
}

// Stats delegates to the inner query.
func (q *trackedQuery) Stats() *stats.Statistics {
	return q.inner.Stats()
}

// Close delegates to the inner query.
func (q *trackedQuery) Close() {
	q.inner.Close()
}

// Cancel cancels the tracked query's child context.
func (q *trackedQuery) Cancel() {
	q.cancel()
}

// String delegates to the inner query.
func (q *trackedQuery) String() string {
	return q.inner.String()
}
