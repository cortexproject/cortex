package queryeviction

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/promql-engine/logicalplan"

	"github.com/cortexproject/cortex/pkg/engine"
	querier_stats "github.com/cortexproject/cortex/pkg/querier/stats"
)

// Compile-time check that mockEngine implements engine.QueryEngine.
var _ engine.QueryEngine = (*mockEngine)(nil)

// mockEngine is a minimal implementation of engine.QueryEngine for testing.
type mockEngine struct {
	query promql.Query // the query to return from all methods
	err   error        // optional error to return
}

func (m *mockEngine) NewInstantQuery(_ context.Context, _ storage.Queryable, _ promql.QueryOpts, _ string, _ time.Time) (promql.Query, error) {
	return m.query, m.err
}

func (m *mockEngine) NewRangeQuery(_ context.Context, _ storage.Queryable, _ promql.QueryOpts, _ string, _, _ time.Time, _ time.Duration) (promql.Query, error) {
	return m.query, m.err
}

func (m *mockEngine) MakeInstantQueryFromPlan(_ context.Context, _ storage.Queryable, _ promql.QueryOpts, _ logicalplan.Node, _ time.Time, _ string) (promql.Query, error) {
	return m.query, m.err
}

func (m *mockEngine) MakeRangeQueryFromPlan(_ context.Context, _ storage.Queryable, _ promql.QueryOpts, _ logicalplan.Node, _, _ time.Time, _ time.Duration, _ string) (promql.Query, error) {
	return m.query, m.err
}

// mockQuery is a minimal implementation of promql.Query for testing.
type mockQuery struct {
	execResult *promql.Result
	execFunc   func(ctx context.Context) *promql.Result // optional custom exec
	closed     bool
}

func (q *mockQuery) Exec(ctx context.Context) *promql.Result {
	if q.execFunc != nil {
		return q.execFunc(ctx)
	}
	return q.execResult
}

func (q *mockQuery) Close() {
	q.closed = true
}

func (q *mockQuery) Statement() parser.Statement {
	return nil
}

func (q *mockQuery) Stats() *stats.Statistics {
	return nil
}

func (q *mockQuery) Cancel() {}

func (q *mockQuery) String() string {
	return "mock_query"
}

// ctxWithStats returns a context that has QueryStats initialized.
func ctxWithStats(ctx context.Context) context.Context {
	_, ctx = querier_stats.ContextWithEmptyStats(ctx)
	return ctx
}

func TestEngineWrapper_RegisterAndDeregisterDuringExec(t *testing.T) {
	registry := NewQueryRegistry(testMetricFunc)
	var registeredLen int

	mq := &mockQuery{
		execFunc: func(ctx context.Context) *promql.Result {
			// During exec, the query should be registered.
			registeredLen = registry.Len()
			return &promql.Result{}
		},
	}

	inner := &mockEngine{query: mq}
	wrapper := NewResourceEvictingEngine(inner, registry)

	ctx := ctxWithStats(context.Background())
	query, err := wrapper.NewInstantQuery(ctx, nil, nil, "up", time.Now())
	require.NoError(t, err)

	// Before exec, registry should be empty.
	assert.Equal(t, 0, registry.Len())

	_ = query.Exec(ctx)

	// During exec, the query was registered.
	assert.Equal(t, 1, registeredLen, "query should be registered during Exec")

	// After exec, the query should be deregistered.
	assert.Equal(t, 0, registry.Len(), "query should be deregistered after Exec")
}

func TestEngineWrapper_EvictedQueryReturnsErrQueryEvicted(t *testing.T) {
	registry := NewQueryRegistry(testMetricFunc)

	mq := &mockQuery{
		execFunc: func(ctx context.Context) *promql.Result {
			// Simulate eviction: find the registered query and cancel it.
			victims := registry.FindHeaviest(1, 0)
			require.Len(t, victims, 1, "query should be registered during Exec")
			victims[0].Cancel() // This cancels the child context, simulating evictor behavior.

			// The inner query would see a cancelled context and return an error.
			return &promql.Result{Err: context.Canceled}
		},
	}

	inner := &mockEngine{query: mq}
	wrapper := NewResourceEvictingEngine(inner, registry)

	ctx := ctxWithStats(context.Background())
	query, err := wrapper.NewInstantQuery(ctx, nil, nil, "up", time.Now())
	require.NoError(t, err)

	result := query.Exec(ctx)

	// The result should contain ErrQueryEvicted wrapped in ErrStorage for 500 status.
	require.NotNil(t, result.Err)
	var storageErr promql.ErrStorage
	require.ErrorAs(t, result.Err, &storageErr, "error should be promql.ErrStorage, got: %v", result.Err)
	var evictedErr *ErrQueryEvicted
	assert.True(t, errors.As(storageErr.Err, &evictedErr), "inner error should be ErrQueryEvicted, got: %v", storageErr.Err)
}

func TestEngineWrapper_NonEvictedQueryReturnsNormalResult(t *testing.T) {
	registry := NewQueryRegistry(testMetricFunc)

	expectedResult := &promql.Result{
		Value: promql.Scalar{T: 1000, V: 42.0},
	}

	mq := &mockQuery{execResult: expectedResult}
	inner := &mockEngine{query: mq}
	wrapper := NewResourceEvictingEngine(inner, registry)

	ctx := ctxWithStats(context.Background())
	query, err := wrapper.NewInstantQuery(ctx, nil, nil, "up", time.Now())
	require.NoError(t, err)

	result := query.Exec(ctx)

	// The result should be passed through unchanged.
	assert.NoError(t, result.Err)
	assert.Equal(t, expectedResult.Value, result.Value, "result value should be passed through unchanged")
}

func TestEngineWrapper_NilRegistryIsNoOpPassthrough(t *testing.T) {
	mq := &mockQuery{
		execResult: &promql.Result{Value: promql.Scalar{T: 1000, V: 42.0}},
	}
	inner := &mockEngine{query: mq}

	// Create wrapper with nil registry — should be a no-op passthrough.
	wrapper := NewResourceEvictingEngine(inner, nil)

	ctx := context.Background()

	// Test all four engine methods return the inner query directly (not wrapped).
	instantQuery, err := wrapper.NewInstantQuery(ctx, nil, nil, "up", time.Now())
	require.NoError(t, err)
	assert.Equal(t, mq, instantQuery, "nil registry should return inner query directly for NewInstantQuery")

	rangeQuery, err := wrapper.NewRangeQuery(ctx, nil, nil, "up", time.Now(), time.Now(), time.Minute)
	require.NoError(t, err)
	assert.Equal(t, mq, rangeQuery, "nil registry should return inner query directly for NewRangeQuery")

	instantPlanQuery, err := wrapper.MakeInstantQueryFromPlan(ctx, nil, nil, nil, time.Now(), "up")
	require.NoError(t, err)
	assert.Equal(t, mq, instantPlanQuery, "nil registry should return inner query directly for MakeInstantQueryFromPlan")

	rangePlanQuery, err := wrapper.MakeRangeQueryFromPlan(ctx, nil, nil, nil, time.Now(), time.Now(), time.Minute, "up")
	require.NoError(t, err)
	assert.Equal(t, mq, rangePlanQuery, "nil registry should return inner query directly for MakeRangeQueryFromPlan")
}

func TestEngineWrapper_CloseCallsInnerClose(t *testing.T) {
	registry := NewQueryRegistry(testMetricFunc)

	mq := &mockQuery{
		execResult: &promql.Result{},
	}
	inner := &mockEngine{query: mq}
	wrapper := NewResourceEvictingEngine(inner, registry)

	ctx := ctxWithStats(context.Background())
	query, err := wrapper.NewInstantQuery(ctx, nil, nil, "up", time.Now())
	require.NoError(t, err)

	// Close the tracked query.
	query.Close()

	// Verify the inner query's Close was called.
	assert.True(t, mq.closed, "Close on tracked query should delegate to inner query's Close")
}

func TestEngineWrapper_AllMethodsWrapQuery(t *testing.T) {
	registry := NewQueryRegistry(testMetricFunc)
	mq := &mockQuery{execResult: &promql.Result{}}
	inner := &mockEngine{query: mq}
	wrapper := NewResourceEvictingEngine(inner, registry)

	ctx := ctxWithStats(context.Background())
	now := time.Now()

	// All four methods should return a trackedQuery (not the raw mockQuery).
	q1, err := wrapper.NewInstantQuery(ctx, nil, nil, "up", now)
	require.NoError(t, err)
	_, ok := q1.(*trackedQuery)
	assert.True(t, ok, "NewInstantQuery should return a trackedQuery")

	q2, err := wrapper.NewRangeQuery(ctx, nil, nil, "up", now, now, time.Minute)
	require.NoError(t, err)
	_, ok = q2.(*trackedQuery)
	assert.True(t, ok, "NewRangeQuery should return a trackedQuery")

	q3, err := wrapper.MakeInstantQueryFromPlan(ctx, nil, nil, nil, now, "up")
	require.NoError(t, err)
	_, ok = q3.(*trackedQuery)
	assert.True(t, ok, "MakeInstantQueryFromPlan should return a trackedQuery")

	q4, err := wrapper.MakeRangeQueryFromPlan(ctx, nil, nil, nil, now, now, time.Minute, "up")
	require.NoError(t, err)
	_, ok = q4.(*trackedQuery)
	assert.True(t, ok, "MakeRangeQueryFromPlan should return a trackedQuery")
}
