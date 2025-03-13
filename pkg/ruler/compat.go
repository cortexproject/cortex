package ruler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/querier"
	"github.com/cortexproject/cortex/pkg/querier/stats"
	"github.com/cortexproject/cortex/pkg/ring/client"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	promql_util "github.com/cortexproject/cortex/pkg/util/promql"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

// Pusher is an ingester server that accepts pushes.
type Pusher interface {
	Push(context.Context, *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error)
}

type PusherAppender struct {
	failedWrites prometheus.Counter
	totalWrites  prometheus.Counter

	ctx             context.Context
	pusher          Pusher
	labels          []labels.Labels
	samples         []cortexpb.Sample
	histogramLabels []labels.Labels
	histograms      []cortexpb.Histogram
	userID          string
}

func (a *PusherAppender) AppendHistogram(_ storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	if h == nil && fh == nil {
		return 0, errors.New("no histogram")
	}
	if h != nil {
		a.histograms = append(a.histograms, cortexpb.HistogramToHistogramProto(t, h))
	} else {
		a.histograms = append(a.histograms, cortexpb.FloatHistogramToHistogramProto(t, fh))
	}
	a.histogramLabels = append(a.histogramLabels, l)
	return 0, nil
}

func (a *PusherAppender) Append(_ storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	a.labels = append(a.labels, l)
	a.samples = append(a.samples, cortexpb.Sample{
		TimestampMs: t,
		Value:       v,
	})
	return 0, nil
}

func (a *PusherAppender) SetOptions(opts *storage.AppendOptions) {}

func (a *PusherAppender) AppendHistogramCTZeroSample(ref storage.SeriesRef, l labels.Labels, t, ct int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	// AppendHistogramCTZeroSample is a no-op for PusherAppender as it happens during scrape time only.
	return 0, nil
}

func (a *PusherAppender) AppendCTZeroSample(_ storage.SeriesRef, _ labels.Labels, _, _ int64) (storage.SeriesRef, error) {
	// AppendCTZeroSample is a no-op for PusherAppender as it happens during scrape time only.
	return 0, nil
}

func (a *PusherAppender) AppendExemplar(_ storage.SeriesRef, _ labels.Labels, _ exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, errors.New("exemplars are unsupported")
}

func (a *PusherAppender) Commit() error {
	a.totalWrites.Inc()

	req := cortexpb.ToWriteRequest(a.labels, a.samples, nil, nil, cortexpb.RULE)
	req.AddHistogramTimeSeries(a.histogramLabels, a.histograms)
	// Since a.pusher is distributor, client.ReuseSlice will be called in a.pusher.Push.
	// We shouldn't call client.ReuseSlice here.
	_, err := a.pusher.Push(user.InjectOrgID(a.ctx, a.userID), req)
	if err != nil {
		// Don't report errors that ended with 4xx HTTP status code (series limits, duplicate samples, out of order, etc.)
		if resp, ok := httpgrpc.HTTPResponseFromError(err); !ok || resp.Code/100 != 4 {
			a.failedWrites.Inc()
		}
	}

	a.labels = nil
	a.samples = nil
	a.histogramLabels = nil
	a.histograms = nil
	return err
}

func (a *PusherAppender) UpdateMetadata(_ storage.SeriesRef, _ labels.Labels, _ metadata.Metadata) (storage.SeriesRef, error) {
	return 0, errors.New("update metadata unsupported")
}

func (a *PusherAppender) Rollback() error {
	a.labels = nil
	a.samples = nil
	a.histogramLabels = nil
	a.histograms = nil
	return nil
}

// PusherAppendable fulfills the storage.Appendable interface for prometheus manager
type PusherAppendable struct {
	pusher      Pusher
	userID      string
	rulesLimits RulesLimits

	totalWrites  prometheus.Counter
	failedWrites prometheus.Counter
}

func NewPusherAppendable(pusher Pusher, userID string, limits RulesLimits, totalWrites, failedWrites prometheus.Counter) *PusherAppendable {
	return &PusherAppendable{
		pusher:       pusher,
		userID:       userID,
		rulesLimits:  limits,
		totalWrites:  totalWrites,
		failedWrites: failedWrites,
	}
}

// Appender returns a storage.Appender
func (t *PusherAppendable) Appender(ctx context.Context) storage.Appender {
	return &PusherAppender{
		failedWrites: t.failedWrites,
		totalWrites:  t.totalWrites,

		ctx:    ctx,
		pusher: t.pusher,
		userID: t.userID,
	}
}

// RulesLimits defines limits used by Ruler.
type RulesLimits interface {
	MaxQueryLength(userID string) time.Duration
	RulerTenantShardSize(userID string) int
	RulerMaxRuleGroupsPerTenant(userID string) int
	RulerMaxRulesPerRuleGroup(userID string) int
	RulerQueryOffset(userID string) time.Duration
	DisabledRuleGroups(userID string) validation.DisabledRuleGroups
	RulerExternalLabels(userID string) labels.Labels
}

// EngineQueryFunc returns a new engine query function validating max queryLength.
// Modified from Prometheus rules.EngineQueryFunc
// https://github.com/prometheus/prometheus/blob/v2.39.1/rules/manager.go#L189.
func EngineQueryFunc(engine promql.QueryEngine, frontendClient *frontendClient, q storage.Queryable, overrides RulesLimits, userID string, lookbackDelta time.Duration) rules.QueryFunc {
	return func(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {
		// Enforce the max query length.
		maxQueryLength := overrides.MaxQueryLength(userID)
		if maxQueryLength > 0 {
			expr, err := parser.ParseExpr(qs)
			// If failed to parse expression, skip checking select range.
			// Fail the query in the engine.
			if err == nil {
				// Enforce query length across all selectors in the query.
				length := promql_util.FindNonOverlapQueryLength(expr, 0, 0, lookbackDelta)
				if length > maxQueryLength {
					return nil, validation.LimitError(fmt.Sprintf(validation.ErrQueryTooLong, length, maxQueryLength))
				}
			}
		}

		if frontendClient != nil {
			v, err := frontendClient.InstantQuery(ctx, qs, t)
			if err != nil {
				return nil, err
			}

			return v, nil
		} else {
			q, err := engine.NewInstantQuery(ctx, q, nil, qs, t)
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
					T:      v.T,
					F:      v.V,
					Metric: labels.Labels{},
				}}, nil
			default:
				return nil, errors.New("rule result is not a vector or scalar")
			}
		}
	}
}

func MetricsQueryFunc(qf rules.QueryFunc, queries, failedQueries prometheus.Counter) rules.QueryFunc {
	return func(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {
		queries.Inc()
		result, err := qf(ctx, qs, t)

		// We only care about errors returned by underlying Queryable. Errors returned by PromQL engine are "user-errors",
		// and not interesting here.
		qerr := QueryableError{}
		if err != nil && errors.As(err, &qerr) {
			origErr := qerr.Unwrap()

			// Not all errors returned by Queryable are interesting, only those that would result in 500 status code.
			//
			// We rely on TranslateToPromqlApiError to do its job here... it returns nil, if err is nil.
			// It returns promql.ErrStorage, if error should be reported back as 500.
			// Other errors it returns are either for canceled or timed-out queriers (we're not reporting those as failures),
			// or various user-errors (limits, duplicate samples, etc. ... also not failures).
			//
			// All errors will still be counted towards "evaluation failures" metrics and logged by Prometheus Ruler,
			// but we only want internal errors here.
			if _, ok := querier.TranslateToPromqlAPIError(origErr).(promql.ErrStorage); ok {
				failedQueries.Inc()
			}

			// Return unwrapped error.
			return result, origErr
		}

		return result, err
	}
}

func RecordAndReportRuleQueryMetrics(qf rules.QueryFunc, userID string, evalMetrics *RuleEvalMetrics, logger log.Logger) rules.QueryFunc {
	queryTime := evalMetrics.RulerQuerySeconds.WithLabelValues(userID)
	querySeries := evalMetrics.RulerQuerySeries.WithLabelValues(userID)
	querySample := evalMetrics.RulerQuerySamples.WithLabelValues(userID)
	queryChunkBytes := evalMetrics.RulerQueryChunkBytes.WithLabelValues(userID)
	queryDataBytes := evalMetrics.RulerQueryDataBytes.WithLabelValues(userID)

	return func(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {
		queryStats, ctx := stats.ContextWithEmptyStats(ctx)
		// If we've been passed a counter we want to record the wall time spent executing this request.
		timer := prometheus.NewTimer(nil)

		defer func() {
			querySeconds := timer.ObserveDuration().Seconds()
			queryTime.Add(querySeconds)
			querySeries.Add(float64(queryStats.FetchedSeriesCount))
			querySample.Add(float64(queryStats.FetchedSamplesCount))
			queryChunkBytes.Add(float64(queryStats.FetchedChunkBytes))
			queryDataBytes.Add(float64(queryStats.FetchedDataBytes))
			// Log ruler query stats.
			logMessage := []interface{}{
				"msg", "query stats",
				"component", "ruler",
			}
			if origin := ctx.Value(promql.QueryOrigin{}); origin != nil {
				queryLabels := origin.(map[string]interface{})
				rgMap := queryLabels["ruleGroup"].(map[string]string)
				logMessage = append(logMessage,
					"rule_group", rgMap["name"],
					"namespace", rgMap["file"],
				)
			}
			ruleDetail := rules.FromOriginContext(ctx)
			logMessage = append(logMessage,
				"rule", ruleDetail.Name,
				"rule_kind", ruleDetail.Kind,
				"query", qs,
				"cortex_ruler_query_seconds_total", querySeconds,
				"query_wall_time_seconds", queryStats.WallTime,
				"query_storage_wall_time_seconds", queryStats.QueryStorageWallTime,
				"fetched_series_count", queryStats.FetchedSeriesCount,
				"fetched_chunks_count", queryStats.FetchedChunksCount,
				"fetched_samples_count", queryStats.FetchedSamplesCount,
				"fetched_chunks_bytes", queryStats.FetchedChunkBytes,
				"fetched_data_bytes", queryStats.FetchedDataBytes,
			)
			logMessage = append(logMessage, queryStats.LoadExtraFields()...)
			level.Info(util_log.WithContext(ctx, logger)).Log(logMessage...)
		}()

		result, err := qf(ctx, qs, t)
		return result, err
	}
}

// This interface mimics rules.Manager API. Interface is used to simplify tests.
type RulesManager interface {
	// Starts rules manager. Blocks until Stop is called.
	Run()

	// Stops rules manager. (Unblocks Run.)
	Stop()

	// Updates rules manager state.
	Update(interval time.Duration, files []string, externalLabels labels.Labels, externalURL string, ruleGroupPostProcessFunc rules.GroupEvalIterationFunc) error

	// Returns current rules groups.
	RuleGroups() []*rules.Group
}

// ManagerFactory is a function that creates new RulesManager for given user and notifier.Manager.
type ManagerFactory func(ctx context.Context, userID string, notifier *notifier.Manager, logger log.Logger, frontendPool *client.Pool, reg prometheus.Registerer) (RulesManager, error)

func DefaultTenantManagerFactory(cfg Config, p Pusher, q storage.Queryable, engine promql.QueryEngine, overrides RulesLimits, evalMetrics *RuleEvalMetrics, reg prometheus.Registerer) ManagerFactory {
	// Wrap errors returned by Queryable to our wrapper, so that we can distinguish between those errors
	// and errors returned by PromQL engine. Errors from Queryable can be either caused by user (limits) or internal errors.
	// Errors from PromQL are always "user" errors.
	q = querier.NewErrorTranslateQueryableWithFn(q, WrapQueryableErrors)

	return func(ctx context.Context, userID string, notifier *notifier.Manager, logger log.Logger, frontendPool *client.Pool, reg prometheus.Registerer) (RulesManager, error) {
		var client *frontendClient
		failedQueries := evalMetrics.FailedQueriesVec.WithLabelValues(userID)
		totalQueries := evalMetrics.TotalQueriesVec.WithLabelValues(userID)
		totalWrites := evalMetrics.TotalWritesVec.WithLabelValues(userID)
		failedWrites := evalMetrics.FailedWritesVec.WithLabelValues(userID)

		if cfg.FrontendAddress != "" {
			c, err := frontendPool.GetClientFor(cfg.FrontendAddress)
			if err != nil {
				return nil, err
			}
			client = c.(*frontendClient)
		}
		var queryFunc rules.QueryFunc
		engineQueryFunc := EngineQueryFunc(engine, client, q, overrides, userID, cfg.LookbackDelta)
		metricsQueryFunc := MetricsQueryFunc(engineQueryFunc, totalQueries, failedQueries)
		if cfg.EnableQueryStats {
			queryFunc = RecordAndReportRuleQueryMetrics(metricsQueryFunc, userID, evalMetrics, logger)
		} else {
			queryFunc = metricsQueryFunc
		}

		// We let the Prometheus rules manager control the context so that there is a chance
		// for graceful shutdown of rules that are still in execution even in case the cortex context is canceled.
		prometheusContext := user.InjectOrgID(context.WithoutCancel(ctx), userID)

		return rules.NewManager(&rules.ManagerOptions{
			Appendable:             NewPusherAppendable(p, userID, overrides, totalWrites, failedWrites),
			Queryable:              q,
			QueryFunc:              queryFunc,
			Context:                prometheusContext,
			ExternalURL:            cfg.ExternalURL.URL,
			NotifyFunc:             SendAlerts(notifier, cfg.ExternalURL.URL.String()),
			Logger:                 util_log.GoKitLogToSlog(log.With(logger, "user", userID)),
			Registerer:             reg,
			OutageTolerance:        cfg.OutageTolerance,
			ForGracePeriod:         cfg.ForGracePeriod,
			ResendDelay:            cfg.ResendDelay,
			ConcurrentEvalsEnabled: cfg.ConcurrentEvalsEnabled,
			MaxConcurrentEvals:     cfg.MaxConcurrentEvals,
			DefaultRuleQueryOffset: func() time.Duration {
				return overrides.RulerQueryOffset(userID)
			},
			RestoreNewRuleGroups: cfg.EnableSharding,
		}), nil
	}
}

type QueryableError struct {
	err error
}

func (q QueryableError) Unwrap() error {
	return q.err
}

func (q QueryableError) Error() string {
	return q.err.Error()
}

func WrapQueryableErrors(err error) error {
	if err == nil {
		return err
	}

	return QueryableError{err: err}
}
