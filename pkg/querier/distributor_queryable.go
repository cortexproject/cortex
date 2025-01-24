package querier

import (
	"context"
	"sort"
	"time"

	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/querier/partialdata"
	"github.com/cortexproject/cortex/pkg/querier/series"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
)

// Distributor is the read interface to the distributor, made an interface here
// to reduce package coupling.
type Distributor interface {
	QueryStream(ctx context.Context, from, to model.Time, partialDataEnabled bool, matchers ...*labels.Matcher) (*client.QueryStreamResponse, error)
	QueryExemplars(ctx context.Context, from, to model.Time, matchers ...[]*labels.Matcher) (*client.ExemplarQueryResponse, error)
	LabelValuesForLabelName(ctx context.Context, from, to model.Time, label model.LabelName, hint *storage.LabelHints, partialDataEnabled bool, matchers ...*labels.Matcher) ([]string, error)
	LabelValuesForLabelNameStream(ctx context.Context, from, to model.Time, label model.LabelName, hint *storage.LabelHints, partialDataEnabled bool, matchers ...*labels.Matcher) ([]string, error)
	LabelNames(context.Context, model.Time, model.Time, *storage.LabelHints, bool, ...*labels.Matcher) ([]string, error)
	LabelNamesStream(context.Context, model.Time, model.Time, *storage.LabelHints, bool, ...*labels.Matcher) ([]string, error)
	MetricsForLabelMatchers(ctx context.Context, from, through model.Time, hint *storage.SelectHints, partialDataEnabled bool, matchers ...*labels.Matcher) ([]model.Metric, error)
	MetricsForLabelMatchersStream(ctx context.Context, from, through model.Time, hint *storage.SelectHints, partialDataEnabled bool, matchers ...*labels.Matcher) ([]model.Metric, error)
	MetricsMetadata(ctx context.Context) ([]scrape.MetricMetadata, error)
}

func newDistributorQueryable(distributor Distributor, streamingMetdata bool, labelNamesWithMatchers bool, iteratorFn chunkIteratorFunc, queryIngestersWithin time.Duration, isPartialDataEnabled partialdata.IsCfgEnabledFunc) QueryableWithFilter {
	return distributorQueryable{
		distributor:            distributor,
		streamingMetdata:       streamingMetdata,
		labelNamesWithMatchers: labelNamesWithMatchers,
		iteratorFn:             iteratorFn,
		queryIngestersWithin:   queryIngestersWithin,
		isPartialDataEnabled:   isPartialDataEnabled,
	}
}

type distributorQueryable struct {
	distributor            Distributor
	streamingMetdata       bool
	labelNamesWithMatchers bool
	iteratorFn             chunkIteratorFunc
	queryIngestersWithin   time.Duration
	isPartialDataEnabled   partialdata.IsCfgEnabledFunc
}

func (d distributorQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return &distributorQuerier{
		distributor:          d.distributor,
		mint:                 mint,
		maxt:                 maxt,
		streamingMetadata:    d.streamingMetdata,
		labelNamesMatchers:   d.labelNamesWithMatchers,
		chunkIterFn:          d.iteratorFn,
		queryIngestersWithin: d.queryIngestersWithin,
		isPartialDataEnabled: d.isPartialDataEnabled,
	}, nil
}

func (d distributorQueryable) UseQueryable(now time.Time, _, queryMaxT int64) bool {
	// Include ingester only if maxt is within QueryIngestersWithin w.r.t. current time.
	return d.queryIngestersWithin == 0 || queryMaxT >= util.TimeToMillis(now.Add(-d.queryIngestersWithin))
}

type distributorQuerier struct {
	distributor          Distributor
	mint, maxt           int64
	streamingMetadata    bool
	labelNamesMatchers   bool
	chunkIterFn          chunkIteratorFunc
	queryIngestersWithin time.Duration
	isPartialDataEnabled partialdata.IsCfgEnabledFunc
}

// Select implements storage.Querier interface.
// The bool passed is ignored because the series is always sorted.
func (q *distributorQuerier) Select(ctx context.Context, sortSeries bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	log, ctx := spanlogger.New(ctx, "distributorQuerier.Select")
	defer log.Span.Finish()

	minT, maxT := q.mint, q.maxt
	if sp != nil {
		minT, maxT = sp.Start, sp.End
	}

	// We should manipulate the query mint to query samples up until
	// now - queryIngestersWithin, because older time ranges are covered by the storage. This
	// optimization is particularly important for the blocks storage where the blocks retention in the
	// ingesters could be way higher than queryIngestersWithin.
	if q.queryIngestersWithin > 0 {
		now := time.Now()
		origMinT := minT
		minT = max(minT, util.TimeToMillis(now.Add(-q.queryIngestersWithin)))

		if origMinT != minT {
			level.Debug(log).Log("msg", "the min time of the query to ingesters has been manipulated", "original", origMinT, "updated", minT)
		}

		if minT > maxT {
			level.Debug(log).Log("msg", "empty query time range after min time manipulation")
			return storage.EmptySeriesSet()
		}
	}

	partialDataEnabled := q.partialDataEnabled(ctx)

	// In the recent versions of Prometheus, we pass in the hint but with Func set to "series".
	// See: https://github.com/prometheus/prometheus/pull/8050
	if sp != nil && sp.Func == "series" {
		var (
			ms  []model.Metric
			err error
		)

		if q.streamingMetadata {
			ms, err = q.distributor.MetricsForLabelMatchersStream(ctx, model.Time(minT), model.Time(maxT), sp, partialDataEnabled, matchers...)
		} else {
			ms, err = q.distributor.MetricsForLabelMatchers(ctx, model.Time(minT), model.Time(maxT), sp, partialDataEnabled, matchers...)
		}

		if err != nil && !partialdata.IsPartialDataError(err) {
			return storage.ErrSeriesSet(err)
		}

		seriesSet := series.MetricsToSeriesSet(ctx, sortSeries, ms)

		if partialdata.IsPartialDataError(err) {
			warning := seriesSet.Warnings()
			return series.NewSeriesSetWithWarnings(seriesSet, warning.Add(err))
		}

		return series.MetricsToSeriesSet(ctx, sortSeries, ms)
	}

	return q.streamingSelect(ctx, sortSeries, partialDataEnabled, minT, maxT, matchers)
}

func (q *distributorQuerier) streamingSelect(ctx context.Context, sortSeries, partialDataEnabled bool, minT, maxT int64, matchers []*labels.Matcher) storage.SeriesSet {
	results, err := q.distributor.QueryStream(ctx, model.Time(minT), model.Time(maxT), partialDataEnabled, matchers...)

	if err != nil && !partialdata.IsPartialDataError(err) {
		return storage.ErrSeriesSet(err)
	}

	serieses := make([]storage.Series, 0, len(results.Chunkseries))
	for _, result := range results.Chunkseries {
		// Sometimes the ingester can send series that have no data.
		if len(result.Chunks) == 0 {
			continue
		}

		ls := cortexpb.FromLabelAdaptersToLabels(result.Labels)

		chunks, err := chunkcompat.FromChunks(ls, result.Chunks)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}

		serieses = append(serieses, &storage.SeriesEntry{
			Lset: ls,
			SampleIteratorFn: func(it chunkenc.Iterator) chunkenc.Iterator {
				return q.chunkIterFn(it, chunks, model.Time(minT), model.Time(maxT))
			},
		})
	}

	if len(serieses) == 0 {
		return storage.EmptySeriesSet()
	}

	seriesSet := series.NewConcreteSeriesSet(sortSeries, serieses)

	if partialdata.IsPartialDataError(err) {
		warnings := seriesSet.Warnings()
		return series.NewSeriesSetWithWarnings(seriesSet, warnings.Add(err))
	}

	return seriesSet
}

func (q *distributorQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	var (
		lvs []string
		err error
	)

	partialDataEnabled := q.partialDataEnabled(ctx)

	if q.streamingMetadata {
		lvs, err = q.distributor.LabelValuesForLabelNameStream(ctx, model.Time(q.mint), model.Time(q.maxt), model.LabelName(name), hints, partialDataEnabled, matchers...)
	} else {
		lvs, err = q.distributor.LabelValuesForLabelName(ctx, model.Time(q.mint), model.Time(q.maxt), model.LabelName(name), hints, partialDataEnabled, matchers...)
	}

	if partialdata.IsPartialDataError(err) {
		warnings := annotations.Annotations(nil)
		return lvs, warnings.Add(err), nil
	}

	return lvs, nil, err
}

func (q *distributorQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	partialDataEnabled := q.partialDataEnabled(ctx)

	if len(matchers) > 0 && !q.labelNamesMatchers {
		return q.labelNamesWithMatchers(ctx, hints, partialDataEnabled, matchers...)
	}

	log, ctx := spanlogger.New(ctx, "distributorQuerier.LabelNames")
	defer log.Span.Finish()

	var (
		ln  []string
		err error
	)

	if q.streamingMetadata {
		ln, err = q.distributor.LabelNamesStream(ctx, model.Time(q.mint), model.Time(q.maxt), hints, partialDataEnabled, matchers...)
	} else {
		ln, err = q.distributor.LabelNames(ctx, model.Time(q.mint), model.Time(q.maxt), hints, partialDataEnabled, matchers...)
	}

	if partialdata.IsPartialDataError(err) {
		warnings := annotations.Annotations(nil)
		return ln, warnings.Add(err), nil
	}

	return ln, nil, err
}

// labelNamesWithMatchers performs the LabelNames call by calling ingester's MetricsForLabelMatchers method
func (q *distributorQuerier) labelNamesWithMatchers(ctx context.Context, hints *storage.LabelHints, partialDataEnabled bool, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	log, ctx := spanlogger.New(ctx, "distributorQuerier.labelNamesWithMatchers")
	defer log.Span.Finish()

	var (
		ms  []model.Metric
		err error
	)

	if q.streamingMetadata {
		ms, err = q.distributor.MetricsForLabelMatchersStream(ctx, model.Time(q.mint), model.Time(q.maxt), labelHintsToSelectHints(hints), partialDataEnabled, matchers...)
	} else {
		ms, err = q.distributor.MetricsForLabelMatchers(ctx, model.Time(q.mint), model.Time(q.maxt), labelHintsToSelectHints(hints), partialDataEnabled, matchers...)
	}

	if err != nil && !partialdata.IsPartialDataError(err) {
		return nil, nil, err
	}
	namesMap := make(map[string]struct{})

	for _, m := range ms {
		for name := range m {
			namesMap[string(name)] = struct{}{}
		}
	}

	names := make([]string, 0, len(namesMap))
	for name := range namesMap {
		names = append(names, name)
	}
	sort.Strings(names)

	if partialdata.IsPartialDataError(err) {
		warnings := annotations.Annotations(nil)
		return names, warnings.Add(err), nil
	}

	return names, nil, nil
}

func (q *distributorQuerier) Close() error {
	return nil
}

func (q *distributorQuerier) partialDataEnabled(ctx context.Context) bool {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return false
	}

	return q.isPartialDataEnabled != nil && q.isPartialDataEnabled(userID)
}

type distributorExemplarQueryable struct {
	distributor Distributor
}

func newDistributorExemplarQueryable(d Distributor) storage.ExemplarQueryable {
	return &distributorExemplarQueryable{
		distributor: d,
	}
}

func (d distributorExemplarQueryable) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return &distributorExemplarQuerier{
		distributor: d.distributor,
		ctx:         ctx,
	}, nil
}

type distributorExemplarQuerier struct {
	distributor Distributor
	ctx         context.Context
}

// Select querys for exemplars, prometheus' storage.ExemplarQuerier's Select function takes the time range as two int64 values.
func (q *distributorExemplarQuerier) Select(start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	allResults, err := q.distributor.QueryExemplars(q.ctx, model.Time(start), model.Time(end), matchers...)

	if err != nil {
		return nil, err
	}

	var e exemplar.QueryResult
	ret := make([]exemplar.QueryResult, len(allResults.Timeseries))
	for i, ts := range allResults.Timeseries {
		e.SeriesLabels = cortexpb.FromLabelAdaptersToLabels(ts.Labels)
		e.Exemplars = cortexpb.FromExemplarProtosToExemplars(ts.Exemplars)
		ret[i] = e
	}
	return ret, nil
}

func labelHintsToSelectHints(hints *storage.LabelHints) *storage.SelectHints {
	if hints == nil {
		return nil
	}

	return &storage.SelectHints{
		Limit: hints.Limit,
	}
}
