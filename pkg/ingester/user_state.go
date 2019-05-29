package ingester

import (
	"context"
	"net/http"
	"unsafe"

	"github.com/prometheus/tsdb"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	tsdbLabels "github.com/prometheus/tsdb/labels"
	"github.com/segmentio/fasthash/fnv1a"

	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ingester/index"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/extract"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/weaveworks/common/httpgrpc"
)

// DiscardedSamples metric labels
const (
	perUserSeriesLimit   = "per_user_series_limit"
	perMetricSeriesLimit = "per_metric_series_limit"
)

var (
	memSeries = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "cortex_ingester_memory_series",
		Help: "The current number of series in memory.",
	})
	memSeriesCreatedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_memory_series_created_total",
		Help: "The total number of series that were created per user.",
	}, []string{"user"})
	memSeriesRemovedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingester_memory_series_removed_total",
		Help: "The total number of series that were removed per user.",
	}, []string{"user"})
)

type userState struct {
	cfg                 Config
	limits              *validation.Overrides
	userID              string
	fpLocker            *fingerprintLocker
	fpToSeries          *seriesMap
	mapper              *fpMapper
	index               *index.InvertedIndex
	ingestedAPISamples  *ewmaRate
	ingestedRuleSamples *ewmaRate

	seriesInMetric []metricCounterShard

	memSeriesCreatedTotal prometheus.Counter
	memSeriesRemovedTotal prometheus.Counter

	tsdb *tsdb.DB
}

func newUserState(cfg Config, userID string, limits *validation.Overrides) *userState {
	seriesInMetric := make([]metricCounterShard, 0, metricCounterShards)
	for i := 0; i < metricCounterShards; i++ {
		seriesInMetric = append(seriesInMetric, metricCounterShard{
			m: map[string]int{},
		})
	}

	// Speculatively create a userState object and try to store it
	// in the map.  Another goroutine may have got there before
	// us, in which case this userState will be discarded
	state := &userState{
		cfg:                 cfg,
		userID:              userID,
		limits:              limits,
		fpToSeries:          newSeriesMap(),
		fpLocker:            newFingerprintLocker(16 * 1024),
		index:               index.New(),
		ingestedAPISamples:  newEWMARate(0.2, cfg.RateUpdatePeriod),
		ingestedRuleSamples: newEWMARate(0.2, cfg.RateUpdatePeriod),
		seriesInMetric:      seriesInMetric,

		memSeriesCreatedTotal: memSeriesCreatedTotal.WithLabelValues(userID),
		memSeriesRemovedTotal: memSeriesRemovedTotal.WithLabelValues(userID),
	}
	state.mapper = newFPMapper(state.fpToSeries)
	return state
}

func (u *userState) append(ctx context.Context, req *client.WriteRequest) error {
	if u.cfg.UseTSDB {
		return u.tsdbAppend(ctx, req)
	} else {
		return u.legacyAppend(ctx, req)
	}
}

func (u *userState) legacyAppend(ctx context.Context, req *client.WriteRequest) error {
	var lastPartialErr error
	for _, ts := range req.Timeseries {
		for _, s := range ts.Samples {
			err := u.legacyAppendInternal(ctx, ts.Labels, s.TimestampMs, s.Value, req.Source)
			if err == nil {
				continue
			}

			ingestedSamplesFail.Inc()
			if httpResp, ok := httpgrpc.HTTPResponseFromError(err); ok {
				switch httpResp.Code {
				case http.StatusBadRequest, http.StatusTooManyRequests:
					lastPartialErr = err
					continue
				}
			}

			return err
		}
	}
	return lastPartialErr
}

func (u *userState) legacyAppendInternal(ctx context.Context, labels labelPairs, timestamp int64, value float64, source client.WriteRequest_SourceEnum) error {
	labels.removeBlanks()

	fp, series, err := u.getSeries(labels)
	if err != nil {
		return err
	}
	defer u.fpLocker.Unlock(fp)

	prevNumChunks := len(series.chunkDescs)
	if err := series.add(timestamp, value); err != nil {
		return err
	}

	memoryChunks.Add(float64(len(series.chunkDescs) - prevNumChunks))
	ingestedSamples.Inc()
	switch source {
	case client.RULE:
		u.ingestedRuleSamples.inc()
	case client.API:
		fallthrough
	default:
		u.ingestedAPISamples.inc()
	}

	return err
}

func (u *userState) tsdbAppend(ctx context.Context, req *client.WriteRequest) error {
	appender := u.tsdb.Appender()

	for _, series := range req.Timeseries {
		for _, sample := range series.Samples {
			pairs := labelPairs(series.Labels)
			pairs.removeBlanks()
			labels := client.FromLabelAdaptersToLabels(series.Labels)
			tsbdLabels := *(*tsdbLabels.Labels)(unsafe.Pointer(&labels))

			_, err := appender.Add(tsbdLabels, sample.TimestampMs, sample.Value)
			if err != nil {
				return err
			}
		}
	}

	return appender.Commit()
}

func (u *userState) getSeries(metric labelPairs) (model.Fingerprint, *memorySeries, error) {
	rawFP := client.FastFingerprint(metric)
	u.fpLocker.Lock(rawFP)
	fp := u.mapper.mapFP(rawFP, metric)
	if fp != rawFP {
		u.fpLocker.Unlock(rawFP)
		u.fpLocker.Lock(fp)
	}

	series, ok := u.fpToSeries.get(fp)
	if ok {
		return fp, series, nil
	}

	// There's theoretically a relatively harmless race here if multiple
	// goroutines get the length of the series map at the same time, then
	// all proceed to add a new series. This is likely not worth addressing,
	// as this should happen rarely (all samples from one push are added
	// serially), and the overshoot in allowed series would be minimal.
	if u.fpToSeries.length() >= u.limits.MaxSeriesPerUser(u.userID) {
		u.fpLocker.Unlock(fp)
		validation.DiscardedSamples.WithLabelValues(perUserSeriesLimit, u.userID).Inc()
		return fp, nil, httpgrpc.Errorf(http.StatusTooManyRequests, "per-user series limit (%d) exceeded", u.limits.MaxSeriesPerUser(u.userID))
	}

	metricName, err := extract.MetricNameFromLabelAdapters(metric)
	if err != nil {
		u.fpLocker.Unlock(fp)
		return fp, nil, err
	}

	if !u.canAddSeriesFor(string(metricName)) {
		u.fpLocker.Unlock(fp)
		validation.DiscardedSamples.WithLabelValues(perMetricSeriesLimit, u.userID).Inc()
		return fp, nil, httpgrpc.Errorf(http.StatusTooManyRequests, "per-metric series limit (%d) exceeded for %s: %s", u.limits.MaxSeriesPerMetric(u.userID), metricName, metric)
	}

	u.memSeriesCreatedTotal.Inc()
	memSeries.Inc()

	labels := u.index.Add(metric, fp)
	series = newMemorySeries(u.userID, labels)
	u.fpToSeries.put(fp, series)

	return fp, series, nil
}

func (u *userState) canAddSeriesFor(metric string) bool {
	shard := &u.seriesInMetric[util.HashFP(model.Fingerprint(fnv1a.HashString64(string(metric))))%metricCounterShards]
	shard.mtx.Lock()
	defer shard.mtx.Unlock()

	if shard.m[metric] >= u.limits.MaxSeriesPerMetric(u.userID) {
		return false
	}
	shard.m[metric]++
	return true
}

func (u *userState) removeSeries(fp model.Fingerprint, metric labels.Labels) {
	u.fpToSeries.del(fp)
	u.index.Delete(labels.Labels(metric), fp)

	metricName := metric.Get(model.MetricNameLabel)
	if metricName == "" {
		// Series without a metric name should never be able to make it into
		// the ingester's memory storage.
		panic("No metric name label")
	}

	shard := &u.seriesInMetric[util.HashFP(model.Fingerprint(fnv1a.HashString64(string(metricName))))%metricCounterShards]
	shard.mtx.Lock()
	defer shard.mtx.Unlock()

	shard.m[metricName]--
	if shard.m[metricName] == 0 {
		delete(shard.m, metricName)
	}

	u.memSeriesRemovedTotal.Inc()
	memSeries.Dec()
}

func (u *userState) query(
	ctx context.Context, req *client.QueryRequest,
	add func(context.Context, labels.Labels, ...*desc) error,
	send func(context.Context) error, batchSize int,
) error {
	return u.legacyQuery(ctx, req, add, send, batchSize)
}

func (u *userState) legacyQuery(
	ctx context.Context, req *client.QueryRequest,
	add func(context.Context, labels.Labels, ...*desc) error,
	send func(context.Context) error, batchSize int,
) error {
	log, ctx := spanlogger.New(ctx, "legacyQuery")
	defer log.Finish()

	from, through, matchers, err := client.FromQueryRequest(req)
	if err != nil {
		return err
	}

	filters, matchers := util.SplitFiltersAndMatchers(matchers)
	fps := u.index.Lookup(matchers)
	if len(fps) > u.limits.MaxSeriesPerQuery(u.userID) {
		return httpgrpc.Errorf(http.StatusRequestEntityTooLarge, "exceeded maximum number of series in a query")
	}

	level.Debug(log).Log("series", len(fps))

	// We only hold one FP lock at once here, so no opportunity to deadlock.
	i := 0
outer:
	for ; i < len(fps); i++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		fp := fps[i]
		u.fpLocker.Lock(fp)
		series, ok := u.fpToSeries.get(fp)
		if !ok {
			u.fpLocker.Unlock(fp)
			continue
		}

		for _, filter := range filters {
			if !filter.Matches(series.metric.Get(filter.Name)) {
				u.fpLocker.Unlock(fp)
				continue outer
			}
		}

		chunks := make([]*desc, 0, len(series.chunkDescs))
		for _, chunk := range series.chunkDescs {
			if !(chunk.FirstTime.After(through) || chunk.LastTime.Before(from)) {
				chunks = append(chunks, chunk)
			}
		}

		err := add(ctx, series.metric, chunks...)
		u.fpLocker.Unlock(fp)
		if err != nil {
			return err
		}

		if batchSize > 0 && (i+1)%batchSize == 0 && send != nil {
			if err = send(ctx); err != nil {
				return nil
			}
		}
	}

	if batchSize > 0 && i%batchSize > 0 && send != nil {
		return send(ctx)
	}
	return nil
}

func (u *userState) tsdbQuery(
	ctx context.Context, req *client.QueryRequest,
	add func(context.Context, labels.Labels, ...*desc) error,
	send func(context.Context) error, batchSize int,
) error {
	log, ctx := spanlogger.New(ctx, "tsdbQuery")
	defer log.Finish()

	from, through, matchers, err := client.FromQueryRequest(req)
	if err != nil {
		return err
	}

	querier, err := u.tsdb.Querier(int64(from), int64(through))
	if err != nil {
		return err
	}

	seriesSet, err := querier.Select(convertMatchers(matchers)...)
	if err != nil {
		return err
	}

	i := 0
	for ; seriesSet.Next(); i++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		series := seriesSet.At()
		tsdbChunks := series.Chunks()
		bigchunk := encoding.NewBigchunk()
		bigchunk.AddSmallChunks(tsdbChunks)
		tsdbLabels := series.Labels()
		labels := *(*labels.Labels)(unsafe.Pointer(&tsdbLabels))
		desc := desc{
			C: bigchunk,
			//			FirstTime: .
		}

		if err := add(ctx, labels, &desc); err != nil {
			return err
		}

		if batchSize > 0 && (i+1)%batchSize == 0 && send != nil {
			if err = send(ctx); err != nil {
				return nil
			}
		}
	}

	if batchSize > 0 && i%batchSize > 0 && send != nil {
		return send(ctx)
	}

	return seriesSet.Err()
}

func (u *userState) labelNames() []string {
	return u.index.LabelNames()
}

func convertMatchers(oms []*labels.Matcher) []tsdbLabels.Matcher {
	ms := make([]tsdbLabels.Matcher, 0, len(oms))
	for _, om := range oms {
		ms = append(ms, convertMatcher(om))
	}
	return ms
}

func convertMatcher(m *labels.Matcher) tsdbLabels.Matcher {
	switch m.Type {
	case labels.MatchEqual:
		return tsdbLabels.NewEqualMatcher(m.Name, m.Value)

	case labels.MatchNotEqual:
		return tsdbLabels.Not(tsdbLabels.NewEqualMatcher(m.Name, m.Value))

	case labels.MatchRegexp:
		res, err := tsdbLabels.NewRegexpMatcher(m.Name, "^(?:"+m.Value+")$")
		if err != nil {
			panic(err)
		}
		return res

	case labels.MatchNotRegexp:
		res, err := tsdbLabels.NewRegexpMatcher(m.Name, "^(?:"+m.Value+")$")
		if err != nil {
			panic(err)
		}
		return tsdbLabels.Not(res)
	}
	panic("storage.convertMatcher: invalid matcher type")
}
